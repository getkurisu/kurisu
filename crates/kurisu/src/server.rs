//! The Kurisu server: owns the session store and the drivers, and serves
//! line-JSON requests on a 0600 unix socket (used by the CLI and by the
//! `kurisu mcp` tool server that every session gets).

mod trust;
#[cfg(test)]
mod oauth_tests;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agent_client_protocol::v1::schema::{
    ContentBlock, LoadSessionRequest, NewSessionRequest, PermissionOptionId, RequestPermissionOutcome,
    RequestPermissionRequest, SelectedPermissionOutcome, SessionId, StopReason,
};
use anyhow::{anyhow, bail, Context, Result};
use claude_cli_acp::{ClaudeCli, SessionOpts};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::{broadcast, mpsc, Mutex, Notify, OnceCell};

use crate::codex::{CodexConfig, CodexDriver};
use crate::driver::{agent_text, Driver};
use crate::paths::Paths;
use crate::store::{self, Store};

pub const LEAD_PROMPT: &str = "\
You are the Lead of a Kurisu team. Kurisu runs every agent as a separate, \
long-lived PEER session; there are no sub-agents, so do not use the Task or \
Agent tools. The `kurisu-mcp` tools are your live roster and messaging: \
call whoami and peers_list to see who exists (never rely on a roster file); \
session_create {name, description, harness?, model?} starts a worker (Claude \
with Haiku by default; harness codex runs it on Codex). session_update {id, \
harness, model?} switches a worker (or yourself: use your own id; it happens \
right after your reply) to another harness: Kurisu archives it and \
starts a replacement with the same name, folder and history access, which \
reads the old session's history (session_handoff) and carries on; \
session_send {to, msg} sends a peer a message and returns its reply; \
session_history, board_post and board_read help you coordinate. When asked to \
look at another session and continue from it, call session_handoff {id} first \
(then session_history with search or before to dig deeper). Keep your own \
status current with set_my_status. When you delegate, find the workers with \
peers_list, give each a short self-contained task, and answer the user with a \
concise summary that says which worker said what. Being responsive to the user \
is the job: the time you spend diving deep, reading many files or running \
commands is time the user waits with no answer. So delegate by default - hand \
exploration, builds, tests and multi-file work to workers - and keep for \
yourself only quick lookups, decisions and the final report. Do not redo a \
worker's task locally while it runs. Tell the user in one line what \
you are handing to whom before you start, and never go quiet for a long \
stretch: send a short update when you delegate, when a worker reports back, \
and whenever you are about to do something long-running, expensive or \
destructive - ask first for those. With session_send wait:false you can hand \
off and answer the user at once; when the worker's turn ends Kurisu sends you \
a [Kurisu callback] message (done, error, stopped or interrupted, with its reply), so you need not poll; pass \
notify:false to skip it. Turns cut off by a Kurisu restart are resumed automatically.";

pub fn worker_prompt(name: &str) -> String {
    format!(
        "You are `{name}`, a peer session in a Kurisu team (no sub-agents: do not \
         use the Task or Agent tools). Messages from other sessions arrive as \
         normal prompts. Answer concisely and directly. The `kurisu-mcp` tools \
         (whoami, peers_list, session_send, set_my_status, board_post) tell you \
         who you are and who your peers are; keep your status current. When asked \
         to look at another session and continue from it, call session_handoff {{id}} \
         first, then session_history (search, before) for detail."
    )
}

/// Always true for every session, whatever the harness or browser state. Short and
/// factual: models otherwise invent tool names and deny capabilities they have.
const CAPABILITY_MANIFEST: &str = "\
Your capabilities: you run a real CLI on this machine, so you can run shell commands and read or edit files in your \
own workspace folder (and the paths you are given); the user can reach you over Telegram (and Kurisu's other \
surfaces), and you have whatever MCP servers are enabled for this session (`kurisu-mcp` is always there). To show \
the user a file you made or saw, call `send_file` with a path inside your own folder and a caption: a jpg/png goes \
as a photo, anything else as a document. In OpenCode code mode that call is \
`tools[\"kurisu-mcp\"].send_file({path: \"shot.jpg\", caption: \"...\"})` inside `execute`; a bare \
`send_file {path}` is not a tool call there. It returns `{\"ok\":true,...,\"message_id\":N}` only when the paired chat platform (Telegram or Discord) \
accepted the file. If a tool call fails or is cancelled, say what failed and quote its error; never say you sent, \
saved or did something unless a tool result says so. Do not offer a guess about your own capabilities.";

/// Compact, always-present index of the kurisu-mcp tools. OpenCode's code-mode
/// catalogue is budget-limited (with lb attached it showed 26 of 63 tools), so a
/// tool the model cannot see behaves like a tool that does not exist - and a weak
/// model then invents success instead of reporting "no such tool".
const KURISU_TOOLS: &str = "\
Your `kurisu-mcp` tools, all of which exist even when a shortened tool catalogue omits them (in code mode call them \
as `tools[\"kurisu-mcp\"].<name>({...})` inside `execute`, or find one with `search(\"<name>\")`): whoami, \
peers_list, peer_get, session_create, session_send, session_update, session_status, session_history, \
session_handoff, send_file (deliver a file to the user's paired chat), set_my_status, set_my_description, \
board_post, board_read, wake_at, wake_list, wake_cancel, run_background, transcribe, telegram_history, usage, version. \
For a command that can outlast your turn (a long build, test suite or download), start it with run_background and end \
your turn: Kurisu wakes you with its exit code and output when it exits, and your reply then goes to whoever sent the \
message you were working on. A turn is never cut off while it makes progress; only a long stretch with no output or \
activity at all ends it.";

/// The lead's ask_user (lead-only; workers ask their creator in text). Non-Claude
/// harnesses have no AskUserQuestion, so they are told when to use it.
const ASK_USER: &str = "\
When you need the user to decide something before you can go on, above all a choice between options, call \
kurisu-mcp's ask_user {question, choices?, multi_select?, header?} instead of writing the question in your reply \
and ending your turn: the user answers on their phone by tapping a choice (or typing or speaking an answer), and the \
answer comes back as the tool result, so you continue in the same turn. If it returns `pending`, the question stays \
open: end your turn, and the answer arrives later as a new message. If it returns `dismissed`, go on without it or \
stop and say so.";

/// Live Browser installed and switched on for this session.
const LB_ON: &str = "\
You have the Live Browser MCP tools (lb_*: open, snapshot, click, type, text, screenshot, handover…): a real Chrome \
shared by the team, which the user can watch and take over from their phone (/browser in Telegram). Use it for web \
pages; ask for a handover for logins, captchas or 2FA. Your tabs are your own: open reuses your tab, close_tab \
closes one you no longer need, and all of them close when your session's CLI stops. \
To send the user a picture of a page, do these three steps and check each result: 1) `tools.lb.open({url: ...})`, \
then `tools.lb.status()` and note its `tab` id; 2) in your folder run `lb shot --tab <tab id> --out shot.jpg` - the \
lb CLI is on your PATH and LB_SERVER/LB_PASSCODE are in your environment, and success prints \
`saved shot.jpg (... bytes JPEG)`; 3) call `tools[\"kurisu-mcp\"].send_file({path: \"shot.jpg\", caption: \"...\"})` \
inside `execute` and require `ok:true`. If any step fails, quote its exact error - a screenshot you took is NOT a \
screenshot the user received until send_file returns ok.";

/// Live Browser installed but off for this session: it is a switch, not a missing ability.
const LB_OFF: &str = "\
Live Browser (lb, a real Chrome) is installed on this machine and is only off for this session. It is off for you \
now. Do not tell the user you have no browser or only web fetching: you do have a browser. When the user asks to \
browse, open a page or check a site, turn it on with session_update {id, browser: \"on\"} (your own id, or a worker \
you created); it applies from that session's next turn. The user can also enable it with `ku mcp enable <session> \
lb` or /mcp in Telegram, and session_update {id, browser: \"off\"} releases it when done.";

/// No Live Browser on this machine at all.
const LB_ABSENT: &str = "\
Live Browser (lb, a real Chrome) is not installed on this machine, so this session has no browser tools. The user \
can set it up with `ku browser setup`. Do not claim a browser you do not have.";

/// The system prompt Kurisu gives a session (any harness).
/// `browser`: None when Live Browser is not set up; Some(on) when it is, on or off for this session.
fn session_prompt(s: &store::Session, browser: Option<bool>) -> String {
    let mut base = if s.role == "lead" { LEAD_PROMPT.to_string() } else { worker_prompt(&s.name) };
    base.push_str("\n\n");
    base.push_str(CAPABILITY_MANIFEST);
    base.push_str("\n\n");
    base.push_str(KURISU_TOOLS);
    if s.role == "lead" {
        // Lead-only tools (see mcp::LEAD_ONLY), named in the index like the rest.
        base.truncate(base.len() - 1);
        base.push_str(", ask_user.");
        if s.harness != "claude" {
            base.push(' ');
            base.push_str(ASK_USER);
        }
    }
    base.push_str("\n\n");
    base.push_str(match browser {
        Some(true) => LB_ON,
        Some(false) => LB_OFF,
        None => LB_ABSENT,
    });
    base.push_str("\n\nDurable rules and memory go in AGENTS.md in your folder, not in harness-specific memory \
(e.g. Claude's auto-memory): a harness switch (Claude/Codex/OpenCode) keeps the folder but loses that.");
    if s.harness == "claude" {
        base.push_str(" Your folder's CLAUDE.md imports it with `@AGENTS.md`; keep that line.");
    }
    format!("{base}\n\nAbout Kurisu itself: {} If the user asks you to change or add a feature to Kurisu, use those \
`ku source` commands (ask before installing anything; `ku source check` shows what is missing).", crate::source::rebuild_note())
}

/// Claude Code does not read AGENTS.md: give a Claude session's folder a CLAUDE.md that
/// imports it, unless the folder already has a CLAUDE.md (never overwritten).
fn ensure_claude_md(cwd: &Path) {
    let path = cwd.join("CLAUDE.md");
    if !path.exists() {
        if let Err(e) = std::fs::write(&path, "@AGENTS.md\n") {
            eprintln!("could not create {}: {e}", path.display());
        }
    }
}

/// Rule seeded into a new AGENTS.md when Telegram is the chat channel.
const MESSENGER_RULE: &str = "Replies go to a messenger app: write plain text, short sentences and paragraphs, \
no markdown tables/headers/bold/code fences; simple dashes or numbers for lists.\n";

/// Seed a default AGENTS.md (never overwriting one) carrying the messenger rule, but only
/// when the home has Telegram set up (`telegram.json`). Without Telegram nothing is written.
fn ensure_agents_md(home: &Path, cwd: &Path) {
    let path = cwd.join("AGENTS.md");
    if !path.exists() && home.join("telegram.json").is_file() {
        if let Err(e) = std::fs::write(&path, MESSENGER_RULE) {
            eprintln!("could not create {}: {e}", path.display());
        }
    }
}

/// Sessions whose turn a clean shutdown cut off (JSON list of ids, read at the next start).
const INTERRUPTED_FILE: &str = "interrupted.json";
/// The Live Browser's per-session MCP row: off by default, toggled like any server.
const LB: &str = "lb";
/// Stored config of the lb row (the real launch entry is built per session from browser.json).
const LB_ROW_CONFIG: &str = "built in: Kurisu's Live Browser";
/// Marker: existing lb rows were switched off once when lb stopped being built in.
const LB_OFF_MARKER: &str = "lb-off-by-default.done";
/// Marker: the lead's lb row was switched on once (the default on a real home).
const LB_LEAD_ON_MARKER: &str = "lb-lead-on-by-default.done";

/// The family should never have to type an MCP command to browse: on a home that has
/// Telegram (a real one, not a test home) and Live Browser installed, the lead session
/// gets the browser by default. Workers stay off.
fn lead_gets_lb_by_default(home: &Path) -> bool {
    crate::browser::active(home).is_some() && home.join("telegram.json").is_file()
}
/// First words of the message that resumes a session after a restart.
const RESUME_TAG: &str = "[Kurisu: resume after restart]";
/// How long a draining server lets in-flight turns finish before it stops anyway.
pub const DRAIN_TIMEOUT_SECS: u64 = 60;

/// How to start the server: where it listens and whether it may own the instance.
///
/// A standby server answers on its own socket (a slot next to the public one) but
/// starts no Telegram poller, no web listener, no browser and no resumed turns:
/// it is the candidate a deploy probes, not a second live instance.
#[derive(Clone, Debug, Default)]
pub struct ServeOpts {
    /// Bind this concrete path instead of `<KURISU_HOME>/kurisu.sock`.
    pub socket: Option<PathBuf>,
    /// Probe-only: no Telegram, no web, no browser, no resumed turns, and turns wait
    /// until `promote` (so a message arriving mid-handover is queued, not run twice).
    pub standby: bool,
}
/// Longest worker reply quoted in a callback (characters).
const CALLBACK_REPLY_MAX: usize = 1500;

/// How a background turn ended, for its callback: (done|stopped|error, reply or error).
fn turn_outcome(res: &Result<Value>) -> (&'static str, String) {
    match res {
        Ok(v) if v["stop_reason"].as_str().is_some_and(|r| r.contains("Cancelled")) => ("stopped", v["reply"].as_str().unwrap_or("").to_string()),
        Ok(v) => ("done", v["reply"].as_str().unwrap_or("").to_string()),
        Err(e) if format!("{e:#}").contains(claude_cli_acp::TURN_STOPPED) => ("stopped", "the session was stopped before it answered".into()),
        Err(e) => ("error", format!("{e:#}")),
    }
}

/// Actor of a turn the CLI started on its own (Claude Code resuming when a
/// `run_in_background` command it launched finished): Kurisu follows that turn
/// instead of sending a prompt.
const SELF_TURN_ACTOR: &str = "kurisu:self-turn";
/// Inbound marker row recorded for such a turn.
const SELF_TURN_TEXT: &str = "[Kurisu: the CLI resumed on its own (a background command it started has finished); following that turn]";
/// Callback outcome prefix for a follow-up turn nobody sent (background job or self-started turn).
const FOLLOWUP: &str = "follow-up";

/// The callback message a sender gets when a wait:false turn ends.
fn callback_text(worker: &str, outcome: &str, seq: i64, detail: &str) -> String {
    let detail = detail.trim();
    let mut d: String = detail.chars().take(CALLBACK_REPLY_MAX).collect();
    if d.len() < detail.len() {
        d.push_str(&format!("… [truncated; full reply: session_history {worker}]"));
    }
    if d.is_empty() {
        d = "(no reply text)".into();
    }
    let head = if outcome.starts_with(FOLLOWUP) {
        format!("{worker} followed up on your message (#{seq}) after its turn had ended")
    } else {
        format!("{worker} finished your wait:false message (#{seq})")
    };
    format!("[Kurisu callback] {head}: {outcome}.\n{d}\n\
(This is an automatic notice; no callback is sent for your answer to it.)")
}

/// Longest tool input or output kept per call in the history (characters of JSON).
const TOOL_DATA_MAX: usize = 8000;

/// Keep a JSON value as is if it is small, else a truncated string of it.
fn clip_json(v: &Value, max: usize) -> Value {
    let text = match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    if text.len() <= max {
        return v.clone();
    }
    let mut cut = max;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    json!(format!("{}… [{} more characters]", &text[..cut], text.len() - cut))
}

/// `harness not found` with the fix, unless `installed(harness)`.
fn require_cli_with(harness: &str, installed: impl Fn(&str) -> bool) -> Result<()> {
    if installed(harness) {
        return Ok(());
    }
    if harness == "agy" { bail!("agy not found on PATH; install: curl -fsSL https://antigravity.google/cli/install.sh | bash"); }
    if harness == "fx" { bail!("fx not found on PATH (or ~/.local/bin); install fx and log in to a provider (`fx login codex`)"); }
    if harness == "pi" { bail!("pi not found on PATH (or ~/.pi/agent/bin); install pi and log in to a provider (`pi auth`)"); }
    let other = if harness == "claude" { "codex" } else { "claude" };
    if installed(other) {
        bail!("{harness} not found on PATH; install it or use --harness {other}");
    }
    bail!("{harness} not found on PATH, and neither is {other}: install Claude Code or Codex and log in to it");
}

/// The default harness from the `clis` op: logged-in first (Claude before Codex), then installed.
fn pick_harness(v: &Value) -> Result<String> {
    let cli = |n: &str| v["clis"].as_array().and_then(|a| a.iter().find(|c| c["name"] == n).cloned()).unwrap_or(Value::Null);
    let (c, x, o, a) = (cli("claude"), cli("codex"), cli("opencode"), cli("agy"));
    let usable = |c: &Value| c["installed"] == json!(true) && c["logged_in"] != json!(false);
    for (ok, h) in [(usable(&c), "claude"), (usable(&x), "codex"), (usable(&o), "opencode"), (usable(&a), "agy"), (c["installed"] == json!(true), "claude"), (x["installed"] == json!(true), "codex"), (o["installed"] == json!(true), "opencode"), (a["installed"] == json!(true), "agy")] {
        if ok {
            return Ok(h.into());
        }
    }
    bail!("no supported CLI found on PATH: install Claude Code, Codex or OpenCode and configure its provider")
}

/// The permission modes the installed `claude` accepts (from `claude --help`), cached.
fn claude_modes() -> Vec<String> {
    static MODES: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    MODES.get_or_init(|| {
        if cfg!(test) {
            return ["acceptEdits", "auto", "bypassPermissions", "default", "plan"].iter().map(|m| m.to_string()).collect();
        }
        let bin = crate::harness::resolve("claude").unwrap_or_else(|| PathBuf::from("claude"));
        let help = std::process::Command::new(bin).arg("--help").output().map(|o| String::from_utf8_lossy(&o.stdout).into_owned()).unwrap_or_default();
        parse_modes(&help)
    }).clone()
}

/// `--permission-mode <mode> ... (choices: "acceptEdits", "auto", ...)` -> the choices.
fn parse_modes(help: &str) -> Vec<String> {
    let Some(i) = help.find("--permission-mode") else { return vec![] };
    let rest = &help[i..];
    let Some(j) = rest.find("choices:") else { return vec![] };
    let list = &rest[j + 8..];
    let end = list.find(')').unwrap_or(list.len());
    list[..end].split(',').map(|c| c.trim().trim_matches('"').to_string()).filter(|c| !c.is_empty()).collect()
}

/// The mode a session runs in: its own setting, else `auto` when the CLI has it, else `default`.
pub fn effective_mode(set: Option<&str>, supported: &[String]) -> String {
    let has = |m: &str| supported.iter().any(|x| x == m);
    match set {
        Some(m) => m.to_string(),
        None if has("auto") => "auto".into(),
        None => "default".into(),
    }
}

/// The `--permission-mode` value for Claude: newer CLIs call `default` `manual`.
pub fn claude_mode_arg(mode: &str, supported: &[String]) -> String {
    let has = |m: &str| supported.iter().any(|x| x == m);
    match mode {
        "default" if !has("default") && has("manual") => "manual".into(),
        "auto" if !supported.is_empty() && !has("auto") => if has("default") { "default".into() } else { "manual".into() },
        m => m.to_string(),
    }
}

/// Codex's nearest equivalent: (approval policy, sandbox).
pub fn codex_mode(mode: &str) -> (&'static str, &'static str) {
    match mode {
        // Routine work inside the workspace runs; escalations ask (Codex's own "auto").
        "auto" | "acceptEdits" => ("on-request", "workspace-write"),
        // Ask before anything beyond trusted read-only commands.
        "default" => ("untrusted", "workspace-write"),
        "plan" => ("on-request", "read-only"),
        "bypassPermissions" => ("never", "danger-full-access"),
        _ => ("on-request", "workspace-write"),
    }
}

fn take8(s: &str) -> String {
    s.chars().take(8).collect()
}

/// Shorten text to `max` characters (on a char boundary).
fn clip_text(t: &str, max: usize) -> String {
    if t.chars().count() <= max {
        return t.to_string();
    }
    let s: String = t.chars().take(max).collect();
    format!("{s}… [{} more characters]", t.chars().count() - max)
}

/// Seconds an approval request waits for an answer (`KURISU_APPROVAL_TTL`, default 600).
pub fn approval_ttl() -> u64 {
    std::env::var("KURISU_APPROVAL_TTL").ok().and_then(|v| v.parse().ok()).filter(|v| *v >= 10).unwrap_or(600)
}

fn now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

/// Pick a loopback `redirect_uri` a browser on this host can actually reach.
/// Candidates come from the configured listeners; each one is preflighted with a
/// real TCP connect, so a port that nothing holds is never handed out. When no
/// candidate answers, the conventional default is returned with `false` so the
/// message can say auto-callback is off and the paste-back is the way through.
async fn pick_loopback_redirect(home: &Path) -> (String, bool) {
    let timeout = std::time::Duration::from_millis(400);
    match pick_listening_loopback(crate::remote::loopback_callback_ports(home), timeout).await {
        Some(port) => (callback_uri(port), true),
        None => (callback_uri(crate::remote::default_web_port(home)), false),
    }
}

fn callback_uri(port: u16) -> String {
    format!("http://127.0.0.1:{port}/api/v1/oauth/callback")
}

/// First candidate that actually accepts a TCP connection, in the given order.
async fn pick_listening_loopback(ports: impl IntoIterator<Item = u16>, timeout: std::time::Duration) -> Option<u16> {
    for port in ports {
        if crate::remote::loopback_listening(port, timeout).await {
            return Some(port);
        }
    }
    None
}

fn outcome(allow: bool) -> RequestPermissionOutcome {
    let id = if allow { claude_cli_acp::ALLOW_ONCE } else { claude_cli_acp::REJECT_ONCE };
    RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(PermissionOptionId::new(id)))
}

/// The harness-native "always allow" answer for this session: Codex turns it
/// into `acceptForSession`; Claude's adapter accepts the same option id.
fn outcome_always() -> RequestPermissionOutcome {
    RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(PermissionOptionId::new(crate::codex::ALLOW_ALWAYS)))
}

/// A pre-approval rule settled a permission prompt: the answer the harness gets.
struct Settled {
    allow: bool,
    message: String,
}

/// Text prepended to a prompt when the session's previous turn failed: the CLI
/// still holds the failed message, so say plainly that it must not be answered.
pub fn failed_turn_preamble(err: &str) -> String {
    let err: String = err.chars().take(160).collect();
    format!(
        "[Kurisu: your previous turn failed ({err}) and that message was abandoned. \
         Do not answer it. Reply only to the message below.]\n\n"
    )
}

/// The `turn failed` line kept in the transcript: the driver's error (which includes
/// the CLI's own last stderr lines) plus a hint when the reason is a known external
/// limit, so nobody spends turns looking for a Kurisu bug.
pub fn turn_failed_message(err: &anyhow::Error) -> String {
    let text = format!("{err:#}");
    match failure_hint(&text) {
        Some(hint) => format!("turn failed: {text}\n({hint})"),
        None => format!("turn failed: {text}"),
    }
}

/// One line for failure reasons that belong to the CLI account, not Kurisu.
pub fn failure_hint(text: &str) -> Option<&'static str> {
    let t = text.to_ascii_lowercase();
    const QUOTA: &[&str] = &[
        "usage limit", "quota", "rate limit", "rate_limit", "too many requests", "429 too many",
        "upgrade to pro", "out of credits", "credit balance", "insufficient_quota",
    ];
    QUOTA.iter().any(|p| t.contains(p)).then_some(
        "this is the CLI account's own limit, not Kurisu; retry at the time it names, or switch harness/model",
    )
}

/// Request ids of questions asked with kurisu-mcp's ask_user (not a harness prompt).
const ASK_PREFIX: &str = "ask_user-";

/// How long an ask_user question stays open for an answer (KURISU_ASK_TTL, seconds;
/// default 1 h). It outlives the asking turn; a late answer arrives as a new message.
pub fn ask_ttl() -> u64 {
    std::env::var("KURISU_ASK_TTL").ok().and_then(|v| v.parse().ok()).filter(|v| *v >= 10).unwrap_or(3600)
}

/// How long one ask_user call blocks (KURISU_ASK_WAIT_SECS; default 100 s): under
/// Claude Code's 120 s background cut-off and the MCP clients' timeouts.
pub fn ask_wait() -> std::time::Duration {
    std::time::Duration::from_secs(std::env::var("KURISU_ASK_WAIT_SECS").ok().and_then(|v| v.parse().ok()).filter(|v| *v >= 1).unwrap_or(100))
}

/// ask_user's result when the call stops waiting but the question stays open.
fn ask_still_open() -> Value {
    json!({"status": "pending", "answer": null, "message": "No answer yet. The question stays open on the user's phone; when they answer, \
it arrives as a new message (\"Answer to your question '...': ...\"). Do not ask again: end your turn now, saying you are waiting \
for their answer (or first finish work that does not depend on it)."})
}

/// ask_user's result when no answer came.
fn ask_dismissed(message: &str) -> Value {
    json!({"status": "dismissed", "answer": null, "message": message})
}

/// Map user answers onto AskUserQuestion's `answers` (`{question text: answer}`).
/// Keys may be the question text, its header or its index; values a string
/// (a label or free text) or a list of labels (joined with ", ").
pub fn fill_answers(input: &Value, answers: &Value) -> Result<Value> {
    let qs = input["questions"].as_array().ok_or_else(|| anyhow!("the question has no questions[]"))?;
    let given = answers.as_object().ok_or_else(|| anyhow!("answers must be an object"))?;
    let mut out = serde_json::Map::new();
    for (i, q) in qs.iter().enumerate() {
        let text = q["question"].as_str().unwrap_or_default();
        let header = q["header"].as_str().unwrap_or_default();
        let v = given.get(text).or_else(|| given.get(header).filter(|_| !header.is_empty())).or_else(|| given.get(&i.to_string()))
            .ok_or_else(|| anyhow!("no answer for question {i}: {text:?}"))?;
        let ans = match v {
            Value::String(s) => s.clone(),
            Value::Array(xs) => xs.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(", "),
            _ => bail!("answer for {text:?} must be a string or a list of strings"),
        };
        if ans.trim().is_empty() {
            bail!("empty answer for {text:?}");
        }
        out.insert(text.to_string(), Value::String(ans));
    }
    Ok(Value::Object(out))
}

/// Largest accepted attachment (decoded).
pub const MAX_UPLOAD: usize = 25 * 1024 * 1024;

/// A safe file name: letters, digits, `.`, `_`, `-`; no leading dot; at most 80 characters.
pub fn safe_filename(name: &str) -> String {
    let base = name.rsplit(['/', '\\']).next().unwrap_or("");
    let s: String = base.chars().map(|c| if c.is_ascii_alphanumeric() || "._-".contains(c) { c } else { '_' }).collect();
    let s = s.trim_start_matches('.').chars().take(80).collect::<String>();
    if s.is_empty() { "file".into() } else { s }
}

/// Decode base64 (optionally a `data:<mime>;base64,` URL).
pub fn decode_b64(data: &str) -> Result<Vec<u8>> {
    use base64::Engine;
    let raw = match data.find(";base64,") {
        Some(i) if data.starts_with("data:") => &data[i + 8..],
        _ => data,
    };
    let bytes = base64::engine::general_purpose::STANDARD.decode(raw.trim()).context("invalid base64 data")?;
    if bytes.len() > MAX_UPLOAD {
        bail!("attachment is {} bytes; the limit is {MAX_UPLOAD}", bytes.len());
    }
    Ok(bytes)
}

/// Validate an MCP server entry as passed to `--mcp-config`.
pub fn validate_mcp(name: &str, config: &Value) -> Result<()> {
    if name.is_empty() || name.len() > 40 || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        bail!("invalid MCP server name {name:?} (letters, digits, '-', '_'; at most 40)");
    }
    if name == crate::mcp::SERVER_NAME {
        bail!("{name} is Kurisu's own server and is always on");
    }
    let o = config.as_object().ok_or_else(|| anyhow!("config must be an object"))?;
    let strings = |k: &str| o.get(k).is_none_or(|v| v.as_object().is_some_and(|m| m.values().all(Value::is_string)));
    match o.get("type").and_then(Value::as_str).unwrap_or("stdio") {
        "stdio" => {
            if !o.get("command").is_some_and(|c| c.as_str().is_some_and(|c| !c.is_empty())) {
                bail!("a stdio MCP server needs a command");
            }
            if o.get("headers").is_some() || o.get("client_id").is_some() {
                bail!("stdio MCP servers do not take headers or client_id (use env instead)");
            }
            if !o.get("args").is_none_or(|a| a.as_array().is_some_and(|a| a.iter().all(Value::is_string))) || !strings("env") {
                bail!("args must be a list of strings and env an object of strings");
            }
        }
        "http" | "sse" => {
            if !o.get("url").and_then(Value::as_str).is_some_and(|u| u.starts_with("http://") || u.starts_with("https://")) {
                bail!("an http/sse MCP server needs an http(s) url");
            }
            if !strings("headers") || o.get("headers").and_then(Value::as_object).is_some_and(|m| m.keys().any(|k| k.trim().is_empty())) {
                bail!("headers must be an object of strings");
            }
            if !o.get("client_id").is_none_or(|v| v.as_str().is_some_and(|s| !s.trim().is_empty())) {
                bail!("client_id must be a non-empty string");
            }
        }
        t => bail!("unknown MCP server type {t:?} (stdio, http or sse)"),
    }
    Ok(())
}

/// Mask a header value for display: show `****last4` when long enough (`>= 8` chars), else `****`.
/// Idempotent when called on an already-masked value.
pub fn mask_header_value(v: &str) -> String {
    if v.starts_with("****") || v == "<set>" {
        return v.to_string();
    }
    let chars: Vec<char> = v.chars().collect();
    if chars.len() >= 8 {
        let tail: String = chars[chars.len() - 4..].iter().collect();
        format!("****{tail}")
    } else {
        "****".to_string()
    }
}

/// An MCP entry for display: env and header values are hidden (`<set>` for env, `****last4` for headers).
pub fn redact_mcp(config: &Value) -> Value {
    let mut c = config.clone();
    if let Some(m) = c.get_mut("env").and_then(Value::as_object_mut) {
        for v in m.values_mut() {
            *v = json!("<set>");
        }
    }
    for k in ["headers", "http_headers", "env_http_headers"] {
        if let Some(m) = c.get_mut(k).and_then(Value::as_object_mut) {
            for v in m.values_mut() {
                let masked = mask_header_value(v.as_str().unwrap_or(""));
                *v = json!(masked);
            }
        }
    }
    c
}

#[derive(Clone)]
struct OpencodeModels {
    at: std::time::Instant,
    models: Vec<Value>,
    note: String,
    /// False only for the synthetic default entry: a real catalogue is worth
    /// reusing if a later CLI call fails.
    real: bool,
}

/// Tests that point KURISU_OPENCODE_BIN at a fixture share process-wide env;
/// serialize them instead of racing each other's fixtures.
#[cfg(test)]
pub(crate) static OPENCODE_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub struct Server {
    started_at: i64,
    started: std::time::Instant,
    paths: Paths,
    store: Mutex<Store>,
    claude: ClaudeCli,
    codex: OnceCell<Arc<CodexDriver>>,
    opencode: Mutex<HashMap<String, Arc<crate::opencode::OpenCodeDriver>>>,
    /// Codex's launch policy (sandbox from KURISU_CODEX_SANDBOX), shared by its threads.
    codex_cfg: CodexConfig,
    live: Mutex<HashSet<String>>,
    /// Sessions whose model/effort/name changed: relaunch (resume) before their next turn.
    relaunch: Mutex<HashSet<String>>,
    /// Self-requested harness switches, run when the session's turn has ended: (harness, model, actor).
    pending_switch: Mutex<HashMap<String, (String, Option<String>, String)>>,
    /// `clis` op result and when it was computed (the checks run the CLIs).
    clis_cache: Mutex<Option<(std::time::Instant, Value)>>,
    /// `opencode models` output and when it was fetched (KURISU_OPENCODE_MODELS_TTL_SECS, default 180).
    opencode_models: Mutex<Option<OpencodeModels>>,
    /// Codex thread id -> its rollout file.
    rollout_paths: Mutex<HashMap<String, PathBuf>>,
    /// Fingerprint (tools, prompt, binary) each live CLI was launched with.
    launched_fp: Mutex<HashMap<String, String>>,
    /// Sessions whose CLI conversation was just restarted (their next prompt says how to catch up).
    fresh_start: Mutex<HashSet<String>>,
    busy: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    // Interrupt holds the gate until cleanup; queued workers cannot start a
    // new harness turn while the old turn is being cancelled.
    turn_gates: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    active_turns: Mutex<HashMap<String, i64>>,
    /// Per session: who sent the message its latest turn answered (a session id, or
    /// None for the user) and that message's seq. A follow-up turn nobody sent (a
    /// background job ended) reports back to them.
    requesters: Mutex<HashMap<String, (Option<String>, i64)>>,
    /// Sessions with a self-started turn queued or being followed.
    self_turns: Mutex<HashSet<String>>,
    stop: Notify,
    /// Signals the accept task to close the listener (drain): no new clients.
    stop_accept: Arc<Notify>,
    /// A standby server holds turns until `promote` has reconciled the store.
    ready: std::sync::atomic::AtomicBool,
    ready_notify: Arc<Notify>,
    /// Started as a standby (probe) server: no Telegram, web, browser or resumes yet.
    standby: std::sync::atomic::AtomicBool,
    /// Live turn output (assistant deltas, tool start/update, turn state) for SSE clients.
    events: broadcast::Sender<Value>,
    /// The tool a session is running now (or ran last) in its current turn.
    activity: Mutex<HashMap<String, Value>>,
    /// When a session was compacted: its context size is unknown until the next status line.
    context_override: Mutex<HashMap<String, i64>>,
    /// Sessions that were mid-turn when the previous server stopped (resumed at startup).
    interrupted: Mutex<Vec<String>>,
    /// Tests turn this off: messages are recorded but no CLI turn is started.
    turns_enabled: std::sync::atomic::AtomicBool,
    /// Transcript usage per session, cached by (path, length, mtime).
    usage_cache: Mutex<HashMap<String, (PathBuf, u64, std::time::SystemTime, claude_cli_acp::usage::Usage)>>,
    /// The optional built-in Telegram channel (`ku telegram setup`).
    pub telegram: crate::telegram::Channel,
    pub discord: crate::discord::Channel,
    pub channels: Arc<crate::channels::state::State>,
    channel_posts: Mutex<HashSet<String>>,
    /// Remote access from the phone (`/terminal`), off until `ku terminal enable`.
    pub remote: crate::remote::Hub,
    /// Kurisu's own `lb web` (Live Browser add-on), started on demand.
    pub browser: crate::browser::Supervisor,
    /// Injected HTTP client for OAuth discovery, DCR, and code exchange (mockable in unit tests).
    http_client: std::sync::RwLock<Arc<dyn crate::oauth::HttpClient>>,
    /// Transient refresh failures omit that server for this turn, retaining its token for retry.
    mcp_unavailable: Mutex<HashSet<(String, String)>>,
    /// Waiting ask_user calls, by their approval's request id: woken with the tool result.
    asks: Mutex<HashMap<String, tokio::sync::oneshot::Sender<Value>>>,
}

/// Context and spend fields from a status-line capture (`None` fields: not captured yet).
fn context_fields(sl: Option<&claude_cli_acp::StatusLine>) -> Value {
    let Some(sl) = sl else {
        return json!({"context_tokens": null, "context_window": null, "context_used_pct": null, "cost_usd": null, "metrics_at": null});
    };
    let cw = &sl.raw["context_window"];
    let cu = &cw["current_usage"];
    let n = |v: &Value, k: &str| v.get(k).and_then(Value::as_u64).unwrap_or(0);
    let mut tokens = n(cu, "input_tokens") + n(cu, "cache_creation_input_tokens") + n(cu, "cache_read_input_tokens");
    let size = cw.get("context_window_size").and_then(Value::as_u64);
    let pct = cw.get("used_percentage").and_then(Value::as_f64);
    if tokens == 0 {
        if let (Some(p), Some(sz)) = (pct, size) {
            tokens = (p / 100.0 * sz as f64).round() as u64;
        }
    }
    json!({"context_tokens": tokens, "context_window": size, "context_used_pct": pct,
           "cost_usd": sl.raw["cost"].get("total_cost_usd").and_then(Value::as_f64), "metrics_at": sl.captured_at})
}

/// What a fresh primary server does to the store at startup: remember turns a restart
/// cut off, reset statuses, run one-time migrations, cancel unanswerable approvals.
/// A standby server defers all of this to `promote`, so it never races the live one.
fn reconcile_startup(paths: &Paths, store: &Store) -> Result<Vec<String>> {
    // A turn that was running when the previous server stopped (restart, deploy) is
    // resumed at startup (resume_interrupted); remember those sessions before the reset.
    let mut interrupted: Vec<String> = store.list()?.into_iter()
        .filter(|s| s.harness_session_id.is_some() && s.lifecycle == "active"
                && matches!(s.status.as_str(), "starting" | "working" | "awaiting_approval"))
        .map(|s| s.id).collect();
    // Turns a clean shutdown cut off (statuses were reset to "stopped" then).
    let noted = paths.home.join(INTERRUPTED_FILE);
    if let Ok(b) = std::fs::read(&noted) {
        for id in serde_json::from_slice::<Vec<String>>(&b).unwrap_or_default() {
            if !interrupted.contains(&id) && store.get(&id)?.is_some_and(|s| s.lifecycle == "active") {
                interrupted.push(id);
            }
        }
        let _ = std::fs::remove_file(&noted);
    }
    for s in store.list()? {
        // Never-run sessions stay "new"; everything else is not running after a restart.
        if s.harness_session_id.is_some() {
            store.set_status(&s.id, "stopped")?;
        }
    }
    // lb used to be built into every session: switch existing lb rows off once (they
    // can be turned back on per session); later enables are kept.
    let marker = paths.home.join(LB_OFF_MARKER);
    if !marker.exists() {
        let disabled = store.mcp_disable_all_sessions(LB)?;
        for s in &disabled {
            paths.scrub_mcp_credentials(s, LB);
        }
        let _ = std::fs::write(&marker, "lb is off by default; enable per session\n");
    }
    // On a real home the lead is the exception: it starts with the browser on so the
    // family never types an MCP command. Runs once; a row the user switched off stays off.
    let lead_marker = paths.home.join(LB_LEAD_ON_MARKER);
    if !lead_marker.exists() && lead_gets_lb_by_default(&paths.home) {
        for lead in store.list()?.iter().filter(|s| s.role == "lead" && s.lifecycle == "active") {
            if store.mcp_switched_off(&lead.id, LB)? {
                continue;
            }
            if store.mcp_set(&lead.id, LB, Some(&json!(LB_ROW_CONFIG)), Some(true))? {
                store.log_event("kurisu", &lead.id, "mcp_enabled", LB)?;
            }
        }
        let _ = std::fs::write(&lead_marker, "the lead session gets Live Browser by default\n");
    }
    // Approvals still pending from before a restart can no longer be answered.
    for a in store.approvals(None, false)? {
        store.approval_decide(&a.id, "cancelled", "kurisu", Some("the server restarted"), None)?;
    }
    Ok(interrupted)
}

impl Server {
    pub(crate) fn version_info(&self) -> Value {
        let mut info = crate::version::info();
        info["started_at"] = json!(self.started_at);
        info["uptime_seconds"] = json!(self.started.elapsed().as_secs());
        info
    }

    pub fn new(paths: Paths) -> Result<Self> {
        Self::new_mode(paths, false)
    }

    /// `standby`: start as a deploy candidate. The store is opened (so reads work) but
    /// nothing is reconciled, no CLI is resumed, and no sidecar is started until
    /// `promote`. The live server stays the only writer of session status.
    pub fn new_mode(paths: Paths, standby: bool) -> Result<Self> {
        std::fs::create_dir_all(&paths.home)?;
        let store = Store::open(&paths.db())?;
        let channels = Arc::new(crate::channels::state::State::open(&paths.home)?);
        let interrupted = if standby { Vec::new() } else { reconcile_startup(&paths, &store)? };
        let me = std::env::current_exe()?.to_string_lossy().into_owned();
        let mut cfg = claude_cli_acp::Config::new(paths.home.join("claude"), vec![me, "hook".into()]);
        cfg.tmux_socket = paths.tmux_socket();
        if let Ok(bin) = std::env::var("KURISU_CLAUDE_BIN") {
            cfg.claude_bin = bin;
        } else if let Some(bin) = crate::harness::resolve("claude") {
            cfg.claude_bin = bin.to_string_lossy().into_owned();
        }
        // The hook's own deadline is a fallback: the server expires requests 10 s earlier.
        cfg.permission_timeout = std::time::Duration::from_secs(approval_ttl() + 10);
        // KURISU_MCP_TIMEOUT: seconds a launch waits for the session's MCP servers (default 45).
        if let Some(t) = std::env::var("KURISU_MCP_TIMEOUT").ok().and_then(|v| v.parse::<u64>().ok()).filter(|t| *t >= 5) {
            cfg.mcp_timeout = std::time::Duration::from_secs(t);
        }
        Ok(Self {
            started_at: chrono::Utc::now().timestamp(),
            started: std::time::Instant::now(),
            paths,
            store: Mutex::new(store),
            claude: ClaudeCli::new(cfg),
            codex: OnceCell::new(),
            opencode: Mutex::new(HashMap::new()),
            codex_cfg: CodexConfig::from_env(),
            live: Mutex::default(),
            relaunch: Mutex::default(),
            pending_switch: Mutex::default(),
            clis_cache: Mutex::default(),
            opencode_models: Mutex::default(),
            rollout_paths: Mutex::default(),
            launched_fp: Mutex::default(),
            fresh_start: Mutex::default(),
            busy: Mutex::default(),
            turn_gates: Mutex::default(),
            active_turns: Mutex::default(),
            requesters: Mutex::default(),
            self_turns: Mutex::default(),
            stop: Notify::new(),
            stop_accept: Arc::default(),
            ready: std::sync::atomic::AtomicBool::new(!standby),
            ready_notify: Arc::default(),
            standby: std::sync::atomic::AtomicBool::new(standby),
            events: broadcast::channel(1024).0,
            usage_cache: Mutex::default(),
            telegram: Default::default(), discord: Default::default(), channels, channel_posts: Mutex::default(),
            remote: crate::remote::Hub::with_inherited_web()?,
            browser: Default::default(),
            activity: Mutex::default(),
            context_override: Mutex::default(),
            interrupted: Mutex::new(interrupted),
            turns_enabled: std::sync::atomic::AtomicBool::new(true),
            http_client: std::sync::RwLock::new(Arc::new(reqwest::Client::new())),
            mcp_unavailable: Mutex::default(),
            asks: Mutex::default(),
        })
    }

    pub fn http_client(&self) -> Arc<dyn crate::oauth::HttpClient> {
        self.http_client.read().unwrap().clone()
    }

    pub fn set_http_client(&self, client: Arc<dyn crate::oauth::HttpClient>) {
        *self.http_client.write().unwrap() = client;
    }

    /// The OpenCode catalogue comes from OpenCode itself (`opencode models`),
    /// which already reads `opencode.json`, providers and auth. Short cache:
    /// `/model` must not spawn a process on every keystroke; a new provider
    /// shows up when it expires, without restarting Kurisu.
    async fn opencode_models(&self) -> (Vec<Value>, String) {
        let ttl = std::env::var("KURISU_OPENCODE_MODELS_TTL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(180u64);
        let cached = self.opencode_models.lock().await.clone();
        if let Some(c) = &cached {
            if c.at.elapsed() < std::time::Duration::from_secs(ttl) {
                return (c.models.clone(), c.note.clone());
            }
        }
        let previous = cached.filter(|c| c.real);
        let config_path = crate::opencode::config_path();
        let configured = config_path.as_deref().map(crate::opencode::config_models).and_then(Result::ok);
        let built = match crate::opencode::list_models().await {
            Ok(ids) => {
                let default = configured.as_ref().and_then(|(_, d)| d.clone());
                let models: Vec<Value> = ids.iter().map(|id| json!({
                    "id": id, "label": id, "verified": true,
                    "default": default.as_deref() == Some(id.as_str()),
                })).collect();
                OpencodeModels {
                    at: std::time::Instant::now(),
                    models,
                    note: format!("Discovered from `{} models` ({} models)", crate::opencode::bin(), ids.len()),
                    real: true,
                }
            }
            Err(e) => {
                if let Some((ids, default)) = configured.filter(|(ids, _)| !ids.is_empty()) {
                    let models: Vec<Value> = ids.iter().map(|id| json!({
                        "id": id, "label": id, "verified": true,
                        "default": default.as_deref() == Some(id.as_str()),
                    })).collect();
                    let source = config_path.as_deref().map(|p| p.display().to_string())
                        .unwrap_or_else(|| "opencode.json".into());
                    OpencodeModels {
                        at: std::time::Instant::now(),
                        models,
                        note: format!("Could not run `{} models` ({e:#}); showing the models configured in {source}. Providers added with `opencode auth` may be missing.", crate::opencode::bin()),
                        real: true,
                    }
                } else if let Some(previous) = previous {
                    OpencodeModels {
                        at: std::time::Instant::now(),
                        models: previous.models,
                        note: format!("Could not refresh the OpenCode model list ({e:#}); showing the last known list"),
                        real: true,
                    }
                } else {
                    OpencodeModels {
                        at: std::time::Instant::now(),
                        models: vec![json!({"id": "default", "label": "OpenCode default (model list unavailable)",
                                            "verified": true, "default": true})],
                        note: format!("Could not list OpenCode models ({e:#}); falling back to OpenCode's own default. Any model id is still accepted."),
                        real: false,
                    }
                }
            }
        };
        *self.opencode_models.lock().await = Some(built.clone());
        (built.models, built.note)
    }

    async fn opencode(&self) -> Result<Arc<crate::opencode::OpenCodeDriver>> { self.opencode_for("catalogue").await }

    // ACP MCP registrations are directory-scoped upstream. A process per Kurisu
    // session prevents two peers sharing a folder from receiving each other's identity.
    async fn opencode_for(&self, key: &str) -> Result<Arc<crate::opencode::OpenCodeDriver>> {
        let mut drivers=self.opencode.lock().await;
        if let Some(d)=drivers.get(key).filter(|d|d.alive.load(std::sync::atomic::Ordering::Acquire)) { return Ok(d.clone()); }
        // One process per Kurisu session: give it the lb CLI's env when that session
        // has the browser on, so `lb shot --out` works from its shell (not only the
        // lb MCP tools). The catalogue driver gets none.
        let session = self.store.lock().await.get(key).ok().flatten();
        let harness = match session.as_ref().map(|s| s.harness.as_str()) { Some("agy") => "agy", Some("fx") => "fx", Some("pi") => "pi", _ => "opencode" };
        // agy's ACP argv depends on the mode: headless agy cannot prompt, so only
        // `auto` gets --dangerously-skip-permissions. A mode change relaunches.
        let mode = session.as_ref().and_then(|s| s.permission_mode.clone());
        let mut env = match &session {
            Some(s) => self.session_lb(&s.id).await.map(|b| b.shell_env()).unwrap_or_default(),
            None => Vec::new(),
        };
        // fx takes its provider and model from the process env; a model change relaunches.
        if let Some(s) = session.as_ref().filter(|s| s.harness == "fx") { env.extend(crate::fx::session_env(&s.model)?); }
        if harness == "pi" { env.extend(crate::pi::bridge_env(&self.paths.home)); }
        let bin = match harness { "agy" => crate::agy::bin(), "fx" => crate::fx::bin(), "pi" => std::env::current_exe()?.to_string_lossy().into_owned(), _ => crate::opencode::bin() };
        let driver=Arc::new(crate::acp::AcpDriver::spawn_for_mode(harness, bin, env, mode.as_deref()).await?);
        drivers.insert(key.to_string(),driver.clone());
        self.live.lock().await.remove(key);
        Ok(driver)
    }

    async fn close_driver(&self, session: &store::Session) {
        let Some(hid)=&session.harness_session_id else { return };
        let id=SessionId::new(hid.clone());
        if matches!(session.harness.as_str(), "opencode" | "agy" | "fx" | "pi") {
            let driver=self.opencode.lock().await.get(&session.id).cloned();
            if let Some(driver)=driver {
                let _=driver.close(&id).await;
                driver.shutdown().await;
            }
        } else {
            let _=self.claude.close(&id).await;
        }
    }

    async fn opencode_mcp(&self, s: &store::Session) -> Result<Value> {
        let mut mcp = vec![json!({"name":crate::mcp::SERVER_NAME,"command":std::env::current_exe()?,"args":["mcp"],
            "env":[{"name":"KURISU_HOME","value":self.paths.home.to_string_lossy()},{"name":"KURISU_SESSION","value":s.id}]})];
        for (name, cfg, enabled) in self.runtime_mcp_rows(&s.id).await? {
            if !enabled || name == crate::mcp::SERVER_NAME || name == LB { continue; }
            if cfg["command"].is_string() {
                let env: Vec<Value> = cfg["env"].as_object().map(|o| o.iter().map(|(k,v)|json!({"name":k,"value":v})).collect()).unwrap_or_default();
                mcp.push(json!({"name":name,"command":cfg["command"],"args":cfg.get("args").cloned().unwrap_or(json!([])),"env":env}));
            } else if cfg["url"].is_string() {
                if cfg["type"]=="sse" { bail!("OpenCode ACP does not support SSE MCP server {name}; use HTTP or stdio"); }
                let headers: Vec<Value> = cfg["headers"].as_object().map(|o|o.iter().map(|(k,v)|json!({"name":k,"value":v})).collect()).unwrap_or_default();
                mcp.push(json!({"type":"http","name":name,"url":cfg["url"],"headers":headers}));
            }
        }
        if let Some(b)=self.session_lb(&s.id).await {
            let env: Vec<Value>=b.mcp_env(&s.name).as_object().map(|o|o.iter().map(|(k,v)|json!({"name":k,"value":v})).collect()).unwrap_or_default();
            mcp.push(json!({"name":"lb","command":b.lb,"args":["mcp"],"env":env}));
        }
        Ok(json!(mcp))
    }

    async fn opencode_session(&self, s: &store::Session, id: Option<&SessionId>) -> Result<SessionId> {
        let mcp = self.opencode_mcp(s).await?;
        let d=self.opencode_for(&s.id).await?;
        let result=d.session(id,PathBuf::from(&s.cwd),mcp).await?;
        let id=match id { Some(id)=>id.clone(), None=>SessionId::new(result["sessionId"].as_str().ok_or_else(||anyhow!("missing ACP sessionId"))?.to_string()) };
        d.configure(&id,&s.model,s.effort.as_deref(),s.permission_mode.as_deref()).await?;
        Ok(id)
    }

    async fn codex(&self) -> Result<Arc<CodexDriver>> {
        self.codex
            .get_or_try_init(|| async {
                // Codex runs one app-server for every session, so the lb env is
                // process-wide; the two values are the same for every session.
                let mut cfg = self.codex_cfg.clone();
                if let Some(b) = crate::browser::active(&self.paths.home) {
                    cfg.env = b.shell_env();
                }
                CodexDriver::spawn(cfg).await.map(Arc::new)
            })
            .await
            .cloned()
    }

    /// Follow the live turn output (the `/web` SSE stream does).
    pub fn subscribe_events(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }

    fn publish(&self, event: Value) {
        let _ = self.events.send(event);
    }

    /// Whether the lb row is enabled for a session (from an already-held store guard).
    fn lb_on(st: &store::Store, id: &str) -> bool {
        st.mcp_list(id).ok().is_some_and(|rows| rows.iter().any(|(n, _, on)| n == LB && *on))
    }

    /// What the running harness session's toolset depends on: its MCP servers (name, config,
    /// on/off), whether lb is on, the kurisu-mcp tool schemas, and the Kurisu build itself.
    /// An ACP session fixes its toolset when it is created (`session/load` does not extend
    /// it), so any of these changing means the harness session must be recreated.
    fn toolset_fingerprint(rows: &[(String, Value, bool)], lb_on: bool, build: &str) -> String {
        use std::hash::{Hash, Hasher};
        let mut entries: Vec<(String, String, bool)> = rows.iter()
            .filter(|(n, _, _)| n != LB)
            .map(|(n, c, e)| (n.clone(), c.to_string(), *e)).collect();
        entries.sort();
        let mut h = std::collections::hash_map::DefaultHasher::new();
        entries.hash(&mut h);
        lb_on.hash(&mut h);
        crate::mcp::tools().to_string().hash(&mut h);
        build.hash(&mut h);
        format!("{:016x}", h.finish())
    }

    /// The Kurisu build a toolset would be created with. `ku` upgrades swap the binary, so
    /// its size and mtime identify the build; the package version is the fallback.
    fn build_id() -> String {
        let exe = std::env::current_exe().and_then(|p| std::fs::metadata(p));
        match exe {
            Ok(m) => format!("{}:{}:{}", env!("CARGO_PKG_VERSION"), m.len(),
                m.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_nanos()).unwrap_or(0)),
            Err(_) => env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Should an existing harness session be replaced? Its persisted toolset fingerprint is
    /// what its ACP session/thread was created with. A row from before this field has none:
    /// replace once (an old build's ACP session cannot gain new tools on `session/load`).
    fn toolset_is_stale(persisted: Option<&str>, now: &str, has_harness_session: bool) -> bool {
        match persisted {
            Some(f) => f != now,
            None => has_harness_session,
        }
    }

    /// (fingerprint, lb on) for a session, read under one store lock.
    async fn toolset_state(&self, id: &str) -> Result<(String, bool)> {
        let rows = self.runtime_mcp_rows(id).await?;
        let lb = crate::browser::active(&self.paths.home).is_some()
            && rows.iter().any(|(n, _, on)| n == LB && *on);
        Ok((Self::toolset_fingerprint(&rows, lb, &Self::build_id()), lb))
    }

    /// Resolve private OAuth headers only for launch/fingerprinting, never persisted config.
    async fn runtime_mcp_rows(&self, id: &str) -> Result<Vec<(String, Value, bool)>> {
        let mut rows = self.store.lock().await.mcp_list(id)?;
        let unavailable = self.mcp_unavailable.lock().await;
        for (name, config, enabled) in &mut rows {
            if !*enabled || !Self::needs_oauth_token(config) { continue; }
            if unavailable.contains(&(id.to_string(), name.clone())) {
                *enabled = false;
                continue;
            }
            match crate::oauth::Token::load(&self.paths.mcp_oauth_token(id, name)) {
                Ok(Some(token)) => {
                    if let Some(access) = token.access_token().filter(|_| !token.expires_within(now(), 0)
                        && token.0.get("resource").is_none_or(|resource| *resource == config["url"])) {
                        if !config["headers"].is_object() { config["headers"] = json!({}); }
                        config["headers"]["Authorization"] = json!(format!("Bearer {access}"));
                    } else {
                        *enabled = false;
                    }
                }
                Ok(None) => {} // A public remote server may not need OAuth at all.
                Err(_) => *enabled = false,
            }
        }
        Ok(rows)
    }

    async fn oauth_probe(&self, config: &Value, token: &crate::oauth::Token) -> Result<u16> {
        let mut headers: Vec<(String, String)> = config["headers"].as_object().into_iter().flatten()
            .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string()))).collect();
        headers.push(("Authorization".into(), format!("Bearer {}", token.access_token().unwrap_or_default())));
        let http = self.http_client();
        tokio::time::timeout(std::time::Duration::from_secs(10), http.get_with_headers(config["url"].as_str().unwrap_or_default(), &headers))
            .await.map_err(|_| anyhow!("OAuth probe timed out"))?
            .map(|r| r.status).map_err(|_| anyhow!("OAuth probe failed"))
    }

    /// Compare again under the store lock: disable/remove or a new callback must win over
    /// an in-flight refresh, which must never resurrect or overwrite their credentials.
    async fn replace_oauth_token(&self, id: &str, name: &str, config: &Value, old: &crate::oauth::Token, new: &Value) -> Result<bool> {
        let st = self.store.lock().await;
        if !st.mcp_list(id)?.iter().any(|(n, c, on)| n == name && *on && c == config)
            || crate::oauth::Token::load(&self.paths.mcp_oauth_token(id, name))?.is_none_or(|t| t.0 != old.0) {
            return Ok(false);
        }
        self.paths.write_mcp_oauth_token(id, name, new)?;
        Ok(true)
    }

    /// Pre-turn only: at most one refresh and one retry. Auth failure never fails a turn.
    async fn prepare_mcp_oauth(&self, id: &str) -> Vec<String> {
        self.mcp_unavailable.lock().await.retain(|(session, _)| session != id);
        let rows = self.store.lock().await.mcp_list(id).unwrap_or_default();
        let mut notices = vec![];
        for (name, config, on) in rows {
            if !on || !Self::needs_oauth_token(&config) { continue; }
            let Ok(Some(token)) = crate::oauth::Token::load(&self.paths.mcp_oauth_token(id, &name)) else { continue };
            if token.access_token().is_none() { continue; }
            if token.0.get("resource").is_some_and(|resource| *resource != config["url"]) { continue; }
            if !token.expires_within(now(), 60) && !matches!(self.oauth_probe(&config, &token).await, Ok(401)) { continue; }
            let http = self.http_client();
            let result = token.refresh(&*http, config["url"].as_str().unwrap_or_default(), now()).await;
            let rotated = match result {
                Ok(rotated) => rotated,
                Err(_) => {
                    self.mcp_unavailable.lock().await.insert((id.to_string(), name.clone()));
                    notices.push(format!("MCP server {name} is temporarily unavailable: token refresh failed; retry next turn."));
                    continue;
                }
            };
            let replacement = rotated.as_ref().map(|token| token.0.clone()).unwrap_or_else(|| json!({"state": "needs-auth"}));
            match self.replace_oauth_token(id, &name, &config, &token, &replacement).await {
                Ok(false) => continue,
                Err(_) => {
                    self.mcp_unavailable.lock().await.insert((id.to_string(), name.clone()));
                    notices.push(format!("MCP server {name} is temporarily unavailable: could not save refreshed credentials."));
                    continue;
                }
                _ => {}
            }
            // Persist rotation before the probe: a crash/stream timeout must not lose the
            // new refresh token, and a concurrent disable/remove skips the retry entirely.
            let needs_auth = match rotated {
                Some(rotated) if matches!(self.oauth_probe(&config, &rotated).await, Ok(401)) => {
                    let marked = self.replace_oauth_token(id, &name, &config, &rotated, &json!({"state": "needs-auth"})).await;
                    if marked.is_err() { self.mcp_unavailable.lock().await.insert((id.to_string(), name.clone())); }
                    !matches!(marked, Ok(false))
                }
                Some(_) => false,
                None => true,
            };
            if needs_auth {
                notices.push(format!("MCP server {name} needs authorization. Ask to reconnect {name}."));
            }
        }
        notices
    }

    /// True when this server's credential is an OAuth token in the per-session token file:
    /// a remote transport that carries no static Authorization header. With no token file the
    /// honest answer is "reconnect", not "nothing to do" — a stdio server that never needed a
    /// token is the only case where `None` is the truth.
    fn needs_oauth_token(config: &Value) -> bool {
        let remote = matches!(config.get("type").and_then(Value::as_str), Some("http") | Some("sse"))
            || (config.get("url").is_some() && config.get("command").is_none());
        if !remote {
            return false;
        }
        !config
            .get("headers")
            .and_then(Value::as_object)
            .is_some_and(|m| m.keys().any(|k| k.eq_ignore_ascii_case("authorization")))
    }

    fn mcp_auth_state(&self, session_id: &str, server: &str, is_remote: bool) -> store::AuthState {
        let Ok(bytes) = std::fs::read(self.paths.mcp_oauth_token(session_id, server)) else {
            return if is_remote { store::AuthState::NeedsAuth } else { store::AuthState::None };
        };
        let Ok(v) = serde_json::from_slice::<Value>(&bytes) else {
            return store::AuthState::Failed { reason: "invalid token file".into() };
        };
        if v["state"] == "needs-auth" || v["access_token"].as_str().is_none_or(str::is_empty) {
            return store::AuthState::NeedsAuth;
        }
        let expires_at = v.get("expires_at").and_then(Value::as_i64);
        if expires_at.is_some_and(|exp| exp <= now()) {
            store::AuthState::Expired
        } else {
            store::AuthState::Authorized { expires_at }
        }
    }

    pub async fn mcp_disable_all(&self, name: &str) -> Result<usize> {
        let sessions = self.store.lock().await.mcp_disable_all_sessions(name)?;
        for s in &sessions {
            self.paths.scrub_mcp_credentials(s, name);
            self.relaunch.lock().await.insert(s.clone());
        }
        Ok(sessions.len())
    }

    async fn mcp_connect(&self, _caller: Option<&str>, req: &Value) -> Result<Value> {
        let (session_name, server_name) = match (
            req.get("session").and_then(Value::as_str),
            req.get("server").and_then(Value::as_str),
        ) {
            (Some(sess), Some(srv)) => (sess, srv),
            (Some(sess), None) => {
                let srv = req
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("missing server name"))?;
                (sess, srv)
            }
            (None, Some(srv)) => {
                let sess = req
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("missing session name"))?;
                (sess, srv)
            }
            (None, None) => bail!("missing session and server name"),
        };

        let sess_id = self.store.lock().await.resolve_req(session_name)?;
        let sess = self
            .store
            .lock()
            .await
            .get(&sess_id)?
            .ok_or_else(|| anyhow!("no session {session_name}"))?;

        let st = self.store.lock().await;
        let rows = st.mcp_list(&sess.id)?;
        let existing = rows.into_iter().find(|(n, ..)| n == server_name);
        let server_url = req
            .get("url")
            .and_then(Value::as_str)
            .or_else(|| {
                existing
                    .as_ref()
                    .and_then(|(_, cfg, _)| cfg.get("url").and_then(Value::as_str))
            })
            .ok_or_else(|| anyhow!("missing MCP server URL for {server_name}"))?
            .to_string();
        drop(st);

        let (redirect_uri, auto_callback) = if let Some(r) = req.get("redirect_uri").and_then(Value::as_str) {
            (r.to_string(), true)
        } else {
            // Loopback on purpose: a phone cannot reach this machine, so the browser
            // lands on a page that will not load and the person pastes the URL back.
            // But loopback still has to be *answerable*: preflight, never mint a dead port.
            pick_loopback_redirect(&self.paths.home).await
        };

        let http = self.http_client();
        let meta = crate::oauth::discover(&server_url, &*http).await?;

        let mut client_id = existing
            .as_ref()
            .and_then(|(_, cfg, _)| cfg.get("client_id").and_then(Value::as_str))
            .filter(|id| !id.is_empty())
            .map(String::from);

        if client_id.is_none() {
            let registered_id = crate::oauth::register_client(&meta, &redirect_uri, &*http).await?;
            let st = self.store.lock().await;
            let mut cfg = existing
                .as_ref()
                .map(|(_, c, _)| c.clone())
                .unwrap_or_else(|| json!({"type": "http", "url": server_url}));
            cfg["client_id"] = json!(&registered_id);
            st.mcp_set(&sess.id, server_name, Some(&cfg), None)?;
            drop(st);
            client_id = Some(registered_id);
        }
        let client_id = client_id.unwrap();

        let (verifier, challenge) = crate::oauth::pkce_pair();
        let state = format!("{:032x}", rand::random::<u128>());
        let auth_url = crate::oauth::authorize_url(&meta, &client_id, &redirect_uri, &challenge, &state);

        let pending = json!({
            "session": sess.id,
            "server": server_name,
            "state": state,
            "code_verifier": verifier,
            "redirect_uri": redirect_uri,
            "token_endpoint": meta.token_endpoint,
            "issuer": meta.issuer,
            "resource": server_url,
            "client_id": client_id,
            "expires_at": chrono::Utc::now().timestamp() + 600,
        });

        let pending_path = self.paths.mcp_oauth_pending(&sess.id, server_name);
        crate::telegram::write_private(&pending_path, &serde_json::to_vec_pretty(&pending)?)?;

        // Chat-ready: the agent relays this text as-is. When a loopback listener
        // answered the preflight the browser finishes on its own; otherwise say so
        // plainly instead of implying an automatic callback that cannot happen.
        let message = if auto_callback {
            format!(
                "Open this link and approve:\n{auth_url}\n\n\
                 This browser should land on a callback page that finishes the sign-in. \
                 If that page does not load, copy the whole address bar and send it back here."
            )
        } else {
            format!(
                "Open this link and approve:\n{auth_url}\n\n\
                 Auto-callback is off: no local callback listener answered, so your browser \
                 will land on a page that won't load. Copy the whole address bar and send it back here."
            )
        };

        Ok(json!({
            "authorize_url": auth_url,
            "message": message,
            "state": state,
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "auto_callback": auto_callback
        }))
    }

    /// Finish an authorize flow with what came back: the `code`/`state` query parameters
    /// directly (the loopback web route), or the redirect URL the person pasted into chat
    /// (`url`, and `text`/`paste` from a channel). The state stays single-use.
    async fn mcp_callback(&self, caller: Option<&str>, req: &Value) -> Result<Value> {
        let pasted = ["url", "redirect_url", "text", "paste"]
            .iter()
            .find_map(|k| req.get(*k).and_then(Value::as_str));
        let (code, state) = match pasted {
            Some(raw) => match crate::oauth::parse_callback_paste(raw)? {
                crate::oauth::PastedCallback::Code { code, state } => (code, state),
                crate::oauth::PastedCallback::Error { error, description } => bail!(
                    "authorization was not granted: {error}{}",
                    description.map(|d| format!(" ({d})")).unwrap_or_default()
                ),
            },
            None => (
                req.get("code")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("missing code: paste the whole redirect URL as `url`, or pass `code` and `state`"))?
                    .to_string(),
                req.get("state")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("missing state: paste the whole redirect URL as `url`, or pass `code` and `state`"))?
                    .to_string(),
            ),
        };

        let (session_arg, server_arg) = match (
            req.get("session").and_then(Value::as_str),
            req.get("server").and_then(Value::as_str),
        ) {
            (Some(sess), Some(srv)) => (Some(sess), Some(srv)),
            (Some(sess), None) => (Some(sess), req.get("name").and_then(Value::as_str)),
            (None, Some(srv)) => (req.get("name").and_then(Value::as_str), Some(srv)),
            (None, None) => (None, req.get("name").and_then(Value::as_str)),
        };

        let (pending_path, pending) = if let (Some(sess_name), Some(srv_name)) = (session_arg, server_arg) {
            let sess_id = self.store.lock().await.resolve_req(sess_name)?;
            let sess = self
                .store
                .lock()
                .await
                .get(&sess_id)?
                .ok_or_else(|| anyhow!("no session {sess_name}"))?;
            let path = self.paths.mcp_oauth_pending(&sess.id, srv_name);
            let bytes = std::fs::read(&path)
                .map_err(|_| anyhow!("no pending OAuth flow found for {srv_name}"))?;
            let val: Value = serde_json::from_slice(&bytes)?;
            (path, val)
        } else {
            // Find pending flow by state across sessions
            let mut found: Option<(PathBuf, Value)> = None;
            let sessions_dir = self.paths.home.join("sessions");
            if let Ok(entries) = std::fs::read_dir(&sessions_dir) {
                for entry in entries.flatten() {
                    let mcp_dir = entry.path().join("mcp-oauth");
                    if let Ok(files) = std::fs::read_dir(&mcp_dir) {
                        for f in files.flatten() {
                            let fname = f.file_name();
                            let fstr = fname.to_string_lossy();
                            if fstr.starts_with("pending-") && fstr.ends_with(".json") {
                                if let Ok(bytes) = std::fs::read(f.path()) {
                                    if let Ok(v) = serde_json::from_slice::<Value>(&bytes) {
                                        if v.get("state").and_then(Value::as_str) == Some(state.as_str()) {
                                            found = Some((f.path(), v));
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if found.is_some() {
                        break;
                    }
                }
            }
            found.ok_or_else(|| anyhow!("no pending OAuth flow found for state"))?
        };

        let expected_state = pending
            .get("state")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("corrupt pending flow: missing state"))?;
        if expected_state != state.as_str() {
            bail!("OAuth state mismatch");
        }

        let now = chrono::Utc::now().timestamp();
        let expires_at = pending
            .get("expires_at")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        if now > expires_at {
            let _ = std::fs::remove_file(&pending_path);
            bail!("pending OAuth flow has expired");
        }

        // An agent may finish only a flow that belongs to it or to a session it created.
        // The loopback web route has no caller and stays trusted as the host itself.
        if let Some(c) = caller {
            let owner = pending.get("session").and_then(Value::as_str).unwrap_or_default();
            let allowed = owner == c || self.store.lock().await.is_ancestor(c, owner)?;
            if !allowed {
                bail!("permission denied: this OAuth flow belongs to another session");
            }
        }

        let token_endpoint = pending
            .get("token_endpoint")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing token_endpoint in pending flow"))?;
        let code_verifier = pending
            .get("code_verifier")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing code_verifier in pending flow"))?;
        let redirect_uri = pending
            .get("redirect_uri")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing redirect_uri in pending flow"))?;
        let client_id = pending
            .get("client_id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing client_id in pending flow"))?;
        let session_id = pending
            .get("session")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing session in pending flow"))?;
        let server_name = pending
            .get("server")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing server in pending flow"))?;

        // RFC 6749 §4.1.3: the token endpoint takes application/x-www-form-urlencoded,
        // never JSON (Notion and others reject a JSON body with invalid_request).
        let exchange_form = crate::oauth::form_encode(&[
            ("grant_type", "authorization_code"),
            ("code", code.as_str()),
            ("redirect_uri", redirect_uri),
            ("client_id", client_id),
            ("code_verifier", code_verifier),
        ]);

        let http = self.http_client();
        let resp = http.post_form(token_endpoint, &exchange_form).await?;
        if resp.status != 200 && resp.status != 201 {
            // Surface the provider's own error text verbatim (error/error_description).
            bail!("token exchange failed (HTTP {}): {}", resp.status, resp.body);
        }

        let mut token_doc: Value = resp.json()?;
        if token_doc.get("access_token").and_then(Value::as_str).is_none() {
            bail!("token response missing access_token");
        }

        if token_doc.get("expires_at").is_none() {
            if let Some(dur) = token_doc.get("expires_in").and_then(Value::as_i64) {
                token_doc["expires_at"] = json!(now + dur);
            }
        }
        token_doc["client_id"] = json!(client_id);
        token_doc["token_endpoint"] = json!(token_endpoint);
        token_doc["issuer"] = pending["issuer"].clone();
        token_doc["updated_at"] = json!(now);

        {
            let st = self.store.lock().await;
            let rows = st.mcp_list(session_id)?;
            let (_, config, _) = rows.iter().find(|(name, config, on)| name == server_name && *on
                && pending.get("resource").is_none_or(|resource| *resource == config["url"]))
                .ok_or_else(|| anyhow!("MCP server changed, was disabled or removed during authorization; reconnect it"))?;
            token_doc["resource"] = config["url"].clone();
            self.paths.write_mcp_oauth_token(session_id, server_name, &token_doc)?;
        }
        let _ = std::fs::remove_file(&pending_path);

        self.relaunch.lock().await.insert(session_id.to_string());
        let _ = self
            .store
            .lock()
            .await
            .log_event("user", session_id, "mcp_authorized", server_name);
        let auth = self.mcp_auth_state(session_id, server_name, true);
        Ok(json!({
            "session": session_id,
            "server": server_name,
            "auth": auth,
            "auth_state": auth.as_str()
        }))
    }

    /// The Live Browser config, if it is set up and turned on for this session.
    async fn session_lb(&self, id: &str) -> Option<crate::browser::Config> {
        let b = crate::browser::active(&self.paths.home)?;
        let st = self.store.lock().await;
        Self::lb_on(&st, id).then_some(b)
    }

    /// For the prompt: None if Live Browser is not set up, else whether it is on for this
    /// session. Takes an already-held store guard: never lock the store again while
    /// holding it (run_turn computes this fingerprint under the same guard).
    fn browser_state(&self, st: &store::Store, id: &str) -> Option<bool> {
        crate::browser::active(&self.paths.home)?;
        Some(Self::lb_on(st, id))
    }

    /// Give one newly created lead its browser on a real home, so the family never types an
    /// MCP command. Workers keep the default (off); nothing happens for other roles.
    async fn default_lead_lb(&self, id: &str, role: &str) -> Result<()> {
        if role != "lead" || !lead_gets_lb_by_default(&self.paths.home) {
            return Ok(());
        }
        let st = self.store.lock().await;
        if st.mcp_switched_off(id, LB)? {
            return Ok(()); // the user's own off stays off
        }
        if st.mcp_set(id, LB, Some(&json!(LB_ROW_CONFIG)), Some(true))? {
            st.log_event("kurisu", id, "mcp_enabled", LB)?;
        }
        Ok(())
    }

    /// Turn Live Browser on or off for a session (a normal per-session MCP row named lb,
    /// whose launch config Kurisu builds). Returns whether anything changed.
    async fn set_session_lb(&self, actor: &str, id: &str, on: bool) -> Result<bool> {
        if on && crate::browser::active(&self.paths.home).is_none() {
            bail!("Live Browser is not set up on this machine (ku browser setup)");
        }
        let st = self.store.lock().await;
        let changed = st.mcp_set(id, LB, Some(&json!(LB_ROW_CONFIG)), Some(on))?;
        if changed {
            st.log_event(actor, id, if on { "mcp_enabled" } else { "mcp_disabled" }, LB)?;
            drop(st);
            self.relaunch.lock().await.insert(id.to_string());
        }
        Ok(changed)
    }

    /// Claude launch options for one session: model, peer tools, no sub-agents.
    async fn claude_opts(&self, s: &store::Session) -> Result<SessionOpts> {
        let dir = self.paths.session_dir(&s.id);
        std::fs::create_dir_all(&dir)?;
        let me = std::env::current_exe()?.to_string_lossy().into_owned();
        let mut mcp = json!({"mcpServers": {crate::mcp::SERVER_NAME: {
            "type": "stdio", "command": me, "args": ["mcp"],
            "env": {"KURISU_HOME": self.paths.home, "KURISU_SESSION": s.id},
            // Always in the prompt with the live schemas, never deferred behind ToolSearch:
            // a schema fetched in an earlier turn would otherwise outlive an upgrade.
            "alwaysLoad": true
        }}});
        // Live Browser add-on: only for sessions that have lb turned on (pre-approved below).
        let lb = self.session_lb(&s.id).await;
        if let Some(b) = &lb {
            mcp["mcpServers"]["lb"] = b.mcp_entry(&s.name);
        }
        // The session's own MCP servers (stored in Kurisu; the user's ~/.claude.json is never used).
        for (name, config, enabled) in self.runtime_mcp_rows(&s.id).await? {
            if enabled && name != crate::mcp::SERVER_NAME && name != LB {
                mcp["mcpServers"][name] = config;
            }
        }
        let mcp_path = dir.join("mcp.json");
        // Private: it can hold the lb passcode and the session's MCP secrets.
        crate::telegram::write_private(&mcp_path, &serde_json::to_vec_pretty(&mcp)?)?;
        let prompt_path = dir.join("system-prompt.md");
        let browser = { let st = self.store.lock().await; self.browser_state(&st, &s.id) };
        std::fs::write(&prompt_path, session_prompt(s, browser))?;
        let p = |x: &Path| x.to_string_lossy().into_owned();
        let mut effort = vec![];
        if let Some(e) = &s.effort {
            effort = vec!["--effort".to_string(), e.clone()];
        }
        Ok(SessionOpts {
            args: [effort, vec![
                "--model".into(), s.model.clone(),
                // Explicit, so the user's own default is never inherited.
                "--permission-mode".into(), claude_mode_arg(&effective_mode(s.permission_mode.as_deref(), &claude_modes()), &claude_modes()),
                "--mcp-config".into(), p(&mcp_path), "--strict-mcp-config".into(),
                "--allowedTools".into(), if lb.is_some() { format!("mcp__{},mcp__lb", crate::mcp::SERVER_NAME) } else { format!("mcp__{}", crate::mcp::SERVER_NAME) },
                "--disallowedTools".into(), "Task,Agent".into(),
                "--append-system-prompt-file".into(), p(&prompt_path),
            ]]
            .concat(),
            env: lb.as_ref().map(|b| b.shell_env()).unwrap_or_default(),
        })
    }

    /// thread/start extras for a Codex session: the kurisu-mcp server (as a config
    /// override; the user's ~/.codex/config.toml is not edited) and Kurisu's prompt.
    async fn codex_extra(&self, s: &store::Session) -> Result<Value> {
        let me = std::env::current_exe()?.to_string_lossy().into_owned();
        // A Claude model id (the create default) means "Codex's own default model".
        let model = (!s.model.starts_with("claude-") && s.model != "default").then(|| s.model.clone());
        let mut config = json!({format!("mcp_servers.{}", crate::mcp::SERVER_NAME): {
            "command": me, "args": ["mcp"],
            "env": {"KURISU_HOME": self.paths.home, "KURISU_SESSION": s.id},
            // Kurisu's own tools run without a prompt, as for Claude (--allowedTools mcp__kurisu-mcp).
            "default_tools_approval_mode": "approve",
            // ask_user blocks up to ask_wait(), longer than Codex's 60 s default.
            "tool_timeout_sec": ask_wait().as_secs() + 60
        }});
        let lb = self.session_lb(&s.id).await;
        if let Some(b) = &lb {
            config["mcp_servers.lb"] = json!({"command": b.lb, "args": ["mcp"],
                "env": b.mcp_env(&s.name), "default_tools_approval_mode": "approve"});
        }
        for (name, mut cfg, enabled) in self.runtime_mcp_rows(&s.id).await? {
            if enabled && name != crate::mcp::SERVER_NAME && name != LB {
                if cfg["url"].is_string() {
                    // Codex names the remote header map http_headers (Claude/ACP use headers).
                    if let Some(headers) = cfg.as_object_mut().and_then(|c| c.remove("headers")) {
                        cfg["http_headers"] = headers;
                    }
                }
                config[format!("mcp_servers.{name}")] = cfg;
            }
        }
        // A narrow sandbox still gets the network a Claude session has (see CodexConfig).
        if let Some(extra) = self.codex_cfg.config_overrides().as_object() {
            for (k, v) in extra {
                config[k] = v.clone();
            }
        }
        if let Some(e) = s.effort.as_deref().filter(|e| store::effort_levels("codex").contains(e)) {
            config["model_reasoning_effort"] = json!(e);
        }
        let browser = { let st = self.store.lock().await; self.browser_state(&st, &s.id) };
        let mut extra = json!({
            "model": model,
            "config": config,
            "developerInstructions": session_prompt(s, browser),
        });
        // The session's permission mode, as Codex's approval policy + sandbox. Without a
        // mode set, KURISU_CODEX_SANDBOX (the driver's default) still applies.
        let mode = s.permission_mode.clone().or_else(|| std::env::var("KURISU_CODEX_SANDBOX").is_err().then(|| "auto".to_string()));
        if let Some(m) = mode {
            let (policy, sandbox) = codex_mode(&m);
            extra["approvalPolicy"] = json!(policy);
            extra["sandbox"] = json!(sandbox);
        }
        Ok(extra)
    }

    /// What a session's CLI was given: kurisu-mcp's tool list, the session prompt and
    /// the ku binary itself (size and mtime). A change means its CLI is out of date.
    fn fingerprint(&self, s: &store::Session, with_binary: bool, browser: Option<bool>) -> String {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        crate::mcp::tools().to_string().hash(&mut h);
        session_prompt(s, browser).hash(&mut h);
        if with_binary {
            // A new permission mode (set, or a new default) needs a relaunch too.
            effective_mode(s.permission_mode.as_deref(), &claude_modes()).hash(&mut h);
        }
        if let (true, Ok(m)) = (with_binary, std::env::current_exe().and_then(std::fs::metadata)) {
            m.len().hash(&mut h);
            m.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_nanos()).hash(&mut h);
        }
        format!("{:016x}", h.finish())
    }

    /// Claude Code freezes the schema of every tool the model loaded through ToolSearch into
    /// the transcript (`deferred_tools_record`) and replays it on resume, so a session that
    /// loaded kurisu-mcp tools before an upgrade keeps their old schemas. True if any frozen
    /// kurisu-mcp schema differs from the current one.
    async fn frozen_tools_stale(&self, hid: &str) -> bool {
        let Some(path) = self.claude.transcript_path(&SessionId::new(hid.to_string())).await else { return false };
        let Ok(text) = tokio::fs::read_to_string(&path).await else { return false };
        let prefix = format!("mcp__{}__", crate::mcp::SERVER_NAME);
        let current: HashMap<String, Value> = crate::mcp::tools().as_array().cloned().unwrap_or_default().into_iter()
            .filter_map(|t| Some((t["name"].as_str()?.to_string(), t))).collect();
        for line in text.lines().filter(|l| l.contains("deferred_tools_record") && l.contains(&prefix)) {
            let Ok(v) = serde_json::from_str::<Value>(line) else { continue };
            for e in v.pointer("/attachment/entries").and_then(Value::as_array).into_iter().flatten() {
                let Some(short) = e["name"].as_str().and_then(|n| n.strip_prefix(&prefix)) else { continue };
                match current.get(short) {
                    Some(t) if t["description"] == e["description"] && t["inputSchema"] == e["input_schema"] => {}
                    _ => return true,
                }
            }
        }
        false
    }

    async fn ensure_live(&self, name: &str, actor: &str) -> Result<SessionId> {
        let s = self.store.lock().await.get(name)?.ok_or_else(|| anyhow!("no session {name}"))?;
        if s.lifecycle != "active" {
            bail!("session {name} is {}", s.lifecycle);
        }
        // A missing CLI fails at once with a fix, instead of a 60 s startup timeout.
        if !self.live.lock().await.contains(name) {
            Self::require_cli(&s.harness)?;
            // Live Browser add-on: its lb MCP needs lb web up (only when this session uses it).
            if let Some(b) = self.session_lb(&s.id).await {
                if let Err(e) = self.browser.ensure(&self.paths.home, &b).await {
                    eprintln!("browser: {e:#}");
                }
            }
        }
        if matches!(s.harness.as_str(), "opencode" | "agy" | "fx" | "pi") { self.opencode_for(&s.id).await?; }
        // The toolset is fixed when a harness session is created. OpenCode's ACP has no
        // hot-add: `session/load` brings back the ACP session (and its toolset) as it was,
        // and only `session/new` reads the current mcpServers. So when the stored toolset
        // or the Kurisu build changed, start a new ACP session and seed it with a note to
        // catch up from Kurisu's history (the harness-switch path). Never re-send servers
        // on session/load and hope. The fingerprint is persisted so a Kurisu restart does
        // not hide that an existing ACP session still holds the old tools.
        let (toolset_fp, _) = self.toolset_state(&s.id).await?;
        let persisted = self.store.lock().await.launch_fp(&s.id)?;
        let stale_tools = Self::toolset_is_stale(persisted.as_deref(), &toolset_fp, s.harness_session_id.is_some());
        let running = { let l = self.live.lock().await; l.contains(name) || l.contains(&s.name) || l.contains(&s.id) };
        if stale_tools && matches!(s.harness.as_str(), "opencode" | "agy" | "fx" | "pi") {
            self.close_driver(&s).await;
            let mut l = self.live.lock().await;
            l.remove(name);
            l.remove(&s.name);
            l.remove(&s.id);
            drop(l);
            self.relaunch.lock().await.remove(name);
            let new = self.opencode_session(&s, None).await?;
            {
                let st = self.store.lock().await;
                st.rebind_harness_id(name, &new.to_string(),
                    "its toolset changed (Kurisu build or MCP servers): new ACP session")?;
                st.set_launch_fp(&s.id, &toolset_fp)?;
                st.set_status(name, "idle")?;
                st.log_event("kurisu", name, "refreshed",
                    "its toolset changed: started a new ACP session with the current tools")?;
            }
            self.fresh_start.lock().await.insert(name.to_string());
            self.live.lock().await.insert(name.to_string());
            return Ok(new);
        }
        if stale_tools && running {
            // Other harnesses re-read their MCP config when the CLI process starts: close it
            // so the resume below starts a process with the new server set.
            self.close_driver(&s).await;
            let mut l = self.live.lock().await;
            l.remove(name);
            l.remove(&s.name);
            l.remove(&s.id);
            drop(l);
            self.relaunch.lock().await.remove(name);
            self.store.lock().await.log_event("kurisu", name, "refreshed",
                "its MCP servers changed: restarting its CLI with the new toolset")?;
        }
        // Kurisu was upgraded (or the session's prompt changed) while this CLI ran: relaunch it.
        let browser = { let st = self.store.lock().await; self.browser_state(&st, &s.id) };
        let fp = self.fingerprint(&s, true, browser);
        if self.live.lock().await.contains(name) && self.launched_fp.lock().await.get(name).is_some_and(|f| *f != fp) {
            self.relaunch.lock().await.insert(name.to_string());
            self.store.lock().await.log_event("kurisu", name, "refreshed", "Kurisu changed since its CLI started: relaunching")?;
        }
        self.launched_fp.lock().await.insert(name.to_string(), fp);
        if self.relaunch.lock().await.remove(name) && self.live.lock().await.remove(name) {
            // Model/effort/name changed: close now, resume below with the new launch options.
            self.close_driver(&s).await;
        }
        let cwd = PathBuf::from(&s.cwd);
        std::fs::create_dir_all(&cwd).ok();
        ensure_agents_md(&self.paths.home, &cwd);
        if s.harness == "claude" {
            ensure_claude_md(&cwd);
        }
        if let Some(id) = &s.harness_session_id {
            let id = SessionId::new(id.clone());
            if !self.live.lock().await.contains(name) {
                // The previous CLI run's cost restarts at 0 in the new run: keep it in the session's total.
                if let Some(sl) = self.status_line(&s).await {
                    if let Some(c) = sl.raw["cost"].get("total_cost_usd").and_then(Value::as_f64) {
                        self.store.lock().await.cost_fold(name, c, sl.captured_at as i64)?;
                    }
                }
                match s.harness.as_str() {
                    "claude" if self.frozen_tools_stale(&id.to_string()).await => {
                        // Resuming would bring back pre-upgrade tool schemas: start a fresh
                        // conversation; the next prompt points it at its own Kurisu history.
                        let opts = self.claude_opts(&s).await?;
                        let new = self.claude.new_session_with(NewSessionRequest::new(cwd), &opts).await?.session_id;
                        self.store.lock().await.rebind_harness_id(name, &new.to_string(),
                            "its transcript held kurisu-mcp tool schemas from before an upgrade")?;
                        self.fresh_start.lock().await.insert(name.to_string());
                        self.store.lock().await.set_launch_fp(&s.id, &toolset_fp)?;
                        self.live.lock().await.insert(name.into());
                        self.store.lock().await.set_status(name, "idle")?;
                        return Ok(new);
                    }
                    "claude" => {
                        let opts = self.claude_opts(&s).await?;
                        self.claude.load_session_with(LoadSessionRequest::new(id.clone(), cwd), &opts).await?
                    }
                    "opencode" | "agy" | "fx" | "pi" => { self.opencode_session(&s, Some(&id)).await?; },
                    "codex" => self.codex().await?.load_session_with(&id, cwd, &self.codex_extra(&s).await?).await?,
                    h => bail!("harness {h} not implemented"),
                }
                self.store.lock().await.set_launch_fp(&s.id, &toolset_fp)?;
                self.live.lock().await.insert(name.into());
                let st = self.store.lock().await;
                st.set_status(name, "idle")?;
                st.log_event(actor, name, "resumed", &format!("{} {}", s.model, s.effort.clone().unwrap_or_default()))?;
            }
            return Ok(id);
        }
        let id = match s.harness.as_str() {
            "claude" => {
                let opts = self.claude_opts(&s).await?;
                self.claude.new_session_with(NewSessionRequest::new(cwd), &opts).await?.session_id
            }
            "opencode" | "agy" | "fx" | "pi" => self.opencode_session(&s, None).await?,
            "codex" => self.codex().await?.new_session_with(cwd, &self.codex_extra(&s).await?).await?,
            h => bail!("harness {h} not implemented"),
        };
        let st = self.store.lock().await;
        st.bind_harness_id(name, &id.to_string())?;
        st.set_launch_fp(&s.id, &toolset_fp)?;
        st.set_status(name, "idle")?;
        st.log_event(actor, name, "started", &s.model)?;
        drop(st);
        self.live.lock().await.insert(name.into());
        Ok(id)
    }

    async fn busy_lock(&self, name: &str) -> Arc<Mutex<()>> {
        self.busy.lock().await.entry(name.into()).or_default().clone()
    }

    async fn turn_gate(&self, name: &str) -> Arc<Mutex<()>> {
        self.turn_gates.lock().await.entry(name.into()).or_default().clone()
    }

    async fn delivery(&self, name: &str, seq: i64, state: &str) -> Result<()> {
        self.store.lock().await.merge_message_data(seq, &json!({"delivery": state}))?;
        self.publish(json!({"type": "delivery", "session": name, "seq": seq, "state": state}));
        Ok(())
    }

    /// Is a turn running in this session right now?
    async fn is_busy(&self, name: &str) -> bool {
        self.busy_lock(name).await.try_lock().is_err()
    }

    /// Record an incoming message (observers see it at once); returns its seq.
    async fn record(&self, name: &str, from: &str, text: &str) -> Result<i64> {
        let st = self.store.lock().await;
        let s = st.get(name)?.ok_or_else(|| anyhow!("no session {name}"))?;
        if s.lifecycle != "active" {
            bail!("session {} is {}", s.name, s.lifecycle);
        }
        st.append_data(name, from, text, &json!({"delivery": "queued"}))
    }

    /// Send and wait for the reply.
    async fn send(&self, name: &str, text: &str, from: &str, actor: &str) -> Result<Value> {
        let seq = self.record(name, from, text).await?;
        self.run_turn(name, text, actor, seq).await
    }

    /// Send without waiting (`wait:false`) or into the running turn (`mode:"steer"`).
    async fn send_async(self: &Arc<Self>, name: &str, text: &str, from: &str, actor: &str, steer: bool) -> Result<Value> {
        self.send_async_notify(name, text, from, actor, steer, None).await
    }

    /// send_async; with `notify` (a session id) that session gets a callback message when
    /// the turn ends (done, error, stopped, or interrupted by a restart). Not for a
    /// message steered into a running turn.
    async fn send_async_notify(self: &Arc<Self>, name: &str, text: &str, from: &str, actor: &str, steer: bool,
                               notify: Option<&str>) -> Result<Value> {
        self.send_async_channel(name, text, from, actor, steer, notify, None).await
    }

    async fn send_async_channel(self: &Arc<Self>, name: &str, text: &str, from: &str, actor: &str, steer: bool,
                               notify: Option<&str>, event: Option<&str>) -> Result<Value> {
        let busy = self.is_busy(name).await;
        let seq = if let Some(key) = event {
            let mut posted = self.channel_posts.lock().await;
            let st=self.store.lock().await;
            let s=st.get(name)?.ok_or_else(||anyhow!("no session {name}"))?;
            if s.lifecycle != "active" { bail!("session {} is {}",s.name,s.lifecycle); }
            let (seq,fresh,delivery)=st.append_channel_once(key,name,from,text)?;
            if posted.contains(key) || (!fresh && delivery != "queued") {
                return Ok(json!({"id":name,"seq":seq,"duplicate":true,"queued":busy}));
            }
            posted.insert(key.to_string()); seq
        } else { self.record(name, from, text).await? };
        let display = self.store.lock().await.get(name)?.map(|x| x.name).unwrap_or_default();
        if steer && busy {
            let gate = self.turn_gate(name).await;
            let _steer = gate.lock().await;
            let s = self.store.lock().await.get(name)?.ok_or_else(|| anyhow!("no session {name}"))?;
            if let (Some(hid), "claude", true) = (&s.harness_session_id, s.harness.as_str(), self.live.lock().await.contains(name)) {
                match self.claude.steer(&SessionId::new(hid.clone()), text).await {
                    Ok(()) => {
                        self.store.lock().await.log_event(actor, name, "steered", &format!("message {seq}"))?;
                        self.delivery(name, seq, "steered").await?;
                        return Ok(json!({"id": name, "name": display, "seq": seq, "queued": false, "steered": true}));
                    }
                    Err(e) => eprintln!("steer into {name} failed, queueing instead: {e:#}"),
                }
            }
        }
        let cb = match notify {
            Some(sender) => Some(self.store.lock().await.callback_add(name, sender, seq)?),
            None => None,
        };
        self.spawn_turn(name, text, actor, seq, cb.into_iter().collect(), None);
        Ok(json!({"id": name, "name": display, "seq": seq, "queued": busy, "steered": false, "callback": cb.is_some()}))
    }

    /// Run a recorded message's turn in the background, then deliver its callbacks.
    fn spawn_turn(self: &Arc<Self>, name: &str, text: &str, actor: &str, seq: i64, callbacks: Vec<i64>, note: Option<String>) {
        if !self.turns_enabled.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        let (me, name2, text2, actor2) = (self.clone(), name.to_string(), text.to_string(), actor.to_string());
        tokio::spawn(async move {
            let res = me.run_turn(&name2, &text2, &actor2, seq).await;
            if let Err(e) = &res {
                eprintln!("background turn in {name2} failed: {e:#}");
            }
            let (outcome, detail) = turn_outcome(&res);
            let outcome = match &note { Some(n) => format!("{n}, then {outcome}"), None => outcome.to_string() };
            for id in callbacks {
                if let Err(e) = me.deliver_callback(id, &outcome, &detail).await {
                    eprintln!("callback #{id} from {name2} failed: {e:#}");
                }
            }
        });
    }

    /// Tell a callback's sender how the turn ended (queued if the sender is busy). A
    /// callback never asks for a callback, so they cannot loop.
    async fn deliver_callback(self: &Arc<Self>, id: i64, outcome: &str, detail: &str) -> Result<()> {
        let (c, worker, sender_ok) = {
            let st = self.store.lock().await;
            let Some(c) = st.callback_take(id)? else { return Ok(()) };
            let worker = st.get(&c.target)?.map(|x| x.name).unwrap_or_else(|| c.target.clone());
            let ok = st.get(&c.sender)?.is_some_and(|x| x.lifecycle == "active");
            (c, worker, ok)
        };
        if !sender_ok {
            self.store.lock().await.log_event("kurisu", &c.sender, "callback_dropped", &format!("from {worker}: the sender is not active"))?;
            return Ok(());
        }
        let text = callback_text(&worker, outcome, c.seq, detail);
        self.send_async(&c.sender, &text, &format!("peer:{worker}"), &c.target, false).await?;
        Ok(())
    }

    /// Remember who a turn answers, for a later follow-up turn nobody sent. Wakes,
    /// callbacks and follow-ups themselves keep the previous requester.
    async fn note_requester(&self, name: &str, text: &str, actor: &str, seq: i64) {
        if actor == "kurisu" || actor == SELF_TURN_ACTOR || text.starts_with("[Kurisu callback]") {
            return;
        }
        let peer = match self.store.lock().await.get(actor) {
            Ok(Some(s)) if s.id != name => Some(s.id),
            _ => None,
        };
        self.requesters.lock().await.insert(name.to_string(), (peer, seq));
    }

    /// Start a follow-up turn nobody sent (`text` already recorded as `seq`), and
    /// report its reply to the original requester as a callback.
    async fn spawn_followup(self: &Arc<Self>, name: &str, text: &str, actor: &str, seq: i64,
                            requester: Option<(String, i64)>, why: &str) -> Result<()> {
        let mut cbs = vec![];
        if let Some((sender, orig)) = requester {
            if sender != name {
                cbs.push(self.store.lock().await.callback_add(name, &sender, orig)?);
            }
        }
        self.store.lock().await.log_event("kurisu", name, "followup", why)?;
        self.spawn_turn(name, text, actor, seq, cbs, Some(format!("{FOLLOWUP}: {why}")));
        Ok(())
    }

    /// `run_background`: start a detached shell job for a session; it is woken
    /// (and its requester told) when the job exits.
    async fn bg_run(&self, name: &str, command: &str, cwd: Option<&str>) -> Result<Value> {
        let s = self.store.lock().await.get(name)?.ok_or_else(|| anyhow!("no session {name}"))?;
        let folder = crate::jobs::resolve_cwd(Path::new(&s.cwd), cwd)?;
        let (requester, seq) = match self.requesters.lock().await.get(&s.id).cloned() {
            Some((r, seq)) => (r, seq),
            None => (None, self.active_turns.lock().await.get(&s.id).copied().unwrap_or(0)),
        };
        let at = now();
        let id = format!("{}-{}", chrono::Local::now().format("%H%M%S"), uuid::Uuid::new_v4().simple().to_string().get(..6).unwrap_or("0"));
        let dir = self.paths.session_dir(&s.id);
        let (cmd, f2, id2, sid) = (command.to_string(), folder.clone(), id.clone(), s.id.clone());
        let job = tokio::task::spawn_blocking(move || crate::jobs::start(&dir, &id2, &sid, &cmd, &f2, requester, seq, at)).await??;
        let log = crate::jobs::log_path(&crate::jobs::jobs_dir(&self.paths.session_dir(&s.id)).join(&job.id));
        self.store.lock().await.log_event(name, &s.id, "bg_job_started", &format!("job {} (pid {}): {}", job.id, job.pid,
            command.chars().take(200).collect::<String>()))?;
        Ok(json!({"ok": true, "job": job.id, "pid": job.pid, "folder": folder, "log": log,
                  "note": "Running. End your turn now (say what you are waiting for); Kurisu wakes you with the exit code and \
output tail when it exits, and your reply then goes to whoever sent the message you were working on."}))
    }

    /// Ticker: wake sessions whose background jobs ended, and follow turns a
    /// Claude CLI started on its own. Never blocks on a running turn.
    async fn watch_followups(self: &Arc<Self>) -> Result<()> {
        let root = self.paths.home.join("sessions");
        let done = tokio::task::spawn_blocking(move || crate::jobs::finished(&root, now())).await?;
        for f in done {
            if !crate::jobs::claim(&f.dir) {
                continue;
            }
            let target = f.job.session.clone();
            let text = crate::jobs::wake_text(&f);
            let why = crate::jobs::outcome(&f);
            let seq = match self.record(&target, "peer:kurisu", &text).await {
                Ok(seq) => seq,
                Err(e) => {
                    eprintln!("background job {} of {target}: not delivered: {e:#}", f.job.id);
                    continue;
                }
            };
            let requester = f.job.requester.clone().map(|r| (r, f.job.request_seq));
            self.spawn_followup(&target, &text, "kurisu", seq, requester, &why).await?;
        }
        // Claude Code resumes by itself when a `run_in_background` command ends.
        let live: Vec<String> = self.live.lock().await.iter().cloned().collect();
        for name in live {
            let Some(s) = self.store.lock().await.get(&name)? else { continue };
            let (Some(hid), "claude", "active") = (s.harness_session_id.clone(), s.harness.as_str(), s.lifecycle.as_str()) else { continue };
            if self.self_turns.lock().await.contains(&s.id) || self.is_busy(&s.id).await {
                continue;
            }
            if !self.claude.self_turn_pending(&SessionId::new(hid)).await {
                continue;
            }
            self.self_turns.lock().await.insert(s.id.clone());
            let seq = self.record(&s.id, "peer:kurisu", SELF_TURN_TEXT).await?;
            let requester = self.requesters.lock().await.get(&s.id).cloned().and_then(|(r, seq)| r.map(|r| (r, seq)));
            let (me, id) = (self.clone(), s.id.clone());
            // The marker is cleared when the turn ends (spawn_turn runs it in the background).
            self.spawn_followup(&s.id, SELF_TURN_TEXT, SELF_TURN_ACTOR, seq, requester,
                                "the CLI resumed on its own after a background command finished").await?;
            tokio::spawn(async move {
                // Wait for the queued turn to start and end, then allow the next detection.
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                let lock = me.busy_lock(&id).await;
                let _g = lock.lock().await;
                me.self_turns.lock().await.remove(&id);
            });
        }
        Ok(())
    }

    /// At startup: resume sessions whose turn the previous server cut off (and sessions
    /// with undelivered callbacks), once. A session interrupted again during its resume
    /// turn is not resumed a second time; its senders get an "interrupted" callback.
    /// KURISU_NO_RESUME=1 turns resuming off (senders are still told).
    pub async fn resume_interrupted(self: &Arc<Self>) -> Result<()> {
        let interrupted = std::mem::take(&mut *self.interrupted.lock().await);
        let callbacks = self.store.lock().await.callbacks()?;
        let mut targets = interrupted.clone();
        for c in &callbacks {
            if !targets.contains(&c.target) {
                targets.push(c.target.clone());
            }
        }
        let resume_on = std::env::var("KURISU_NO_RESUME").map_or(true, |v| v.is_empty() || v == "0");
        for t in targets {
            let rows: Vec<i64> = callbacks.iter().filter(|c| c.target == t).map(|c| c.id).collect();
            let (active, looped) = {
                let st = self.store.lock().await;
                let active = st.get(&t)?.is_some_and(|x| x.lifecycle == "active");
                (active, st.last_inbound(&t)?.is_some_and(|m| m.starts_with(RESUME_TAG)))
            };
            if !active || looped || !resume_on {
                let why = if !active { "the session is no longer active" } else if looped { "it was interrupted again while resuming" }
                          else { "resuming is off (KURISU_NO_RESUME)" };
                for id in rows {
                    self.deliver_callback(id, "interrupted by a Kurisu restart", &format!("not resumed: {why}")).await?;
                }
                self.store.lock().await.log_event("kurisu", &t, "resume_skipped", why)?;
                continue;
            }
            let text = format!("{RESUME_TAG} Kurisu restarted (e.g. a deploy) while you were working, so your last turn \
was cut off{}. Check what was already done (your folder, git, session_history with your own id) before you continue, \
and do not repeat steps that completed (deploys, merges, messages sent). Then finish the task and reply.",
                               if interrupted.contains(&t) { "" } else { " or messages to you were not answered yet" });
            let seq = self.record(&t, "peer:kurisu", &text).await?;
            self.store.lock().await.log_event("kurisu", &t, "resumed_after_restart", &format!("{} callback(s) pending", rows.len()))?;
            self.spawn_turn(&t, &text, "kurisu", seq, rows, Some("interrupted by a Kurisu restart and resumed".into()));
        }
        Ok(())
    }

    /// Run one turn for an already recorded message (waits for any running turn first).
    async fn run_turn(&self, name: &str, text: &str, actor: &str, seq: i64) -> Result<Value> {
        // A standby (deploy candidate) or a promoted-but-not-reconciled server must not
        // start a second turn against the same session: wait until promotion is done.
        while !self.ready.load(std::sync::atomic::Ordering::SeqCst) {
            let n = self.ready_notify.notified();
            if self.ready.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            n.await;
        }
        let lock = self.busy_lock(name).await;
        let _turn = lock.lock().await;
        let gate = self.turn_gate(name).await;
        let start = gate.lock().await;
        self.active_turns.lock().await.insert(name.into(), seq);
        drop(start);
        self.note_requester(name, text, actor, seq).await;
        self.undo_snapshot(name, seq).await;
        let result = self.run_turn_locked(name, text, actor, seq).await;
        self.active_turns.lock().await.remove(name);
        result
    }

    async fn run_turn_locked(&self, name: &str, text: &str, actor: &str, seq: i64) -> Result<Value> {
        self.delivery(name, seq, "delivered").await?;
        let (harness, previous_failure) = {
            let st = self.store.lock().await;
            let s = st.get(name)?.ok_or_else(|| anyhow!("no session {name}"))?;
            if s.lifecycle != "active" {
                st.append(name, "error", &format!("not delivered: session {} is {}", s.name, s.lifecycle))?;
                bail!("session {} is {}", s.name, s.lifecycle);
            }
            // A failed turn leaves its message in the CLI's context; the next prompt says so.
            let previous_failure = st
                .last_outcome(name)?
                .filter(|(role, text)| role == "error" && (text.starts_with("turn failed") || text.starts_with("turn stopped")))
                .map(|(_, text)| text.trim_start_matches("turn failed: ").trim_start_matches("turn stopped: ").to_string());
            st.set_status(name, "starting")?;
            (s.harness, previous_failure)
        };
        let sid = self.store.lock().await.get(name)?.map(|s| s.id).unwrap_or_else(|| name.to_string());
        let self_turn = actor == SELF_TURN_ACTOR;
        let mcp_notices = if self_turn { vec![] } else { self.prepare_mcp_oauth(&sid).await };
        let previous_failure = previous_failure.filter(|_| !self_turn);
        let notice = previous_failure.as_ref().map(|e| {
            let e: String = e.chars().take(160).collect();
            format!("the previous message to this session failed ({e}) and was not answered; this reply answers only the new message")
        });
        let mut prompt_text = match &previous_failure {
            Some(e) => format!("{}{text}", failed_turn_preamble(e)),
            None => text.to_string(),
        };
        if !mcp_notices.is_empty() {
            prompt_text = format!("[Kurisu MCP: {}]\n\n{prompt_text}", mcp_notices.join(" "));
        }
        // A note queued for this turn (/undo of the previous one).
        let note = if self_turn { None } else { self.store.lock().await.take_pending_note(&sid)? };
        if let Some(note) = note {
            prompt_text = format!("{note}\n\n{prompt_text}");
        }
        // After an upgrade the conversation may still hold old tool schemas or old answers
        // about what the tools can do: say so once, on the first turn with the new tools.
        {
            let st = self.store.lock().await;
            let s = st.get(name)?.ok_or_else(|| anyhow!("no session {name}"))?;
            let browser = self.browser_state(&st, &s.id);
            let fp = self.fingerprint(&s, false, browser);
            let changed = match st.seen_fp(name)? {
                Some(old) => old != fp,
                // Sessions from before fingerprints: tell them once if they already have history.
                None => st.reply_count(name)? > 0,
            };
            if changed && !self_turn {
                prompt_text = format!("[Kurisu note: Kurisu was updated since your last turn. Your kurisu-mcp tools and instructions \
may have changed: rely on the tool definitions you have now, not on earlier results or earlier answers about what the tools \
cannot do.]\n\n{prompt_text}");
                st.log_event("kurisu", name, "tools_updated", "told the session its tools changed")?;
            }
            if !self_turn {
                st.set_seen_fp(name, &fp)?;
            }
        }
        let text = prompt_text.as_str();
        let id = match self.ensure_live(name, actor).await {
            Ok(id) => id,
            Err(e) => {
                let st = self.store.lock().await;
                st.append(name, "error", &format!("could not start the session: {e:#}"))?;
                st.set_status(name, "error")?;
                st.log_event(actor, name, "failed", &format!("{e:#}"))?;
                self.publish(json!({"type": "turn", "session": sid, "status": "error"}));
                return Err(e);
            }
        };
        let fresh_note;
        let text = if !self_turn && self.fresh_start.lock().await.remove(name) {
            fresh_note = format!("[Kurisu note: Kurisu restarted your CLI conversation so it has your current tools (the old one \
held an outdated toolset). Your earlier conversation is in Kurisu's history: first call the kurisu-mcp tool \
session_handoff with id \"{name}\" (your own session) to catch up, then handle the message below. Your tools are current.]\n\n{text}");
            fresh_note.as_str()
        } else {
            text
        };
        self.store.lock().await.set_status(name, "working")?;
        self.publish(json!({"type": "turn", "session": sid, "status": "working"}));
        let (tx, mut rx) = mpsc::channel::<agent_client_protocol::v1::schema::SessionNotification>(256);
        // Collect the turn's updates; tool calls also update the session's live activity.
        let collect = async {
            let mut all: Vec<agent_client_protocol::v1::schema::SessionNotification> = vec![];
            // Tool calls go to the history as `tool` rows: tool call id -> (row, data).
            let mut tool_rows: HashMap<String, (i64, Value)> = HashMap::new();
            while let Some(n) = rx.recv().await {
                use agent_client_protocol::v1::schema::SessionUpdate;
                match &n.update {
                    SessionUpdate::AgentMessageChunk(c) => {
                        if let ContentBlock::Text(t) = &c.content {
                            let mut ev = json!({"type": "delta", "session": sid, "kind": "text", "text": t.text});
                            // A change of message id marks a new assistant message.
                            // Forward it so clients can keep messages separate even
                            // though the finished history row concatenates them.
                            if let Some(mid) = &c.message_id {
                                ev["message_id"] = json!(&*mid.0);
                            }
                            self.publish(ev);
                        }
                    }
                    SessionUpdate::AgentThoughtChunk(c) => {
                        if let ContentBlock::Text(t) = &c.content { self.publish(json!({"type":"delta","session":sid,"kind":"reasoning","text":t.text})); }
                    }
                    SessionUpdate::ToolCall(t) => {
                        let tool = t.name.clone().unwrap_or_else(|| t.title.clone());
                        // ACP titles are often generic ("shell"); prefer the real command
                        // or path from the tool input everywhere this call is shown.
                        let title = crate::activity::title(&tool, &t.title, t.raw_input.as_ref());
                        self.activity.lock().await.insert(name.to_string(),
                            json!({"tool": tool, "title": title, "state": "running", "at": now()}));
                        let data = json!({"tool_call_id": t.tool_call_id.to_string(), "tool": tool, "status": "running",
                                          "input": clip_json(t.raw_input.as_ref().unwrap_or(&Value::Null), TOOL_DATA_MAX)});
                        let seq = self.store.lock().await.append_data(name, "tool", &title, &data).ok();
                        if let Some(seq) = seq {
                            tool_rows.insert(t.tool_call_id.to_string(), (seq, data.clone()));
                        }
                        self.publish(json!({"type": "tool", "session": sid, "tool_call_id": t.tool_call_id.to_string(),
                                            "tool": tool, "title": title, "status": "running", "seq": seq,
                                            "input": data["input"].clone()}));
                    }
                    SessionUpdate::ToolCallUpdate(u) => {
                        // The real input can arrive on the update instead of the call:
                        // re-derive the title then so the live activity and Web UI improve.
                        let mut specific: Option<String> = None;
                        if let Some(a) = self.activity.lock().await.get_mut(name) {
                            if let Some(status)=u.fields.status {
                                a["state"]=json!(if matches!(status,agent_client_protocol::v1::schema::ToolCallStatus::Completed|agent_client_protocol::v1::schema::ToolCallStatus::Failed) {"done"} else {"running"});
                            }
                            a["at"] = json!(now());
                            if let Some(input) = &u.fields.raw_input {
                                let title = crate::activity::title(a["tool"].as_str().unwrap_or(""),
                                                                   a["title"].as_str().unwrap_or(""), Some(input));
                                a["title"] = json!(title);
                                specific = Some(title);
                            }
                        }
                        if let Some((seq, data)) = tool_rows.get_mut(&u.tool_call_id.to_string()) {
                            let status = u.fields.status.map(|st| format!("{st:?}").to_lowercase()).unwrap_or_else(|| data["status"].as_str().unwrap_or("running").to_string());
                            data["status"] = json!(status);
                            if let Some(input)=&u.fields.raw_input { data["input"]=clip_json(input,TOOL_DATA_MAX); }
                            if let Some(out) = &u.fields.raw_output {
                                data["output"] = clip_json(out, TOOL_DATA_MAX);
                            }
                            if let Some(title) = &specific {
                                data["title"] = json!(title);
                                let _ = self.store.lock().await.set_message_text(*seq, title);
                            }
                            let _ = self.store.lock().await.set_message_data(*seq, data);
                            let mut event = json!({"type": "tool", "session": sid, "tool_call_id": u.tool_call_id.to_string(),
                                                   "status": data["status"].clone(), "input":data["input"].clone(), "output": data["output"].clone(), "seq": *seq});
                            if let Some(title) = &specific { event["title"] = json!(title); }
                            self.publish(event);
                        }
                    }
                    _ => {}
                }
                all.push(n);
            }
            all
        };
        let res = match harness.as_str() {
            "claude" => {
                let req = agent_client_protocol::v1::schema::PromptRequest::new(
                    id.clone(),
                    vec![agent_client_protocol::v1::schema::ContentBlock::Text(
                        agent_client_protocol::v1::schema::TextContent::new(text),
                    )],
                );
                // Permission prompts arrive on `ptx` while the turn runs.
                let (ptx, prx) = mpsc::channel::<RequestPermissionRequest>(16);
                let perms = self.permissions(name, prx);
                let run = async {
                    if !self_turn {
                        self.claude.prompt_with_permissions(req, tx, Some(ptx)).await
                    } else if self.claude.self_turn_pending(&id).await {
                        // Not a prompt: follow the turn the CLI started on its own.
                        self.claude.follow_self_turn(&id, tx, Some(ptx)).await
                    } else {
                        // Already taken up by a turn that ran in between: nothing to follow.
                        drop((tx, ptx));
                        Ok(agent_client_protocol::v1::schema::PromptResponse::new(StopReason::EndTurn))
                    }
                };
                let (res, (), all) = tokio::join!(run, perms, collect);
                self.end_turn_pending(name).await;
                (res, all)
            }
            "opencode" | "agy" | "fx" | "pi" => {
                let driver = self.opencode_for(name).await?;
                let (ptx, prx) = mpsc::channel::<RequestPermissionRequest>(16);
                let perms = self.permissions(name, prx);
                let context = { let st = self.store.lock().await; self.browser_state(&st, name) };
                let session = self.store.lock().await.get(name)?.ok_or_else(||anyhow!("missing session"))?;
                let instructions = session_prompt(&session, context);
                let (res, (), all) = tokio::join!(driver.prompt_with_context(&id, text, tx, Some(ptx), Some(&instructions)), perms, collect);
                self.end_turn_pending(name).await;
                (res, all)
            }
            _ => {
                let codex = self.codex().await?;
                let (ptx, prx) = mpsc::channel::<RequestPermissionRequest>(16);
                let perms = self.permissions(name, prx);
                let (res, (), all) = tokio::join!(codex.prompt_with_permissions(&id, text, tx, Some(ptx)), perms, collect);
                self.end_turn_pending(name).await;
                (res, all)
            }
        };
        let (res, updates) = res;
        self.activity.lock().await.remove(name);
        // agy replays a loaded conversation as session/update frames; a replay
        // that trails past session/load would otherwise be summed into this
        // turn's reply. Keep only the text that answers this prompt.
        let reply = if harness == "agy" {
            crate::driver::turn_reply(&updates, text)
        } else {
            agent_text(&updates)
        };
        if harness == "agy" {
            let driver = self.opencode.lock().await.get(name).cloned();
            if let Some(driver) = driver {
                if let Some(usage) = driver.usage.lock().await.get(&id.to_string()).cloned() {
                    self.store.lock().await.set_harness_metrics(name, &usage)?;
                }
            }
        }
        let st = self.store.lock().await;
        let stopped = res.as_ref().is_err_and(|e| format!("{e:#}").contains(claude_cli_acp::TURN_STOPPED));
        let cancelled = res.as_ref().is_ok_and(|r| matches!(&r.stop_reason, StopReason::Cancelled));
        let turn_status = if cancelled || stopped { "stopped" } else if res.is_ok() { "idle" } else { "error" };
        st.set_status(name, turn_status)?;
        self.publish(json!({"type": "turn", "session": sid, "status": turn_status}));
        if stopped {
            // `stop` already logged the event; the message stays in the CLI's context.
            st.append(name, "error", "turn stopped: the session was stopped before it answered")?;
        } else if let Err(e) = &res {
            st.append(name, "error", &turn_failed_message(e))?;
            st.log_event(actor, name, "failed", &format!("{e:#}"))?;
        }
        let resp = res?;
        let reply = if mcp_notices.is_empty() { reply } else { format!("{}\n\n{reply}", mcp_notices.join("\n")) };
        if !reply.trim().is_empty() {
            st.append(name, "agent", &reply)?;
        } else if cancelled {
            st.append(name, "error", "turn stopped: the session was stopped before it answered")?;
        }
        drop(st);
        if harness == "claude" {
            self.capture_compactions(name).await;
        } else if harness == "codex" && !cancelled {
            self.context_override.lock().await.remove(name);
        }
        let st = self.store.lock().await;
        let display = st.get(name)?.map(|x| x.name).unwrap_or_default();
        Ok(json!({"id": name, "name": display, "reply": reply, "stop_reason": format!("{:?}", resp.stop_reason), "notice": notice}))
    }

    /// Turn a turn's permission prompts into approvals, for either harness.
    async fn permissions(&self, name: &str, mut prx: mpsc::Receiver<RequestPermissionRequest>) {
        while let Some(p) = prx.recv().await {
            let rid = p.tool_call.tool_call_id.to_string();
            match self.on_permission(name, p).await {
                Ok(Some(s)) => {
                    if let Err(e) = self.respond_approval(name, &rid, &outcome(s.allow), Some(&s.message)).await {
                        eprintln!("approval answer failed for {name}: {e:#}");
                    }
                }
                Ok(None) => {}
                Err(e) => eprintln!("approval handling failed for {name}: {e:#}"),
            }
        }
    }

    /// A permission prompt from a session's CLI: settle it by rule, or queue it for the user.
    /// `Some` when a rule settled it (the caller answers the harness).
    async fn on_permission(&self, name: &str, p: RequestPermissionRequest) -> Result<Option<Settled>> {
        let rid = p.tool_call.tool_call_id.to_string();
        let tool = p.tool_call.fields.name.clone().unwrap_or_else(|| "tool".into());
        let input = p.tool_call.fields.raw_input.clone().unwrap_or(Value::Null);
        let question = tool == "AskUserQuestion";
        let title = if question {
            format!("Question: {}", input["questions"][0]["question"].as_str().unwrap_or("(no text)"))
        } else {
            p.tool_call.fields.title.clone().unwrap_or_else(|| tool.clone())
        };
        let meta = p.meta.clone().unwrap_or_default();
        let cwd = meta.get("cwd").and_then(Value::as_str).unwrap_or("").to_string();
        let hook_deadline = meta.get("expires_at").and_then(Value::as_i64).unwrap_or(i64::MAX);
        let expires = (now() + approval_ttl() as i64).min(hook_deadline);
        let st = self.store.lock().await;
        let s = st.get(name)?.ok_or_else(|| anyhow!("no session {name}"))?;
        let a = st.approval_create(name, &rid, &tool, &title, &input, &cwd, expires)?;
        // Questions always go to the user; rules only cover tool permissions.
        let rules = if question { vec![] } else { st.rules()? };
        if let Some(rule) = crate::approvals::decide(&rules, &s, &tool, &input, &cwd) {
            let allow = rule.decision == "allow";
            let by = format!("rule#{}", rule.id);
            let status = if allow { "approved" } else { "denied" };
            st.approval_decide(&a.id, status, &by, Some(&format!("{} by rule #{}", status, rule.id)), Some(rule.id))?;
            st.log_event(&by, name, &format!("approval_{status}"), &format!("{} {title}", a.id))?;
            drop(st);
            let msg = format!("This tool call was denied by the user's Kurisu rule #{}.", rule.id);
            return Ok(Some(Settled { allow, message: msg }));
        }
        st.append(name, "approval", &format!("[approval:{}] {title}", a.id))?;
        st.log_event(name, name, "approval_requested", &format!("{} {title}", a.id))?;
        st.set_status(name, "awaiting_approval")?;
        Ok(None)
    }

    /// Answer a permission prompt in whichever harness asked: Claude reads a decision
    /// file next to its hook; the Codex driver resolves the waiting app-server request.
    async fn respond_approval(&self, name: &str, request_id: &str, answer: &RequestPermissionOutcome, message: Option<&str>) -> Result<()> {
        if request_id.starts_with(ASK_PREFIX) {
            // Only denials reach here for a question (answers go through `answer`).
            self.resolve_ask(request_id, ask_dismissed(message.unwrap_or("The user dismissed the question."))).await;
            return Ok(());
        }
        let (harness, hid) = {
            let st = self.store.lock().await;
            let s = st.get(name)?.ok_or_else(|| anyhow!("no session {name}"))?;
            (s.harness.clone(), s.harness_session_id.unwrap_or_default())
        };
        if matches!(harness.as_str(), "opencode" | "agy" | "fx" | "pi") { return self.opencode_for(name).await?.respond_permission(request_id, answer).await; }
        if harness == "codex" {
            return self.codex().await?.respond_permission(request_id, answer).await;
        }
        self.claude.respond_permission(&SessionId::new(hid), request_id, answer, message).await
    }

    /// The user's answer to a pending approval (single use).
    async fn decide(&self, key: &str, allow: bool, always: bool, session_wide: bool, reason: Option<String>) -> Result<Value> {
        let st = self.store.lock().await;
        let a = st.approval_resolve(key)?;
        if a.status != "pending" {
            bail!("approval {} is already {}", a.id, a.status);
        }
        if allow && a.kind == "question" {
            bail!("approval {} is a question: answer it (ku answer / POST /approvals/{{id}}/answer), or deny it to dismiss", a.id);
        }
        let status = if allow { "approved" } else { "denied" };
        let mut rule = None;
        if allow && always {
            // "Always" = a session rule for this tool. The plain CLI/API form stays
            // limited to the session's folder; the Telegram "always allow" card asks
            // for the whole session (any path), matching what the button says.
            rule = Some(st.rule_add("session", &a.session, &a.tool, !session_wide, None, "allow", "user")?);
        }
        if !st.approval_decide(&a.id, status, "user", reason.as_deref(), rule.as_ref().map(|r| r.id))? {
            bail!("approval {} is no longer pending", a.id);
        }
        st.log_event("user", &a.session, &format!("approval_{status}"), &format!("{} {}{}", a.id, a.title,
                     reason.as_deref().map(|r| format!(" ({r})")).unwrap_or_default()))?;
        if let Some(r) = &rule {
            st.log_event("user", &a.session, "rule_added", &format!("#{} allow {} in its folder", r.id, r.tool))?;
        }
        let still_pending = !st.approvals(Some(&a.session), false)?.is_empty();
        drop(st);
        if !still_pending {
            self.settle_status(&a).await?;
        }
        let what = if a.kind == "question" { "dismissed the question" } else { "denied this tool call" };
        let msg = match &reason {
            Some(r) => format!("The user {what}: {r}"),
            None => format!("The user {what}."),
        };
        let answer = if allow && session_wide { outcome_always() } else { outcome(allow) };
        self.respond_approval(&a.session, &a.request_id, &answer, Some(&msg)).await?;
        Ok(json!({"id": a.id, "status": status, "session": a.session, "session_name": a.session_name, "title": a.title,
                  "rule": rule}))
    }

    /// Answer a pending AskUserQuestion. `answers` maps a question (its text,
    /// header or index) to a label or free text, or to a list of labels.
    async fn answer(self: &Arc<Self>, key: &str, answers: &Value) -> Result<Value> {
        let st = self.store.lock().await;
        let a = st.approval_resolve(key)?;
        if a.kind != "question" {
            bail!("approval {} is not a question; approve or deny it", a.id);
        }
        if a.status != "pending" {
            bail!("question {} is already {}", a.id, a.status);
        }
        let filled = fill_answers(&a.input, answers)?;
        let summary: Vec<String> = filled.as_object().into_iter().flatten().map(|(_, v)| v.as_str().unwrap_or("").to_string()).collect();
        if !st.approval_decide(&a.id, "approved", "user", Some(&summary.join("; ")), None)? {
            bail!("question {} is no longer pending", a.id);
        }
        st.approval_set_answer(&a.id, &filled)?;
        st.log_event("user", &a.session, "question_answered", &format!("{} {} -> {}", a.id, a.title, summary.join("; ")))?;
        let hid = st.get(&a.session)?.and_then(|s| s.harness_session_id).unwrap_or_default();
        let settled = st.approvals(Some(&a.session), false)?.is_empty();
        drop(st);
        if settled {
            self.settle_status(&a).await?;
        }
        if a.request_id.starts_with(ASK_PREFIX) {
            let text = summary.join("; ");
            if !self.resolve_ask(&a.request_id, json!({"status": "answered", "answer": text})).await {
                // Its call already returned (timed out, or the turn ended): resume the asker.
                let q = a.input["questions"][0]["question"].as_str().unwrap_or("").to_string();
                let msg = format!("Answer to your question '{q}': {text}");
                self.send_async(&a.session, &msg, "user", "user", false).await?;
            }
        } else {
            let mut input = a.input.clone();
            input["answers"] = filled.clone();
            self.claude.allow_with_input(&SessionId::new(hid), &a.request_id, input).await?;
        }
        Ok(json!({"id": a.id, "status": "approved", "session": a.session, "session_name": a.session_name, "title": a.title, "answer": filled}))
    }

    /// Cancel a session's pending approvals (its turn ended or was stopped).
    async fn cancel_pending(&self, name: &str, why: &str) {
        self.cancel_pending_but(name, why, false).await
    }

    /// The turn ended: harness prompts die with it, but an ask_user question stays open
    /// on the user's phone. Its blocked call (if any) is told so, and a later answer
    /// arrives as a new message (see `answer`).
    async fn end_turn_pending(&self, name: &str) {
        self.cancel_pending_but(name, "the turn ended", true).await
    }

    async fn cancel_pending_but(&self, name: &str, why: &str, keep_asks: bool) {
        let st = self.store.lock().await;
        let mut asks = vec![];
        let mut kept = vec![];
        for a in st.approvals(Some(name), false).unwrap_or_default() {
            if keep_asks && a.request_id.starts_with(ASK_PREFIX) {
                kept.push(a.request_id);
                continue;
            }
            if st.approval_decide(&a.id, "cancelled", "kurisu", Some(why), None).unwrap_or(false) {
                let _ = st.log_event("kurisu", name, "approval_cancelled", &format!("{} {} ({why})", a.id, a.title));
                if a.request_id.starts_with(ASK_PREFIX) {
                    asks.push(a.request_id);
                }
            }
        }
        drop(st);
        for rid in asks {
            self.resolve_ask(&rid, ask_dismissed(&format!("The question was withdrawn ({why}): the user did not answer it."))).await;
        }
        for rid in kept {
            self.resolve_ask(&rid, ask_still_open()).await;
        }
    }

    /// Set the status once a session's last approval is settled: `working` while its turn runs.
    /// An ask_user question can outlive the turn; settling it then leaves the session idle.
    async fn settle_status(&self, a: &store::Approval) -> Result<()> {
        let status = if a.request_id.starts_with(ASK_PREFIX) && !self.is_busy(&a.session).await { "idle" } else { "working" };
        self.store.lock().await.set_status(&a.session, status)
    }

    /// Wake a waiting ask_user call with its tool result.
    /// True if a blocked call was still there to take it.
    async fn resolve_ask(&self, request_id: &str, result: Value) -> bool {
        match self.asks.lock().await.remove(request_id) {
            Some(tx) => tx.send(result).is_ok(),
            None => false,
        }
    }

    /// ask_user from a session's MCP tools: a question approval like Claude's
    /// AskUserQuestion (same cards, `ku answer`, POST /approvals/{id}/answer), whose
    /// answer becomes the tool result. Blocks until answered, dismissed, stopped or expired.
    async fn ask_user(&self, caller: &str, req: &Value) -> Result<Value> {
        let question = req["question"].as_str().map(str::trim).filter(|q| !q.is_empty()).ok_or_else(|| anyhow!("missing question"))?;
        let s = self.store.lock().await.get(caller)?.ok_or_else(|| anyhow!("no session {caller}"))?;
        // Only the lead puts question cards in front of the owner.
        if s.role != "lead" {
            bail!("ask_user is only for the lead session: ask {} in your reply instead",
                  s.created_by_name.as_deref().map(|n| format!("`{n}` (who created you)")).unwrap_or_else(|| "in text".into()));
        }
        let choices: Vec<String> = match &req["choices"] {
            Value::Array(xs) => xs.iter().filter_map(|x| x.as_str().or(x["label"].as_str())).map(str::trim).filter(|x| !x.is_empty()).map(String::from).collect(),
            Value::String(x) if !x.trim().is_empty() => x.split([',', '\n']).map(str::trim).filter(|x| !x.is_empty()).map(String::from).collect(),
            _ => vec![],
        };
        let multi = req["multi_select"].as_bool().unwrap_or(false) || req["multi_select"].as_str() == Some("true");
        let header = req["header"].as_str().map(str::trim).filter(|h| !h.is_empty()).map(|h| h.chars().take(40).collect::<String>());
        let mut q = json!({"question": question, "options": choices.iter().map(|c| json!({"label": c})).collect::<Vec<_>>(), "multiSelect": multi});
        if let Some(h) = header { q["header"] = json!(h); }
        let input = json!({"questions": [q]});
        let rid = format!("{ASK_PREFIX}{}", uuid::Uuid::new_v4().simple());
        let title = format!("Question: {question}");
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.asks.lock().await.insert(rid.clone(), tx);
        {
            let st = self.store.lock().await;
            let a = st.approval_create(caller, &rid, "AskUserQuestion", &title, &input, &s.cwd, now() + ask_ttl() as i64)?;
            st.append(caller, "approval", &format!("[approval:{}] {title}", a.id))?;
            st.log_event(caller, caller, "approval_requested", &format!("{} {title}", a.id))?;
            st.set_status(caller, "awaiting_approval")?;
        }
        // Wait a bounded time: CLIs cut long MCP calls (Claude Code backgrounds them after
        // 120 s and ends the turn). After that the question stays open and the answer comes
        // back as a new message.
        match tokio::time::timeout(ask_wait(), rx).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(_)) => Ok(ask_dismissed("The question was withdrawn.")),
            Err(_) => {
                self.asks.lock().await.remove(&rid);
                let st = self.store.lock().await;
                if st.get(caller)?.is_some_and(|x| x.status == "awaiting_approval") {
                    st.set_status(caller, "working")?;
                }
                Ok(ask_still_open())
            }
        }
    }

    /// Deny approvals past their expiry (the hook also denies on its own timer).
    async fn expire_approvals(&self) -> Result<()> {
        let expired = self.store.lock().await.approvals_expired(now())?;
        for a in expired {
            let st = self.store.lock().await;
            if !st.approval_decide(&a.id, "expired", "kurisu", Some("no answer before it expired"), None)? {
                continue;
            }
            st.log_event("kurisu", &a.session, "approval_expired", &format!("{} {}", a.id, a.title))?;
            let settled = st.approvals(Some(&a.session), false)?.is_empty() && st.get(&a.session)?.is_some_and(|s| s.status == "awaiting_approval");
            drop(st);
            if settled {
                self.settle_status(&a).await?;
            }
            let _ = self.respond_approval(&a.session, &a.request_id, &outcome(false), Some(&format!("This tool call was denied because the approval request timed out: the user did not answer within {} minutes. \
The user may be away; continue without it if you can, or ask again later.", (approval_ttl() + 59) / 60))).await;
        }
        Ok(())
    }

    /// Store compaction summaries the CLI made on its own (auto-compact during a turn),
    /// so the history keeps them: any transcript compaction beyond those already stored.
    async fn capture_compactions(&self, name: &str) {
        let Ok(Some(s)) = self.store.lock().await.get(name) else { return };
        let Some(hid) = s.harness_session_id.clone() else { return };
        let Some(path) = self.claude.transcript_path(&SessionId::new(hid)).await else { return };
        let all = claude_cli_acp::compactions_from_transcript(&tokio::fs::read_to_string(&path).await.unwrap_or_default());
        let st = self.store.lock().await;
        let have = st.count_role(name, "compaction").unwrap_or(0) as usize;
        for c in all.iter().skip(have).filter(|c| !c.summary.is_empty()) {
            let _ = st.append(name, "compaction", &c.summary);
            let _ = st.log_event("kurisu", name, "compacted", &format!("{} compact, pre {} tokens",
                c.trigger.as_deref().unwrap_or("auto"), c.pre_tokens.map(|n| n.to_string()).unwrap_or("?".into())));
        }
    }

    /// Everything a new agent needs to continue another session's work, from Kurisu's
    /// own history (any harness): who it was, its latest compaction summary, and the
    /// newest messages and tool calls after it, within a character budget.
    async fn handoff(&self, name: &str, max_chars: usize) -> Result<Value> {
        let st = self.store.lock().await;
        let s = st.get(name)?.ok_or_else(|| anyhow!("no session {name}"))?;
        // After harness switches, the history is spread over a chain of sessions
        // (each `replaces` the previous one): read it newest first, back to the start.
        let mut chain = vec![s.clone()];
        while let Some(prev) = chain.last().and_then(|x| x.replaces.clone()) {
            if chain.len() >= 20 || chain.iter().any(|x| x.id == prev) {
                break;
            }
            match st.get(&prev)? {
                Some(p) => chain.push(p),
                None => break,
            }
        }
        // Newest first until the budget is spent, then back to chronological order.
        let mut rows = vec![];
        let mut used = 0usize;
        let mut omitted = 0i64;
        let mut summary: Option<(i64, String, String)> = None; // (seq, text, session id)
        'chain: for (ci, link) in chain.iter().enumerate() {
            let own = st.last_compaction(&link.id)?;
            let after = own.as_ref().map(|(seq, _)| *seq);
            // The summary is always sent: reserve its share of the budget first.
            if let Some((_, t)) = &own {
                used += t.len().min(max_chars / 2);
            }
            let q = store::HistoryQuery { after, limit: 500, tools: true, ..Default::default() };
            let mut before = None;
            loop {
                let (page, _) = st.history_page(&link.id, &store::HistoryQuery { before, after, ..q.clone() })?;
                if page.is_empty() {
                    break;
                }
                before = page.first().map(|m| m.seq);
                for m in page.iter().rev() {
                    let cap = if m.role == "tool" { 300 } else { 4000 };
                    let mut e = json!({"seq": m.seq, "role": m.role, "at": m.at, "text": clip_text(&m.text, cap)});
                    if ci > 0 {
                        e["session"] = json!(format!("{} ({})", take8(&link.id), link.harness));
                    }
                    if let Some(d) = &m.data {
                        e["status"] = d["status"].clone();
                        if let Some(out) = d.get("output") {
                            e["output"] = json!(clip_text(&out.as_str().map(String::from).unwrap_or_else(|| out.to_string()), 300));
                        }
                    }
                    let cost = e.to_string().len();
                    if used + cost > max_chars && !rows.is_empty() {
                        omitted += st.count_range(&link.id, after.unwrap_or(0), m.seq)?;
                        for older in &chain[ci + 1..] {
                            omitted += st.count_range(&older.id, 0, i64::MAX)?;
                        }
                        break 'chain;
                    }
                    used += cost;
                    rows.push(e);
                }
            }
            // A compaction summary covers everything before it: stop there.
            if let Some((seq, t)) = own {
                summary = Some((seq, t, link.id.clone()));
                break;
            }
        }
        rows.reverse();
        let mut total = 0;
        for link in &chain {
            total += st.history_page(&link.id, &store::HistoryQuery { limit: 1, tools: true, ..Default::default() })?.1;
        }
        Ok(json!({
            "session": {"id": s.id, "name": s.name, "role": s.role, "description": s.description, "status_note": s.note,
                        "harness": s.harness, "model": s.model, "cwd": s.cwd, "status": s.status, "lifecycle": s.lifecycle},
            "chain": chain.iter().map(|x| json!({"id": x.id, "harness": x.harness, "model": x.model, "lifecycle": x.lifecycle})).collect::<Vec<_>>(),
            "compaction_summary": summary.map(|(seq, t, sid)| json!({"seq": seq, "session": sid, "text": clip_text(&t, max_chars / 2)})),
            "messages": rows,
            "omitted_older": omitted,
            "total_messages": total,
            "how_to_continue": "This is the other session's history as Kurisu stored it (its own CLI context is not transferred). \
After harness switches it spans the chain of sessions it replaced (messages from earlier ones carry a session tag). \
Read the summary and messages, check the files in its cwd if you need them, use session_history {id, search|before, tools:true} \
for anything older or omitted, then continue the work and tell the user what you picked up.",
        }))
    }

    /// Deliver every due wake as a normal message (queued behind a running turn).
    /// A wake more than `LATE_AFTER` seconds past due (e.g. the server was down) fires
    /// once and is marked late; a repeating wake then moves to its next time after now.
    async fn fire_wakes(self: &Arc<Self>) -> Result<()> {
        let now = now();
        let due = self.store.lock().await.wakes_due(now)?;
        for w in due {
            let late = now - w.at > crate::wakes::LATE_AFTER;
            let next = w.repeat.as_deref().and_then(|r| crate::wakes::Cron::parse(r).ok()).and_then(|c| c.next_after(now));
            let mut head = format!("[Scheduled wake #{} set by {} for {}", w.id, w.created_by_name, crate::wakes::show(w.at));
            if late {
                head.push_str(&format!("; LATE: delivered at {} (Kurisu was not running at the due time)", crate::wakes::show(now)));
            }
            if let Some(r) = &w.repeat {
                head.push_str(&format!("; repeats '{r}', next {}", next.map(crate::wakes::show).unwrap_or_else(|| "never".into())));
            }
            let text = format!("{head}]\n{}", w.message);
            let from = if w.created_by == "user" { "user" } else { w.created_by.as_str() };
            let res = self.send_async(&w.session, &text, from, "kurisu", false).await;
            let err = res.as_ref().err().map(|e| format!("{e:#}"));
            let st = self.store.lock().await;
            st.wake_fired(w.id, now, late, err.as_deref(), next)?;
            let detail = format!("wake #{}{}{}", w.id, if late { " (late)" } else { "" },
                                 err.as_deref().map(|e| format!(": {e}")).unwrap_or_default());
            st.log_event("kurisu", &w.session, if err.is_some() { "wake_failed" } else { "wake_fired" }, &detail)?;
        }
        Ok(())
    }

    /// The user may schedule anything; a session only itself and its descendants.
    fn may_wake(st: &Store, caller: Option<&str>, target: &str) -> Result<()> {
        let Some(c) = caller else { return Ok(()) };
        if c == target || st.is_ancestor(c, target)? {
            return Ok(());
        }
        let n = |id: &str| st.get(id).ok().flatten().map(|x| x.name).unwrap_or_else(|| id.to_string());
        bail!("permission denied: {} may only schedule wakes for itself or sessions it created (directly or indirectly), not {}", n(c), n(target))
    }

    async fn wake_add(&self, caller: Option<&str>, target: &str, at: Option<&str>, repeat: Option<&str>, message: &str) -> Result<Value> {
        let st = self.store.lock().await;
        let s = st.get(target)?.ok_or_else(|| anyhow!("no session {target}"))?;
        Self::may_wake(&st, caller, target)?;
        if message.trim().is_empty() {
            bail!("message is empty");
        }
        if message.len() > 8000 {
            bail!("message is too long (at most 8000 characters)");
        }
        if s.lifecycle == "archived" {
            bail!("session {} is archived", s.name);
        }
        if st.wake_pending_count(target)? >= 50 {
            bail!("session {} already has 50 pending wakes: cancel some first", s.name);
        }
        let now = now();
        let cron = repeat.map(crate::wakes::Cron::parse).transpose()?;
        let when = match (at, &cron) {
            (Some(a), _) => crate::wakes::parse_at(a, now)?,
            (None, Some(c)) => c.next_after(now).ok_or_else(|| anyhow!("that repeat never fires"))?,
            (None, None) => bail!("give `at` (e.g. 'in 30m', '06:00', '2026-11-08 06:00') or `repeat` (cron)"),
        };
        if when < now - crate::wakes::LATE_AFTER {
            bail!("{} is in the past", crate::wakes::show(when));
        }
        if when > now + 5 * 366 * 86400 {
            bail!("{} is more than 5 years away", crate::wakes::show(when));
        }
        let actor = caller.unwrap_or("user");
        let w = st.wake_add(target, actor, when, message, repeat)?;
        st.log_event(actor, target, "wake_scheduled", &format!("wake #{} for {}{}", w.id, crate::wakes::show(when),
                                                               repeat.map(|r| format!(", repeats '{r}'")).unwrap_or_default()))?;
        let mut v = json!(w);
        v["at_local"] = json!(crate::wakes::show(when));
        v["in_seconds"] = json!(when - now);
        Ok(v)
    }

    fn wake_json(w: &store::Wake) -> Value {
        let mut v = json!(w);
        v["at_local"] = json!(crate::wakes::show(w.at));
        v
    }

    /// Snapshot the session's folder before the turn started by message `seq` (/undo).
    /// A failure only means that turn's files cannot be undone.
    async fn undo_snapshot(&self, name: &str, seq: i64) {
        let Ok(Some(s)) = self.store.lock().await.get(name) else { return };
        let snaps = crate::undo::Snapshots::new(&self.paths.home, &s.id, Path::new(&s.cwd));
        if let Err(e) = snaps.snapshot(seq).await {
            eprintln!("undo snapshot of {} before message {seq} failed: {e:#}", s.name);
            let _ = self.store.lock().await.log_event("kurisu", &s.id, "undo_snapshot_failed", &format!("message {seq}: {e:#}"));
        }
    }

    /// /undo: take back the session's last turn. Its files are restored from the
    /// snapshot taken before it, its rows leave Kurisu's history, and the CLI forgets
    /// it (Codex: `thread/revert`; other harnesses: a note on the next prompt).
    async fn undo(&self, name: &str, actor: &str) -> Result<Value> {
        let lock = self.busy_lock(name).await;
        let Ok(_turn) = lock.try_lock() else { bail!("the session is busy: /undo works once its turn has ended (/stop first)") };
        let gate = self.turn_gate(name).await;
        let _start = gate.lock().await;
        let s = self.store.lock().await.get(name)?.ok_or_else(|| anyhow!("no session {name}"))?;
        let Some(prompt) = self.store.lock().await.last_prompt(&s.id)? else { bail!("nothing to undo: the session has no turns") };
        let start = prompt.seq;
        let snaps = crate::undo::Snapshots::new(&self.paths.home, &s.id, Path::new(&s.cwd));
        let snapshotted = snaps.turns().await.unwrap_or_default().contains(&start);
        let (files_ok, files) = if snapshotted {
            match snaps.restore(start).await {
                Ok(r) => (true, crate::undo::describe(&r)),
                Err(e) => (false, format!("files NOT restored: {e:#}")),
            }
        } else {
            (false, "files NOT restored: no snapshot was taken before that turn (see the session's events)".into())
        };
        let _ = snaps.drop_from(start).await;
        // The conversation.
        let mut native = false;
        let conversation = if s.harness_session_id.is_none() {
            native = true;
            "the CLI has no conversation yet: nothing to take back there".to_string()
        } else if s.harness == "codex" {
            let reverted = async {
                let id = self.ensure_live(name, actor).await?;
                self.codex().await?.revert_last_turn(&id, prompt.at).await
            }.await;
            match reverted {
                Ok(turn) => {
                    native = true;
                    self.context_override.lock().await.insert(name.to_string(), now());
                    format!("Codex conversation reverted (thread/revert before turn {turn})")
                }
                Err(e) => format!("Codex could not revert its conversation ({e:#}); it will be told on the next prompt instead"),
            }
        } else {
            format!("{} has no undo Kurisu can drive: it will be told on the next prompt to disregard that turn, \
but the turn stays in its context", s.harness)
        };
        if !native {
            let clip: String = prompt.text.chars().take(200).collect();
            let files_note = if files_ok { "and restored the files in your folder to how they were before it" }
                             else { "but could NOT restore the files in your folder, so changes you made may still be there" };
            self.store.lock().await.add_pending_note(&s.id, &format!(
                "[Kurisu note: the user ran /undo on your previous turn (their message: \"{clip}\"). Kurisu removed that turn from \
its history {files_note}. Treat that message and your reply to it as withdrawn: do not continue that work or rely on it unless \
the user asks again.]"))?;
        }
        let st = self.store.lock().await;
        let removed = st.remove_messages_from(&s.id, start)?;
        let updates: Vec<i64> = removed.iter().filter_map(|m| m.data.as_ref()?["telegram_update"].as_i64()).collect();
        st.set_status(name, "idle")?;
        st.log_event(actor, &s.id, "undone", &format!("message {start}: {} history rows removed; {files}; {conversation}", removed.len()))?;
        drop(st);
        self.publish(json!({"type": "undo", "session": s.id, "from_seq": start}));
        let clip: String = prompt.text.chars().take(80).collect();
        let text = format!("Undid the last turn of {} (message: \"{clip}\").\nFiles: {files}.\nConversation: {conversation}.\n\
Kurisu history: removed {} row(s).\nNot undone: anything outside the session folder or ignored by its .gitignore, and outside \
effects (commands' side effects elsewhere, messages sent, pushes, API calls).",
            s.name, removed.len());
        Ok(json!({"id": s.id, "name": s.name, "harness": s.harness, "seq": start, "prompt": prompt.text,
                  "files_restored": files_ok, "files": files, "conversation": conversation, "conversation_reverted": native,
                  "history_removed": removed.len(), "telegram_updates": updates, "text": text}))
    }

    /// Compact a session's context with the CLI's own `/compact` (refused while busy).
    async fn compact(&self, name: &str, actor: &str) -> Result<Value> {
        let lock = self.busy_lock(name).await;
        let Ok(_turn) = lock.try_lock() else { bail!("the session is busy: compact it when its turn has ended") };
        let gate = self.turn_gate(name).await;
        let start = gate.lock().await;
        self.active_turns.lock().await.insert(name.into(), -1);
        drop(start);
        let result = self.compact_locked(name, actor).await;
        self.active_turns.lock().await.remove(name);
        result
    }

    async fn compact_locked(&self, name: &str, actor: &str) -> Result<Value> {
        let s = self.store.lock().await.get(name)?.ok_or_else(|| anyhow!("no session {name}"))?;
        if s.harness_session_id.is_none() {
            bail!("the session has not started yet: nothing to compact");
        }
        self.prepare_mcp_oauth(&s.id).await;
        let id = self.ensure_live(name, actor).await?;
        self.store.lock().await.set_status(name, "working")?;
        let timeout = std::time::Duration::from_secs(10 * 60);
        if s.harness == "codex" {
            // Even a failed/timed-out compact may have changed upstream history.
            // Keep old rollout counts hidden until a subsequent successful turn.
            self.context_override.lock().await.insert(name.to_string(), now());
        }
        let res = match s.harness.as_str() {
            "claude" => self.claude.compact(&id, timeout).await,
            "codex" => match self.codex().await {
                Ok(driver) => driver.compact(&id, timeout).await,
                Err(e) => Err(e),
            },
            "opencode" | "agy" | "fx" | "pi" => self.opencode_for(name).await?.compact(&id).await,
            other => Err(anyhow!("compact is not available for {other} sessions")),
        };
        let st = self.store.lock().await;
        st.set_status(name, "idle")?;
        match res {
            Ok(c) => {
                let detail = if c.summary.is_empty() {
                    format!("{} manual compaction completed; no readable summary reported", s.harness)
                } else {
                    format!("pre {} tokens, summary {} chars (about {} tokens)", c.pre_tokens.map(|n| n.to_string()).unwrap_or("?".into()),
                            c.summary.len(), c.post_tokens_estimate.unwrap_or(0))
                };
                st.log_event(actor, name, "compacted", &detail)?;
                if !c.summary.is_empty() {
                    st.append(name, "compaction", &c.summary)?;
                }
                self.context_override.lock().await.insert(name.to_string(), now());
                Ok(json!({"id": name, "name": s.name, "pre_tokens": c.pre_tokens, "post_tokens_estimate": c.post_tokens_estimate,
                          "summary": c.summary, "trigger": c.trigger}))
            }
            Err(e) => {
                st.log_event(actor, name, "compact_failed", &format!("{e:#}"))?;
                Err(e)
            }
        }
    }

    /// Save an attachment inside the session's folder (`.kurisu-uploads/`), so the
    /// session can read it without a permission prompt. Returns its path.
    async fn save_upload(&self, name: &str, filename: &str, data_b64: &str) -> Result<PathBuf> {
        let cwd = self.store.lock().await.get(name)?.ok_or_else(|| anyhow!("no session {name}"))?.cwd;
        let bytes = decode_b64(data_b64)?;
        let dir = PathBuf::from(cwd).join(".kurisu-uploads");
        std::fs::create_dir_all(&dir)?;
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        // Same name in the same second: add -2, -3, ...
        let (ts, safe) = (now(), safe_filename(filename));
        let mut n = 1;
        let (path, mut f) = loop {
            let path = dir.join(if n == 1 { format!("{ts}-{safe}") } else { format!("{ts}-{n}-{safe}") });
            match std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(&path) {
                Ok(f) => break (path, f),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && n < 100 => n += 1,
                Err(e) => return Err(e).with_context(|| format!("create {}", path.display())),
            }
        };
        f.write_all(&bytes)?;
        Ok(path)
    }

    /// `files: [{name, data_b64}]` and `images: [{mime?, name?, data_b64}]` on a send:
    /// saved in the folder, and listed after the text.
    async fn attach(&self, name: &str, req: &Value, text: &str) -> Result<String> {
        let mut paths = vec![];
        for (key, default) in [("images", "image"), ("files", "file")] {
            for f in req.get(key).and_then(Value::as_array).into_iter().flatten() {
                let data = f.get("data_b64").and_then(Value::as_str).ok_or_else(|| anyhow!("{key}[]: missing data_b64"))?;
                let mut fname = f.get("name").and_then(Value::as_str).unwrap_or(default).to_string();
                if key == "images" && !fname.contains('.') {
                    let ext = f.get("mime").and_then(Value::as_str).and_then(|m| m.strip_prefix("image/")).unwrap_or("png");
                    fname = format!("{fname}.{ext}");
                }
                paths.push(self.save_upload(name, &fname, data).await?.to_string_lossy().into_owned());
            }
        }
        if paths.is_empty() {
            return Ok(text.to_string());
        }
        let list = paths.iter().map(|p| format!("- {p}")).collect::<Vec<_>>().join("\n");
        let mut out = format!("{text}\n\n[Attached (in your folder; read them with the Read tool):\n{list}]");
        // Voice notes: transcribed when the user has set up speech-to-text; else passed through as files.
        if let Some(cfg) = crate::stt::active(&self.paths.home) {
            for p in paths.iter().filter(|p| crate::stt::is_audio(p)) {
                let res = crate::stt::transcribe(&cfg, Path::new(p)).await;
                if let Err(e) = &res {
                    eprintln!("speech-to-text failed for {p}: {e:#}");
                }
                out.push_str("\n\n");
                out.push_str(&crate::stt::note(p, &res));
            }
        }
        Ok(out)
    }

    /// `transcribe`: an audio file to text with the configured speech-to-text.
    async fn transcribe(&self, caller: Option<&str>, path: &str) -> Result<Value> {
        let cfg = crate::stt::active(&self.paths.home).ok_or_else(|| anyhow!(
            "speech-to-text is not set up in Kurisu (it is optional): the user can run `ku stt setup`, or `ku stt use --command ...` / `--mcp ...`"))?;
        let mut p = PathBuf::from(path);
        if p.is_relative() {
            let base = match caller {
                Some(c) => self.store.lock().await.get(c)?.map(|x| PathBuf::from(x.cwd)),
                None => None,
            };
            p = base.unwrap_or_else(|| std::env::current_dir().unwrap_or_default()).join(p);
        }
        let started = std::time::Instant::now();
        let text = crate::stt::transcribe(&cfg, &p).await?;
        Ok(json!({"path": p, "text": text, "seconds": (started.elapsed().as_secs_f64() * 10.0).round() / 10.0}))
    }

    /// Send one file from a session's own workspace folder to the paired Telegram chat.
    /// A file-exfiltration surface, so keep it tight: the session's own folder only, no `..`,
    /// resolved symlinks included, and Telegram's own photo/document size caps.
    async fn send_file(&self, caller: Option<&str>, path: &str, caption: Option<String>) -> Result<Value> {
        let id = caller.ok_or_else(|| anyhow!("send_file is for sessions: it sends a file from the session's own folder"))?;
        let s = self.store.lock().await.get(id)?.ok_or_else(|| anyhow!("missing session {id}"))?;
        let root = std::fs::canonicalize(&s.cwd).with_context(|| format!("cannot open the session's folder {}", s.cwd))?;
        let raw = PathBuf::from(path);
        if raw.components().any(|c| matches!(c, std::path::Component::ParentDir)) {
            bail!("path refused: {path} contains '..'; send_file only sends files inside this session's folder ({})", root.display());
        }
        let joined = if raw.is_absolute() { raw } else { root.join(raw) };
        let canon = std::fs::canonicalize(&joined).map_err(|e| anyhow!("no such file {}: {e}", joined.display()))?;
        if !canon.starts_with(&root) {
            bail!("path refused: {} is outside this session's folder ({}); send_file only sends files inside it", canon.display(), root.display());
        }
        // Resolve the originating person's route, following delegated session ancestry.
        let mut target=id.to_string();
        for _ in 0..32 {
            if let Some(to)=self.channels.destination(&target)? {
                if let Some(person)=self.caller_person(id).await? {
                    if to.person!=person || (person!="owner" && to.channel!="discord") {return Err(anyhow!(crate::channels::state::Denied));}
                }
                if to.channel == "discord" {
                    let sent=self.discord.send_file(&to,&canon,caption.as_deref().unwrap_or("")).await?;
                    return Ok(json!({"ok":true,"path":canon,"kind":sent["kind"],"bytes":sent["bytes"],"message_id":sent["message_id"]}));
                }
                break;
            }
            let parent=self.store.lock().await.get(&target)?.and_then(|s|s.created_by_session);
            match parent {Some(p)=>target=p,None=>break}
        }
        if self.caller_person(id).await?.as_deref().is_some_and(|p|p!="owner") {
            bail!("No private Discord route for this session; file remains unsent.");
        }
        // Size and kind first: the answer does not depend on Telegram being up.
        let kind = crate::telegram::tg::upload_kind(&canon)?;
        match crate::telegram::load_config(&self.paths.home) {
            None => bail!("Telegram is not set up for this Kurisu home, so there is no chat to send the file to (the user can run `ku telegram setup`)"),
            Some(c) if !c.enabled => bail!("Telegram is configured for this home but turned off, so the file cannot be sent (the user can turn it on again)"),
            Some(_) => {}
        }
        let sent = self.telegram.send_file(&canon, caption.as_deref()).await?;
        Ok(json!({"ok": true, "path": canon, "kind": if kind.photo { "photo" } else { "document" }, "bytes": kind.bytes,
                  "message_id": sent["message_id"]}))
    }

    /// A Codex session's usage from its rollout file (path cached per thread).
    async fn codex_usage(&self, s: &store::Session) -> Option<crate::codex::RolloutUsage> {
        let (Some(tid), "codex") = (&s.harness_session_id, s.harness.as_str()) else { return None };
        let cached = self.rollout_paths.lock().await.get(tid).cloned();
        let path = match cached {
            Some(p) => p,
            None => {
                let t = tid.clone();
                let p = tokio::task::spawn_blocking(move || crate::codex::find_rollout(&t)).await.ok()??;
                self.rollout_paths.lock().await.insert(tid.clone(), p.clone());
                p
            }
        };
        crate::codex::rollout_usage(&tokio::fs::read_to_string(&path).await.ok()?)
    }

    /// Token usage from a session's transcript (cached until the file changes); Codex from its rollout.
    async fn transcript_usage(&self, s: &store::Session) -> Option<claude_cli_acp::usage::Usage> {
        if s.harness == "agy" {
            let raw = self.store.lock().await.harness_metrics(&s.id).ok().flatten()?;
            let n = |a: &str, b: &str| raw[a].as_u64().or_else(|| raw[b].as_u64()).unwrap_or(0);
            return Some(claude_cli_acp::usage::Usage {
                input_tokens: n("inputTokens", "input_tokens"), output_tokens: n("outputTokens", "output_tokens"),
                cache_read_input_tokens: n("cachedReadTokens", "cache_read_tokens"),
                cache_creation_input_tokens: n("cachedWriteTokens", "cache_write_tokens"), models: vec![s.model.clone()],
                ..Default::default()
            });
        }
        if s.harness == "codex" {
            let u = self.codex_usage(s).await?;
            return Some(claude_cli_acp::usage::Usage {
                messages: u.turns, input_tokens: u.input_uncached, output_tokens: u.output,
                cache_creation_input_tokens: u.cache_write, cache_read_input_tokens: u.cached_input,
                models: vec![s.model.clone()],
            });
        }
        let (Some(hid), "claude") = (&s.harness_session_id, s.harness.as_str()) else { return None };
        let cached = self.usage_cache.lock().await.get(&s.id).cloned();
        let path = match &cached {
            Some((p, ..)) => p.clone(),
            None => self.claude.transcript_path(&SessionId::new(hid.clone())).await?,
        };
        let meta = tokio::fs::metadata(&path).await.ok()?;
        let mtime = meta.modified().ok()?;
        if let Some((_, len, t, u)) = &cached {
            if *len == meta.len() && *t == mtime {
                return Some(u.clone());
            }
        }
        let u = claude_cli_acp::usage::from_transcript(&tokio::fs::read_to_string(&path).await.unwrap_or_default());
        self.usage_cache.lock().await.insert(s.id.clone(), (path, meta.len(), mtime, u.clone()));
        Some(u)
    }

    async fn status_line(&self, s: &store::Session) -> Option<claude_cli_acp::StatusLine> {
        let (Some(hid), "claude") = (&s.harness_session_id, s.harness.as_str()) else { return None };
        self.claude.status_line(&SessionId::new(hid.clone())).await
    }

    /// Context, spend and token fields for a session row.
    /// Context and cost fields: the status line, corrected by a newer compact and
    /// with the cost summed over the session's CLI runs.
    async fn context_for(&self, s: &store::Session) -> Value {
        let sl = self.status_line(s).await;
        let mut f = context_fields(sl.as_ref());
        if s.harness == "agy" {
            if let Ok(Some(usage)) = self.store.lock().await.harness_metrics(&s.id) {
                if let Some(cost) = usage.get("cost_usd").or_else(|| usage.get("cost").filter(|c| c["currency"] == "USD")) {
                    f["cost_usd"] = cost.get("amount").unwrap_or(cost).clone();
                }
                if let Some(used) = usage.get("used") { f["context_tokens"] = used.clone(); }
                if let Some(size) = usage.get("size") { f["context_window"] = size.clone(); }
                if let (Some(used), Some(size)) = (usage["used"].as_f64(), usage["size"].as_f64().filter(|s| *s > 0.0)) {
                    f["context_used_pct"] = json!(used * 100.0 / size);
                }
            }
            return f;
        }
        let captured = sl.as_ref().map(|x| x.captured_at as i64).unwrap_or(0);
        // Codex: context from its rollout file (Codex reports no cost).
        if s.harness == "codex" {
            if let Some(u) = self.codex_usage(s).await {
                f["context_tokens"] = json!(u.context_tokens);
                f["context_window"] = json!(u.context_window);
                if let (Some(t), Some(w)) = (u.context_tokens, u.context_window.filter(|w| *w > 0)) {
                    f["context_used_pct"] = json!((t as f64 * 1000.0 / w as f64).round() / 10.0);
                }
            }
        }
        // After a compact the old size is wrong, and the summary alone underestimates it
        // (the system prompt and tools stay): report it as pending until the next turn.
        if let Some(at) = self.context_override.lock().await.get(&s.id).copied() {
            if captured <= at {
                f["context_tokens"] = Value::Null;
                f["context_used_pct"] = Value::Null;
                f["context_pending"] = json!(true);
            }
        }
        let (base, folded_at) = self.store.lock().await.cost_state(&s.id).unwrap_or((0.0, 0));
        let run = if captured > folded_at { f["cost_usd"].as_f64().unwrap_or(0.0) } else { 0.0 };
        f["cost_usd_run"] = json!(run);
        f["cost_usd"] = if base > 0.0 || sl.is_some() { json!(((base + run) * 1e6).round() / 1e6) } else { Value::Null };
        f
    }

    async fn add_metrics(&self, s: &store::Session, v: &mut Value) {
        v["permission_mode_effective"] = if matches!(s.harness.as_str(), "opencode" | "agy" | "fx" | "pi") { json!(s.permission_mode.as_deref().unwrap_or("default")) } else { json!(effective_mode(s.permission_mode.as_deref(), &claude_modes())) };
        if let (Value::Object(o), Value::Object(f)) = (&mut *v, self.context_for(s).await) {
            o.extend(f);
        }
        if s.harness == "agy" {
            let driver = self.opencode.lock().await.get(&s.id).cloned();
            let live = match (driver, &s.harness_session_id) {
                (Some(driver), Some(id)) => {
                    v["available_commands"] = driver.commands.lock().await.get(id).cloned().unwrap_or(json!([]));
                    driver.usage.lock().await.get(id).cloned()
                },
                _ => None,
            };
            if let Some(usage) = live.or(self.store.lock().await.harness_metrics(&s.id).ok().flatten()) {
                v["usage"] = usage.clone();
                for (alias, camel, snake) in [("input", "inputTokens", "input_tokens"), ("output", "outputTokens", "output_tokens"), ("cache_read", "cachedReadTokens", "cache_read_tokens")] {
                    v["usage"][alias] = usage.get(camel).or_else(|| usage.get(snake)).cloned().unwrap_or(Value::Null);
                }
                if let Some(cost) = usage.get("cost_usd").or_else(|| usage.get("cost").filter(|c| c["currency"] == "USD")) {
                    v["cost_usd"] = cost.get("amount").unwrap_or(cost).clone();
                }
                if let Some(used) = usage.get("used") { v["context_tokens"] = used.clone(); }
                if let Some(size) = usage.get("size") { v["context_window"] = size.clone(); }
                return;
            }
        }
        v["usage"] = match self.transcript_usage(s).await {
            Some(u) => json!({"input": u.input_tokens, "output": u.output_tokens, "cache_read": u.cache_read_input_tokens,
                              "cache_write": u.cache_creation_input_tokens, "messages": u.messages}),
            None => Value::Null,
        };
    }

    async fn usage(&self, name: Option<&str>) -> Result<Value> {
        let sessions = self.store.lock().await.list()?;
        let mut total = claude_cli_acp::usage::Usage::default();
        let mut per = serde_json::Map::new();
        let mut limits: Option<claude_cli_acp::StatusLine> = None;
        for s in &sessions {
            // Plan limits are account-wide: take the newest capture from any session.
            if let Some(sl) = self.status_line(s).await {
                if sl.raw.get("rate_limits").is_some_and(|r| r.is_object()) && limits.as_ref().is_none_or(|l| l.captured_at < sl.captured_at) {
                    limits = Some(sl);
                }
            }
        }
        for s in sessions.iter().filter(|s| name.is_none_or(|n| n == s.id)) {
            let Some(u) = self.transcript_usage(s).await else { continue };
            total.add(&u);
            let mut row = json!(u);
            if let Value::Object(f) = self.context_for(s).await {
                row.as_object_mut().unwrap().extend(f);
            }
            // Archived sessions share names with their replacements: label them by id.
            let label = if s.lifecycle == "archived" || per.contains_key(&s.name) { format!("{} ({}, {})", s.name, take8(&s.id), s.harness) } else { s.name.clone() };
            per.insert(label, row);
        }
        let rate_limits = match &limits {
            Some(l) => json!({"five_hour": l.raw["rate_limits"].get("five_hour"), "seven_day": l.raw["rate_limits"].get("seven_day"),
                              "captured_at": l.captured_at, "source": "Claude Code status line"}),
            None => Value::Null,
        };
        Ok(json!({"sessions": per, "total": total, "rate_limits": rate_limits,
                  "note": if limits.is_none() { "rate limits appear after a session's first turn, and only for subscription logins (not API keys)" } else { "" }}))
    }

    async fn handle(self:&Arc<Self>,req:Value)->Result<Value> {
        if let Some(caller)=req["caller"].as_str() {
            if let Some(person)=self.caller_person(caller).await? {
                // The local owner remains the trusted host operator; Discord UI calls are still explicitly scoped.
                if person!="owner" {return self.person_request(person,req).await;}
            }
        }
        self.handle_unchecked(req).await
    }
    async fn handle_unchecked(self: &Arc<Self>, req: Value) -> Result<Value> {
        let s = |k: &str| req.get(k).and_then(Value::as_str).map(String::from).ok_or_else(|| anyhow!("missing {k}"));
        let opt = |k: &str| req.get(k).and_then(Value::as_str).filter(|v| !v.is_empty()).map(String::from);
        // Calls coming from a session's MCP tools carry the caller's id (KURISU_SESSION).
        let caller = match opt("caller") {
            Some(c) => Some(self.store.lock().await.resolve_req(&c)?),
            None => None,
        };
        // Any `name` argument may be a UUID or a live name: resolve it to the id here.
        let op = s("op")?;
        // Sidecar configuration requests may arrive through the newly switched
        // public socket while the previous process still owns those sidecars.
        if op == "telegram_reload" || op == "discord_reload" || op == "browser_reload" || op.starts_with("remote_") || op.starts_with("mcp_server") {
            while !self.ready.load(std::sync::atomic::Ordering::SeqCst) {
                let notified = self.ready_notify.notified();
                if self.ready.load(std::sync::atomic::Ordering::SeqCst) { break; }
                notified.await;
            }
        }
        let key = if matches!(op.as_str(), "create" | "lead" | "mcp_connect" | "mcp_callback") { None } else { opt("name") };
        let target = match &key {
            Some(k) => Some(self.store.lock().await.resolve_req(k)?),
            None => None,
        };
        let s = |k: &str| -> Result<String> {
            if k == "name" {
                if let Some(t) = &target {
                    return Ok(t.clone());
                }
            }
            req.get(k).and_then(Value::as_str).map(String::from).ok_or_else(|| anyhow!("missing {k}"))
        };
        match op.as_str() {
            "doctor" => Ok(json!({"protocol": crate::doctor::PROTOCOL, "requested": req.get("protocol"), "build": self.version_info(), "lb_pid": self.browser.pid(), "web_build":crate::web::build_stamp(), "mcp_server": self.mcp_server_status().await})),
            "version" => Ok(self.version_info()),
            "ping" => Ok(json!({"pid": std::process::id(), "rss_kb": crate::paths::rss_kb(), "home": self.paths.home,
                "build": self.version_info()})),
            "discord_reload" => {
                if caller.is_some() { bail!("permission denied: only the user manages Discord"); }
                Ok(json!({"state":self.discord.reload(self.clone(),self.paths.home.clone()).await?}))
            }
            "discord_status" => Ok(json!({"state":self.discord.state().await,"delivery":self.channels.health()?})),
            "discord_notify" => {
                if caller.is_some() { bail!("permission denied: only the user sends notices"); }
                self.discord.notify(&s("text")?).await?; Ok(json!({"sent":true}))
            }
            "discord_pair" => {
                if caller.is_some() { bail!("permission denied: only the local user may pair people"); }
                let person=s("person")?;
                let selected=match opt("session").or(self.channels.session(&person)?) {
                    Some(id)=>self.store.lock().await.resolve_req(&id)?,
                    None=>{
                        let created=self.clone().handle_pub(json!({"op":"create","name":format!("discord-{}",&uuid::Uuid::new_v4().simple().to_string()[..10]),"role":"worker","description":format!("Discord chat for {person}")})).await?;
                        created["id"].as_str().ok_or_else(||anyhow!("No session created"))?.to_string()
                    }
                };
                let code=self.channels.mint(&person,&selected,opt("user").as_deref(),300)?;
                Ok(json!({"person":person,"session":selected,"code":code,"expires_in":300}))
            }
            "telegram_reload" => {
                if caller.is_some() {
                    bail!("permission denied: only the user manages the Telegram channel");
                }
                let st = self.telegram.reload(self.clone(), self.paths.home.clone()).await?;
                Ok(json!({"state": st}))
            }
            "telegram_status" => Ok(json!({"state": self.telegram.state().await})),
            "telegram_history" => {
                // What Kurisu itself saw in the owner's DM: the Bot API cannot
                // fetch arbitrary past messages, so this is the honest source.
                let cfg = crate::telegram::load_config(&self.paths.home)
                    .ok_or_else(|| anyhow!("Telegram is not set up on this Kurisu (ku telegram setup), so there is no chat history"))?;
                if let Some(asked) = opt("chat").filter(|c| !c.trim().is_empty()) {
                    let c = asked.trim();
                    let me = ["dm", "me", "owner", "self"].contains(&c) || c == cfg.chat_id;
                    if !me {
                        bail!("telegram_history only reads the owner's DM (chat {}), not {c}", cfg.chat_id);
                    }
                }
                let num = |k: &str| req.get(k).and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|t| t.trim().parse().ok())));
                let limit = num("limit").unwrap_or(20).clamp(1, 200);
                let q = store::TgQuery {
                    before: num("before"),
                    limit: limit + 1,
                    search: opt("search"),
                    unanswered: req.get("unanswered").is_some_and(|v| v.as_bool() == Some(true) || v.as_str() == Some("true")),
                };
                let (mut page, total) = self.store.lock().await.tg_page(&cfg.chat_id, &q)?;
                let more = page.len() as i64 > limit;
                if more {
                    page.remove(0);
                }
                let (first, last) = (page.first().map(|m| m.seq), page.last().map(|m| m.seq));
                for m in &mut page {
                    m.text = clip_text(&m.text, 4000);
                }
                Ok(json!({"chat": cfg.chat_id, "order": "oldest first",
                          "total": total, "messages": page, "has_more": more,
                          "first_seq": first, "last_seq": last}))
            }
            "remote_reload" | "remote_link" | "remote_bind" | "remote_close" | "remote_decide" | "remote_close_attempt" | "remote_offer" | "remote_enable" | "remote_disable" | "remote_declined" | "mcp_server_status" | "mcp_server_enable" | "mcp_server_disable" | "mcp_server_url" | "mcp_oauth_card" | "mcp_oauth_decide" | "mcp_oauth_revoke" if caller.is_some() => {
                bail!("permission denied: only the user manages remote access")
            }
            "remote_link" => {
                let kind = match opt("kind").as_deref() {
                    Some("browser") => "browser",
                    Some("web") => "web",
                    _ => "terminal",
                };
                // /terminal picks a directory first; validate it here, once, so a
                // link never promises a shell in a folder that is gone.
                let dir = if kind == "terminal" {
                    match opt("dir").filter(|d| !d.trim().is_empty()) {
                        Some(d) => {
                            let p = std::path::PathBuf::from(d.trim());
                            if !p.is_absolute() {
                                bail!("terminal directory {d} is not an absolute path");
                            }
                            if !p.is_dir() {
                                bail!("terminal directory {d} does not exist");
                            }
                            Some(d.trim().to_string())
                        }
                        None => None,
                    }
                } else {
                    None
                };
                if let Some(target)=req.get("channel_target") {
                    let to:crate::channels::Address=serde_json::from_value(target.clone())?;
                    if to.channel!="discord" || to.person!="owner" {bail!("remote access is owner-only");}
                    self.remote.link_for_channel(self,kind,req["direct"].as_bool().unwrap_or(false),dir.as_deref(),Some(to)).await
                }else{self.remote.link(self, kind, req["direct"].as_bool().unwrap_or(false), dir.as_deref()).await}
            }
            "remote_reload" => {
                if opt("kind").as_deref() != Some("web") { bail!("remote_reload requires kind web"); }
                self.remote.reload_web(self).await
            }
            "remote_bind" => {
                let kind = match opt("kind").as_deref() {
                    Some("browser") => "browser",
                    Some("web") => "web",
                    _ => "terminal",
                };
                let listen = s("listen")?;
                self.remote.bind(self, kind, &listen).await
            }
            "remote_close" => {
                let kind = opt("kind");
                Ok(json!({"closed": self.remote.close_all(self, kind.as_deref(), &opt("why").unwrap_or_else(|| "closed".into())).await}))
            }
            "remote_offer" => {
                // Is /terminal (/browser, /web) on? If not: where it would listen (for the enable card).
                let browser = opt("kind").as_deref() == Some("browser");
                let web = opt("kind").as_deref() == Some("web");
                let on = if browser {
                    crate::browser::active(&self.paths.home).is_some()
                } else if web {
                    crate::remote::load_web(&self.paths.home).is_some_and(|c| c.enabled)
                } else {
                    crate::remote::load(&self.paths.home).is_some_and(|c| c.enabled)
                };
                if on {
                    return Ok(json!({"enabled": true}));
                }
                let port = if browser { 8793 } else if web { crate::remote::default_web_port(&self.paths.home) } else { 8790 };
                match crate::remote::detect_address().await {
                    Some(d) => Ok(json!({"enabled": false, "where": d.describe(port), "lb_installed": browser && crate::stt::which_bin("lb").is_some()})),
                    None => Ok(json!({"enabled": false, "where": null, "error": crate::remote::NO_ADDRESS})),
                }
            }
            "remote_enable" => {
                let browser = opt("kind").as_deref() == Some("browser");
                let web = opt("kind").as_deref() == Some("web");
                let d = crate::remote::detect_address().await.context(crate::remote::NO_ADDRESS)?;
                let summary = if web {
                    let port = crate::remote::default_web_port(&self.paths.home);
                    // Bind before writing the config: the enable card must not
                    // say "Enabled" when the port is already taken.
                    self.remote.bind(self, "web", &d.listen(port)).await?;
                    let c = crate::remote::enable_web(&self.paths.home, &d, port)?;
                    format!("web chat on: {}", c.public_url.clone().unwrap_or_else(|| format!("http://{}", c.listen)))
                } else if browser {
                    let r = crate::browser::setup(&self.paths.home, None, None, 8792, None, false).await;
                    match r {
                        Ok(sum) => sum,
                        Err(e) => {
                            self.audit("user", "terminal", "remote_enable_failed", &format!("browser: {e:#}")).await;
                            return Err(e);
                        }
                    }
                } else {
                    let c = crate::remote::enable_terminal(&self.paths.home, &d, 8790)?;
                    format!("remote terminal on: {}", c.public_url.clone().unwrap_or_else(|| format!("http://{}", c.listen)))
                };
                let what = if web { "web" } else if browser { "browser" } else { "terminal" };
                self.audit("user", "terminal", "remote_enabled", &format!("{what} from Telegram: {summary}")).await;
                Ok(json!({"summary": summary}))
            }
            "remote_declined" => {
                self.audit("user", "terminal", "remote_enable_declined", &opt("kind").unwrap_or_else(|| "terminal".into())).await;
                Ok(json!({"ok": true}))
            }
            "remote_disable" => {
                let browser = opt("kind").as_deref() == Some("browser");
                let web = opt("kind").as_deref() == Some("web");
                let kind = if browser { "browser" } else if web { "web" } else { "terminal" };
                let was = if web {
                    crate::remote::disable_web(&self.paths.home)?
                } else if browser {
                    match crate::browser::load(&self.paths.home) {
                        Some(mut b) if b.enabled => {
                            b.enabled = false;
                            crate::telegram::write_private(&crate::browser::config_path(&self.paths.home), &serde_json::to_vec_pretty(&b)?)?;
                            true
                        }
                        _ => false,
                    }
                } else {
                    crate::remote::disable_terminal(&self.paths.home)?
                };
                let closed = self.remote.close_all(self, Some(kind), &format!("/{kind} disable")).await;
                if web { self.remote.reload_web(self).await?; }
                if browser {
                    self.browser.stop().await;
                }
                self.audit("user", "terminal", "remote_disabled", &format!("{kind} from Telegram")).await;
                Ok(json!({"was_on": was, "closed": closed}))
            }
            "mcp_server_status" => Ok(self.mcp_server_status().await),
            "mcp_server_enable" => {
                self.remote.enable_mcp(self).await?;
                let st = self.mcp_server_status().await;
                self.audit("user", "mcp", "mcp_server_enabled", &format!("outward MCP server on {}", st["listen"].as_str().unwrap_or(""))).await;
                Ok(st)
            }
            "mcp_server_disable" => {
                let was = self.remote.disable_mcp(self).await?;
                self.audit("user", "mcp", "mcp_server_disabled", "outward MCP server stopped").await;
                Ok(json!({"was_on": was, "status": self.mcp_server_status().await}))
            }
            "mcp_server_url" => self.mcp_server_url().await,
            // Outward MCP OAuth: the listener asks for the owner's card; the card's
            // buttons decide; the status card signs clients out. User-only (above).
            "mcp_oauth_card" => {
                let expires = req.get("expires_at").and_then(Value::as_i64).unwrap_or(0);
                self.telegram.oauth_card(&s("id")?, &s("client_name")?, &s("redirect_host")?,
                                         &opt("scope").unwrap_or_else(|| "mcp".into()), expires).await?;
                Ok(json!({"sent": true}))
            }
            "mcp_oauth_decide" => {
                let approve = req.get("approve").and_then(Value::as_bool).unwrap_or(false);
                let name = crate::mcp_out::oauth_decide(&self.paths.home, &s("id")?, approve)?;
                self.audit("user", "mcp", if approve { "mcp_oauth_approved" } else { "mcp_oauth_denied" },
                           &format!("outward MCP sign-in for {name}")).await;
                Ok(json!({"client_name": name, "approved": approve}))
            }
            "mcp_oauth_revoke" => {
                let id = opt("client_id");
                let n = crate::mcp_out::oauth_revoke(&self.paths.home, id.as_deref())?;
                self.audit("user", "mcp", "mcp_oauth_revoked", &format!("outward MCP sign-out {} ({n} tokens)", id.as_deref().unwrap_or("all"))).await;
                Ok(json!({"removed": n}))
            }
            "browser_status" => Ok(json!({"running": self.browser.running().await, "line": crate::browser::status_line(&self.paths.home, Some(self.browser.running().await))})),
            "browser_reload" => {
                if caller.is_some() {
                    bail!("permission denied");
                }
                if crate::browser::active(&self.paths.home).is_none() {
                    self.remote.close_all(self, Some("browser"), "Live Browser turned off").await;
                    self.browser.stop().await;
                }
                Ok(json!({"ok": true}))
            }
            "remote_decide" => {
                let approve = req.get("approve").and_then(Value::as_bool).unwrap_or(false);
                let ip = self.remote.decide(self, &s("attempt")?, approve).await?;
                Ok(json!({"ip": ip}))
            }
            "remote_close_attempt" => Ok(json!({"closed": self.remote.close_attempt(self, &s("attempt")?).await})),
            "telegram_notify" => {
                if caller.is_some() {
                    bail!("permission denied: only the user sends channel notices");
                }
                self.telegram.notify(&s("text")?).await?;
                Ok(json!({"sent": true}))
            }
            "import_lookup" => {
                let st = self.store.lock().await;
                let found = st.import_of(&s("source")?, &s("source_id")?)?;
                let lead = st.list()?.into_iter().find(|x| x.role == "lead" && x.lifecycle != "archived").map(|x| json!({"id": x.id, "name": x.name}));
                let taken = match opt("new_name") {
                    Some(n) => st.resolve(&n)?.is_some(),
                    None => false,
                };
                Ok(json!({"session": found.map(|(id, name)| json!({"id": id, "name": name})), "lead": lead, "name_taken": taken}))
            }
            "import_session" => {
                if caller.is_some() {
                    bail!("permission denied: only the user imports sessions");
                }
                self.import_session(&req).await
            }
            "clis" => Ok(self.clis(req.get("fresh").and_then(Value::as_bool).unwrap_or(false)).await),
            "list" => Ok(json!(self.store.lock().await.list()?)),
            "create" => {
                let name = s("name")?;
                let role = opt("role").unwrap_or_else(|| "worker".into());
                if caller.is_some() && role != "worker" {
                    bail!("sessions created by a session are workers; only the user creates a lead");
                }
                let cwd = opt("cwd").map(PathBuf::from).unwrap_or_else(|| self.paths.workspace(&name));
                if !cwd.is_absolute() {
                    bail!("cwd must be an absolute path");
                }
                let harness = match opt("harness") {
                    Some(h) => h,
                    None => self.default_harness().await?,
                };
                // Codex without a model: its own configured default.
                let model = opt("model").unwrap_or_else(|| self.default_model(&harness));
                if !["claude", "codex", "opencode", "agy", "fx", "pi"].contains(&harness.as_str()) {
                    bail!("harness must be claude, codex, opencode, agy or fx");
                }
                Self::require_cli(&harness)?;
                let description = opt("description").unwrap_or_default();
                let mode = opt("permission_mode");
                if let Some(m) = &mode {
                    if !store::PERMISSION_MODES.contains(&m.as_str()) {
                        bail!("permission mode must be one of {}", store::PERMISSION_MODES.join("|"));
                    }
                    if m == "bypassPermissions" && caller.is_some() {
                        bail!("permission denied: only the user can give a session bypassPermissions");
                    }
                }
                let created = self.store.lock().await.create(&name, &harness, &cwd.to_string_lossy(), &role, &model,
                                                             &description, caller.as_deref())?;
                if let Some(m) = &mode {
                    self.store.lock().await.update_field(caller.as_deref().unwrap_or("user"), &created.id, "permission_mode", m)?;
                }
                let mut v = json!(self.store.lock().await.get(&created.id)?.unwrap_or(created.clone()));
                // A lead created on a real home gets the browser without an MCP command;
                // workers stay off (they can be granted it by the user or their lead).
                self.default_lead_lb(&created.id, &role).await?;
                if let Some(w) = store::model_warning(&harness, &model) {
                    v["warning"] = json!(w);
                }
                Ok(v)
            }
            "models" => {
                let harness = opt("harness").unwrap_or_else(|| "claude".into());
                if harness == "opencode" {
                    let (models, note) = self.opencode_models().await;
                    let mut out = json!({"harness": harness, "models": models, "note": note});
                    // A selected model still needs ACP's model-specific config
                    // (effort levels); the ids themselves remain the CLI's.
                    if let Some(model) = opt("model") {
                        match self.opencode().await {
                            Ok(d) => match d.discover(self.paths.home.clone(), Some(&model)).await {
                                Ok(v) => {
                                    let configs = v["configOptions"].as_array().cloned().unwrap_or_default();
                                    out["config_options"] = json!(configs);
                                }
                                Err(e) => out["note"] = json!(format!("{note}; effort discovery via OpenCode ACP failed: {e:#}")),
                            },
                            Err(e) => out["note"] = json!(format!("{note}; effort discovery via OpenCode ACP failed: {e:#}")),
                        }
                    }
                    return Ok(out);
                }
                let list: Vec<Value> = store::model_catalogue(&harness).into_iter()
                    .map(|(id, label)| json!({"id": id, "label": label, "verified": true, "default": id == store::default_model(&harness)})).collect();
                let note = match harness.as_str() {
                    "claude" => "other claude-* ids are accepted with an unverified-model warning",
                    "pi" => "pi provider/model ids from `pi --list-models` (ollama/deepseek-v4.1-flash:cloud); `default` uses pi's saved choice; effort maps to pi thinking levels",
                    "fx" => "provider/model ids (codex/gpt-6-luna, ollama/deepseek-v4.1-flash:cloud); the active provider's models come from `fx models`, any provider/model fx knows is accepted; `default` uses fx's saved choice",
                    "agy" => "model ids from `agy models` on this account (cached 5 min, refreshed on re-login); effort maps to thought_level high|medium|low (default high)",
                    "codex" => "Codex model ids from `codex debug models`; other ids (for example a custom provider's) are also accepted",
                    _ => "no catalogue for this harness yet: any model id is accepted",
                };
                Ok(json!({"harness": harness, "models": list, "note": note}))
            }
            "lead_get" => {
                // The user's Lead: the bridge's default session. Created on request.
                let st = self.store.lock().await;
                let leads: Vec<store::Session> = st.list()?.into_iter().filter(|x| x.role == "lead" && x.lifecycle != "archived").collect();
                let pick = leads.iter().find(|x| x.name == "lead").or(leads.first()).cloned();
                match pick {
                    Some(x) => Ok(json!(x)),
                    None if req.get("create").and_then(Value::as_bool).unwrap_or(false) => {
                        drop(st);
                        let harness = match opt("harness") {
                            Some(h) => h,
                            None => self.default_harness().await?,
                        };
                        Self::require_cli(&harness)?;
                        let model = opt("model").unwrap_or_else(|| self.default_model(&harness));
                        Ok(json!(self.store.lock().await.create("lead", &harness, &self.paths.workspace("lead").to_string_lossy(), "lead", &model,
                                                                "Lead: talks to the user, delegates to peers, reports back", None)?))
                    }
                    None => bail!("no lead yet (create it with create:true, or POST /lead with a task)"),
                }
            }
            "messages_after" => {
                let after = req.get("after").and_then(Value::as_i64).unwrap_or(0);
                let limit = req.get("limit").and_then(Value::as_i64).unwrap_or(200).clamp(1, 1000);
                let st = self.store.lock().await;
                Ok(json!({"messages": st.messages_after_named(after, limit)?, "msg_seq": st.max_message_seq()?}))
            }
            "lead" => {
                // Start (or reuse) the user's Lead and give it a task.
                let existing = self.store.lock().await.resolve(&opt("lead").unwrap_or_else(|| "lead".into()))?;
                let lead = match existing {
                    Some(id) => id,
                    None => {
                        let harness = match opt("harness") {
                            Some(h) => h,
                            None => self.default_harness().await?,
                        };
                        if !["claude", "codex", "opencode", "agy", "fx", "pi"].contains(&harness.as_str()) {
                            bail!("harness must be claude, codex, opencode, agy or fx");
                        }
                        Self::require_cli(&harness)?;
                        let model = opt("model").unwrap_or_else(|| self.default_model(&harness));
                        self.store.lock().await.create("lead", &harness, &self.paths.workspace("lead").to_string_lossy(), "lead", &model,
                                                       "Lead: talks to the user, delegates to peers, reports back", None)?
                            .id
                    }
                };
                self.default_lead_lb(&lead, "lead").await?;
                self.send(&lead, &s("text")?, "user", "user").await
            }
            "update" => {
                let target = s("name")?;
                let fields = req.get("fields").and_then(Value::as_object).cloned().unwrap_or_default();
                if let Some(h) = fields.get("harness").and_then(Value::as_str) {
                    if let Some(k) = fields.keys().find(|k| !["harness", "model"].contains(&k.as_str())) {
                        bail!("change the harness on its own (with an optional model); {k} cannot be changed in the same update");
                    }
                    let model = fields.get("model").and_then(Value::as_str);
                    return self.reharness(caller.as_deref(), &target, h, model, true, false).await;
                }
                self.update(caller.as_deref(), &target, &fields).await
            }
            "reharness" => {
                let target = s("name")?;
                let seed = req.get("seed").and_then(Value::as_bool).unwrap_or(true);
                let wait = req.get("wait").and_then(Value::as_bool).unwrap_or(false);
                self.reharness(caller.as_deref(), &target, &s("harness")?, opt("model").as_deref(), seed, wait).await
            }
            "audit" => Ok(json!(self.store.lock().await.audit(target.as_deref())?)),
            "changes" => {
                // Incremental sync for live clients: new messages and audit rows since the
                // given global sequence numbers, plus the current session list.
                let st = self.store.lock().await;
                let msg_after = req.get("msg_after").and_then(Value::as_i64).unwrap_or(-1);
                let audit_after = req.get("audit_after").and_then(Value::as_i64).unwrap_or(-1);
                let (messages, audit) = if msg_after < 0 || audit_after < 0 {
                    (json!([]), json!([]))
                } else {
                    (json!(st.messages_after(msg_after, 500)?), json!(st.audit_after(None, audit_after)?))
                };
                Ok(json!({"msg_seq": st.max_message_seq()?, "audit_seq": st.max_audit_seq()?, "messages": messages, "audit": audit,
                          "approvals": st.approvals(None, false)?}))
            }
            "peers" | "list_full" => {
                let st = self.store.lock().await;
                let all = req.get("include_archived").and_then(Value::as_bool).unwrap_or(false);
                let mut out = vec![];
                for x in st.list()? {
                    if x.lifecycle == "archived" && !all {
                        continue;
                    }
                    let mut v = json!(x);
                    v["children"] = json!(st.children(&x.id)?);
                    v["replies"] = json!(st.reply_count(&x.id)?);
                    if let Some((seq, role, text, at)) = st.last_message(&x.id)? {
                        let preview: String = text.chars().take(120).collect();
                        v["last"] = json!({"seq": seq, "role": role, "text": preview, "at": at});
                    }
                    out.push((x, v));
                }
                drop(st);
                let mut rows = vec![];
                for (x, mut v) in out {
                    self.add_metrics(&x, &mut v).await;
                    v["activity"] = self.activity.lock().await.get(&x.id).cloned().unwrap_or(Value::Null);
                    rows.push(v);
                }
                Ok(json!(rows))
            }
            "status" => {
                let st = self.store.lock().await;
                let name = s("name")?;
                let x = st.get(&name)?.ok_or_else(|| anyhow!("no session {name}"))?;
                let mut v = json!(x);
                v["children"] = json!(st.children(&name)?);
                drop(st);
                self.add_metrics(&x, &mut v).await;
                v["activity"] = self.activity.lock().await.get(&x.id).cloned().unwrap_or(Value::Null);
                Ok(v)
            }
            "history" => {
                let st = self.store.lock().await;
                match req.get("tail").and_then(Value::as_i64) {
                    Some(tail) => {
                        let offset = req.get("offset").and_then(Value::as_i64).unwrap_or(0);
                        Ok(json!(st.history_tail(&s("name")?, tail.clamp(1, 200), offset.max(0))?))
                    }
                    None if caller.is_some() => Ok(json!(st.history_tail(&s("name")?, 20, 0)?)),
                    None if req.get("tools").and_then(Value::as_bool) == Some(true) => Ok(json!(st.history_with_tools(&s("name")?, req.get("after").and_then(Value::as_i64).unwrap_or(0), 500)?)),
                    None => Ok(json!(st.history(&s("name")?, req.get("after").and_then(Value::as_i64).unwrap_or(0), 500)?)),
                }
            }
            "wake_add" => {
                let target = s("name")?;
                self.wake_add(caller.as_deref(), &target, opt("at").as_deref(), opt("repeat").as_deref(), &s("message")?).await
            }
            "wakes" => {
                let st = self.store.lock().await;
                let all = req.get("all").and_then(Value::as_bool).unwrap_or(false);
                let mut list = st.wakes(None, all)?;
                if let Some(n) = target.as_ref() {
                    list.retain(|w| &w.session == n);
                }
                if let Some(c) = caller.as_deref() {
                    // A session sees wakes for itself and its descendants, and the ones it set.
                    let mut keep = vec![];
                    for w in list {
                        if w.session == c || w.created_by == c || st.is_ancestor(c, &w.session)? {
                            keep.push(w);
                        }
                    }
                    list = keep;
                }
                Ok(json!(list.iter().map(Self::wake_json).collect::<Vec<_>>()))
            }
            "bg_run" => {
                let name = target.clone().ok_or_else(|| anyhow!("missing name"))?;
                if caller.as_deref().is_some_and(|c| c != name) {
                    bail!("permission denied: a session starts background jobs only for itself");
                }
                self.bg_run(&name, &s("command")?, opt("cwd").as_deref()).await
            }
            "wake_cancel" => {
                let id = req.get("id").and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|t| t.trim_start_matches('#').parse().ok())))
                    .ok_or_else(|| anyhow!("missing id"))?;
                let st = self.store.lock().await;
                let w = st.wake_get(id)?.ok_or_else(|| anyhow!("no wake #{id}"))?;
                if let Some(c) = caller.as_deref() {
                    if !(w.created_by == c || w.session == c || st.is_ancestor(c, &w.session)?) {
                        bail!("permission denied: wake #{id} is not for you or a session you created");
                    }
                }
                if !st.wake_cancel(id)? {
                    bail!("wake #{id} is {}, not pending", w.status);
                }
                let actor = caller.clone().unwrap_or_else(|| "user".into());
                st.log_event(&actor, &w.session, "wake_cancelled", &format!("wake #{id}"))?;
                Ok(json!({"cancelled": id, "session": w.session, "session_name": w.session_name}))
            }
            "history_page" => {
                let name = s("name")?;
                // Agents sometimes pass numbers and booleans as strings.
                let num = |k: &str| req.get(k).and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|t| t.trim().parse().ok())));
                let limit = num("limit").or(num("tail")).unwrap_or(30).clamp(1, 200);
                let roles: Vec<String> = match req.get("roles") {
                    Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).map(String::from).collect(),
                    Some(Value::String(t)) => t.split(',').map(str::trim).filter(|x| !x.is_empty()).map(String::from).collect(),
                    _ => vec![],
                };
                let q = store::HistoryQuery {
                    before: num("before"),
                    after: num("after"),
                    limit: limit + 1,
                    search: opt("search"),
                    roles,
                    tools: req.get("tools").is_some_and(|v| v.as_bool() == Some(true) || v.as_str() == Some("true")),
                };
                let max_chars = num("max_chars").map(|n| n.max(50) as usize);
                let st = self.store.lock().await;
                let x = st.get(&name)?.ok_or_else(|| anyhow!("no session {name}"))?;
                let (mut page, total) = st.history_page(&name, &q)?;
                let forward = q.after.is_some() && q.before.is_none();
                let more = page.len() as i64 > limit;
                if more {
                    if forward { page.pop(); } else { page.remove(0); }
                }
                if let Some(m) = max_chars {
                    for msg in &mut page {
                        msg.text = clip_text(&msg.text, m);
                        if let Some(d) = msg.data.as_mut() {
                            for k in ["input", "output"] {
                                if let Some(v) = d.get(k).cloned() {
                                    d[k] = clip_json(&v, m);
                                }
                            }
                        }
                    }
                }
                let (first, last) = (page.first().map(|m| m.seq), page.last().map(|m| m.seq));
                Ok(json!({"session": {"id": x.id, "name": x.name, "harness": x.harness, "model": x.model},
                          "total": total, "messages": page,
                          "has_more": more, "direction": if forward { "after" } else { "before" },
                          "first_seq": first, "last_seq": last}))
            }
            "handoff" => {
                let max = req.get("max_chars").and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|t| t.parse().ok()))).unwrap_or(24000).clamp(500, 200_000) as usize;
                self.handoff(&s("name")?, max).await
            }
            "set_description" => {
                let name = s("name")?;
                self.store.lock().await.set_description(&name, &s("text")?)?;
                Ok(json!({"ok": name}))
            }
            "set_note" => {
                let name = s("name")?;
                self.store.lock().await.set_note(&name, &s("text")?)?;
                Ok(json!({"ok": name}))
            }
            "board_post" => {
                let author = match &caller {
                    Some(c) => self.store.lock().await.get(c)?.map(|x| x.name).unwrap_or_default(),
                    None => "user".into(),
                };
                Ok(json!({"seq": self.store.lock().await.board_post(&author, &s("text")?)?}))
            }
            "board_read" => {
                let tail = req.get("tail").and_then(Value::as_i64).unwrap_or(20).clamp(1, 200);
                Ok(json!(self.store.lock().await.board_read(tail)?))
            }
            "send" => {
                let name = s("name")?;
                if caller.as_deref() == Some(name.as_str()) {
                    bail!("a session cannot send to itself");
                }
                let from = match &caller {
                    Some(c) => format!("peer:{}", self.store.lock().await.get(c)?.map(|x| x.name).unwrap_or_default()),
                    None => "user".into(),
                };
                let actor = caller.clone().unwrap_or_else(|| "user".into());
                let text = self.attach(&name, &req, &s("text").unwrap_or_default()).await?;
                if text.trim().is_empty() {
                    bail!("missing text");
                }
                let wait = req.get("wait").and_then(Value::as_bool).unwrap_or(true);
                // The Telegram update this prompt came from (user only): /undo deletes its messages.
                let tg_update = req.get("telegram_update").and_then(Value::as_i64).filter(|_| caller.is_none());
                if let (Some(u), false) = (tg_update, wait) {
                    let steer = opt("mode").as_deref() == Some("steer");
                    let v = self.send_async_notify(&name, &text, &from, &actor, steer, None).await?;
                    if let Some(seq) = v["seq"].as_i64() {
                        self.store.lock().await.merge_message_data(seq, &json!({"telegram_update": u}))?;
                    }
                    return Ok(v);
                }
                let steer = opt("mode").as_deref() == Some("steer");
                if let Some(m) = opt("mode").filter(|m| m != "steer" && m != "queue") {
                    bail!("unknown send mode {m:?} (queue or steer)");
                }
                // wait:false from a session: a callback when the turn ends, unless notify:false.
                let notify = req.get("notify").and_then(Value::as_bool).unwrap_or(true);
                let cb = caller.as_deref().filter(|_| !wait && notify);
                if !wait || steer {
                    if let Some(event)=opt("channel_event") {
                        if caller.is_some() { bail!("channel event IDs are user-only"); }
                        self.send_async_channel(&name,&text,&from,&actor,steer,cb,Some(&event)).await
                    } else { self.send_async_notify(&name, &text, &from, &actor, steer, cb).await }
                } else {
                    self.send(&name, &text, &from, &actor).await
                }
            }
            "usage" => self.usage(target.as_deref()).await,
            // Telegram /usage and the kurisu-mcp `usage` tool: one report.
            "usage_report" => {
                let raw = self.usage(None).await?;
                let claude = crate::hostcheck::which("claude").is_some();
                let codex_installed = crate::hostcheck::which("codex").is_some();
                let codex = async {
                    if !codex_installed { return None; }
                    // Spawn/initialize is bounded too, including installed but broken CLIs.
                    let result = match tokio::time::timeout(std::time::Duration::from_secs(6), self.codex()).await {
                        Ok(Ok(driver)) => driver.rate_limits().await.ok(),
                        _ => None,
                    };
                    Some(match result {
                        Some(v) => crate::telegram::shape_codex_usage(&v, now()),
                        None => crate::telegram::codex_usage_unavailable(),
                    })
                };
                let estimate = async { if claude { crate::telegram::local_usage_estimate().await } else { None } };
                let agy = async {
                    let configured = crate::agy::bin();
                    let bin = crate::hostcheck::which("agy")
                        .or_else(|| std::path::Path::new(&configured).is_file().then(|| configured.clone()));
                    match bin { Some(bin) => crate::agy::quota(&bin, now()).await, None => None }
                };
                let (codex, estimate, agy) = tokio::join!(codex, estimate, agy);
                let mut report=crate::telegram::usage_report(&raw, estimate, claude, codex, agy, now());
                if crate::opencode::find_bin().is_some() {
                    let note="OpenCode: provider quota unavailable through ACP; check the provider console. No quota or cost is inferred.";
                    report["harnesses"]["opencode"]=json!({"source":"opencode_acp","confidence":"unavailable","note":note});
                    for key in ["summary","text"] {
                        let prior=report[key].as_str().unwrap_or("");
                        report[key]=json!(if !claude && report["harnesses"].get("codex").is_none() {note.to_string()} else {format!("{prior}\n\n{note}")});
                    }
                }
                Ok(report)
            }
            "mcp_list" => {
                let id = s("name")?;
                let sess = self.store.lock().await.get(&id)?.ok_or_else(|| anyhow!("no session {id}"))?;
                let rows = self.store.lock().await.mcp_list(&sess.id)?;
                let available: HashSet<String> = self.runtime_mcp_rows(&sess.id).await?.into_iter()
                    .filter(|(_, _, on)| *on).map(|(name, _, _)| name).collect();
                let (fp, _) = self.toolset_state(&sess.id).await?;
                // "enabled" is what the store says; "loaded" is what the running CLI actually
                // has. An ACP session keeps the toolset it was created with, so a change is
                // pending until the next turn starts a new one: never report a tool it lacks.
                let live = { let l = self.live.lock().await; l.contains(&sess.name) || l.contains(&sess.id) };
                let persisted = self.store.lock().await.launch_fp(&sess.id)?;
                let current = live && persisted.as_deref() == Some(fp.as_str());
                let list: Vec<Value> = rows.iter()
                    .map(|(n, c, e)| {
                        let auth = self.mcp_auth_state(&sess.id, n, Self::needs_oauth_token(c));
                        json!({"name": n, "enabled": e, "loaded": *e && current && available.contains(n), "config": redact_mcp(c), "auth": auth, "auth_state": auth.as_str()})
                    }).collect();
                let builtin = vec![crate::mcp::SERVER_NAME.to_string()];
                let note = if live && !current {
                    "its CLI is already running with an older toolset: the change applies from its next turn"
                } else {
                    "changes apply from the session's next turn (it starts a new CLI conversation with the current tools)"
                };
                Ok(json!({"builtin": builtin, "servers": list, "live": live, "loaded": current, "note": note}))
            }
            "mcp_set" | "mcp_remove" => {
                if caller.is_some() {
                    bail!("permission denied: only the user changes a session's MCP servers");
                }
                let name = s("name")?;
                let server = s("server")?;
                if server == LB && op == "mcp_set" {
                    // Live Browser: Kurisu builds its config; only on/off (default on when added).
                    let on = req.get("enabled").and_then(Value::as_bool).unwrap_or(true);
                    let changed = self.set_session_lb("user", &name, on).await?;
                    let auth = store::AuthState::None;
                    return Ok(json!({"name": name, "server": server, "changed": changed, "enabled": on, "auth": auth, "auth_state": auth.as_str(), "note": "applies from the session's next turn"}));
                }
                let st = self.store.lock().await;
                let old_url = st.mcp_list(&name)?.into_iter().find(|(n, ..)| n == &server).map(|(_, c, _)| c["url"].clone());
                let endpoint_changed = req.get("config").is_some_and(|c| old_url.as_ref().is_some_and(|url| *url != c["url"]));
                let (changed, what) = if op == "mcp_remove" {
                    (st.mcp_remove(&name, &server)?, "mcp_removed".to_string())
                } else {
                    let config = req.get("config").filter(|c| !c.is_null());
                    if let Some(c) = config {
                        validate_mcp(&server, c)?;
                    } else if server == crate::mcp::SERVER_NAME {
                        bail!("{server} is Kurisu's own server and is always on");
                    }
                    let enabled = req.get("enabled").and_then(Value::as_bool);
                    let existed = st.mcp_list(&name)?.iter().any(|(n, ..)| n == &server);
                    let what = if !existed { "mcp_added" } else if enabled == Some(false) { "mcp_disabled" } else if enabled == Some(true) { "mcp_enabled" } else { "mcp_changed" };
                    (st.mcp_set(&name, &server, config, enabled)?, what.to_string())
                };
                let is_remote = if op == "mcp_remove" {
                    false
                } else {
                    st.mcp_list(&name)?
                        .into_iter()
                        .find(|(n, ..)| n == &server)
                        .map(|(_, c, _)| Self::needs_oauth_token(&c))
                        .unwrap_or(false)
                };
                if changed {
                    st.log_event("user", &name, &what, &server)?;
                    self.relaunch.lock().await.insert(name.clone());
                }
                drop(st);
                if endpoint_changed || op == "mcp_remove" || req.get("enabled").and_then(Value::as_bool) == Some(false) {
                    self.paths.scrub_mcp_credentials(&name, &server);
                }
                let auth = self.mcp_auth_state(&name, &server, is_remote);
                Ok(json!({"name": name, "server": server, "changed": changed, "auth": auth, "auth_state": auth.as_str(), "note": "applies from the session's next turn"}))
            }
            "mcp_disable_all" => {
                if caller.is_some() {
                    bail!("permission denied: only the user changes a session's MCP servers");
                }
                let server = s("server")?;
                let count = self.mcp_disable_all(&server).await?;
                Ok(json!({"server": server, "count": count}))
            }
            "mcp_connect" => self.mcp_connect(caller.as_deref(), &req).await,
            "mcp_callback" => self.mcp_callback(caller.as_deref(), &req).await,
            "undo" => {
                let name = s("name")?;
                if let Some(c) = &caller {
                    if !self.store.lock().await.is_ancestor(c, &name)? {
                        bail!("permission denied: only the user or an ancestor may undo a session's turn");
                    }
                }
                let name = self.store.lock().await.resolve_req(&name)?;
                self.undo(&name, caller.as_deref().unwrap_or("user")).await
            }
            "compact" => {
                let name = s("name")?;
                if let Some(c) = &caller {
                    if !self.store.lock().await.is_ancestor(c, &name)? {
                        bail!("permission denied: only the user or an ancestor may compact a session");
                    }
                }
                self.compact(&name, caller.as_deref().unwrap_or("user")).await
            }
            "upload" => {
                let name = s("name")?;
                let path = self.save_upload(&name, &opt("filename").unwrap_or_else(|| "file".into()), &s("data_b64")?).await?;
                let size = std::fs::metadata(&path)?.len();
                let mut v = json!({"path": path, "size": size});
                if let (Some(cfg), true) = (crate::stt::active(&self.paths.home), crate::stt::is_audio(&path.to_string_lossy())) {
                    match crate::stt::transcribe(&cfg, &path).await {
                        Ok(t) => v["transcript"] = json!(t),
                        Err(e) => v["transcript_error"] = json!(format!("{e:#}")),
                    }
                }
                Ok(v)
            }
            "transcribe" => self.transcribe(caller.as_deref(), &s("path")?).await,
            "send_file" => self.send_file(caller.as_deref(), &s("path")?, opt("caption")).await,
            "stt_status" => {
                let cfg = crate::stt::active(&self.paths.home);
                Ok(json!({"enabled": cfg.is_some(), "config": cfg, "audio_extensions": crate::stt::AUDIO_EXTS}))
            }
            "approvals" => {
                let all = req.get("all").and_then(Value::as_bool).unwrap_or(false);
                Ok(json!(self.store.lock().await.approvals(target.as_deref(), all)?))
            }
            "approve" | "deny" => {
                if caller.is_some() {
                    bail!("permission denied: only the user answers approvals");
                }
                let always = req.get("always").and_then(Value::as_bool).unwrap_or(false);
                let session_wide = req.get("session_wide").and_then(Value::as_bool).unwrap_or(false);
                self.decide(&s("id")?, op == "approve", always, session_wide, opt("reason")).await
            }
            "ask_user" => {
                let caller = caller.ok_or_else(|| anyhow!("ask_user is a session tool (kurisu-mcp)"))?;
                self.ask_user(&caller, &req).await
            }
            "answer" => {
                if caller.is_some() {
                    bail!("permission denied: only the user answers questions");
                }
                let answers = req.get("answers").cloned().ok_or_else(|| anyhow!("missing answers"))?;
                self.answer(&s("id")?, &answers).await
            }
            "rules" => Ok(json!(self.store.lock().await.rules()?)),
            "rule_add" => {
                if caller.is_some() {
                    bail!("permission denied: only the user sets approval rules");
                }
                let st = self.store.lock().await;
                let scope = opt("scope").unwrap_or_else(|| "role".into());
                let raw = s("target")?;
                let target_id = if scope == "session" { st.resolve_req(&raw)? } else { raw.clone() };
                let in_cwd = req.get("in_cwd").and_then(Value::as_bool).unwrap_or(false);
                let decision = opt("decision").unwrap_or_else(|| "allow".into());
                let r = st.rule_add(&scope, &target_id, &s("tool")?, in_cwd, opt("command_prefix").as_deref(), &decision, "user")?;
                let detail = format!("#{} {} {} for {} {}{}{}", r.id, r.decision, r.tool, scope, raw,
                                     if in_cwd { " in its folder" } else { "" },
                                     r.command_prefix.as_deref().map(|p| format!(" (command starts with {p:?})")).unwrap_or_default());
                // Audit rows need a session target: record role rules against every session with that role.
                let targets: Vec<String> = if scope == "session" { vec![target_id.clone()] }
                    else { st.list()?.into_iter().filter(|x| x.role == raw).map(|x| x.id).collect() };
                for t in targets {
                    st.log_event("user", &t, "rule_added", &detail)?;
                }
                Ok(json!(r))
            }
            "rule_remove" => {
                if caller.is_some() {
                    bail!("permission denied: only the user sets approval rules");
                }
                let id = req.get("id").and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|x| x.trim_start_matches('#').parse().ok())))
                    .ok_or_else(|| anyhow!("missing rule id"))?;
                let st = self.store.lock().await;
                let rule = st.rules()?.into_iter().find(|r| r.id == id).ok_or_else(|| anyhow!("no rule #{id}"))?;
                st.rule_remove(id)?;
                let targets: Vec<String> = if rule.scope == "session" { vec![rule.target.clone()] }
                    else { st.list()?.into_iter().filter(|x| x.role == rule.target).map(|x| x.id).collect() };
                for t in targets {
                    st.log_event("user", &t, "rule_removed", &format!("#{} {} {}", id, rule.decision, rule.tool))?;
                }
                Ok(json!({"removed": id}))
            }
            "stop" => {
                let name = s("name")?;
                let session=self.store.lock().await.get(&name)?;
                if let Some(session)=session { self.close_driver(&session).await; }
                let was_live = self.live.lock().await.remove(&name);
                self.cancel_pending(&name, "the session was stopped").await;
                // A running turn notices the closed CLI and ends; wait for it to release the session.
                let was_busy = self.is_busy(&name).await;
                for _ in 0..40 {
                    if !self.is_busy(&name).await {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                }
                self.activity.lock().await.remove(&name);
                let st = self.store.lock().await;
                if st.get(&name)?.is_some_and(|x| x.harness_session_id.is_some()) {
                    st.set_status(&name, "stopped")?;
                }
                if was_live {
                    st.log_event(caller.as_deref().unwrap_or("user"), &name, "stopped", "")?;
                }
                let display = st.get(&name)?.map(|x| x.name).unwrap_or_default();
                Ok(json!({"stopped": name, "name": display, "was_running": was_live, "turn_cancelled": was_busy}))
            }
            "interrupt" => {
                let name = s("name")?;
                let (id, harness, display, status) = {
                    let st = self.store.lock().await;
                    let x = st.get(&name)?.ok_or_else(|| anyhow!("no session {name}"))?;
                    (x.harness_session_id.clone(), x.harness.clone(), x.name.clone(), x.status.clone())
                };
                let sid = self.store.lock().await.get(&name)?.map(|x| x.id).unwrap_or_else(|| name.clone());
                // `send` records first and starts the turn in the background; give that
                // worker a moment to take the busy lock before deciding it is idle.
                let mut busy = self.is_busy(&name).await;
                if !busy && matches!(status.as_str(), "starting" | "working") {
                    for _ in 0..20 {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        if self.is_busy(&name).await {
                            busy = true;
                            break;
                        }
                    }
                }
                if !busy {
                    return Ok(json!({"id": sid, "name": display, "interrupted": false, "stopped": true, "reason": "idle"}));
                }
                let gate = self.turn_gate(&name).await;
                let _interrupt = gate.lock().await;
                let target = self.active_turns.lock().await.get(&name).copied();
                if target.is_none() {
                    return Ok(json!({"id": sid, "name": display, "interrupted": false, "stopped": true, "reason": "idle"}));
                }
                let Some(hid) = id else {
                    bail!("{name} has no CLI session to interrupt");
                };
                let cli = SessionId::new(hid);
                let sent = match harness.as_str() {
                    "claude" => self.claude.cancel(&cli).await,
                    "opencode" | "agy" | "fx" | "pi" => self.opencode_for(&name).await?.cancel(&cli).await,
                    _ => self.codex().await?.cancel(&cli).await,
                };
                if let Err(e) = &sent {
                    eprintln!("interrupt {name} failed: {e:#}");
                }
                self.cancel_pending(&name, "the turn was stopped").await;
                let mut stopped = false;
                for _ in 0..50 {
                    if self.active_turns.lock().await.get(&name).copied() != target {
                        stopped = true;
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                // If the graceful cancel did not land, fall back to stopping the Claude
                // CLI; that is the old `stop` behaviour and always ends the turn.
                if !stopped && harness == "claude" {
                    let _ = self.claude.close(&cli).await;
                    self.live.lock().await.remove(&name);
                    for _ in 0..20 {
                        if self.active_turns.lock().await.get(&name).copied() != target {
                            stopped = true;
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
                // OpenCode may accept session/cancel without ever replying to
                // session/prompt. Its process is isolated per Kurisu session:
                // kill/reap it, unblock the prompt, and reload its persisted ACP
                // session on the next send. Do not merely clear the busy lock.
                if !stopped && matches!(harness.as_str(), "opencode" | "agy" | "fx" | "pi") {
                    let driver = self.opencode.lock().await.get(&name).cloned();
                    if let Some(driver) = driver {
                        driver.stop_stuck_turn().await?;
                        self.live.lock().await.remove(&name);
                        for _ in 0..20 {
                            if self.active_turns.lock().await.get(&name).copied() != target {
                                stopped = true;
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                    }
                }
                if !stopped {
                    bail!("{name} is still running after the interrupt");
                }
                self.activity.lock().await.remove(&name);
                let mut closed = vec![];
                {
                    let st = self.store.lock().await;
                    if let Ok(rows) = st.running_tool_rows(&sid) {
                        for (seq, mut data) in rows {
                            data["status"] = json!("cancelled");
                            let _ = st.set_message_data(seq, &data);
                            closed.push((seq, data));
                        }
                    }
                }
                for (seq, data) in closed {
                    self.publish(json!({"type": "tool", "session": sid, "tool_call_id": data["tool_call_id"],
                                        "status": "cancelled", "seq": seq}));
                }
                Ok(json!({"id": sid, "name": display, "interrupted": sent.is_ok(), "stopped": stopped}))
            }
            "deploy_web_fd" => {
                if caller.is_some() { bail!("permission denied"); }
                Ok(json!({"exported":self.remote.export_web_fd(std::path::Path::new(&s("receiver")?)).await?}))
            }
            "deploy_web_sessions" => {
                if caller.is_some() || !self.standby.load(std::sync::atomic::Ordering::SeqCst) { bail!("permission denied"); }
                self.remote.import_web_sessions(&req["sessions"]).await?;
                Ok(json!({"imported":true}))
            }
            "drain" => {
                // Stop accepting new clients, release the sidecars that must be unique
                // (Telegram poller, web listener, browser), let in-flight turns finish,
                // then stop. A deploy's new server takes over after this returns.
                self.stop_accept.notify_waiters();
                self.stop_accept.notify_one();
                let started = std::time::Instant::now();
                let timeout = req["timeout"].as_u64().unwrap_or(DRAIN_TIMEOUT_SECS).min(300);
                let busy = self.wait_idle(std::time::Duration::from_secs(timeout)).await;
                let web_sessions = self.remote.web_sessions().await;
                self.telegram.stop().await;
                self.discord.stop().await;
                crate::deploy::record_event(&self.paths, "poller_stopped");
                self.remote.stop().await;
                self.browser.stop().await;
                self.stop.notify_one();
                Ok(json!({"drained": busy.is_empty(), "web_sessions":web_sessions, "still_busy": busy, "waited_ms": started.elapsed().as_millis() as u64}))
            }
            "promote" => self.promote().await,
            "deploy_state" => Ok(json!({
                "pid": std::process::id(),
                "standby": self.standby.load(std::sync::atomic::Ordering::SeqCst),
                "ready": self.ready.load(std::sync::atomic::Ordering::SeqCst),
                "telegram": self.telegram.state().await,
                "socket": self.paths.socket(),
                "build": self.version_info(),
            })),
            "shutdown" => {
                self.stop.notify_one();
                Ok(json!({"shutting_down": true}))
            }
            op => bail!("unknown op {op}"),
        }
    }

    /// Switch a session to another harness. A CLI's live context cannot move, so this
    /// archives the session and creates a new one with the same name, role, description,
    /// folder and parent on the target harness; children, pending wakes, MCP servers and
    /// session rules move over; both sessions are linked (`replaces` / `replaced_by`).
    /// With `seed`, the new session's first message asks it to read the old one's history
    /// (session_handoff) and carry on. Same authority as session_update.
    async fn reharness(self: &Arc<Self>, caller: Option<&str>, target: &str, harness: &str, model: Option<&str>,
                       seed: bool, wait: bool) -> Result<Value> {
        let actor = caller.unwrap_or("user").to_string();
        let mut self_switch = false;
        let old = {
            let st = self.store.lock().await;
            let old = st.get(target)?.ok_or_else(|| anyhow!("no session {target}"))?;
            if let Some(c) = caller {
                if c == target {
                    self_switch = true;
                } else if !st.is_ancestor(c, target)? {
                    let n = |id: &str| st.get(id).ok().flatten().map(|x| x.name).unwrap_or_else(|| id.to_string());
                    bail!("permission denied: {} is not an ancestor of {} (only the user or a session's creator chain may switch its harness)", n(c), n(target));
                }
            }
            old
        };
        if !["claude", "codex", "opencode", "agy", "fx", "pi"].contains(&harness) {
            bail!("harness must be claude, codex, opencode, agy or fx");
        }
        if harness == old.harness {
            bail!("{} already runs on {harness} (to change its model, update model instead)", old.name);
        }
        // Before archiving anything: the target CLI must be installed.
        Self::require_cli(harness).map_err(|e| anyhow!("cannot switch {} to {harness}: {e:#}", old.name))?;
        if old.lifecycle == "archived" {
            bail!("{} is archived", old.name);
        }
        if self_switch {
            // The caller's own turn is running: switch right after it ends (server tick).
            store::validate_model(harness, &model.map(String::from).unwrap_or_else(|| if harness == "claude" { store::DEFAULT_MODEL.into() } else { "default".into() }))?;
            self.pending_switch.lock().await.insert(target.to_string(), (harness.to_string(), model.map(String::from), actor.clone()));
            self.store.lock().await.log_event(&actor, target, "reharness_scheduled", &format!("{} -> {harness} after the current turn", old.harness))?;
            return Ok(json!({"scheduled": true, "id": old.id, "name": old.name, "from": old.harness, "to": harness,
                             "note": format!("Scheduled: right after this reply ends, Kurisu archives this session and starts a replacement named {} \
on {harness} with the same role, folder and parent. It reads this session's history (session_handoff) and carries on; chat channels follow it. \
Tell the user the switch happens after this reply.", old.name)}));
        }
        self.do_reharness(&actor, &old, harness, model, seed, wait).await
    }

    /// Run self-switches whose turn has ended (called from the server tick).
    async fn run_pending_switches(self: &Arc<Self>) {
        let names: Vec<String> = self.pending_switch.lock().await.keys().cloned().collect();
        for name in names {
            if self.is_busy(&name).await {
                continue;
            }
            let Some((harness, model, actor)) = self.pending_switch.lock().await.remove(&name) else { continue };
            let old = match self.store.lock().await.get(&name) {
                Ok(Some(o)) if o.lifecycle != "archived" => o,
                _ => continue,
            };
            if let Err(e) = self.do_reharness(&actor, &old, &harness, model.as_deref(), true, false).await {
                eprintln!("scheduled harness switch of {} failed: {e:#}", old.name);
                let _ = self.store.lock().await.log_event("kurisu", &name, "reharness_failed", &format!("{e:#}"));
            }
        }
    }

    async fn do_reharness(self: &Arc<Self>, actor: &str, old: &store::Session, harness: &str, model: Option<&str>,
                          seed: bool, wait: bool) -> Result<Value> {
        let target = old.id.as_str();
        let actor = actor.to_string();
        let old = old.clone();
        if self.is_busy(target).await {
            bail!("{} is running a turn: switch its harness when it is idle (or stop it first)", old.name);
        }
        let model = model.map(String::from).unwrap_or_else(|| if harness == "claude" { store::DEFAULT_MODEL.into() } else { "default".into() });
        store::validate_model(harness, &model)?;
        // Archive first (frees the name; closes the CLI), then create; undo the archive on failure.
        self.update(None, target, &serde_json::Map::from_iter([("lifecycle".to_string(), json!("archived"))])).await?;
        let created = self.store.lock().await.create(&old.name, harness, &old.cwd, &old.role, &model, &old.description,
                                                     old.created_by_session.as_deref());
        let new = match created {
            Ok(n) => n,
            Err(e) => {
                let _ = self.store.lock().await.update_field("kurisu", target, "lifecycle", &old.lifecycle);
                return Err(e.context("could not create the replacement session; the old one was restored"));
            }
        };
        let moved = {
            let st = self.store.lock().await;
            if !old.note.is_empty() {
                st.set_note(&new.id, &old.note)?;
            }
            if let Some(m) = old.permission_mode.as_ref().filter(|m|(harness!="opencode" || ["default","plan"].contains(&m.as_str())) && (harness!="agy" || ["auto","default","acceptEdits","plan"].contains(&m.as_str())) && (harness!="fx" || m.as_str()!="plan")) {
                st.update_field("kurisu", &new.id, "permission_mode", m)?;
            }
            let m = st.transfer(&old.id, &new.id)?;
            for (name, _, enabled) in st.mcp_list(&new.id)? {
                if enabled {
                    if let Some(token) = crate::oauth::Token::load(&self.paths.mcp_oauth_token(&old.id, &name))? {
                        self.paths.write_mcp_oauth_token(&new.id, &name, &token.0)?;
                    }
                }
            }
            st.log_event(&actor, &old.id, "reharnessed", &format!("{} {} -> {} {} (new session {})", old.harness, old.model, harness, model, new.id))?;
            st.log_event(&actor, &new.id, "replaces", &format!("{} ({} {})", old.id, old.harness, old.model))?;
            m
        };
        self.channels.replaced(&old.id, &new.id)?;
        self.telegram.session_replaced(&old.id, &new.id).await;
        let mut seeded = Value::Null;
        if seed {
            let text = format!(
                "[Kurisu: harness switch] You replace session `{name}` ({oid}), which ran on {oh} ({om}) and is now archived. \
Its CLI context does not carry over, but Kurisu kept its history. Call the kurisu-mcp tool session_handoff with id \"{oid}\" now, \
read it (session_history for more), then reply in a few lines: what the session was doing and what you will do next. \
Your folder is the same: {cwd}.",
                name = old.name, oid = old.id, oh = old.harness, om = old.model, cwd = old.cwd);
            seeded = if wait {
                self.send(&new.id, &text, &actor, &actor).await?
            } else {
                self.send_async(&new.id, &text, &actor, &actor, false).await?
            };
        }
        let new = self.store.lock().await.get(&new.id)?.ok_or_else(|| anyhow!("new session vanished"))?;
        Ok(json!({"old": {"id": old.id, "name": old.name, "harness": old.harness, "model": old.model, "lifecycle": "archived"},
                  "new": new, "moved": {"children": moved.0, "wakes": moved.1, "mcp_servers": moved.2, "rules": moved.3},
                  "seed": seeded,
                  "note": if seed { "the old session is archived (its history stays readable); the new one has the same name and folder and has \
already been sent a message to read the old history (session_handoff) and carry on: its reply arrives in its own channel, \
so you do not need to brief it" } else { "the old session is archived (its history stays readable); the new one has the same name and folder; \
it has not been sent anything yet" }}))
    }

    /// session_update: the user may change anything; a session may change its descendants,
    /// and itself only for SELF_FIELDS.
    async fn update(&self, caller: Option<&str>, target: &str, fields: &serde_json::Map<String, Value>) -> Result<Value> {
        let actor = caller.unwrap_or("user").to_string();
        let mut effective = target.to_string();
        {
            let st = self.store.lock().await;
            st.get(target)?.ok_or_else(|| anyhow!("no session {target}"))?;
            if let Some(c) = caller {
                // A session that switched harness is archived and replaced by a
                // new row with the same name. Its own MCP env still carries the
                // old id, and `id: "<name>"` resolves to the new row, so an id
                // equality check would call that "not an ancestor of itself".
                // Treat the whole replacement chain as the same session, and
                // apply the update to the live tip that will actually run.
                let is_self = c == target
                    || st.replacement_tip(c)? == st.replacement_tip(target)?;
                if is_self {
                    // A session may tune itself (model/effort from its next turn),
                    // but not its lifecycle, role or name.
                    if let Some(bad) = fields.keys().find(|k| !SELF_FIELDS.contains(&k.as_str())) {
                        bail!("permission denied: a session cannot change its own {bad} (it may change its own {})", SELF_FIELDS.join(", "));
                    }
                    effective = st.replacement_tip(target)?;
                } else if !st.is_ancestor(c, target)? {
                    let n = |id: &str| st.get(id).ok().flatten().map(|x| x.name).unwrap_or_else(|| id.to_string());
                    bail!("permission denied: {} is not an ancestor of {} (only the user or a session's creator chain may modify it)", n(c), n(target));
                }
                if fields.get("permission_mode").and_then(Value::as_str) == Some("bypassPermissions") {
                    bail!("permission denied: only the user can give a session bypassPermissions");
                }
                if fields.get("role").and_then(Value::as_str) == Some("lead") {
                    bail!("permission denied: only the user can make a session a lead");
                }
            }
        }
        let mut fields = fields.clone();
        // "status" is an alias of "task" (the status note).
        if let Some(v) = fields.remove("status") {
            fields.entry("task".to_string()).or_insert(v);
        }
        // browser: "on" | "off" turns Live Browser on or off for the session.
        let browser = fields.remove("browser");
        let fields = &fields;
        if fields.is_empty() && browser.is_none() {
            bail!("nothing to update");
        }
        let order = ["task", "description", "model", "effort", "permission_mode", "role", "lifecycle", "name"];
        if let Some(bad) = fields.keys().find(|k| !order.contains(&k.as_str())) {
            bail!("field {bad} cannot be updated (allowed: {})", order.join(", "));
        }
        let name = effective;
        let mut applied = vec![];
        let mut unchanged = vec![];
        if let Some(b) = browser {
            let on = match b.as_str().or_else(|| b.as_bool().map(|x| if x { "on" } else { "off" })) {
                Some("on") | Some("true") => true,
                Some("off") | Some("false") => false,
                _ => bail!("browser must be on or off"),
            };
            if self.set_session_lb(&actor, &name, on).await? {
                applied.push(json!({"field": "browser", "new": if on { "on" } else { "off" }}));
            } else {
                unchanged.push("browser");
            }
        }
        let cur=self.store.lock().await.get(&name)?.ok_or_else(||anyhow!("missing session"))?;
        if cur.harness=="opencode" && fields.contains_key("effort") {
            let model=fields.get("model").and_then(Value::as_str).unwrap_or(&cur.model);
            let v=self.opencode().await?.discover(PathBuf::from(&cur.cwd),Some(model)).await?;
            let effort=fields["effort"].as_str().unwrap_or("");
            let supported=v["configOptions"].as_array().and_then(|cs|cs.iter().find(|c|c["id"]=="effort")).and_then(|c|c["options"].as_array());
            if effort!="default" && !supported.is_some_and(|a|a.iter().any(|o|o["value"]==effort)) { bail!("OpenCode model {model} does not advertise effort {effort:?}"); }
        }
        for f in order {
            let Some(v) = fields.get(f) else { continue };
            let v = v.as_str().map(String::from).unwrap_or_else(|| v.to_string());
            if !self.store.lock().await.update_field(&actor, &name, f, &v)? {
                unchanged.push(f);
                continue;
            }
            match f {
                "model" | "effort" | "permission_mode" => {
                    self.relaunch.lock().await.insert(name.clone());
                }
                "lifecycle" if v != "active" => {
                    let session=self.store.lock().await.get(&name)?;
                    if let Some(session)=session { self.close_driver(&session).await; }
                    self.live.lock().await.remove(&name);
                    self.store.lock().await.set_status(&name, "stopped")?;
                }
                _ => {}
            }
            applied.push(json!({"field": f, "new": v}));
        }
        let cur = self.store.lock().await.get(&name)?;
        let display = cur.as_ref().map(|x| x.name.clone()).unwrap_or_default();
        let warning = fields.get("model").and_then(Value::as_str).and_then(|m| store::model_warning(cur.as_ref().map(|x| x.harness.as_str()).unwrap_or("claude"), m));
        Ok(json!({"updated": name, "name": display, "changes": applied, "unchanged": unchanged, "by": actor, "warning": warning,
                  "note": "model/effort changes take effect from the session's next turn; the id never changes"}))
    }

    /// Create one session from another system's session (`ku import`): same name, folder,
    /// model, effort, role and parent; its full history with original times; and, when
    /// given, the CLI session id to resume (a copied transcript). Idempotent per source id.
    async fn import_session(&self, req: &Value) -> Result<Value> {
        let g = |k: &str| req.get(k).and_then(Value::as_str).filter(|v| !v.is_empty()).map(String::from);
        let (source, source_id) = (g("source").unwrap_or_else(|| "import".into()), g("source_id").ok_or_else(|| anyhow!("missing source_id"))?);
        let st = self.store.lock().await;
        if let Some((id, name)) = st.import_of(&source, &source_id)? {
            return Ok(json!({"id": id, "name": name, "already": true}));
        }
        let name = g("new_name").ok_or_else(|| anyhow!("missing new_name"))?;
        let harness = g("harness").unwrap_or_else(|| "claude".into());
        if !["claude", "codex", "opencode", "agy", "fx", "pi"].contains(&harness.as_str()) {
            bail!("harness must be claude, codex, opencode, agy or fx");
        }
        let role = g("role").unwrap_or_else(|| "worker".into());
        let model = g("model").unwrap_or_else(|| self.default_model(&harness));
        let cwd = g("cwd").map(PathBuf::from).unwrap_or_else(|| self.paths.workspace(&name));
        let created_by = g("created_by");
        let title = g("title").unwrap_or_else(|| name.clone());
        let messages = req.get("messages").and_then(Value::as_array).cloned().unwrap_or_default();
        st.begin()?;
        let res: Result<store::Session> = (|| {
            let s = st.create(&name, &harness, &cwd.to_string_lossy(), &role, &model, &g("description").unwrap_or_default(), created_by.as_deref())?;
            if let Some(h) = g("harness_session_id") {
                st.bind_harness_id(&s.id, &h)?;
            }
            for m in &messages {
                let role = m["role"].as_str().unwrap_or("user");
                let text = m["text"].as_str().unwrap_or("");
                let data = m.get("data").filter(|d| d.is_object()).map(|d| {
                    let mut d = d.clone();
                    for k in ["input", "output"] {
                        if let Some(v) = d.get(k).cloned() {
                            d[k] = clip_json(&v, TOOL_DATA_MAX);
                        }
                    }
                    d
                });
                st.append_at(&s.id, role, text, data.as_ref(), m["at"].as_i64().unwrap_or_else(now))?;
            }
            st.import_record(&source, &source_id, &s.id, &title, messages.len() as i64, g("harness_session_id").as_deref())?;
            st.log_event("user", &s.id, "imported", &format!("from {source} {source_id} ({title}): {} messages{}", messages.len(),
                         if g("harness_session_id").is_some() { ", resumes the copied CLI transcript" } else { "" }))?;
            Ok(s)
        })();
        match res {
            Ok(s) => {
                st.commit()?;
                if let Some(e) = g("effort") {
                    st.update_field("user", &s.id, "effort", &e)?;
                }
                Ok(json!({"id": s.id, "name": s.name, "messages": messages.len(), "already": false}))
            }
            Err(e) => {
                st.rollback();
                Err(e)
            }
        }
    }

    /// Installed CLIs (claude, codex, stt), cached for 60 s (the checks run the CLIs).
    async fn clis(&self, fresh: bool) -> Value {
        if let Some((at, v)) = self.clis_cache.lock().await.clone() {
            if !fresh && at.elapsed() < std::time::Duration::from_secs(60) {
                return v;
            }
        }
        let (c, x, o, a, f, p, t) = tokio::join!(crate::hostcheck::claude(), crate::hostcheck::codex(), crate::hostcheck::opencode(), crate::hostcheck::agy(), crate::hostcheck::fx(), crate::hostcheck::pi(), crate::hostcheck::stt());
        let mut t = t;
        t["kurisu_stt"] = json!(crate::stt::active(&self.paths.home).is_some());
        let v = json!({"clis": [c.to_json(), x.to_json(), o.to_json(), a.to_json(), f.to_json(), p.to_json(), t], "checked_at": now(),
                       "note": "login state comes only from the CLIs' own status commands; credentials are never read"});
        *self.clis_cache.lock().await = Some((std::time::Instant::now(), v.clone()));
        v
    }

    /// Whether a harness's CLI is on PATH (cheap; no CLI is run). Unit tests assume both.
    fn cli_installed(harness: &str) -> bool {
        if cfg!(test) {
            return true;
        }
        // The shared resolver, not PATH alone: a deploy-started daemon has a narrow PATH.
        crate::harness::HARNESSES.contains(&harness) && crate::harness::resolve(harness).is_some()
    }

    /// Fail fast, with a fix, when a harness's CLI is missing.
    fn require_cli(harness: &str) -> Result<()> {
        require_cli_with(harness, Self::cli_installed)
    }

    /// The harness for a new session when none is given: KURISU_DEFAULT_HARNESS or
    /// `default_harness` in <KURISU_HOME>/config.json; otherwise the installed CLI that is
    /// logged in (Claude first when both are), then any installed one.
    async fn default_harness(&self) -> Result<String> {
        let configured = std::env::var("KURISU_DEFAULT_HARNESS").ok().filter(|h| !h.is_empty()).or_else(|| {
            std::fs::read(self.paths.home.join("config.json")).ok()
                .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
                .and_then(|v| v["default_harness"].as_str().map(String::from))
        });
        if let Some(h) = configured {
            if !["claude", "codex", "opencode", "agy", "fx", "pi"].contains(&h.as_str()) {
                bail!("default harness {h:?} must be claude, codex, opencode, agy or fx");
            }
            return Ok(h);
        }
        if cfg!(test) {
            return Ok("claude".into());
        }
        pick_harness(&self.clis(false).await)
    }

    /// The model for a new session when none is given: `default_model` in
    /// <KURISU_HOME>/config.json when non-empty; otherwise the harness default.
    fn default_model(&self, harness: &str) -> String {
        std::fs::read(self.paths.home.join("config.json")).ok()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
            .and_then(|v| v["default_model"].as_str().filter(|m| !m.is_empty()).map(String::from))
            .unwrap_or_else(|| store::default_model(harness).into())
    }

    /// Ops for the in-process Telegram channel (same as the socket API).
    /// (Boxed: the channel is started from `handle`, so its own calls back in need a concrete type.)
    pub(crate) fn handle_pub(self: &Arc<Self>, req: Value) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value>> + Send + '_>> {
        Box::pin(self.handle(req))
    }

    pub(crate) fn handle_person_pub(self:&Arc<Self>,person:String,req:Value)->std::pin::Pin<Box<dyn std::future::Future<Output=Result<Value>>+Send+'_>> {
        Box::pin(self.person_request(person,req))
    }

    pub fn home(&self) -> PathBuf {
        self.paths.home.clone()
    }

    /// The outward MCP server as the `/mcp_server` card and `ku doctor` report it:
    /// the truth comes from the remote Hub (is the listener bound?), not the flag.
    pub(crate) async fn mcp_server_status(&self) -> Value {
        let cfg = crate::mcp_out::load_config(&self.paths.home).unwrap_or_default();
        let listening = self.remote.listening("mcp").await;
        let addr = crate::mcp_out::socket_addr(&cfg).ok();
        let listen = listening.clone().or_else(|| addr.map(|a| a.to_string()));
        let token = std::fs::read_to_string(crate::mcp_out::token_path(&self.paths.home)).ok()
            .map(|t| t.trim().to_string()).filter(|t| !t.is_empty());
        let url = match (addr, token.as_deref()) {
            (Some(a), tok) => crate::mcp_out::masked_url(a, tok),
            (None, _) => String::new(),
        };
        let mut note = String::new();
        if listening.is_none() {
            // Someone else holding the port is the most useful thing to say, and
            // it is never ours to stop: report it and touch nothing.
            note = addr.and_then(|a| crate::remote::port_owner(a.port()))
                .map(|owner| format!("port in use by {owner}"))
                .unwrap_or_default();
            if note.is_empty() {
                note = self.remote.mcp_error().await.unwrap_or_default();
            }
        }
        let mut out = json!({
            "enabled": cfg.enabled,
            "listening": listening.is_some(),
            "listen": listen.unwrap_or_else(|| format!("{}:{}", cfg.host, cfg.port)),
            "token_path": crate::mcp_out::token_path(&self.paths.home),
            "token_mode": crate::mcp_out::token_mode(&self.paths.home),
            "url": url,
            "sign_in_url": addr.map(|a| format!("http://{a}/mcp")).unwrap_or_default(),
            "oauth_clients": crate::mcp_out::oauth_clients(&self.paths.home),
            "note": note,
        });
        if out["listening"] == json!(true) {
            out["pid"] = json!(std::process::id());
            out["uptime_seconds"] = json!(self.started.elapsed().as_secs());
        }
        out
    }

    /// The full URL and token, only behind the card's Show URL button.
    pub(crate) async fn mcp_server_url(&self) -> Result<Value> {
        let cfg = crate::mcp_out::load_config(&self.paths.home).unwrap_or_default();
        let addr = crate::mcp_out::socket_addr(&cfg)?;
        let token = crate::mcp_out::ensure_token(&self.paths.home)?;
        Ok(json!({"url": crate::mcp_out::url_for(addr, &token), "listen": addr.to_string(),
                  "token_path": crate::mcp_out::token_path(&self.paths.home),
                  "state": if self.remote.listening("mcp").await.is_some() { "running" } else { "stopped" }}))
    }

    /// The server's unix socket, for handlers that act as the user (the /web UI).
    pub fn socket(&self) -> PathBuf {
        self.paths.socket()
    }

    /// Test-only handle on the store (bridge tests drive history rows directly).
    #[cfg(test)]
    pub(crate) fn test_event(&self, event: Value) { let _=self.events.send(event); }

    #[cfg(test)]
    pub(crate) async fn test_set_activity(&self, sid: &str, tool: &str, title: &str, state: &str) {
        self.activity.lock().await.insert(
            sid.to_string(),
            json!({"tool": tool, "title": title, "state": state, "at": now()}),
        );
    }

    #[cfg(test)]
    pub(crate) async fn test_catalogue(&self, value: Value) { *self.clis_cache.lock().await=Some((std::time::Instant::now(),value)); }

    #[cfg(test)]
    pub(crate) fn test_no_turns(&self) { self.turns_enabled.store(false,std::sync::atomic::Ordering::Relaxed); }

    #[cfg(test)]
    pub(crate) fn test_store(&self) -> &Mutex<Store> {
        &self.store
    }

    /// Test-only entry point for ops (bridges/tools are tested through it).
    #[cfg(test)]
    pub(crate) async fn test_call(self: &Arc<Self>, req: Value) -> Result<Value> {
        self.handle(req).await
    }

    /// Run `f` on the store (the Telegram channel records what it saw there).
    pub(crate) async fn with_store<T>(&self, f: impl FnOnce(&Store) -> Result<T>) -> Result<T> {
        let st = self.store.lock().await;
        f(&st)
    }

    /// An audit row (errors are logged, not returned).
    pub async fn audit(&self, actor: &str, target: &str, event: &str, detail: &str) {
        if let Err(e) = self.store.lock().await.log_event(actor, target, event, detail) {
            eprintln!("audit failed ({event}): {e:#}");
        }
    }

    /// Sessions whose turn is running right now (their busy lock is held).
    async fn busy_sessions(&self) -> Vec<String> {
        let locks = self.busy.lock().await;
        let mut out = vec![];
        for (name, m) in locks.iter() {
            if m.try_lock().is_err() {
                out.push(name.clone());
            }
        }
        out
    }

    /// Wait (bounded) for in-flight turns to finish. Returns who was still running.
    async fn wait_idle(&self, limit: std::time::Duration) -> Vec<String> {
        let deadline = std::time::Instant::now() + limit;
        loop {
            let busy = self.busy_sessions().await;
            if busy.is_empty() || std::time::Instant::now() >= deadline {
                return busy;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    /// Take over the live instance after the public socket was pointed at this server:
    /// reconcile the store, start Telegram and the web listeners, resume cut-off turns,
    /// then let queued turns run. Only valid on a standby server; idempotent.
    pub async fn promote(self: &Arc<Self>) -> Result<Value> {
        let was_standby = self.standby.swap(false, std::sync::atomic::Ordering::SeqCst);
        if was_standby {
            let interrupted = {
                let st = self.store.lock().await;
                reconcile_startup(&self.paths, &st)?
            };
            *self.interrupted.lock().await = interrupted;
        }
        // Let messages that arrived during the handover run now that the store is clean.
        self.ready.store(true, std::sync::atomic::Ordering::SeqCst);
        self.ready_notify.notify_waiters();
        if crate::browser::active(&self.paths.home).is_some() {
            let server = self.clone();
            tokio::spawn(async move { server.browser.sync_pin(&server.paths.home).await; });
        }
        let telegram = match self.telegram.reload(self.clone(), self.paths.home.clone()).await {
            Ok(s) => s.to_string(),
            Err(e) => {
                eprintln!("telegram channel did not start after promotion: {e:#}");
                format!("error: {e:#}")
            }
        };
        if telegram == "running" {
            crate::deploy::record_event(&self.paths, "poller_started");
        }
        let discord = self.discord.reload(self.clone(),self.paths.home.clone()).await?;
        let web = match self.remote.reload_web(self).await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("web listener did not start after promotion: {e:#}");
                json!({"error": format!("{e:#}")})
            }
        };
        let mcp_server = match self.remote.reload_mcp(self).await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("MCP server listener did not start after promotion: {e:#}");
                json!({"error": format!("{e:#}")})
            }
        };
        if let Err(e) = self.resume_interrupted().await {
            eprintln!("resume after promotion: {e:#}");
        }
        Ok(json!({"promoted": true, "standby": self.standby.load(std::sync::atomic::Ordering::SeqCst),
                  "ready": self.ready.load(std::sync::atomic::Ordering::SeqCst),
                  "telegram": telegram, "discord":discord, "web": web, "mcp_server": mcp_server, "pid": std::process::id()}))
    }

    async fn shutdown_children(&self) {
        // A probe shares the home but owns no children. In particular, killing the
        // home's tmux server here would kill the primary's running Claude turns.
        if self.standby.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        self.remote.stop().await;
        self.browser.stop().await;
        self.telegram.stop().await;
        self.discord.stop().await;
        let names: Vec<String> = self.live.lock().await.drain().collect();
        // Turns still running are cut off by this shutdown: note them for the next start.
        let mut busy = vec![];
        for name in &names {
            if self.is_busy(name).await {
                busy.push(name.clone());
            }
        }
        if !busy.is_empty() {
            if let Err(e) = std::fs::write(self.paths.home.join(INTERRUPTED_FILE), serde_json::to_vec(&busy).unwrap_or_default()) {
                eprintln!("could not note the interrupted turns: {e}");
            }
        }
        for name in names {
            let _ = self.store.lock().await.set_status(&name, "stopped");
        }
        // One private tmux server per Kurisu home: stopping it closes every CLI.
        for d in self.opencode.lock().await.values() { d.shutdown().await; }
        self.claude.kill_all().await;
    }
}

pub async fn serve(paths: Paths) -> Result<()> {
    serve_with(paths, ServeOpts::default()).await
}

/// Run the server. `opts.socket` binds a concrete path (a deploy's slot); `opts.standby`
/// skips every sidecar and every store mutation until `promote`.
pub async fn serve_with(paths: Paths, opts: ServeOpts) -> Result<()> {
    let server = Arc::new(if opts.standby {
        Server::new_mode(paths.clone(), true)?
    } else {
        Server::new(paths.clone())?
    });
    // Live Browser: an install older than LB_PINNED must not keep serving the old
    // UI/MCP until someone happens to run setup again (see Supervisor::sync_pin).
    if !opts.standby && crate::browser::active(&paths.home).is_some() {
        let server = server.clone();
        tokio::spawn(async move { server.browser.sync_pin(&server.paths.home).await; });
    }
    let requested = opts.socket.clone().unwrap_or_else(|| paths.socket());
    // Never unlink the public symlink: bind (and later clean up) the concrete slot.
    let actual = crate::paths::concrete_socket_path(&requested)?;
    let _ = std::fs::remove_file(&actual);
    let listener = crate::paths::bind_private(&actual)?;
    let inode = crate::paths::file_id(&actual).ok();
    eprintln!("kurisu server listening on {} (pid {}, {})", actual.display(), std::process::id(),
              if opts.standby { "standby" } else { "primary" });
    if !opts.standby {
        if let Err(e) = server.remote.reload_web(&server).await {
            eprintln!("web listener did not start: {e:#}");
        }
        if let Err(e) = server.remote.reload_mcp(&server).await {
            eprintln!("MCP server listener did not start: {e:#}");
        }
        if let Err(e)=server.discord.reload(server.clone(),paths.home.clone()).await {eprintln!("Discord channel did not start: {e}");}
        match server.telegram.reload(server.clone(), paths.home.clone()).await {
            Ok("running") => eprintln!("telegram channel started"),
            Ok(_) => {}
            Err(e) => eprintln!("telegram channel did not start: {e:#}"),
        }
        if let Err(e) = server.resume_interrupted().await {
            eprintln!("resume after restart: {e:#}");
        }
    }
    // The accept loop owns the listener so `drain` can really close it (a unix socket
    // left bound after the handover would still show up in `ss`).
    let accept_stop = server.stop_accept.clone();
    let (tx, mut rx) = mpsc::channel::<UnixStream>(64);
    let accept = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = accept_stop.notified() => break,
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => { if tx.send(stream).await.is_err() { break; } }
                    Err(e) => { eprintln!("accept failed: {e}"); break; }
                }
            }
        }
    });
    let mut expiry = tokio::time::interval(std::time::Duration::from_secs(2));
    loop {
        tokio::select! {
            maybe = rx.recv() => {
                if let Some(stream) = maybe {
                    let server = server.clone();
                    tokio::spawn(async move { let _ = connection(server, stream).await; });
                }
            }
            _ = server.stop.notified() => break,
            _ = expiry.tick(), if !server.standby.load(std::sync::atomic::Ordering::SeqCst) => {
                let _ = server.expire_approvals().await;
                server.run_pending_switches().await;
                server.remote.tick(&server).await;
                let busy = !server.live.lock().await.is_empty() || server.remote.browser_viewers().await;
                server.browser.tick(busy).await;
                if let Err(e) = server.fire_wakes().await {
                    eprintln!("scheduled wakes: {e:#}");
                }
                if let Err(e) = server.watch_followups().await {
                    eprintln!("follow-up turns: {e:#}");
                }
            }
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    accept.abort();
    server.shutdown_children().await;
    crate::paths::remove_if_same(&actual, inode);
    Ok(())
}

async fn connection(server: Arc<Server>, stream: UnixStream) -> Result<()> {
    let (r, mut w) = stream.into_split();
    let mut lines = BufReader::new(r).lines();
    while let Some(line) = lines.next_line().await? {
        let req = match serde_json::from_str::<Value>(&line) {
            Ok(req) => req,
            Err(e) => {
                let mut out = serde_json::to_vec(&json!({"error": format!("bad json: {e}")}))?;
                out.push(b'\n');
                w.write_all(&out).await?;
                continue;
            }
        };
        // `watch` keeps the connection open and streams this session's events
        // (newline-delimited JSON) after a normal `{"ok": ...}` acknowledgement.
        // Subscribing before the ack matters: a caller may start a turn as soon
        // as it sees the ack and must not miss the first deltas.
        if req.get("op").and_then(Value::as_str) == Some("watch") {
            let Some(name) = req.get("name").and_then(Value::as_str).filter(|n| !n.is_empty()) else {
                let mut out = serde_json::to_vec(&json!({"error": "missing name"}))?;
                out.push(b'\n');
                w.write_all(&out).await?;
                continue;
            };
            let sid = match server.store.lock().await.resolve_req(name) {
                Ok(id) => id,
                Err(e) => {
                    let mut out = serde_json::to_vec(&json!({"error": format!("{e:#}")}))?;
                    out.push(b'\n');
                    w.write_all(&out).await?;
                    continue;
                }
            };
            let mut events = server.subscribe_events();
            let mut ack = serde_json::to_vec(&json!({"ok": {"watching": sid, "session": sid}}))?;
            ack.push(b'\n');
            w.write_all(&ack).await?;
            // A keepalive both detects a vanished reader and gives a quiet
            // session something to write, so a closed bridge is not held open.
            let mut keepalive = tokio::time::interval_at(
                tokio::time::Instant::now() + std::time::Duration::from_secs(20),
                std::time::Duration::from_secs(20),
            );
            loop {
                tokio::select! {
                    ev = events.recv() => match ev {
                        Ok(v) => {
                            if v["session"].as_str() != Some(sid.as_str()) {
                                continue;
                            }
                            let mut out = serde_json::to_vec(&v)?;
                            out.push(b'\n');
                            if w.write_all(&out).await.is_err() { break; }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    },
                    _ = keepalive.tick() => {
                        if w.write_all(b"{\"type\":\"ping\"}\n").await.is_err() { break; }
                    }
                }
            }
            return Ok(());
        }
        let reply = match server.handle(req).await {
            Ok(v) => json!({"ok": v}),
            Err(e) => json!({"error": format!("{e:#}")}),
        };
        let mut out = serde_json::to_vec(&reply)?;
        out.push(b'\n');
        w.write_all(&out).await?;
    }
    Ok(())
}

pub async fn call(sock: &Path, req: Value) -> Result<Value> {
    call_budget(sock, req, std::time::Duration::from_secs(3)).await
}

/// `call`, but with an explicit connect budget: diagnostics that must answer quickly
/// about a dead server use a short one instead of riding out a deploy's handover.
pub async fn call_budget(sock: &Path, req: Value, budget: std::time::Duration) -> Result<Value> {
    match call_raw(sock, req, budget).await {
        Err(e) if crate::doctor::unknown_op(&e.to_string()) => {
            // Query directly, once: older daemons may not know version either.
            let daemon = tokio::time::timeout(std::time::Duration::from_millis(500), call_raw(sock, json!({"op":"version"}), budget)).await.ok().and_then(Result::ok).unwrap_or(Value::Null);
            bail!("{}", crate::doctor::older_binary_hint(&e.to_string(), &daemon))
        }
        result => result,
    }
}
/// Connect to the server, riding out a deploy's socket handover: if the path is briefly
/// missing or refused (symlink being repointed), retry for up to `budget` instead of
/// failing the call. Clients (`ku`, /web, MCP) all go through here.
pub async fn connect_retry(sock: &Path, budget: std::time::Duration) -> Result<UnixStream> {
    let deadline = std::time::Instant::now() + budget;
    loop {
        match UnixStream::connect(sock).await {
            Ok(s) => return Ok(s),
            Err(e) => {
                if std::time::Instant::now() >= deadline {
                    return Err(e).with_context(|| format!("connect {}", sock.display()));
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

async fn call_raw(sock: &Path, req: Value, budget: std::time::Duration) -> Result<Value> {
    let stream = connect_retry(sock, budget).await?;
    let (r, mut w) = stream.into_split();
    let mut line = serde_json::to_vec(&req)?;
    line.push(b'\n');
    w.write_all(&line).await?;
    let mut lines = BufReader::new(r).lines();
    let reply: Value = serde_json::from_str(&lines.next_line().await?.context("no reply")?)?;
    match reply.get("error") {
        Some(e) => bail!("{}", e.as_str().unwrap_or("error")),
        None => Ok(reply["ok"].clone()),
    }
}

/// Fields a session may change on itself via session_update.
const SELF_FIELDS: [&str; 6] = ["task", "status", "description", "model", "effort", "browser"];

/// Codex is wired but not part of Milestone 1's demo; keep the trait in use.
#[allow(dead_code)]
fn _assert_driver<D: Driver>() {}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn version_is_server_owned_and_in_ping() {
        let base = std::env::var_os("KURISU_TEST_TMPDIR").map(std::path::PathBuf::from).unwrap_or_else(|| std::path::PathBuf::from("/tmp"));
        let dir = tempfile::tempdir_in(&base).unwrap();
        let server = Arc::new(Server::new(Paths { home: dir.path().to_owned() }).unwrap());
        let info = h(&server, json!({"op":"version", "git_sha":"caller-spoof"})).await.unwrap();
        assert_eq!(info["git_sha"], env!("KURISU_BUILD_COMMIT"));
        assert_eq!(info["profile"], env!("KURISU_PROFILE"));
        assert_eq!(info["target"], env!("KURISU_TARGET"));
        assert!(info["started_at"].as_i64().unwrap() > 0);
        assert!(info["uptime_seconds"].is_u64());
        let ping = h(&server, json!({"op":"ping"})).await.unwrap();
        assert_eq!(ping["build"]["git_sha"], info["git_sha"]);
        let doctor = h(&server, json!({"op":"doctor", "protocol":crate::doctor::PROTOCOL})).await.unwrap();
        assert_eq!(doctor["protocol"], crate::doctor::PROTOCOL);
        assert_eq!(doctor["build"]["git_sha"], info["git_sha"]);
        assert!(doctor["lb_pid"].is_null());
    }
    #[tokio::test]
    async fn mcp_server_enable_serves_disable_closes_and_a_restart_rearms() {
        use std::net::TcpListener;
        let base = std::env::var_os("KURISU_TEST_TMPDIR").map(std::path::PathBuf::from).unwrap_or_else(|| std::path::PathBuf::from("/tmp"));
        let dir = tempfile::tempdir_in(&base).unwrap();
        let home = Paths { home: dir.path().to_owned() };
        let server = Arc::new(Server::new(home.clone()).unwrap());
        // A free port on loopback, so the test never touches the real Tailscale one.
        let port = { let l = TcpListener::bind("127.0.0.1:0").unwrap(); l.local_addr().unwrap().port() };
        let cfg = crate::mcp_out::Config { enabled: false, host: "127.0.0.1".into(), port };
        crate::mcp_out::save_config(&home.home, &cfg).unwrap();

        // Off: the card reports stopped, from the Hub (no listener), not from a guess.
        let st = h(&server, json!({"op":"mcp_server_status"})).await.unwrap();
        assert_eq!((st["enabled"].as_bool(), st["listening"].as_bool()), (Some(false), Some(false)));
        assert_eq!(st["listen"], format!("127.0.0.1:{port}"));
        assert!(st["note"].as_str().unwrap().is_empty(), "nothing is wrong yet: {}", st);

        // Enable: really bound and answering initialize + tools/list on that port.
        let st = h(&server, json!({"op":"mcp_server_enable"})).await.unwrap();
        assert_eq!(st["listening"], json!(true));
        assert_eq!(st["pid"], json!(std::process::id()));
        assert!(crate::mcp_out::load_config(&home.home).unwrap().enabled, "the switch is persisted");
        // The status line carries the masked URL only; the full one needs the URL op.
        let url_op = h(&server, json!({"op":"mcp_server_url"})).await.unwrap();
        let full = url_op["url"].as_str().unwrap().to_string();
        assert!(full.starts_with(&format!("http://127.0.0.1:{port}/mcp/")));
        let token = full.rsplit('/').next().unwrap().to_string();
        let shown = st["url"].as_str().unwrap().to_string();
        assert!(shown.ends_with(&format!("{}…", &token[..8])), "{shown}");
        assert!(!shown.contains(&token), "the status line must not carry the whole token");
        assert_eq!(st["token_mode"], json!("0600"));

        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let rpc = |id: i64, method: &str| json!({"jsonrpc": "2.0", "id": id, "method": method, "params": {}});
        let init: Value = client.post(&full)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}))
            .send().await.unwrap().json().await.unwrap();
        assert_eq!(init["result"]["serverInfo"]["name"], crate::mcp_out::SERVER_NAME);
        let list: Value = client.post(&full).json(&rpc(2, "tools/list")).send().await.unwrap().json().await.unwrap();
        assert_eq!(list["result"]["tools"].as_array().unwrap().len(), 4);

        // Disable: the switch is off and the port is closed again.
        let r = h(&server, json!({"op":"mcp_server_disable"})).await.unwrap();
        assert_eq!(r["was_on"], json!(true));
        assert_eq!(r["status"]["listening"], json!(false));
        assert!(!crate::mcp_out::load_config(&home.home).unwrap().enabled);
        assert!(tokio::time::timeout(std::time::Duration::from_secs(2), tokio::net::TcpStream::connect(("127.0.0.1", port))).await
            .map(|r| r.is_err()).unwrap_or(false), "the port must be closed after Disable");

        // A restart with the switch on brings the listener back with no tap.
        h(&server, json!({"op":"mcp_server_enable"})).await.unwrap();
        server.remote.stop().await;
        let st = h(&server, json!({"op":"mcp_server_status"})).await.unwrap();
        assert_eq!(st["listening"], json!(false), "stop released it like a shutdown");
        let back = server.remote.reload_mcp(&server).await.unwrap();
        assert_eq!(back["enabled"], json!(true));
        assert_eq!(h(&server, json!({"op":"mcp_server_status"})).await.unwrap()["listening"], json!(true));
        let reply: Value = client.post(&full).json(&rpc(3, "ping")).send().await.unwrap().json().await.unwrap();
        assert_eq!(reply["result"], json!({}));
        server.remote.stop().await;
        drop(server);
    }

    #[tokio::test]
    async fn mcp_server_enable_refuses_a_port_someone_else_holds_and_leaves_the_switch_off() {
        use std::net::TcpListener;
        let base = std::env::var_os("KURISU_TEST_TMPDIR").map(std::path::PathBuf::from).unwrap_or_else(|| std::path::PathBuf::from("/tmp"));
        let dir = tempfile::tempdir_in(&base).unwrap();
        let home = Paths { home: dir.path().to_owned() };
        let server = Arc::new(Server::new(home.clone()).unwrap());
        // A "manual ku mcp serve" stand-in: it holds the port and is not ours to kill.
        let held = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = held.local_addr().unwrap().port();
        crate::mcp_out::save_config(&home.home, &crate::mcp_out::Config { enabled: true, host: "127.0.0.1".into(), port }).unwrap();
        let error = h(&server, json!({"op":"mcp_server_enable"})).await.unwrap_err().to_string();
        assert!(error.contains("already in use"), "{error}");
        assert!(!crate::mcp_out::load_config(&home.home).unwrap().enabled, "the switch goes back to off");
        let st = h(&server, json!({"op":"mcp_server_status"})).await.unwrap();
        assert_eq!(st["listening"], json!(false));
        let note = st["note"].as_str().unwrap();
        assert!(note.starts_with("port in use by pid "), "{note}");
        // The same failure during a daemon start must not kill the holder.
        crate::mcp_out::save_config(&home.home, &crate::mcp_out::Config { enabled: true, host: "127.0.0.1".into(), port }).unwrap();
        assert!(server.remote.reload_mcp(&server).await.is_err());
        assert!(!crate::mcp_out::load_config(&home.home).unwrap().enabled, "a failed start clears the switch too");
        assert!(tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok(), "the foreign listener is untouched");
        drop(held);
        drop(server);
    }

    #[tokio::test]
    async fn codex_compact_routes_and_holds_session_lock_through_completion() {
        let (dir, driver) = crate::codex::compact_test_driver("success").await;
        let s = Arc::new(Server::new(Paths { home: dir.path().join("home") }).unwrap());
        let session = s.store.lock().await.create("fixture", "codex", dir.path().to_str().unwrap(), "worker", "default", "", None).unwrap();
        let id = session.id;
        let error = h(&s, json!({"op": "compact", "name": id})).await.unwrap_err();
        assert!(error.to_string().contains("has not started"));
        s.store.lock().await.bind_harness_id(&id, "fixture-thread").unwrap();
        s.store.lock().await.append(&id, "user", "preserve this history").unwrap();
        s.live.lock().await.insert(id.clone());
        s.codex.set(driver).ok().unwrap();
        // Busy rejection must happen before issuing any compaction RPC.
        let lock = s.busy_lock(&id).await;
        let guard = lock.lock().await;
        assert!(s.compact(&id, "user").await.unwrap_err().to_string().contains("busy"));
        assert!(!dir.path().join("started").exists());
        drop(guard);
        let compact = tokio::spawn({ let s = s.clone(); let id = id.clone(); async move { h(&s, json!({"op": "compact", "name": id})).await } });
        crate::codex::wait_test_file(&dir.path().join("started")).await;
        assert!(s.is_busy(&id).await);
        assert!(!compact.is_finished());
        assert!(s.compact(&id, "user").await.is_err());
        let prompt = tokio::spawn({ let s = s.clone(); let id = id.clone(); async move { s.run_turn(&id, "after compact", "user", 0).await } });
        tokio::task::yield_now().await;
        assert!(!prompt.is_finished());
        assert!(!dir.path().join("prompted").exists());
        std::fs::write(dir.path().join("release"), "").unwrap();
        let result = compact.await.unwrap().unwrap();
        assert_eq!(result["summary"], "");
        assert!(result["post_tokens_estimate"].is_null());
        assert_eq!(prompt.await.unwrap().unwrap()["reply"], "OK");
        let st = s.store.lock().await;
        let after = st.get(&id).unwrap().unwrap();
        assert_eq!(after.harness_session_id.as_deref(), Some("fixture-thread"));
        assert_eq!(after.status, "idle");
        assert!(st.history(&id, 0, 100).unwrap().iter().any(|m| m.text == "preserve this history"));
        assert_eq!(st.count_role(&id, "compaction").unwrap(), 0, "do not invent summary text");
    }

    #[tokio::test]
    async fn undo_codex_restores_files_reverts_thread_and_removes_history() {
        let (dir, driver) = crate::codex::compact_test_driver("success").await;
        let work = tempfile::Builder::new().prefix("undo-work-")
            .tempdir_in(std::env::var("KURISU_TEST_TMPDIR").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned())).unwrap();
        let w = work.path();
        let s = Arc::new(Server::new(Paths { home: dir.path().join("home") }).unwrap());
        let id = s.store.lock().await.create("fixture", "codex", w.to_str().unwrap(), "worker", "default", "", None).unwrap().id;
        s.store.lock().await.bind_harness_id(&id, "fixture-thread").unwrap();
        s.live.lock().await.insert(id.clone());
        s.codex.set(driver).ok().unwrap();
        assert!(h(&s, json!({"op": "undo", "name": id})).await.unwrap_err().to_string().contains("nothing to undo"));
        std::fs::write(w.join("notes.txt"), "v1").unwrap();
        h(&s, json!({"op": "send", "name": id, "text": "keep this turn"})).await.unwrap();
        std::fs::write(w.join("notes.txt"), "v2").unwrap();
        let r = h(&s, json!({"op": "send", "name": id, "text": "write zebra-file"})).await.unwrap();
        assert_eq!(r["reply"], "OK");
        // What the turn did:
        std::fs::write(w.join("notes.txt"), "v3").unwrap();
        std::fs::write(w.join("zebra.txt"), "new").unwrap();
        let u = h(&s, json!({"op": "undo", "name": "fixture"})).await.unwrap();
        assert_eq!(u["files_restored"], true, "{u}");
        assert_eq!(u["conversation_reverted"], true, "{u}");
        assert_eq!(u["history_removed"], 2, "the prompt and its reply");
        assert_eq!(std::fs::read_to_string(w.join("notes.txt")).unwrap(), "v2");
        assert!(!w.join("zebra.txt").exists());
        assert_eq!(std::fs::read_to_string(dir.path().join("reverted")).unwrap(), "prompt-turn");
        assert!(u["text"].as_str().unwrap().contains("thread/revert"), "{u}");
        {
            let st = s.store.lock().await;
            let q = crate::store::HistoryQuery { limit: 50, search: Some("zebra".into()), ..Default::default() };
            assert_eq!(st.history_page(&id, &q).unwrap().0.len(), 0);
            assert!(st.history(&id, 0, 50).unwrap().iter().any(|m| m.text == "keep this turn"));
            assert_eq!(st.take_pending_note(&id).unwrap(), None, "Codex reverted natively: no note");
        }
        // A second /undo takes back the turn before.
        let u2 = h(&s, json!({"op": "undo", "name": id})).await.unwrap();
        assert_eq!(u2["files_restored"], true);
        assert_eq!(std::fs::read_to_string(w.join("notes.txt")).unwrap(), "v1");
        assert!(s.store.lock().await.history(&id, 0, 50).unwrap().is_empty());
    }

    #[tokio::test]
    async fn undo_without_native_revert_queues_a_note_and_reports_missing_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let work = tempfile::Builder::new().prefix("undo-work-")
            .tempdir_in(std::env::var("KURISU_TEST_TMPDIR").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned())).unwrap();
        let w = work.path();
        let s = Arc::new(Server::new(Paths { home: dir.path().join("home") }).unwrap());
        let id = s.store.lock().await.create("agyfix", "agy", w.to_str().unwrap(), "worker", crate::store::default_model("agy"), "", None).unwrap().id;
        s.store.lock().await.bind_harness_id(&id, "agy-conv").unwrap();
        let seq = s.store.lock().await.append_data(&id, "user", "delete report", &json!({"delivery": "delivered", "telegram_update": 77})).unwrap();
        std::fs::write(w.join("report.md"), "keep").unwrap();
        s.undo_snapshot(&id, seq).await;
        std::fs::remove_file(w.join("report.md")).unwrap();
        s.store.lock().await.append(&id, "agent", "deleted it").unwrap();
        let u = h(&s, json!({"op": "undo", "name": id})).await.unwrap();
        assert_eq!(u["files_restored"], true);
        assert_eq!(u["conversation_reverted"], false);
        assert_eq!(u["telegram_updates"], json!([77]));
        assert_eq!(std::fs::read_to_string(w.join("report.md")).unwrap(), "keep");
        let note = s.store.lock().await.take_pending_note(&id).unwrap().unwrap();
        assert!(note.contains("/undo") && note.contains("delete report") && note.contains("restored the files"), "{note}");
        // A turn with no snapshot: history still goes, files are reported as not restored.
        s.store.lock().await.append_data(&id, "user", "no snapshot", &json!({"delivery": "delivered"})).unwrap();
        let u = h(&s, json!({"op": "undo", "name": id})).await.unwrap();
        assert_eq!(u["files_restored"], false);
        assert!(u["text"].as_str().unwrap().contains("NOT restored"), "{u}");
        assert!(s.store.lock().await.history(&id, 0, 50).unwrap().is_empty());
    }

    #[tokio::test]
    async fn codex_compact_failure_restores_status_and_releases_session_lock() {
        let (dir, driver) = crate::codex::compact_test_driver("failure").await;
        let s = Arc::new(Server::new(Paths { home: dir.path().join("home") }).unwrap());
        let session = s.store.lock().await.create("fixture", "codex", dir.path().to_str().unwrap(), "worker", "default", "", None).unwrap();
        s.store.lock().await.bind_harness_id(&session.id, "fixture-thread").unwrap();
        s.live.lock().await.insert(session.id.clone());
        s.codex.set(driver).ok().unwrap();
        std::fs::write(dir.path().join("release"), "").unwrap();
        assert!(s.compact(&session.id, "user").await.unwrap_err().to_string().contains("failed"));
        assert!(!s.is_busy(&session.id).await);
        assert_eq!(s.store.lock().await.get(&session.id).unwrap().unwrap().status, "idle");
        assert!(s.context_override.lock().await.get(&session.id).is_some());
    }



    #[tokio::test]
    async fn live_browser_is_off_by_default_and_toggled_per_session() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths { home: dir.path().to_path_buf() };
        let (lead, w);
        {
            // before this change: an lb row that was on (the old Telegram "Add lb")
            let s = Arc::new(Server::new(paths.clone()).unwrap());
            lead = h(&s, json!({"op": "create", "name": "lead", "role": "lead"})).await.unwrap()["id"].as_str().unwrap().to_string();
            w = h(&s, json!({"op": "create", "name": "w1", "caller": lead})).await.unwrap()["id"].as_str().unwrap().to_string();
            // no Live Browser set up: it cannot be turned on
            let e = h(&s, json!({"op": "update", "name": w, "caller": lead, "fields": {"browser": "on"}})).await.unwrap_err();
            assert!(e.to_string().contains("not set up"), "{e}");
            s.store.lock().await.mcp_set(&lead, LB, Some(&json!({"command": "lb", "args": ["mcp"]})), Some(true)).unwrap();
            std::fs::remove_file(paths.home.join(LB_OFF_MARKER)).unwrap();
        }
        // set Live Browser up (browser.json pointing at an existing lb file)
        let lb = dir.path().join("lb-bin");
        std::fs::write(&lb, "").unwrap();
        let cfg = crate::browser::Config { enabled: true, lb: lb.to_string_lossy().into(), port: 8792, passcode: "p".repeat(64),
                                           listen: "127.0.0.1:8793".into(), public_url: None, max_tabs: None, tab_idle_secs: None, isolated: false };
        std::fs::write(crate::browser::config_path(&paths.home), serde_json::to_vec(&cfg).unwrap()).unwrap();
        let s = Arc::new(Server::new(paths.clone()).unwrap());
        // the old row was switched off once; nothing is built in except kurisu-mcp
        assert!(s.session_lb(&lead).await.is_none());
        assert!(s.session_lb(&w).await.is_none());
        let l = h(&s, json!({"op": "mcp_list", "name": lead})).await.unwrap();
        assert_eq!(l["builtin"], json!([crate::mcp::SERVER_NAME]));
        let wsess = s.store.lock().await.get(&w).unwrap().unwrap();
        assert!(session_prompt(&wsess, { let st = s.store.lock().await; s.browser_state(&st, &w) }).contains("It is off for you now."));
        // on via session_update (ancestor, or the session itself), and via the user's mcp_set
        h(&s, json!({"op": "update", "name": w, "caller": lead, "fields": {"browser": "on"}})).await.unwrap();
        assert!(s.session_lb(&w).await.is_some());
        assert!(!session_prompt(&wsess, { let st = s.store.lock().await; s.browser_state(&st, &w) }).contains("off for you"));
        h(&s, json!({"op": "update", "name": w, "caller": w, "fields": {"browser": "off"}})).await.unwrap();
        assert!(s.session_lb(&w).await.is_none());
        assert!(h(&s, json!({"op": "update", "name": lead, "caller": w, "fields": {"browser": "on"}})).await.is_err());
        h(&s, json!({"op": "mcp_set", "name": lead, "server": "lb", "enabled": true})).await.unwrap();
        assert!(s.session_lb(&lead).await.is_some());
        let sess = s.store.lock().await.get(&lead).unwrap().unwrap();
        let opts = s.claude_opts(&sess).await.unwrap();
        assert!(opts.args.join(" ").contains("mcp__lb"));
        let sess = s.store.lock().await.get(&w).unwrap().unwrap();
        let opts = s.claude_opts(&sess).await.unwrap();
        assert!(!opts.args.join(" ").contains("mcp__lb"));
        drop(s);
        // an enable after the change survives restarts (the switch-off ran once)
        let s = Arc::new(Server::new(paths).unwrap());
        assert!(s.session_lb(&lead).await.is_some());
    }

    #[tokio::test]
    async fn a_real_home_gives_the_lead_the_browser_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths { home: dir.path().to_path_buf() };
        let (lead, switched_off, w);
        {
            let s = Arc::new(Server::new(paths.clone()).unwrap());
            lead = h(&s, json!({"op": "create", "name": "lead", "role": "lead"})).await.unwrap()["id"].as_str().unwrap().to_string();
            switched_off = h(&s, json!({"op": "create", "name": "lead-2", "role": "lead"})).await.unwrap()["id"].as_str().unwrap().to_string();
            w = h(&s, json!({"op": "create", "name": "w1", "caller": lead})).await.unwrap()["id"].as_str().unwrap().to_string();
            // The user switched this lead's browser off themselves before the migration.
            let st = s.store.lock().await;
            st.mcp_set(&switched_off, LB, Some(&json!({"command": "lb", "args": ["mcp"]})), Some(false)).unwrap();
            st.log_event("user", &switched_off, "mcp_disabled", LB).unwrap();
        }
        // Now it is a real home: lb installed and Telegram configured.
        let lb = dir.path().join("lb-bin");
        std::fs::write(&lb, "").unwrap();
        let cfg = crate::browser::Config { enabled: true, lb: lb.to_string_lossy().into(), port: 8792, passcode: "p".repeat(64),
                                           listen: "127.0.0.1:8793".into(), public_url: None, max_tabs: None, tab_idle_secs: None, isolated: false };
        std::fs::write(crate::browser::config_path(&paths.home), serde_json::to_vec(&cfg).unwrap()).unwrap();
        std::fs::write(paths.home.join("telegram.json"), b"{}").unwrap();
        let s = Arc::new(Server::new(paths.clone()).unwrap());
        // The lead starts with the browser: the prompt says so.
        assert!(s.session_lb(&lead).await.is_some());
        let lsess = s.store.lock().await.get(&lead).unwrap().unwrap();
        assert!(session_prompt(&lsess, { let st = s.store.lock().await; s.browser_state(&st, &lead) }).contains("lb_*"));
        // A row the user switched off stays off.
        assert!(s.session_lb(&switched_off).await.is_none());
        // Workers are still off, but told the browser exists and how to enable it.
        assert!(s.session_lb(&w).await.is_none());
        let wsess = s.store.lock().await.get(&w).unwrap().unwrap();
        let wp = session_prompt(&wsess, { let st = s.store.lock().await; s.browser_state(&st, &w) });
        assert!(wp.contains("It is off for you now.") && wp.contains("session_update {id, browser: \"on\"}"), "{wp}");
        assert!(!wp.contains("lb_*"), "{wp}");
        drop(s);
        // A lead created later on the same home gets the same default; the migration marker exists.
        let s = Arc::new(Server::new(paths.clone()).unwrap());
        let late = h(&s, json!({"op": "create", "name": "lead-3", "role": "lead"})).await.unwrap()["id"].as_str().unwrap().to_string();
        assert!(s.session_lb(&late).await.is_some());
        assert!(paths.home.join(LB_LEAD_ON_MARKER).exists());
    }

    #[tokio::test]
    async fn send_file_is_confined_to_the_session_folder_and_explains_refusals() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths { home: dir.path().to_path_buf() };
        let s = Arc::new(Server::new(paths.clone()).unwrap());
        let w = h(&s, json!({"op": "create", "name": "w1"})).await.unwrap()["id"].as_str().unwrap().to_string();
        let cwd = PathBuf::from(s.store.lock().await.get(&w).unwrap().unwrap().cwd);
        std::fs::create_dir_all(&cwd).unwrap();
        let send = |path: Value| json!({"op": "send_file", "caller": w, "path": path});
        // A real file inside the folder, and one outside it.
        std::fs::write(cwd.join("shot.jpg"), b"not really a jpeg").unwrap();
        let outside = dir.path().join("secret.txt");
        std::fs::write(&outside, b"secret").unwrap();
        // `..` is refused before any lookup.
        let e = h(&s, send(json!("../secret.txt"))).await.unwrap_err().to_string();
        assert!(e.contains("contains '..'") && e.contains("only sends files inside"), "{e}");
        // An absolute path (or a symlink) that resolves outside is refused.
        let e = h(&s, send(json!(outside.to_string_lossy()))).await.unwrap_err().to_string();
        assert!(e.contains("outside this session's folder"), "{e}");
        std::os::unix::fs::symlink(&outside, cwd.join("link.txt")).unwrap();
        let e = h(&s, send(json!("link.txt"))).await.unwrap_err().to_string();
        assert!(e.contains("outside this session's folder"), "{e}");
        // Telegram's own caps, with the photo/document distinction in the message.
        let big_photo = cwd.join("big.jpg");
        std::fs::File::create(&big_photo).unwrap().set_len(crate::telegram::tg::MAX_TG_PHOTO_BYTES + 1).unwrap();
        let e = h(&s, send(json!("big.jpg"))).await.unwrap_err().to_string();
        assert!(e.contains("10.0 MB") && e.contains("photo"), "{e}");
        let big_doc = cwd.join("big.zip");
        std::fs::File::create(&big_doc).unwrap().set_len(crate::telegram::tg::MAX_TG_FILE_BYTES + 1).unwrap();
        let e = h(&s, send(json!("big.zip"))).await.unwrap_err().to_string();
        assert!(e.contains("50.0 MB") && e.contains("document"), "{e}");
        // A file that passes every check says Telegram is not set up, instead of failing silently.
        let e = h(&s, send(json!("shot.jpg"))).await.unwrap_err().to_string();
        assert!(e.contains("Telegram is not set up for this Kurisu home") && e.contains("ku telegram setup"), "{e}");
        // Configured but switched off is its own message.
        let cfg = json!({"token": "t", "chat_id": "1", "enabled": false});
        std::fs::write(paths.home.join("telegram.json"), cfg.to_string()).unwrap();
        let e = h(&s, send(json!("shot.jpg"))).await.unwrap_err().to_string();
        assert!(e.contains("turned off"), "{e}");
        // Not a file, and no such file.
        let e = h(&s, send(json!("nope.jpg"))).await.unwrap_err().to_string();
        assert!(e.contains("no such file"), "{e}");
    }

    #[tokio::test]
    async fn mcp_list_separates_enabled_from_loaded_in_the_running_cli() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths { home: dir.path().to_path_buf() };
        let s = Arc::new(Server::new(paths.clone()).unwrap());
        let id = h(&s, json!({"op": "create", "name": "dev"})).await.unwrap()["id"].as_str().unwrap().to_string();
        h(&s, json!({"op": "mcp_set", "name": "dev", "server": "gh", "config": {"command": "gh-mcp"}})).await.unwrap();
        let list = |v: Value| v["servers"].as_array().unwrap().clone();
        // Nothing is live yet: enabled, not loaded, and the note says when it applies.
        let v = h(&s, json!({"op": "mcp_list", "name": "dev"})).await.unwrap();
        assert_eq!((v["live"].as_bool(), v["loaded"].as_bool()), (Some(false), Some(false)));
        assert_eq!((list(v.clone())[0]["enabled"].as_bool(), list(v.clone())[0]["loaded"].as_bool()), (Some(true), Some(false)));
        assert!(v["note"].as_str().unwrap().contains("next turn"), "{v}");
        // Live with exactly this set (and a matching persisted launch fingerprint): loaded now.
        let (fp, _) = s.toolset_state(&id).await.unwrap();
        s.live.lock().await.insert(id.clone());
        s.store.lock().await.set_launch_fp(&id, &fp).unwrap();
        let v = h(&s, json!({"op": "mcp_list", "name": "dev"})).await.unwrap();
        assert_eq!((v["live"].as_bool(), v["loaded"].as_bool(), list(v.clone())[0]["loaded"].as_bool()), (Some(true), Some(true), Some(true)));
        assert!(!v["note"].as_str().unwrap().contains("older toolset"), "{v}");
        // A store change the running CLI cannot see: enabled, not loaded, and it says why.
        h(&s, json!({"op": "mcp_set", "name": "dev", "server": "gh", "config": {"command": "gh-mcp"}, "enabled": false})).await.unwrap();
        let v = h(&s, json!({"op": "mcp_list", "name": "dev"})).await.unwrap();
        assert_eq!((list(v.clone())[0]["enabled"].as_bool(), list(v.clone())[0]["loaded"].as_bool()), (Some(false), Some(false)));
        assert!(v["note"].as_str().unwrap().contains("older toolset"), "{v}");
        // Disable never claims loaded, even when the fingerprint happens to match.
        let disabled_fp = s.toolset_state(&id).await.unwrap().0;
        s.store.lock().await.set_launch_fp(&id, &disabled_fp).unwrap();
        let v = h(&s, json!({"op": "mcp_list", "name": "dev"})).await.unwrap();
        assert_eq!(list(v)[0]["loaded"].as_bool(), Some(false));
        // The fingerprint moves for every input that changes the toolset, including the build.
        let rows = vec![("gh".to_string(), json!({"command": "a"}), true)];
        let gh_on = Server::toolset_fingerprint(&rows, false, "build-a");
        let gh_off = Server::toolset_fingerprint(&vec![("gh".to_string(), json!({"command": "a"}), false)], false, "build-a");
        let gh_cfg = Server::toolset_fingerprint(&vec![("gh".to_string(), json!({"command": "b"}), true)], false, "build-a");
        let lb = Server::toolset_fingerprint(&rows, true, "build-a");
        let built = Server::toolset_fingerprint(&rows, false, "build-b");
        assert_ne!(gh_on, gh_off);
        assert_ne!(gh_on, gh_cfg);
        assert_ne!(gh_on, lb);
        assert_ne!(gh_on, built, "a new Kurisu build must invalidate an ACP session's toolset");
        assert_eq!(gh_on, Server::toolset_fingerprint(&rows, false, "build-a"), "stable");
        // A matching persisted fingerprint resumes; a changed one (or none, for an existing
        // ACP session) must start a new ACP session.
        assert!(!Server::toolset_is_stale(Some(&gh_on), &gh_on, true));
        assert!(Server::toolset_is_stale(Some(&gh_on), &built, true));
        assert!(Server::toolset_is_stale(None, &gh_on, true), "a pre-field ACP session is replaced once");
        assert!(!Server::toolset_is_stale(None, &gh_on, false), "a brand-new session needs no replacement");
    }

    #[tokio::test]
    async fn a_turn_with_live_browser_set_up_does_not_deadlock() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths { home: dir.path().to_path_buf() };
        let lb = dir.path().join("lb-bin");
        std::fs::write(&lb, "").unwrap();
        let cfg = crate::browser::Config { enabled: true, lb: lb.to_string_lossy().into(), port: 8792, passcode: "p".repeat(64),
                                           listen: "127.0.0.1:8793".into(), public_url: None, max_tabs: None, tab_idle_secs: None, isolated: false };
        std::fs::write(crate::browser::config_path(&paths.home), serde_json::to_vec(&cfg).unwrap()).unwrap();
        let s = Arc::new(Server::new(paths.clone()).unwrap());
        let id = h(&s, json!({"op": "create", "name": "dev"})).await.unwrap()["id"].as_str().unwrap().to_string();
        // Pretend its CLI is live, so ensure_live returns without starting one.
        s.store.lock().await.bind_harness_id(&id, "11111111-2222-3333-4444-555555555555").unwrap();
        s.live.lock().await.insert("dev".into());
        // Before the fix, the fingerprint block awaited a second store lock while holding
        // one and hung forever (the server never reached its accept loop).
        let r = tokio::time::timeout(std::time::Duration::from_secs(2), s.run_turn("dev", "hi", "test", 0)).await;
        assert!(r.is_ok(), "run_turn deadlocked with Live Browser configured");
    }


    #[test]
    fn callback_outcome_and_text() {
        assert_eq!(turn_outcome(&Ok(json!({"reply": "hi", "stop_reason": "EndTurn"}))), ("done", "hi".to_string()));
        assert_eq!(turn_outcome(&Ok(json!({"reply": "", "stop_reason": "Cancelled"}))).0, "stopped");
        assert_eq!(turn_outcome(&Err(anyhow!("x: {}", claude_cli_acp::TURN_STOPPED))).0, "stopped");
        assert_eq!(turn_outcome(&Err(anyhow!("boom"))), ("error", "boom".to_string()));
        let t = callback_text("w1", "done", 42, &"y".repeat(CALLBACK_REPLY_MAX + 10));
        assert!(t.starts_with("[Kurisu callback] w1 finished your wait:false message (#42): done."), "{t}");
        assert!(t.contains("[truncated; full reply: session_history w1]"));
        assert!(callback_text("w1", "error", 1, "").contains("(no reply text)"));
    }

    async fn texts(s: &Arc<Server>, name: &str) -> Vec<(String, String)> {
        let id = h(s, json!({"op": "status", "name": name})).await.unwrap()["id"].as_str().unwrap().to_string();
        h(s, json!({"op": "history", "name": id})).await.unwrap().as_array().unwrap().iter()
            .map(|m| (m["role"].as_str().unwrap_or("").to_string(), m["text"].as_str().unwrap_or("").to_string())).collect()
    }

    #[tokio::test]
    async fn background_job_wakes_its_session_and_calls_back_the_requester() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths { home: dir.path().to_path_buf() };
        let s = Arc::new(Server::new(paths.clone()).unwrap());
        s.turns_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
        let lead = h(&s, json!({"op": "create", "name": "lead", "role": "lead"})).await.unwrap()["id"].as_str().unwrap().to_string();
        let w = h(&s, json!({"op": "create", "name": "w1", "caller": lead})).await.unwrap()["id"].as_str().unwrap().to_string();
        // w1's current turn answers a message from lead.
        s.note_requester(&w, "build it", &lead, 7).await;
        // Callbacks, wakes and follow-ups do not change who is answered.
        s.note_requester(&w, "[Kurisu callback] x", &lead, 8).await;
        s.note_requester(&w, "wake", "kurisu", 9).await;
        assert_eq!(s.requesters.lock().await.get(&w).cloned(), Some((Some(lead.clone()), 7)));
        // Only the session itself may start its jobs.
        assert!(h(&s, json!({"op": "bg_run", "name": w, "caller": lead, "command": "true"})).await.is_err());
        std::fs::create_dir_all(paths.workspace("w1")).unwrap();
        let r = h(&s, json!({"op": "bg_run", "name": w, "caller": w, "command": "echo built-ok; exit 0"})).await.unwrap();
        assert_eq!(r["ok"], true, "{r}");
        let t0 = std::time::Instant::now();
        loop {
            s.watch_followups().await.unwrap();
            if texts(&s, "w1").await.iter().any(|(_, t)| t.starts_with("[Kurisu background job")) {
                break;
            }
            assert!(t0.elapsed().as_secs() < 20, "job never delivered");
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        let wt = texts(&s, "w1").await;
        let wake = wt.iter().find(|(_, t)| t.starts_with("[Kurisu background job")).unwrap();
        assert_eq!(wake.0, "peer:kurisu");
        assert!(wake.1.contains("exit code 0") && wake.1.contains("built-ok"), "{wake:?}");
        // Its reply goes to lead, about lead's message #7.
        let cbs = s.store.lock().await.callbacks().unwrap();
        assert_eq!(cbs.len(), 1);
        assert_eq!((cbs[0].target.as_str(), cbs[0].sender.as_str(), cbs[0].seq), (w.as_str(), lead.as_str(), 7));
        // Delivered once only.
        s.watch_followups().await.unwrap();
        assert_eq!(texts(&s, "w1").await.iter().filter(|(_, t)| t.starts_with("[Kurisu background job")).count(), 1);
        let t = callback_text("w1", &format!("{FOLLOWUP}: background job j exited with code 0, then done"), 7, "ok");
        assert!(t.contains("followed up on your message (#7)"), "{t}");
    }

    #[tokio::test]
    async fn wait_false_sends_get_callbacks_and_restarts_resume() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths { home: dir.path().to_path_buf() };
        let (lead, w);
        {
            let s = Arc::new(Server::new(paths.clone()).unwrap());
            s.turns_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
            lead = h(&s, json!({"op": "create", "name": "lead", "role": "lead"})).await.unwrap()["id"].as_str().unwrap().to_string();
            w = h(&s, json!({"op": "create", "name": "w1", "caller": lead})).await.unwrap()["id"].as_str().unwrap().to_string();
            // wait:false from a session records a callback; notify:false, wait:true and the user do not
            let r = h(&s, json!({"op": "send", "name": w, "text": "task", "wait": false, "caller": lead})).await.unwrap();
            assert_eq!(r["callback"], true);
            h(&s, json!({"op": "send", "name": w, "text": "quiet", "wait": false, "notify": false, "caller": lead})).await.unwrap();
            h(&s, json!({"op": "send", "name": w, "text": "from user", "wait": false})).await.unwrap();
            let cbs = s.store.lock().await.callbacks().unwrap();
            assert_eq!(cbs.len(), 1);
            assert_eq!((cbs[0].target.as_str(), cbs[0].sender.as_str()), (w.as_str(), lead.as_str()));
            // delivering it messages the sender once (as a peer message), and only once
            s.deliver_callback(cbs[0].id, "done", "all good").await.unwrap();
            s.deliver_callback(cbs[0].id, "done", "again").await.unwrap();
            let lt = texts(&s, "lead").await;
            let got: Vec<_> = lt.iter().filter(|(_, t)| t.starts_with("[Kurisu callback]")).collect();
            assert_eq!(got.len(), 1, "{lt:?}");
            assert_eq!(got[0].0, "peer:w1");
            assert!(got[0].1.contains("done") && got[0].1.contains("all good"));
            // the callback itself asked for no callback
            assert!(s.store.lock().await.callbacks().unwrap().is_empty());
            // now: w1 is mid-turn on a wait:false message when the server goes away
            h(&s, json!({"op": "send", "name": w, "text": "long task", "wait": false, "caller": lead})).await.unwrap();
            let st = s.store.lock().await;
            st.bind_harness_id(&w, "cli-1").unwrap();
            st.set_status(&w, "working").unwrap();
        }
        {
            let s = Arc::new(Server::new(paths.clone()).unwrap());
            s.turns_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
            assert_eq!(*s.interrupted.lock().await, vec![w.clone()]);
            s.resume_interrupted().await.unwrap();
            let wt = texts(&s, "w1").await;
            assert!(wt.last().unwrap().1.starts_with(RESUME_TAG), "{wt:?}");
            // the callback waits for the resumed turn to end
            assert_eq!(s.store.lock().await.callbacks().unwrap().len(), 1);
            // and the resume turn is cut off too
            s.store.lock().await.set_status(&w, "working").unwrap();
        }
        let s = Arc::new(Server::new(paths.clone()).unwrap());
        s.turns_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
        s.resume_interrupted().await.unwrap();
        // not resumed twice; the sender hears it was interrupted
        assert_eq!(texts(&s, "w1").await.iter().filter(|(_, t)| t.starts_with(RESUME_TAG)).count(), 1);
        assert!(s.store.lock().await.callbacks().unwrap().is_empty());
        let last = texts(&s, "lead").await.last().unwrap().1.clone();
        assert!(last.contains("interrupted by a Kurisu restart") && last.contains("interrupted again while resuming"), "{last}");
        drop(s);
        // a clean shutdown resets statuses to "stopped" but notes the cut-off turns in a file
        std::fs::write(paths.home.join(INTERRUPTED_FILE), serde_json::to_vec(&vec![lead.clone()]).unwrap()).unwrap();
        let s = Arc::new(Server::new(paths.clone()).unwrap());
        assert_eq!(*s.interrupted.lock().await, vec![lead.clone()]);
        assert!(!paths.home.join(INTERRUPTED_FILE).exists());
    }


    #[test]
    fn agents_md_gets_messenger_rule_only_with_telegram() {
        let home = tempfile::tempdir().unwrap();
        let w = tempfile::tempdir().unwrap();
        ensure_agents_md(home.path(), w.path());
        assert!(!w.path().join("AGENTS.md").exists(), "no Telegram: default unchanged");
        std::fs::write(home.path().join("telegram.json"), b"{}").unwrap();
        ensure_agents_md(home.path(), w.path());
        let t = std::fs::read_to_string(w.path().join("AGENTS.md")).unwrap();
        assert!(t.contains("Replies go to a messenger app: write plain text"), "{t}");
        std::fs::write(w.path().join("AGENTS.md"), "mine").unwrap();
        ensure_agents_md(home.path(), w.path());
        assert_eq!(std::fs::read_to_string(w.path().join("AGENTS.md")).unwrap(), "mine");
    }

    #[test]
    fn claude_md_imports_agents_md_and_prompt_says_so() {
        let dir = tempfile::tempdir().unwrap();
        ensure_claude_md(dir.path());
        assert_eq!(std::fs::read_to_string(dir.path().join("CLAUDE.md")).unwrap(), "@AGENTS.md\n");
        std::fs::write(dir.path().join("CLAUDE.md"), "mine").unwrap();
        ensure_claude_md(dir.path());
        assert_eq!(std::fs::read_to_string(dir.path().join("CLAUDE.md")).unwrap(), "mine", "an existing CLAUDE.md is kept");
        let st = store::Store::open(&dir.path().join("k.db")).unwrap();
        let c = st.create("w", "claude", "/tmp/w", "worker", store::DEFAULT_MODEL, "", None).unwrap();
        let x = st.create("x", "codex", "/tmp/x", "lead", "default", "", None).unwrap();
        assert!(session_prompt(&c, None).contains("AGENTS.md") && session_prompt(&c, None).contains("@AGENTS.md"));
        assert!(session_prompt(&x, None).contains("AGENTS.md") && !session_prompt(&x, None).contains("@AGENTS.md"));
    }

    use super::*;

    async fn h(s: &Arc<Server>, v: Value) -> Result<Value> {
        // Keep the large operation dispatcher out of each test future's stack.
        Box::pin(s.handle(v)).await
    }

    #[test]
    fn session_prompt_states_capabilities_and_never_denies_the_browser() {
        let sess = |role: &str, harness: &str| store::Session {
            id: "id".into(), name: "w1".into(), role: role.into(), harness: harness.into(), cwd: "/tmp/w".into(),
            harness_session_id: None, model: "m".into(), effort: None, status: "idle".into(), description: String::new(),
            note: String::new(), last_activity: None, created_by_session: None, created_by_name: None,
            lifecycle: "active".into(), created_at: 0, last_user_at: None, replaces: None, replaced_by: None,
            permission_mode: None,
        };
        // The manifest is there for every role and harness, whatever the browser state.
        // Every kurisu-mcp tool must be named in the always-present index, so a
        // budget-limited harness catalogue showing 9 of 21 cannot hide send_file.
        assert_eq!(crate::mcp::tools().as_array().unwrap().len(), 23, "update KURISU_TOOLS when the roster changes");
        for role in ["lead", "worker"] {
            let tool_names: Vec<String> = crate::mcp::tools_for(role == "lead").as_array().unwrap().iter()
                .map(|t| t["name"].as_str().unwrap().to_string()).collect();
            for harness in ["claude", "codex", "opencode", "agy"] {
                let p = session_prompt(&sess(role, harness), None);
                assert!(p.contains("run a real CLI on this machine"), "{role}/{harness}: {p}");
                for fact in ["shell commands", "workspace folder", "Telegram", "MCP servers", "say what failed"] {
                    assert!(p.contains(fact), "{role}/{harness} lacks {fact:?}: {p}");
                }
                for tool in &tool_names {
                    assert!(p.contains(tool.as_str()), "{role}/{harness} lacks tool {tool:?}: {p}");
                }
                assert!(p.contains("tools[\"kurisu-mcp\"].send_file"), "{role}/{harness}: {p}");
                // ask_user is the lead's alone; non-Claude leads are told when to use it.
                assert_eq!(p.contains("ask_user"), role == "lead", "{role}/{harness}");
                assert_eq!(p.contains("call kurisu-mcp's ask_user"), role == "lead" && harness != "claude", "{role}/{harness}");
            }
        }
        let on = session_prompt(&sess("worker", "claude"), Some(true));
        let off = session_prompt(&sess("worker", "claude"), Some(false));
        let absent = session_prompt(&sess("worker", "claude"), None);
        assert_ne!(on, off);
        assert_ne!(off, absent);
        assert_ne!(on, absent);
        // On: the tool paragraph, and no denial of the capability.
        assert!(on.contains("lb_*") && !on.contains("no browser") && !on.contains("not installed on this machine"), "{on}");
        // The documented page -> file -> send_file flow must be possible, with the
        // code-mode call syntax and the lb CLI's env named explicitly.
        assert!(on.contains("lb shot --tab") && on.contains("--out shot.jpg"), "{on}");
        assert!(on.contains("LB_SERVER") && on.contains("LB_PASSCODE"), "{on}");
        assert!(on.contains("tools[\"kurisu-mcp\"].send_file({path: \"shot.jpg\""), "{on}");
        assert!(on.contains("require `ok:true`") && on.contains("NOT a screenshot the user received"), "{on}");
        // Off: a real browser that is switched off, plus how to switch it on.
        assert!(off.contains("It is off for you now."), "{off}");
        assert!(off.contains("do have a browser") && off.contains("Do not tell the user you have no browser"), "{off}");
        assert!(off.contains("session_update {id, browser: \"on\"}"), "{off}");
        // Not set up: say it is not installed instead of claiming an off switch exists.
        assert!(absent.contains("not installed on this machine") && !absent.contains("It is off for you now."), "{absent}");
        assert!(absent.contains("ku browser setup") && !absent.contains("do have a browser"), "{absent}");
    }

    #[tokio::test]
    async fn lineage_authority_and_audit() {
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().to_path_buf() }).unwrap());
        h(&s, json!({"op": "create", "name": "lead", "role": "lead"})).await.unwrap();
        let w = h(&s, json!({"op": "create", "name": "worker-a", "description": "math", "caller": "lead"})).await.unwrap();
        assert_eq!(w["created_by_name"], "lead");
        assert!(crate::store::looks_like_uuid(w["id"].as_str().unwrap()));
        // by UUID works the same as by name
        let wid = w["id"].as_str().unwrap().to_string();
        assert_eq!(h(&s, json!({"op": "status", "name": wid})).await.unwrap()["name"], "worker-a");
        assert_eq!(w["model"], "claude-haiku-5-5");
        h(&s, json!({"op": "create", "name": "sub", "caller": "worker-a"})).await.unwrap();
        // a session cannot mint a lead
        assert!(h(&s, json!({"op": "create", "name": "x", "role": "lead", "caller": "lead"})).await.is_err());
        // ancestors may modify descendants (parent and grandparent)
        h(&s, json!({"op": "update", "name": "worker-a", "caller": "lead", "fields": {"effort": "low", "description": "arithmetic"}})).await.unwrap();
        h(&s, json!({"op": "update", "name": "sub", "caller": "lead", "fields": {"task": "idle"}})).await.unwrap();
        // non-ancestors are refused with a clear error
        let e = h(&s, json!({"op": "update", "name": "lead", "caller": "worker-a", "fields": {"description": "pwned"}})).await.unwrap_err();
        assert!(e.to_string().contains("permission denied: worker-a is not an ancestor of lead"), "{e}");
        assert!(h(&s, json!({"op": "update", "name": "worker-a", "caller": "sub", "fields": {"effort": "max"}})).await.is_err());
        // a session may update its own effort/model/task/status/description, but not its lifecycle/role/name
        let r = h(&s, json!({"op": "update", "name": "worker-a", "caller": "worker-a", "fields": {"effort": "high", "description": "self", "status": "busy"}})).await.unwrap();
        assert_eq!(r["changes"].as_array().unwrap().len(), 3, "{r}");
        assert_eq!(h(&s, json!({"op": "status", "name": "worker-a"})).await.unwrap()["note"], "busy");
        h(&s, json!({"op": "update", "name": wid, "caller": "worker-a", "fields": {"model": "claude-sonnet-5-5"}})).await.unwrap();
        for f in [json!({"lifecycle": "archived"}), json!({"lifecycle": "paused"}), json!({"role": "worker"}), json!({"name": "x"})] {
            let e = h(&s, json!({"op": "update", "name": "worker-a", "caller": "worker-a", "fields": f})).await.unwrap_err();
            assert!(e.to_string().contains("cannot change its own"), "{e}");
        }
        assert!(h(&s, json!({"op": "update", "name": "worker-a", "caller": "lead", "fields": {"role": "lead"}})).await.is_err());
        assert!(h(&s, json!({"op": "update", "name": "worker-a", "caller": "lead", "fields": {"cwd": "/"}})).await.is_err());
        // the user may modify anything
        h(&s, json!({"op": "update", "name": "lead", "fields": {"effort": "high"}})).await.unwrap();
        // model/effort changes are queued for the next turn
        assert!(s.relaunch.lock().await.contains(&wid));
        let peers = h(&s, json!({"op": "peers"})).await.unwrap();
        let lead = peers.as_array().unwrap().iter().find(|p| p["name"] == "lead").unwrap().clone();
        assert_eq!(lead["children"][0]["name"], "worker-a");
        assert!(lead["created_by_session"].is_null());
        // pause blocks sends; archive hides from peers_list
        h(&s, json!({"op": "update", "name": "sub", "caller": "worker-a", "fields": {"lifecycle": "paused"}})).await.unwrap();
        let e = h(&s, json!({"op": "send", "name": "sub", "text": "hi", "caller": "lead"})).await.unwrap_err();
        assert!(e.to_string().contains("paused"), "{e}");
        h(&s, json!({"op": "update", "name": "sub", "fields": {"lifecycle": "archived"}})).await.unwrap();
        assert_eq!(h(&s, json!({"op": "peers"})).await.unwrap().as_array().unwrap().len(), 2);
        let audit = h(&s, json!({"op": "audit"})).await.unwrap();
        let actors: Vec<_> = audit.as_array().unwrap().iter().map(|a| (a["actor_name"].as_str().unwrap().to_string(), a["field"].as_str().unwrap().to_string())).collect();
        // rename keeps the id; the new name resolves, the old one does not
        h(&s, json!({"op": "update", "name": "worker-a", "caller": "lead", "fields": {"name": "math"}})).await.unwrap();
        assert_eq!(h(&s, json!({"op": "status", "name": "math"})).await.unwrap()["id"], wid);
        assert!(h(&s, json!({"op": "status", "name": "worker-a"})).await.is_err());
        assert!(actors.contains(&("lead".into(), "effort".into())));
        assert!(actors.contains(&("user".into(), "effort".into())));
        // sending to yourself is refused
        assert!(h(&s, json!({"op": "send", "name": "lead", "text": "x", "caller": "lead"})).await.is_err());
        // bridge helpers: the lead, the model catalogue, global messages, created_at, cwd/harness checks
        assert_eq!(h(&s, json!({"op": "lead_get"})).await.unwrap()["name"], "lead");
        let m = h(&s, json!({"op": "models"})).await.unwrap();
        assert!(m["models"].as_array().unwrap().iter().any(|x| x["id"] == "claude-haiku-5-5" && x["default"] == true));
        let mc = h(&s, json!({"op": "models", "harness": "codex"})).await.unwrap();
        assert!(mc["models"].as_array().unwrap().iter().any(|x| x["id"] == "default" && x["default"] == true));
        assert!(mc["models"].as_array().unwrap().iter().any(|x| x["id"] == "gpt-6.1-sol"));
        s.store.lock().await.append(&wid, "user", "hello").unwrap();
        let all = h(&s, json!({"op": "messages_after", "after": 0})).await.unwrap();
        assert_eq!(all["messages"][0]["session_name"], "math");
        let st = h(&s, json!({"op": "status", "name": "math"})).await.unwrap();
        assert!(st["created_at"].as_i64().unwrap() > 0 && st["last_user_at"].as_i64().is_some());
        assert!(h(&s, json!({"op": "create", "name": "rel", "cwd": "relative/dir"})).await.is_err());
        assert!(h(&s, json!({"op": "create", "name": "bad", "harness": "nope"})).await.is_err());
    }

    fn perm(rid: &str, tool: &str, input: Value, cwd: &str) -> RequestPermissionRequest {
        let ev = &claude_cli_acp::parse_events(&json!({"hook_event_name": "PermissionRequest", "tool_name": tool, "tool_input": input,
            "cwd": cwd, "kurisu_request_id": rid, "expires_at": now() + 600}).to_string())[0];
        claude_cli_acp::permission_request(&SessionId::new("x"), ev).unwrap()
    }

    #[tokio::test]
    async fn session_wide_always_allow_covers_any_path() {
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().to_path_buf() }).unwrap());
        let w = h(&s, json!({"op": "create", "name": "dev", "cwd": "/w/dev"})).await.unwrap();
        let wid = w["id"].as_str().unwrap().to_string();
        s.store.lock().await.bind_harness_id(&wid, "11111111-2222-3333-4444-555555555555").unwrap();
        let input = json!({"command": "/opt/tool/bin/x --flag=/etc/y"});
        s.on_permission(&wid, perm("w-1", "Bash", input.clone(), "/w/dev")).await.unwrap();
        let id = h(&s, json!({"op": "approvals"})).await.unwrap()[0]["id"].as_str().unwrap().to_string();
        let r = h(&s, json!({"op": "approve", "id": id, "always": true, "session_wide": true})).await.unwrap();
        assert_eq!(r["rule"]["in_cwd"], json!(false));
        // The rule now covers the same command outside the session folder, so it is
        // settled without another card (the folder-limited rule would not match it).
        let settled = s.on_permission(&wid, perm("w-2", "Bash", input, "/w/dev")).await.unwrap();
        assert_eq!(settled.map(|d| d.allow), Some(true));
    }

    #[tokio::test]
    async fn approvals_rules_decisions_and_audit() {
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().to_path_buf() }).unwrap());
        let w = h(&s, json!({"op": "create", "name": "dev", "cwd": "/w/dev"})).await.unwrap();
        let wid = w["id"].as_str().unwrap().to_string();
        let hid = "11111111-2222-3333-4444-555555555555";
        s.store.lock().await.bind_harness_id(&wid, hid).unwrap();
        let decision = |rid: &str| -> Option<Value> {
            let p = dir.path().join("claude").join(hid).join("decisions").join(format!("{rid}.json"));
            std::fs::read(p).ok().map(|b| serde_json::from_slice(&b).unwrap())
        };
        // no rule: queued for the user, session awaits approval
        s.on_permission(&wid, perm("r-1", "Bash", json!({"command": "ls"}), "/w/dev")).await.unwrap();
        let pending = h(&s, json!({"op": "approvals"})).await.unwrap();
        assert_eq!(pending.as_array().unwrap().len(), 1);
        assert_eq!(h(&s, json!({"op": "status", "name": "dev"})).await.unwrap()["status"], "awaiting_approval");
        let id = pending[0]["id"].as_str().unwrap().to_string();
        assert_eq!(pending[0]["title"], "Bash: ls");
        // sessions cannot answer approvals; the user can, once
        assert!(h(&s, json!({"op": "approve", "id": id, "caller": "dev"})).await.is_err());
        let r = h(&s, json!({"op": "approve", "id": &id[..5]})).await.unwrap();
        assert_eq!(r["status"], "approved");
        assert_eq!(decision("r-1").unwrap()["behavior"], "allow");
        assert!(h(&s, json!({"op": "deny", "id": id})).await.unwrap_err().to_string().contains("already approved"));
        assert_eq!(h(&s, json!({"op": "status", "name": "dev"})).await.unwrap()["status"], "working");
        // deny with a reason
        s.on_permission(&wid, perm("r-2", "Write", json!({"file_path": "/etc/x"}), "/w/dev")).await.unwrap();
        let id2 = h(&s, json!({"op": "approvals"})).await.unwrap()[0]["id"].as_str().unwrap().to_string();
        h(&s, json!({"op": "deny", "id": id2, "reason": "not there"})).await.unwrap();
        assert_eq!(decision("r-2").unwrap(), json!({"behavior": "deny", "message": "The user denied this tool call: not there"}));
        // a role rule settles matching calls without asking; others still ask
        h(&s, json!({"op": "rule_add", "scope": "role", "target": "worker", "tool": "Bash", "in_cwd": true})).await.unwrap();
        let settled = s.on_permission(&wid, perm("r-3", "Bash", json!({"command": "cat src/a.rs"}), "/w/dev")).await.unwrap();
        assert_eq!(settled.map(|d| d.allow), Some(true));
        s.respond_approval(&wid, "r-3", &outcome(true), None).await.unwrap();
        assert_eq!(decision("r-3").unwrap()["behavior"], "allow");
        assert!(s.on_permission(&wid, perm("r-4", "Bash", json!({"command": "cat /etc/passwd"}), "/w/dev")).await.unwrap().is_none());
        assert!(decision("r-4").is_none());
        assert_eq!(h(&s, json!({"op": "approvals"})).await.unwrap().as_array().unwrap().len(), 1);
        // the turn ending cancels what is left
        s.cancel_pending(&wid, "the turn ended").await;
        assert!(h(&s, json!({"op": "approvals"})).await.unwrap().as_array().unwrap().is_empty());
        let all = h(&s, json!({"op": "approvals", "all": true})).await.unwrap();
        let statuses: Vec<_> = all.as_array().unwrap().iter().map(|a| a["status"].as_str().unwrap().to_string()).collect();
        assert_eq!(statuses, ["cancelled", "approved", "denied", "approved"]);
        assert_eq!(all[1]["decided_by"], "rule#1");
        // every decision is in the audit trail
        let audit = h(&s, json!({"op": "audit", "name": "dev"})).await.unwrap();
        let evs: Vec<_> = audit.as_array().unwrap().iter().map(|a| format!("{}:{}", a["actor_name"].as_str().unwrap(), a["field"].as_str().unwrap())).collect();
        for e in ["dev:event:approval_requested", "user:event:approval_approved", "user:event:approval_denied", "user:event:rule_added",
                  "rule#1:event:approval_approved", "kurisu:event:approval_cancelled"] {
            assert!(evs.contains(&e.to_string()), "{e} not in {evs:?}");
        }
        // sessions cannot add rules
        assert!(h(&s, json!({"op": "rule_add", "scope": "role", "target": "worker", "tool": "Write", "caller": "dev"})).await.is_err());
        // AskUserQuestion becomes a question: never settled by rules, answered with content
        h(&s, json!({"op": "rule_add", "scope": "role", "target": "worker", "tool": "*", "in_cwd": true})).await.unwrap();
        let q = json!({"questions": [{"question": "Which color?", "header": "Color", "options": [{"label": "Red"}, {"label": "Blue"}], "multiSelect": false}]});
        s.on_permission(&wid, perm("r-5", "AskUserQuestion", q, "/w/dev")).await.unwrap();
        let p = h(&s, json!({"op": "approvals"})).await.unwrap();
        assert_eq!((p[0]["kind"].as_str(), p[0]["title"].as_str()), (Some("question"), Some("Question: Which color?")));
        assert_eq!(p[0]["preview"], "Which color? [Red / Blue]");
        let qid = p[0]["id"].as_str().unwrap().to_string();
        assert!(h(&s, json!({"op": "approve", "id": qid})).await.unwrap_err().to_string().contains("is a question"));
        let r = h(&s, json!({"op": "answer", "id": qid, "answers": {"Color": "Blue"}})).await.unwrap();
        assert_eq!(r["answer"], json!({"Which color?": "Blue"}));
        let d = decision("r-5").unwrap();
        assert_eq!((d["behavior"].as_str(), d["updatedInput"]["answers"]["Which color?"].as_str()), (Some("allow"), Some("Blue")));
    }

    #[tokio::test]
    async fn ask_user_question_outlives_the_turn_and_a_late_answer_resumes_the_asker() {
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().to_path_buf() }).unwrap());
        s.test_no_turns();
        let lead = h(&s, json!({"op": "create", "name": "lead", "role": "lead"})).await.unwrap()["id"].as_str().unwrap().to_string();
        let ask = || {
            let (s, lead) = (s.clone(), lead.clone());
            tokio::spawn(async move {
                h(&s, json!({"op": "ask_user", "caller": lead, "question": "Which fruit?", "choices": ["Mango", "Kiwi", "Papaya"]})).await.unwrap()
            })
        };
        async fn pending(s: &Arc<Server>) -> Value {
            for _ in 0..200 {
                let p = h(s, json!({"op": "approvals"})).await.unwrap();
                if !p.as_array().unwrap().is_empty() { return p[0].clone(); }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            panic!("no pending question");
        }
        let messages = |s: Arc<Server>, id: String| async move {
            s.store.lock().await.history(&id, 0, 500).unwrap().into_iter().map(|m| (m.role, m.text)).collect::<Vec<_>>()
        };
        // It lives an hour by default, far beyond the 10-minute approval TTL.
        let t = ask();
        let q = pending(&s).await;
        assert!(q["expires_at"].as_i64().unwrap() - now() > approval_ttl() as i64);
        // The turn ends (Claude Code backgrounded the call): the call learns it is still
        // open, and the question stays pending instead of being withdrawn.
        s.end_turn_pending(&lead).await;
        let r = t.await.unwrap();
        assert_eq!(r["status"], "pending");
        assert!(r["message"].as_str().unwrap().contains("end your turn"), "{r}");
        let still = h(&s, json!({"op": "approvals"})).await.unwrap();
        assert_eq!(still[0]["id"], q["id"]);
        // A late answer is delivered to the asker as a new message (which starts its turn).
        h(&s, json!({"op": "answer", "id": q["id"], "answers": {"0": "Kiwi"}})).await.unwrap();
        let hist = messages(s.clone(), lead.clone()).await;
        assert!(hist.contains(&("user".to_string(), "Answer to your question 'Which fruit?': Kiwi".to_string())), "{hist:?}");
        assert!(h(&s, json!({"op": "approvals"})).await.unwrap().as_array().unwrap().is_empty());
        // Dismissing a late question just closes it: no message.
        let t = ask();
        let q = pending(&s).await;
        s.end_turn_pending(&lead).await;
        t.await.unwrap();
        let before = messages(s.clone(), lead.clone()).await.len();
        h(&s, json!({"op": "deny", "id": q["id"]})).await.unwrap();
        assert_eq!(messages(s.clone(), lead.clone()).await.len(), before);
        // ...and leaves the session idle, not `working` with no turn (qa on #117).
        assert_eq!(h(&s, json!({"op": "status", "name": "lead"})).await.unwrap()["status"], "idle");
        // Stop still withdraws it.
        let t = ask();
        pending(&s).await;
        s.cancel_pending(&lead, "the turn was stopped").await;
        assert_eq!(t.await.unwrap()["status"], "dismissed");
        assert!(h(&s, json!({"op": "approvals"})).await.unwrap().as_array().unwrap().is_empty());
        // An in-time answer still comes back as the tool result, with no extra message.
        let t = ask();
        let q = pending(&s).await;
        let before = messages(s.clone(), lead.clone()).await.len();
        h(&s, json!({"op": "answer", "id": q["id"], "answers": {"0": "Mango"}})).await.unwrap();
        assert_eq!(t.await.unwrap(), json!({"status": "answered", "answer": "Mango"}));
        assert_eq!(messages(s.clone(), lead.clone()).await.len(), before);
        assert_eq!(ask_wait(), std::time::Duration::from_secs(100), "under Claude Code's 120 s cut-off");
    }

    #[tokio::test]
    async fn ask_user_waits_for_the_answer_and_reuses_question_approvals() {
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().to_path_buf() }).unwrap());
        let lead = h(&s, json!({"op": "create", "name": "lead", "role": "lead"})).await.unwrap()["id"].as_str().unwrap().to_string();
        let ask = |caller: &str| {
            let (s, caller) = (s.clone(), caller.to_string());
            tokio::spawn(async move {
                h(&s, json!({"op": "ask_user", "caller": caller, "question": "Which fruit?", "choices": ["Mango", "Kiwi", "Papaya"], "header": "Fruit"})).await.unwrap()
            })
        };
        async fn pending(s: &Arc<Server>) -> Value {
            for _ in 0..200 {
                let p = h(s, json!({"op": "approvals"})).await.unwrap();
                if !p.as_array().unwrap().is_empty() { return p[0].clone(); }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            panic!("no pending question");
        }
        // Answered: a question approval with the choices; the answer is the tool result.
        let t = ask(&lead);
        let q = pending(&s).await;
        assert_eq!((q["kind"].as_str(), q["title"].as_str()), (Some("question"), Some("Question: Which fruit?")));
        assert_eq!(q["preview"], "Which fruit? [Mango / Kiwi / Papaya]");
        assert_eq!(q["questions"][0]["header"], "Fruit");
        assert_eq!(h(&s, json!({"op": "status", "name": "lead"})).await.unwrap()["status"], "awaiting_approval");
        assert!(!t.is_finished(), "ask_user blocks until answered");
        // only the user answers; approve is refused for questions
        assert!(h(&s, json!({"op": "answer", "id": q["id"], "caller": lead, "answers": {"Fruit": "Kiwi"}})).await.is_err());
        assert!(h(&s, json!({"op": "approve", "id": q["id"]})).await.is_err());
        h(&s, json!({"op": "answer", "id": q["id"], "answers": {"Fruit": "Kiwi"}})).await.unwrap();
        assert_eq!(t.await.unwrap(), json!({"status": "answered", "answer": "Kiwi"}));
        // No turn is running in this test, so the session goes back to idle (in a turn: working).
        assert_eq!(h(&s, json!({"op": "status", "name": "lead"})).await.unwrap()["status"], "idle");
        // Free text (a typed or spoken answer) is accepted as is.
        let t = ask(&lead);
        let q = pending(&s).await;
        h(&s, json!({"op": "answer", "id": q["id"], "answers": {"0": "a ripe banana"}})).await.unwrap();
        assert_eq!(t.await.unwrap()["answer"], "a ripe banana");
        // Dismissed (deny), with the reason.
        let t = ask(&lead);
        let q = pending(&s).await;
        h(&s, json!({"op": "deny", "id": q["id"], "reason": "later"})).await.unwrap();
        let r = t.await.unwrap();
        assert_eq!((r["status"].as_str(), r["message"].as_str()), (Some("dismissed"), Some("The user dismissed the question: later")));
        // Stop / turn end withdraws it.
        let t = ask(&lead);
        pending(&s).await;
        s.cancel_pending(&lead, "the turn ended").await;
        let r = t.await.unwrap();
        assert_eq!(r["status"], "dismissed");
        assert!(r["message"].as_str().unwrap().contains("withdrawn"), "{r}");
        // Lead only: a worker (created by a session, or a chat session) is refused, no card.
        let w = h(&s, json!({"op": "create", "name": "w1", "caller": lead})).await.unwrap()["id"].as_str().unwrap().to_string();
        let chat = h(&s, json!({"op": "create", "name": "chat", "role": "worker"})).await.unwrap()["id"].as_str().unwrap().to_string();
        let e = h(&s, json!({"op": "ask_user", "caller": w, "question": "x"})).await.unwrap_err().to_string();
        assert!(e.contains("only for the lead") && e.contains("`lead`"), "{e}");
        assert!(h(&s, json!({"op": "ask_user", "caller": chat, "question": "x"})).await.is_err());
        assert!(h(&s, json!({"op": "approvals"})).await.unwrap().as_array().unwrap().is_empty());
        // Not callable without a session.
        assert!(h(&s, json!({"op": "ask_user", "question": "x"})).await.is_err());
    }

    #[tokio::test]
    async fn attachments_land_in_the_session_folder() {
        assert_eq!(safe_filename("../../etc/pass wd"), "pass_wd");
        assert_eq!(safe_filename(".bashrc"), "bashrc");
        assert_eq!(safe_filename(""), "file");
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().to_path_buf() }).unwrap());
        let cwd = dir.path().join("w");
        let id = h(&s, json!({"op": "create", "name": "dev", "cwd": cwd})).await.unwrap()["id"].as_str().unwrap().to_string();
        let r = h(&s, json!({"op": "upload", "name": "dev", "filename": "../x.txt", "data_b64": "aGVsbG8="})).await.unwrap();
        let p = PathBuf::from(r["path"].as_str().unwrap());
        assert!(p.starts_with(cwd.join(".kurisu-uploads")) && p.to_string_lossy().ends_with("-x.txt"));
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "hello");
        let t = s.attach(&id, &json!({"images": [{"mime": "image/jpeg", "data_b64": "data:image/jpeg;base64,aGk="}]}), "look").await.unwrap();
        assert!(t.starts_with("look\n\n[Attached") && t.contains(".jpeg"), "{t}");
        assert!(h(&s, json!({"op": "upload", "name": "dev", "data_b64": "!!"})).await.is_err());
        // Speech-to-text is optional: off, a voice file passes through; on, it is transcribed.
        let voice = json!({"files": [{"name": "voice.ogg", "data_b64": "T2dnUw=="}]});
        let t = s.attach(&id, &voice, "").await.unwrap();
        assert!(t.contains("voice.ogg") && !t.contains("transcript"), "{t}");
        assert!(h(&s, json!({"op": "transcribe", "path": "x.ogg"})).await.unwrap_err().to_string().contains("not set up"));
        crate::stt::save(dir.path(), &crate::stt::Config::Command { command: crate::stt::split_command("sh -c 'echo hello from $(basename $1)' _") }).unwrap();
        let t = s.attach(&id, &voice, "listen").await.unwrap();
        assert!(t.starts_with("listen") && t.contains("transcript (may contain mistakes):]\nhello from") && t.contains("-voice.ogg"), "{t}");
        let up = h(&s, json!({"op": "upload", "name": "dev", "filename": "note.opus", "data_b64": "T2dnUw=="})).await.unwrap();
        assert!(up["transcript"].as_str().unwrap().starts_with("hello from"), "{up}");
        let up = h(&s, json!({"op": "upload", "name": "dev", "filename": "a.txt", "data_b64": "aGk="})).await.unwrap();
        assert!(up.get("transcript").is_none());
        let r = h(&s, json!({"op": "transcribe", "path": ".kurisu-uploads/../.kurisu-uploads", "caller": id})).await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn history_pages_searches_and_hands_off() {
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().to_path_buf() }).unwrap());
        let id = h(&s, json!({"op": "create", "name": "old", "cwd": dir.path().join("w")})).await.unwrap()["id"].as_str().unwrap().to_string();
        {
            let st = s.store.lock().await;
            st.append(&id, "user", "first question").unwrap();
            st.append(&id, "agent", "first answer").unwrap();
            st.append(&id, "compaction", "SUMMARY: we chose sqlite").unwrap();
            for i in 0..5 {
                st.append(&id, "user", &format!("q{i}")).unwrap();
                st.append_data(&id, "tool", "Bash: ls", &json!({"tool": "Bash", "status": "completed", "input": {"command": "ls"}, "output": "Cargo.toml"})).unwrap();
                st.append(&id, "agent", &format!("a{i} 100% done_x")).unwrap();
            }
        }
        // Newest page first, paging back with before.
        let p = h(&s, json!({"op": "history_page", "name": "old", "limit": 4})).await.unwrap();
        assert_eq!(p["total"], 13);
        let texts: Vec<&str> = p["messages"].as_array().unwrap().iter().map(|m| m["text"].as_str().unwrap()).collect();
        assert_eq!(texts, ["q3", "a3 100% done_x", "q4", "a4 100% done_x"]);
        assert_eq!(p["messages"].as_array().unwrap().len(), 4);
        assert_eq!(p["has_more"], true);
        let older = h(&s, json!({"op": "history_page", "name": "old", "limit": 200, "before": p["first_seq"]})).await.unwrap();
        assert_eq!(older["messages"].as_array().unwrap().len(), 9);
        assert_eq!(older["has_more"], false);
        // Tools on request, search including LIKE metacharacters, string numbers from agents.
        let t = h(&s, json!({"op": "history_page", "name": "old", "tools": "true", "limit": "200"})).await.unwrap();
        assert_eq!(t["total"], 18);
        assert_eq!(t["messages"][2]["role"], "compaction");
        let q = h(&s, json!({"op": "history_page", "name": "old", "search": "0% DONE_"})).await.unwrap();
        assert_eq!(q["total"], 5);
        assert_eq!(h(&s, json!({"op": "history_page", "name": "old", "search": "Cargo", "tools": true})).await.unwrap()["total"], 5);
        assert_eq!(h(&s, json!({"op": "history_page", "name": "old", "roles": "compaction"})).await.unwrap()["total"], 1);
        // Legacy readers (web channel, bridge polling) do not see tool rows.
        assert!(h(&s, json!({"op": "history", "name": "old"})).await.unwrap().as_array().unwrap().iter().all(|m| m["role"] != "tool"));
        assert!(h(&s, json!({"op": "messages_after", "after": 0})).await.unwrap()["messages"].as_array().unwrap().iter().all(|m| m["role"] != "tool"));
        // Handoff: summary plus everything after it, newest kept when over budget.
        let ho = h(&s, json!({"op": "handoff", "name": "old"})).await.unwrap();
        assert_eq!(ho["compaction_summary"]["text"], "SUMMARY: we chose sqlite");
        assert_eq!(ho["messages"].as_array().unwrap().len(), 15);
        assert_eq!(ho["messages"][0]["text"], "q0");
        assert_eq!(ho["omitted_older"], 0);
        assert_eq!(ho["session"]["name"], "old");
        let small = h(&s, json!({"op": "handoff", "name": "old", "max_chars": 700})).await.unwrap();
        let n = small["messages"].as_array().unwrap().len();
        assert!(n < 15 && small["omitted_older"].as_i64().unwrap() == 15 - n as i64, "{small}");
        assert_eq!(small["messages"].as_array().unwrap().last().unwrap()["text"], "a4 100% done_x");
    }

    #[tokio::test]
    async fn wakes_schedule_authority_and_late_firing() {
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().to_path_buf() }).unwrap());
        let lead = h(&s, json!({"op": "create", "name": "lead", "role": "lead", "cwd": dir.path().join("l")})).await.unwrap()["id"].as_str().unwrap().to_string();
        let kid = h(&s, json!({"op": "create", "name": "kid", "caller": lead, "cwd": dir.path().join("k")})).await.unwrap()["id"].as_str().unwrap().to_string();
        let other = h(&s, json!({"op": "create", "name": "other", "cwd": dir.path().join("o")})).await.unwrap()["id"].as_str().unwrap().to_string();
        // A session may wake itself and its descendants, not others.
        let w = h(&s, json!({"op": "wake_add", "name": kid, "caller": lead, "at": "in 30m", "message": "check the build"})).await.unwrap();
        assert!((1795..=1800).contains(&w["in_seconds"].as_i64().unwrap()), "{w}");
        h(&s, json!({"op": "wake_add", "name": kid, "caller": kid, "at": "in 1h", "message": "me"})).await.unwrap();
        let e = h(&s, json!({"op": "wake_add", "name": lead, "caller": kid, "at": "in 1h", "message": "x"})).await.unwrap_err();
        assert!(e.to_string().contains("permission denied"), "{e}");
        assert!(h(&s, json!({"op": "wake_add", "name": other, "caller": lead, "at": "in 1h", "message": "x"})).await.is_err());
        assert!(h(&s, json!({"op": "wake_add", "name": "kid", "at": "2001-01-01 00:00", "message": "x"})).await.unwrap_err().to_string().contains("past"));
        assert!(h(&s, json!({"op": "wake_add", "name": "kid", "message": "x"})).await.is_err());
        assert!(h(&s, json!({"op": "wake_add", "name": "kid", "at": "in 5m", "repeat": "0 6 * *", "message": "x"})).await.is_err());
        // Lists follow the same lineage; other sees nothing, the user sees all.
        assert_eq!(h(&s, json!({"op": "wakes", "caller": lead})).await.unwrap().as_array().unwrap().len(), 2);
        assert_eq!(h(&s, json!({"op": "wakes", "caller": other})).await.unwrap().as_array().unwrap().len(), 0);
        let rep = h(&s, json!({"op": "wake_add", "name": "other", "repeat": "0 6 * * *", "message": "standup"})).await.unwrap();
        assert_eq!(h(&s, json!({"op": "wakes"})).await.unwrap().as_array().unwrap().len(), 3);
        // Cancel: not by an unrelated session; yes by the creator.
        let id = w["id"].as_i64().unwrap();
        assert!(h(&s, json!({"op": "wake_cancel", "id": id, "caller": other})).await.is_err());
        h(&s, json!({"op": "wake_cancel", "id": id, "caller": lead})).await.unwrap();
        assert!(h(&s, json!({"op": "wake_cancel", "id": id})).await.unwrap_err().to_string().contains("not pending"));
        // Missed while down: fires once, marked late. A paused target makes delivery fail
        // (no CLI is started here); the repeating wake moves on to its next time.
        h(&s, json!({"op": "update", "name": "other", "fields": {"lifecycle": "paused"}})).await.unwrap();
        let rid = rep["id"].as_i64().unwrap();
        s.store.lock().await.db_exec_for_tests(&format!("UPDATE wakes SET at = unixepoch() - 3600 WHERE id = {rid}"));
        s.fire_wakes().await.unwrap();
        let after = s.store.lock().await.wake_get(rid).unwrap().unwrap();
        assert!(after.last_late && after.status == "pending" && after.at > now() && after.fired_count == 1, "{after:?}");
        assert!(after.last_error.as_deref().unwrap_or("").contains("paused"), "{after:?}");
        let audit = h(&s, json!({"op": "audit"})).await.unwrap().to_string();
        assert!(audit.contains("event:wake_scheduled") && audit.contains("event:wake_cancelled") && audit.contains("event:wake_failed"));
    }

    #[tokio::test]
    async fn clis_op_reports_every_harness_including_pi() {
        // Guards the wiring: a probe can exist in hostcheck and still be left out of the payload,
        // which silently hides that harness from Telegram /model and the web CLI list.
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().to_path_buf() }).unwrap());
        let v = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            h(&s, json!({"op": "clis", "fresh": true})),
        )
        .await
        .expect("clis op timed out")
        .unwrap();
        let names: Vec<String> = v["clis"].as_array().unwrap().iter().filter_map(|c| c["name"].as_str().map(String::from)).collect();
        for harness in crate::harness::HARNESSES {
            assert!(names.iter().any(|n| n == harness), "clis op is missing {harness}: {names:?}");
        }
        assert!(names.iter().any(|n| n == "stt"), "clis op is missing stt: {names:?}");
    }

    #[test]
    fn permission_modes_map_per_cli() {
        let help = "  --permission-mode <mode>   Permission mode to use for the session\n      (choices: \"acceptEdits\", \"auto\",\n       \"bypassPermissions\", \"manual\",\n \"dontAsk\", \"plan\")\n  --other";
        let m = parse_modes(help);
        assert_eq!(m, ["acceptEdits", "auto", "bypassPermissions", "manual", "dontAsk", "plan"]);
        assert_eq!(effective_mode(None, &m), "auto");
        assert_eq!(effective_mode(Some("plan"), &m), "plan");
        assert_eq!(claude_mode_arg("default", &m), "manual");
        assert_eq!(claude_mode_arg("auto", &m), "auto");
        let old: Vec<String> = ["acceptEdits", "default", "plan"].iter().map(|x| x.to_string()).collect();
        assert_eq!(effective_mode(None, &old), "default");
        assert_eq!(claude_mode_arg("auto", &old), "default");
        assert_eq!(codex_mode("auto"), ("on-request", "workspace-write"));
        assert_eq!(codex_mode("default"), ("untrusted", "workspace-write"));
        assert_eq!(codex_mode("plan"), ("on-request", "read-only"));
    }

    #[test]
    fn default_harness_and_missing_cli_messages() {
        let clis = |c: Value, x: Value| json!({"clis": [c, x]});
        let cl = |i: bool, l: Option<bool>| json!({"name": "claude", "installed": i, "logged_in": l});
        let cx = |i: bool, l: Option<bool>| json!({"name": "codex", "installed": i, "logged_in": l});
        assert_eq!(pick_harness(&clis(cl(true, Some(true)), cx(true, Some(true)))).unwrap(), "claude");
        assert_eq!(pick_harness(&clis(cl(false, None), cx(true, Some(true)))).unwrap(), "codex");
        assert_eq!(pick_harness(&clis(cl(true, Some(false)), cx(true, Some(true)))).unwrap(), "codex");
        assert_eq!(pick_harness(&clis(cl(true, None), cx(false, None))).unwrap(), "claude");
        assert!(pick_harness(&clis(cl(false, None), cx(false, None))).is_err());
        let e = require_cli_with("claude", |h| h == "codex").unwrap_err().to_string();
        assert_eq!(e, "claude not found on PATH; install it or use --harness codex");
        assert!(require_cli_with("codex", |_| false).unwrap_err().to_string().contains("neither is claude"));
        assert!(require_cli_with("codex", |h| h == "codex").is_ok());
    }

    #[tokio::test]
    async fn handoff_follows_the_switch_chain() {
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().to_path_buf() }).unwrap());
        let a = h(&s, json!({"op": "create", "name": "w", "cwd": dir.path().join("w")})).await.unwrap()["id"].as_str().unwrap().to_string();
        s.store.lock().await.append(&a, "agent", "ORIGINAL plan: use sqlite").unwrap();
        let b = h(&s, json!({"op": "reharness", "name": a, "harness": "codex", "seed": false})).await.unwrap()["new"]["id"].as_str().unwrap().to_string();
        s.store.lock().await.append(&b, "error", "turn failed: codex broke").unwrap();
        let c = h(&s, json!({"op": "reharness", "name": b, "harness": "claude", "seed": false})).await.unwrap()["new"]["id"].as_str().unwrap().to_string();
        let ho = h(&s, json!({"op": "handoff", "name": c})).await.unwrap();
        assert_eq!(ho["chain"].as_array().unwrap().len(), 3);
        let texts: Vec<&str> = ho["messages"].as_array().unwrap().iter().map(|m| m["text"].as_str().unwrap()).collect();
        assert_eq!(texts, ["ORIGINAL plan: use sqlite", "turn failed: codex broke"]);
        assert!(ho["messages"][0]["session"].as_str().unwrap().contains("(claude)"));
        assert_eq!(ho["total_messages"], 2);
        // A compaction summary in an earlier link ends the walk there.
        s.store.lock().await.append(&b, "compaction", "SUMMARY of a").unwrap();
        let ho = h(&s, json!({"op": "handoff", "name": c})).await.unwrap();
        assert_eq!(ho["compaction_summary"]["text"], "SUMMARY of a");
        assert!(ho["messages"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_session_can_update_itself_through_a_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().to_path_buf() }).unwrap());
        let id = |v: Value| v["id"].as_str().unwrap().to_string();
        let lead = id(h(&s, json!({"op": "create", "name": "lead", "role": "lead", "cwd": dir.path().join("l")})).await.unwrap());
        let other = id(h(&s, json!({"op": "create", "name": "other", "cwd": dir.path().join("o")})).await.unwrap());
        // The Lead self-switches; the old row is archived and the replacement keeps the name.
        let r = h(&s, json!({"op": "update", "name": "lead", "caller": lead, "fields": {"harness": "codex"}})).await.unwrap();
        assert_eq!(r["scheduled"], true);
        s.pending_switch.lock().await.clear();
        let old = s.store.lock().await.get(&lead).unwrap().unwrap();
        let r = s.do_reharness(&lead, &old, "codex", None, false, false).await.unwrap();
        let new = id(r["new"].clone());
        assert_ne!(new, lead);
        assert_eq!(s.store.lock().await.get(&lead).unwrap().unwrap().replaced_by.as_deref(), Some(new.as_str()));
        // Its MCP env still has the old id and it names itself "lead": that must be self.
        let r = h(&s, json!({"op": "update", "name": "lead", "caller": lead,
                            "fields": {"model": "default", "effort": "high", "task": "self-update", "description": "after switch"}})).await.unwrap();
        assert_eq!(r["updated"], new);
        let row = s.store.lock().await.get(&new).unwrap().unwrap();
        assert_eq!((row.model.as_str(), row.effort.as_deref(), row.note.as_str(), row.description.as_str()),
                   ("default", Some("high"), "self-update", "after switch"));
        assert!(s.relaunch.lock().await.contains(&new), "model/effort apply from the replacement's next turn");
        // Naming the old id as the target also lands on the live replacement.
        h(&s, json!({"op": "update", "name": lead, "caller": lead, "fields": {"effort": "medium"}})).await.unwrap();
        assert_eq!(s.store.lock().await.get(&new).unwrap().unwrap().effort.as_deref(), Some("medium"));
        // Self lifecycle is still refused.
        let e = h(&s, json!({"op": "update", "name": "lead", "caller": lead, "fields": {"lifecycle": "archived"}})).await.unwrap_err();
        assert!(e.to_string().contains("cannot change its own lifecycle"), "{e}");
        // The ancestor rule for everyone else is unchanged.
        let e = h(&s, json!({"op": "update", "name": "lead", "caller": other, "fields": {"effort": "low"}})).await.unwrap_err();
        assert!(e.to_string().contains("not an ancestor"), "{e}");
    }

    #[tokio::test]
    async fn switching_harness_replaces_the_session() {
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().to_path_buf() }).unwrap());
        let id = |v: Value| v["id"].as_str().unwrap().to_string();
        let lead = id(h(&s, json!({"op": "create", "name": "lead", "role": "lead", "cwd": dir.path().join("l")})).await.unwrap());
        let other = id(h(&s, json!({"op": "create", "name": "other", "cwd": dir.path().join("o")})).await.unwrap());
        let com = id(h(&s, json!({"op": "create", "name": "comedian", "caller": lead, "description": "tells jokes", "cwd": dir.path().join("c")})).await.unwrap());
        let kid = id(h(&s, json!({"op": "create", "name": "kid", "caller": com, "cwd": dir.path().join("k")})).await.unwrap());
        // Codex sessions created without a model use Codex's own default.
        let cx = h(&s, json!({"op": "create", "name": "cx", "harness": "codex", "cwd": dir.path().join("x")})).await.unwrap();
        assert_eq!(cx["model"], "default");
        h(&s, json!({"op": "set_note", "name": com, "text": "warming up"})).await.unwrap();
        h(&s, json!({"op": "wake_add", "name": com, "at": "in 1h", "message": "joke time"})).await.unwrap();
        h(&s, json!({"op": "mcp_set", "name": com, "server": "gh", "config": {"command": "gh-mcp"}})).await.unwrap();
        h(&s, json!({"op": "rule_add", "scope": "session", "target": com, "tool": "Bash", "in_cwd": true})).await.unwrap();
        // Refusals: not an ancestor, itself, same harness, harness mixed with other fields.
        assert!(h(&s, json!({"op": "reharness", "name": com, "harness": "codex", "caller": other, "seed": false})).await.unwrap_err().to_string().contains("permission denied"));
        assert_eq!(h(&s, json!({"op": "reharness", "name": com, "harness": "codex", "caller": com, "seed": false})).await.unwrap()["scheduled"], true);
        s.pending_switch.lock().await.clear();
        assert!(h(&s, json!({"op": "reharness", "name": com, "harness": "claude", "seed": false})).await.unwrap_err().to_string().contains("already"));
        assert!(h(&s, json!({"op": "update", "name": com, "fields": {"harness": "codex", "task": "x"}})).await.is_err());
        // The Lead (an ancestor) switches it.
        let r = h(&s, json!({"op": "reharness", "name": "comedian", "harness": "codex", "caller": lead, "seed": false})).await.unwrap();
        let new = r["new"]["id"].as_str().unwrap().to_string();
        assert_ne!(new, com);
        assert_eq!((r["new"]["name"].as_str(), r["new"]["harness"].as_str(), r["new"]["model"].as_str()), (Some("comedian"), Some("codex"), Some("default")));
        assert_eq!((r["new"]["description"].as_str(), r["new"]["note"].as_str()), (Some("tells jokes"), Some("warming up")));
        assert_eq!(r["new"]["created_by_session"], lead);
        assert_eq!(r["new"]["cwd"], json!(dir.path().join("c")));
        assert_eq!(r["moved"], json!({"children": 1, "wakes": 1, "mcp_servers": 1, "rules": 1}));
        let st = s.store.lock().await;
        let oldrow = st.get(&com).unwrap().unwrap();
        assert_eq!((oldrow.lifecycle.as_str(), oldrow.replaced_by.as_deref()), ("archived", Some(new.as_str())));
        assert_eq!(st.get(&new).unwrap().unwrap().replaces.as_deref(), Some(com.as_str()));
        assert_eq!(st.get(&kid).unwrap().unwrap().created_by_session.as_deref(), Some(new.as_str()));
        assert_eq!(st.resolve("comedian").unwrap().as_deref(), Some(new.as_str()));
        assert_eq!(st.wakes(None, false).unwrap()[0].session, new);
        drop(st);
        let audit = h(&s, json!({"op": "audit"})).await.unwrap().to_string();
        assert!(audit.contains("event:reharnessed") && audit.contains("event:replaces"));
        // The old one's history stays readable by id.
        assert_eq!(h(&s, json!({"op": "history_page", "name": com})).await.unwrap()["session"]["id"], com);
        // A session (the Lead included) may switch itself: scheduled for when its turn ends.
        let r = h(&s, json!({"op": "update", "name": lead, "caller": lead, "fields": {"harness": "codex"}})).await.unwrap();
        assert_eq!(r["scheduled"], true);
        assert!(r["note"].as_str().unwrap().contains("after this reply"));
        assert_eq!(s.store.lock().await.get(&lead).unwrap().unwrap().lifecycle, "active");
        let (hn, _, actor) = s.pending_switch.lock().await.remove(&lead).unwrap();
        assert_eq!((hn.as_str(), actor.as_str()), ("codex", lead.as_str()));
        let old = s.store.lock().await.get(&lead).unwrap().unwrap();
        let r = s.do_reharness(&lead, &old, "codex", None, false, false).await.unwrap();
        let new_lead = r["new"]["id"].as_str().unwrap().to_string();
        assert_eq!((r["new"]["role"].as_str(), r["new"]["created_by_session"].is_null()), (Some("lead"), true));
        assert_eq!(h(&s, json!({"op": "lead_get"})).await.unwrap()["id"], new_lead);
        // The new Lead inherited the children (the codex comedian).
        assert_eq!(s.store.lock().await.get(&new).unwrap().unwrap().created_by_session.as_deref(), Some(new_lead.as_str()));
    }

    #[tokio::test]
    async fn mcp_servers_per_session() {
        assert!(validate_mcp("gh", &json!({"command": "npx", "args": ["-y", "x"], "env": {"T": "s"}})).is_ok());
        assert!(validate_mcp("web", &json!({"type": "http", "url": "https://x.example/mcp"})).is_ok());
        assert!(validate_mcp("web", &json!({"type": "http", "url": "https://x.example/mcp", "headers": {"Authorization": "Bearer tok"}, "client_id": "cid"})).is_ok());
        assert!(validate_mcp("gh", &json!({"command": "npx", "headers": {"Authorization": "Bearer tok"}})).is_err());
        assert!(validate_mcp("gh", &json!({"command": "npx", "client_id": "cid"})).is_err());
        assert!(validate_mcp("kurisu-mcp", &json!({"command": "x"})).is_err());
        assert!(validate_mcp("bad name", &json!({"command": "x"})).is_err());
        assert!(validate_mcp("x", &json!({"type": "http", "url": "file:///etc"})).is_err());
        // AuthState serde round-trip for every variant.
        for state in [
            store::AuthState::None,
            store::AuthState::NeedsAuth,
            store::AuthState::Authorizing { authorize_url: "https://auth.example/authorize?state=1".into(), expires_at: 1700000000 },
            store::AuthState::Authorized { expires_at: Some(1700003600) },
            store::AuthState::Authorized { expires_at: None },
            store::AuthState::Expired,
            store::AuthState::Failed { reason: "token exchange rejected".into() },
        ] {
            let enc = serde_json::to_value(&state).unwrap();
            assert_eq!(enc["state"].as_str(), Some(state.as_str()));
            let dec: store::AuthState = serde_json::from_value(enc).unwrap();
            assert_eq!(dec, state);
        }
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().to_path_buf() }).unwrap());
        let id = h(&s, json!({"op": "create", "name": "dev"})).await.unwrap()["id"].as_str().unwrap().to_string();
        assert!(h(&s, json!({"op": "mcp_set", "name": "dev", "server": "gh", "enabled": false})).await.is_err()); // no config yet
        h(&s, json!({"op": "mcp_set", "name": "dev", "server": "gh", "config": {"command": "gh-mcp", "env": {"TOKEN": "secret"}}})).await.unwrap();
        let raw_tok = "Bearer ntn_super_secret_token_9876";
        let set_r = h(&s, json!({"op": "mcp_set", "name": "dev", "server": "notion", "config": {
            "type": "http", "url": "https://mcp.notion.com/mcp",
            "headers": {"Authorization": raw_tok, "X-Short": "abc"},
            "client_id": "notion-client-1"
        }})).await.unwrap();
        assert_eq!(set_r["auth_state"], "none", "a static Authorization header means no reconnect is needed");
        assert_eq!(set_r["auth"]["state"], "none");
        let l = h(&s, json!({"op": "mcp_list", "name": "dev"})).await.unwrap();
        let l_str = serde_json::to_string(&l).unwrap();
        assert!(!l_str.contains(raw_tok), "raw header token leaked in mcp_list: {l_str}");
        assert!(!l_str.contains("ntn_super_secret"), "raw header token prefix leaked in mcp_list: {l_str}");
        assert_eq!((l["servers"][0]["enabled"].as_bool(), l["servers"][0]["config"]["env"]["TOKEN"].as_str()), (Some(true), Some("<set>")));
        assert_eq!(l["servers"][1]["name"], "notion");
        assert_eq!(l["servers"][1]["config"]["headers"]["Authorization"], "****9876");
        assert_eq!(l["servers"][1]["config"]["headers"]["X-Short"], "****");
        assert_eq!(l["servers"][1]["config"]["client_id"], "notion-client-1");
        assert_eq!(l["servers"][1]["auth_state"], "none");
        // Token file makes AuthState report authorized (or expired when in the past).
        let tok_path = s.paths.mcp_oauth_token(&id, "notion");
        std::fs::create_dir_all(tok_path.parent().unwrap()).unwrap();
        let future_exp = now() + 3600;
        std::fs::write(&tok_path, json!({"access_token": "tok", "expires_at": future_exp}).to_string()).unwrap();
        let l = h(&s, json!({"op": "mcp_list", "name": "dev"})).await.unwrap();
        assert_eq!(l["servers"][1]["auth_state"], "authorized");
        assert_eq!(l["servers"][1]["auth"], json!({"state": "authorized", "expires_at": future_exp}));
        std::fs::write(&tok_path, json!({"access_token": "tok", "expires_at": now() - 10}).to_string()).unwrap();
        let l = h(&s, json!({"op": "mcp_list", "name": "dev"})).await.unwrap();
        assert_eq!(l["servers"][1]["auth_state"], "expired");
        std::fs::remove_file(&tok_path).unwrap();
        // Store reopen (restart) preserves headers and client_id in session_mcp.
        let reopened = store::Store::open(&s.paths.db()).unwrap();
        let rows = reopened.mcp_list(&id).unwrap();
        let (_, notion_cfg, notion_on) = rows.iter().find(|(n, ..)| n == "notion").unwrap();
        assert!(*notion_on);
        assert_eq!(notion_cfg["headers"]["Authorization"], raw_tok);
        assert_eq!(notion_cfg["client_id"], "notion-client-1");
        assert!(s.relaunch.lock().await.contains(&id));
        // merged into the generated --mcp-config and codex_extra; disabled ones are left out
        let sess = s.store.lock().await.get(&id).unwrap().unwrap();
        s.claude_opts(&sess).await.unwrap();
        let cfg: Value = serde_json::from_slice(&std::fs::read(s.paths.session_dir(&id).join("mcp.json")).unwrap()).unwrap();
        assert_eq!(cfg["mcpServers"]["gh"]["env"]["TOKEN"], "secret");
        assert_eq!(cfg["mcpServers"]["notion"]["headers"]["Authorization"], raw_tok);
        assert!(cfg["mcpServers"]["kurisu-mcp"].is_object());
        let cx = s.codex_extra(&sess).await.unwrap();
        assert_eq!(cx["config"]["mcp_servers.gh"]["env"]["TOKEN"], "secret");
        assert_eq!(cx["config"]["mcp_servers.notion"]["http_headers"]["Authorization"], raw_tok);
        // ask_user waits for the user: longer than Codex's 60 s MCP tool timeout.
        assert_eq!(cx["config"]["mcp_servers.kurisu-mcp"]["tool_timeout_sec"], ask_wait().as_secs() + 60);
        h(&s, json!({"op": "mcp_set", "name": "dev", "server": "gh", "enabled": false})).await.unwrap();
        s.claude_opts(&sess).await.unwrap();
        let cfg: Value = serde_json::from_slice(&std::fs::read(s.paths.session_dir(&id).join("mcp.json")).unwrap()).unwrap();
        assert!(cfg["mcpServers"].get("gh").is_none());
        let cx = s.codex_extra(&sess).await.unwrap();
        assert!(cx["config"].get("mcp_servers.gh").is_none());
        assert_eq!(cx["config"]["mcp_servers.notion"]["url"], "https://mcp.notion.com/mcp");
        assert!(h(&s, json!({"op": "mcp_set", "name": "dev", "server": "x", "config": {"command": "y"}, "caller": "dev"})).await.is_err());
        let r = h(&s, json!({"op": "mcp_remove", "name": "dev", "server": "gh"})).await.unwrap();
        assert_eq!(r["changed"], true);

        // Disabling an MCP server destroys token/pending files and strips Authorization from config.
        let tok_path = s.paths.mcp_oauth_token(&id, "notion");
        let pending_path = s.paths.mcp_oauth_pending(&id, "notion");
        std::fs::write(&tok_path, json!({"access_token": "tok"}).to_string()).unwrap();
        std::fs::write(&pending_path, json!({"state": "pending"}).to_string()).unwrap();
        assert!(tok_path.is_file());
        assert!(pending_path.is_file());

        let dis_r = h(&s, json!({"op": "mcp_set", "name": "dev", "server": "notion", "enabled": false})).await.unwrap();
        assert_eq!(dis_r["changed"], true);
        assert_eq!(dis_r["auth_state"], "needs-auth", "a remote server without a token must ask for a reconnect");
        assert!(!tok_path.is_file(), "token file must be deleted on disable");
        assert!(!pending_path.is_file(), "pending file must be deleted on disable");

        let rows = s.store.lock().await.mcp_list(&id).unwrap();
        let (_, notion_cfg, notion_on) = rows.iter().find(|(n, ..)| n == "notion").unwrap();
        assert!(!*notion_on);
        assert!(notion_cfg["headers"].get("Authorization").is_none(), "Authorization header must be stripped from config");
        assert_eq!(notion_cfg["headers"]["X-Short"], "abc", "other headers must be preserved");

        // Re-enabling does NOT resurrect old token or header.
        let en_r = h(&s, json!({"op": "mcp_set", "name": "dev", "server": "notion", "enabled": true})).await.unwrap();
        assert_eq!(en_r["changed"], true);
        assert_eq!(en_r["auth_state"], "needs-auth", "re-enabling does not restore the token: still a reconnect");
        let rows = s.store.lock().await.mcp_list(&id).unwrap();
        let (_, notion_cfg, notion_on) = rows.iter().find(|(n, ..)| n == "notion").unwrap();
        assert!(*notion_on);
        assert!(notion_cfg["headers"].get("Authorization").is_none());
        assert!(!tok_path.is_file());

        // Removing scrubs credentials and deletes row.
        std::fs::write(&tok_path, json!({"access_token": "tok2"}).to_string()).unwrap();
        std::fs::write(&pending_path, json!({"state": "pending2"}).to_string()).unwrap();
        let rem_r = h(&s, json!({"op": "mcp_remove", "name": "dev", "server": "notion"})).await.unwrap();
        assert_eq!(rem_r["changed"], true);
        assert_eq!(rem_r["auth_state"], "none");
        assert!(!tok_path.is_file(), "token file must be deleted on remove");
        assert!(!pending_path.is_file(), "pending file must be deleted on remove");
        let rows = s.store.lock().await.mcp_list(&id).unwrap();
        assert!(rows.iter().find(|(n, ..)| n == "notion").is_none(), "row must be removed");
    }

    #[tokio::test]
    async fn mcp_disable_reports_needs_auth_for_remote_servers() {
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().to_path_buf() }).unwrap());
        let id = h(&s, json!({"op": "create", "name": "s1"})).await.unwrap()["id"].as_str().unwrap().to_string();

        // 1. On a throwaway home with an http server configured (fake header, no token file): disable it, then read auth_state => exactly "needs-auth".
        let cfg = json!({"type": "http", "url": "https://mcp.notion.com/mcp", "headers": {"Authorization": "Bearer fake"}});
        h(&s, json!({"op": "mcp_set", "name": "s1", "server": "notion", "config": cfg})).await.unwrap();
        let dis = h(&s, json!({"op": "mcp_set", "name": "s1", "server": "notion", "enabled": false})).await.unwrap();
        assert_eq!(dis["auth_state"], "needs-auth");
        let v = h(&s, json!({"op": "mcp_list", "name": "s1"})).await.unwrap();
        let notion = v["servers"].as_array().unwrap().iter().find(|x| x["name"] == "notion").unwrap();
        assert_eq!(notion["auth_state"], "needs-auth");

        // 2. Re-enable then disable: still "needs-auth".
        let en = h(&s, json!({"op": "mcp_set", "name": "s1", "server": "notion", "enabled": true})).await.unwrap();
        assert_eq!(en["auth_state"], "needs-auth");
        let dis2 = h(&s, json!({"op": "mcp_set", "name": "s1", "server": "notion", "enabled": false})).await.unwrap();
        assert_eq!(dis2["auth_state"], "needs-auth");

        // 3. A stdio server with no token => still "none" (no regression).
        let cfg_stdio = json!({"command": "npx", "args": ["-y", "example-mcp"]});
        h(&s, json!({"op": "mcp_set", "name": "s1", "server": "gh", "config": cfg_stdio})).await.unwrap();
        let v = h(&s, json!({"op": "mcp_list", "name": "s1"})).await.unwrap();
        let gh = v["servers"].as_array().unwrap().iter().find(|x| x["name"] == "gh").unwrap();
        assert_eq!(gh["auth_state"], "none");
        let dis_gh = h(&s, json!({"op": "mcp_set", "name": "s1", "server": "gh", "enabled": false})).await.unwrap();
        assert_eq!(dis_gh["auth_state"], "none");

        // 4. Token present => authorized
        let tok = s.paths.mcp_oauth_token(&id, "notion");
        std::fs::create_dir_all(tok.parent().unwrap()).unwrap();
        std::fs::write(&tok, json!({"access_token": "a", "expires_in": 3600}).to_string()).unwrap();
        let v = h(&s, json!({"op": "mcp_list", "name": "s1"})).await.unwrap();
        let notion = v["servers"].as_array().unwrap().iter().find(|x| x["name"] == "notion").unwrap();
        assert_eq!(notion["auth_state"], "authorized");

        // 5. Remove notion => row is gone, auth_state is "none"
        let rem = h(&s, json!({"op": "mcp_remove", "name": "s1", "server": "notion"})).await.unwrap();
        assert_eq!(rem["auth_state"], "none");
    }

    #[tokio::test]
    async fn mcp_disable_all_scrubs_all_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().to_path_buf() }).unwrap());
        let s1 = h(&s, json!({"op": "create", "name": "s1"})).await.unwrap()["id"].as_str().unwrap().to_string();
        let s2 = h(&s, json!({"op": "create", "name": "s2"})).await.unwrap()["id"].as_str().unwrap().to_string();

        let cfg = json!({
            "type": "http", "url": "https://mcp.notion.com/mcp",
            "headers": {"Authorization": "Bearer tok", "X-Key": "val"}
        });
        h(&s, json!({"op": "mcp_set", "name": "s1", "server": "notion", "config": cfg})).await.unwrap();
        h(&s, json!({"op": "mcp_set", "name": "s2", "server": "notion", "config": cfg})).await.unwrap();

        let tok1 = s.paths.mcp_oauth_token(&s1, "notion");
        let tok2 = s.paths.mcp_oauth_token(&s2, "notion");
        let pen1 = s.paths.mcp_oauth_pending(&s1, "notion");
        let pen2 = s.paths.mcp_oauth_pending(&s2, "notion");
        std::fs::create_dir_all(tok1.parent().unwrap()).unwrap();
        std::fs::create_dir_all(tok2.parent().unwrap()).unwrap();
        std::fs::write(&tok1, "tok1").unwrap();
        std::fs::write(&tok2, "tok2").unwrap();
        std::fs::write(&pen1, "pen1").unwrap();
        std::fs::write(&pen2, "pen2").unwrap();

        let count = s.mcp_disable_all("notion").await.unwrap();
        assert_eq!(count, 2);

        assert!(!tok1.is_file());
        assert!(!tok2.is_file());
        assert!(!pen1.is_file());
        assert!(!pen2.is_file());

        let st = s.store.lock().await;
        for sid in [&s1, &s2] {
            let rows = st.mcp_list(sid).unwrap();
            let (_, c, on) = rows.iter().find(|(n, ..)| n == "notion").unwrap();
            assert!(!*on);
            assert!(c["headers"].get("Authorization").is_none());
            assert_eq!(c["headers"]["X-Key"], "val");
        }
    }

    #[tokio::test]
    async fn cost_sums_cli_runs_and_compact_updates_context() {
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().to_path_buf() }).unwrap());
        let id = h(&s, json!({"op": "create", "name": "dev"})).await.unwrap()["id"].as_str().unwrap().to_string();
        let hid = "11111111-2222-3333-4444-555555555555";
        s.store.lock().await.bind_harness_id(&id, hid).unwrap();
        let sl_dir = dir.path().join("claude").join(hid);
        std::fs::create_dir_all(&sl_dir).unwrap();
        let write = |cost: f64, tokens: u64, age: u64| {
            let p = sl_dir.join("statusline.json");
            std::fs::write(&p, json!({"cost": {"total_cost_usd": cost}, "context_window": {"context_window_size": 200000,
                "current_usage": {"input_tokens": tokens}, "used_percentage": 1}}).to_string()).unwrap();
            let t = std::time::SystemTime::now() - std::time::Duration::from_secs(age);
            std::fs::File::options().write(true).open(&p).unwrap().set_modified(t).unwrap();
        };
        let st = |s: Arc<Server>| async move { h(&s, json!({"op": "status", "name": "dev"})).await.unwrap() };
        write(0.5, 50000, 100);
        assert_eq!(st(s.clone()).await["cost_usd"], 0.5);
        // the CLI restarts: the old run is folded in, the new run starts again at 0
        let (sl, sess) = { let x = s.store.lock().await.get(&id).unwrap().unwrap(); (s.status_line(&x).await.unwrap(), x) };
        s.store.lock().await.cost_fold(&id, 0.5, sl.captured_at as i64).unwrap();
        s.store.lock().await.cost_fold(&id, 0.5, sl.captured_at as i64).unwrap(); // only once
        assert_eq!(st(s.clone()).await["cost_usd"], 0.5);
        write(0.125, 60000, 50);
        let v = st(s.clone()).await;
        assert_eq!((v["cost_usd"].as_f64(), v["cost_usd_run"].as_f64()), (Some(0.625), Some(0.125)));
        // a compact replaces the context size until the next status line
        s.context_override.lock().await.insert(id.clone(), now());
        let v = st(s.clone()).await;
        assert!(v["context_tokens"].is_null() && v["context_pending"] == true);
        write(0.2, 3000, 0);
        s.context_override.lock().await.insert(id.clone(), now() - 5);
        assert_eq!(st(s.clone()).await["context_tokens"], 3000);
        let _ = sess;
    }

    #[test]
    fn answers_map_by_text_header_or_index() {
        let input = json!({"questions": [
            {"question": "Which color?", "header": "Color", "options": [{"label": "Red"}, {"label": "Blue"}], "multiSelect": false},
            {"question": "Which sizes?", "header": "Size", "options": [{"label": "S"}, {"label": "M"}], "multiSelect": true}]});
        let a = fill_answers(&input, &json!({"Color": "Blue", "1": ["S", "M"]})).unwrap();
        assert_eq!(a, json!({"Which color?": "Blue", "Which sizes?": "S, M"}));
        assert!(fill_answers(&input, &json!({"Which color?": "Red"})).is_err());
        assert!(fill_answers(&input, &json!({"0": "", "1": "S"})).is_err());
    }

    #[test]
    fn failed_turn_preamble_names_the_error() {
        let p = failed_turn_preamble("rate_limit");
        assert!(p.contains("rate_limit") && p.contains("Do not answer it"));
    }

    #[test]
    fn turn_failure_names_the_cli_reason_and_hints_at_its_quota() {
        let e = anyhow!(
            "You've hit your usage limit. Upgrade to Pro or try again at 2:15 PM.\n\
             the CLI wrote to stderr:\n2026-11-08 ERROR rate limited"
        );
        let msg = turn_failed_message(&e);
        assert!(msg.starts_with("turn failed: "), "{msg}");
        assert!(msg.contains("Upgrade to Pro or try again at 2:15 PM"), "{msg}");
        assert!(msg.contains("2026-11-08 ERROR rate limited"), "{msg}");
        assert!(msg.contains("the CLI account's own limit, not Kurisu"), "{msg}");
        // An ordinary protocol failure gets the text, not a misleading hint.
        let plain = turn_failed_message(&anyhow!("codex turn ended with status failed"));
        assert_eq!(plain, "turn failed: codex turn ended with status failed");
    }

    #[tokio::test]
    async fn watch_op_streams_only_the_named_sessions_events() {
        use std::time::Duration;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Server::new(Paths { home: dir.path().join("home") }).unwrap());
        let cwd = dir.path().join("work");
        std::fs::create_dir_all(&cwd).unwrap();
        let id = s.store.lock().await
            .create("watched", "claude", cwd.to_str().unwrap(), "worker", "claude-haiku-5-5", "", None)
            .unwrap().id;
        let sock = s.socket();
        let listener = crate::paths::bind_private(&sock).unwrap();
        let srv = s.clone();
        let accept = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { break };
                let srv = srv.clone();
                tokio::spawn(async move { let _ = connection(srv, stream).await; });
            }
        });
        let stream = tokio::net::UnixStream::connect(&sock).await.unwrap();
        let (r, mut w) = stream.into_split();
        let mut req = serde_json::to_vec(&json!({"op": "watch", "name": "watched"})).unwrap();
        req.push(b'\n');
        w.write_all(&req).await.unwrap();
        let mut lines = BufReader::new(r).lines();
        let ack: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(ack["ok"]["watching"], id);
        // The subscription exists before the ack: events published right after
        // it must arrive, and events for other sessions must not. Reasoning and
        // tool events are part of the stream; the bridge decides what to relay.
        s.publish(json!({"type": "delta", "session": id, "kind": "reasoning", "text": "ignore"}));
        s.publish(json!({"type": "delta", "session": "other", "kind": "text", "text": "ignore"}));
        s.publish(json!({"type": "delta", "session": id, "kind": "text", "message_id": "m1", "text": "Hello"}));
        let first: Value = tokio::time::timeout(Duration::from_secs(2), lines.next_line()).await
            .expect("watch did not deliver the reasoning event").unwrap()
            .map(|line| serde_json::from_str(&line).unwrap()).unwrap();
        let second: Value = tokio::time::timeout(Duration::from_secs(2), lines.next_line()).await
            .expect("watch did not deliver the text event").unwrap()
            .map(|line| serde_json::from_str(&line).unwrap()).unwrap();
        assert_eq!(first["kind"], "reasoning");
        assert_eq!(second["type"], "delta");
        assert_eq!(second["kind"], "text");
        assert_eq!(second["text"], "Hello");
        assert_eq!(second["message_id"], "m1", "clients need the message id to separate assistant messages");
        accept.abort();
    }

    /// ACP gives OpenCode a generic title ("shell"); the command is in rawInput.
    /// Drive the real fixture over stdio and check every surface shows the command.
    #[tokio::test]
    async fn opencode_shell_activity_names_the_command() {
        use std::time::Duration;
        let _env = OPENCODE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/fixtures/opencode-acp.py");
        assert!(fixture.is_file(), "fixture missing at {}", fixture.display());
        std::env::set_var("KURISU_OPENCODE_BIN", &fixture);
        std::env::set_var("KURISU_FIXTURE_HOME", dir.path());
        let s = Arc::new(Server::new(Paths { home: dir.path().join("home") }).unwrap());
        let mut events = s.events.subscribe();
        let cwd = dir.path().join("work");
        let id = h(&s, json!({"op": "create", "name": "open", "harness": "opencode",
                              "model": "fixture/free", "cwd": cwd})).await.unwrap()["id"]
            .as_str().unwrap().to_string();
        let send = tokio::spawn({
            let (s, id) = (s.clone(), id.clone());
            async move { h(&s, json!({"op": "send", "name": id, "text": "FIXTURE_COMMAND"})).await }
        });
        let expected = format!("Shell: echo {}", "x".repeat(295)); // a 300-char command
        assert_eq!(expected.chars().count(), 307);
        let mut status = Value::Null;
        let mut bound = String::new();
        for _ in 0..200 {
            status = h(&s, json!({"op": "status", "name": id})).await.unwrap();
            if let Some(b) = status["harness_session_id"].as_str().filter(|b| !b.is_empty()) {
                bound = b.to_string();
            }
            if !bound.is_empty() && status["activity"]["state"] == "running" { break; }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(status["activity"]["title"].as_str(), Some(expected.as_str()), "{status}");
        assert_eq!(status["activity"]["tool"], "shell");
        assert!(!status["activity"]["title"].as_str().unwrap().contains('\n'));
        // peers_list / session_status read this same activity object.
        let peers = h(&s, json!({"op": "peers"})).await.unwrap();
        let peer = peers.as_array().unwrap().iter().find(|p| p["id"].as_str() == Some(id.as_str())).unwrap();
        assert_eq!(peer["activity"]["title"].as_str(), Some(expected.as_str()));
        // Release the fixture turn and check the finished tool row and live event.
        std::fs::write(dir.path().join(format!("fixture-release-{bound}")), "").unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(10), send).await
            .expect("fixture turn did not finish").unwrap().unwrap();
        assert_eq!(reply["reply"], "PONG");
        let mut saw_tool = false;
        while let Ok(ev) = events.try_recv() {
            if ev["type"] == "tool" && ev["title"].as_str() == Some(expected.as_str()) { saw_tool = true; }
        }
        assert!(saw_tool, "the Web UI tool event did not carry the command");
        let page = h(&s, json!({"op": "history_page", "name": id, "tools": true, "limit": 50})).await.unwrap();
        assert!(page["messages"].as_array().unwrap().iter()
                    .any(|m| m["role"] == "tool" && m["text"].as_str() == Some(expected.as_str())), "{page}");
    }

    #[tokio::test]
    async fn agy_create_send_resume_and_metrics_survive_restart() {
        let _env = OPENCODE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/fixtures/agy-acp.py");
        std::env::set_var("KURISU_AGY_BIN", fixture);
        let paths = Paths { home: dir.path().join("home") };
        let server = Arc::new(Server::new(paths.clone()).unwrap());
        let models = h(&server, json!({"op":"models","harness":"agy"})).await.unwrap();
        let ids: Vec<&str> = models["models"].as_array().unwrap().iter().filter_map(|m| m["id"].as_str()).collect();
        assert!(ids.contains(&"gemini-3.8-flash-high"), "{ids:?}");
        assert!(ids.contains(&"gemini-4-argon-medium"), "{ids:?}");
        assert!(!ids.contains(&"claude-sonnet-4-6"), "stale id from the old hardcoded list: {ids:?}");
        assert!(!ids.contains(&"gemini-3.8-flash"), "bare ids are not reported by `agy models`: {ids:?}");
        assert!(h(&server, json!({"op":"create","name":"bad","harness":"agy","model":"invalid"})).await.unwrap_err().to_string().contains("supported ids"));
        let session = h(&server, json!({"op":"create","name":"antigravity","harness":"agy","cwd":dir.path().join("work")})).await.unwrap();
        let id = session["id"].as_str().unwrap();
        let reply = h(&server, json!({"op":"send","name":id,"text":"ping"})).await.unwrap();
        assert_eq!(reply["reply"], "PONG");
        let status = h(&server, json!({"op":"status","name":id})).await.unwrap();
        assert_eq!(status["harness_session_id"], "agy-conversation-123");
        assert_eq!(status["usage"]["input_tokens"], 10);
        assert_eq!(status["cost_usd"], 0.0);
        server.close_driver(&server.store.lock().await.get(id).unwrap().unwrap()).await;
        let reopened = Arc::new(Server::new(paths).unwrap());
        let status = h(&reopened, json!({"op":"status","name":id})).await.unwrap();
        assert_eq!(status["usage"]["input_tokens"], 10);
        let reply = h(&reopened, json!({"op":"send","name":id,"text":"continue"})).await.unwrap();
        assert_eq!(reply["reply"], "PONG");
        let status = h(&reopened, json!({"op":"status","name":id})).await.unwrap();
        assert_eq!(status["harness_session_id"], "agy-conversation-123");
        std::env::remove_var("KURISU_AGY_BIN");
    }

    #[tokio::test]
    async fn agy_replayed_history_is_not_returned_as_the_next_reply() {
        let _env = OPENCODE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/fixtures/agy-acp.py");
        std::env::set_var("KURISU_AGY_BIN", fixture);
        std::env::set_var("AGY_FIXTURE_HISTORY", dir.path().join("agy-history.jsonl"));
        let paths = Paths { home: dir.path().join("home") };
        let server = Arc::new(Server::new(paths.clone()).unwrap());
        let session = h(&server, json!({"op":"create","name":"replay","harness":"agy","cwd":dir.path().join("work")})).await.unwrap();
        let id = session["id"].as_str().unwrap().to_string();
        let first = h(&server, json!({"op":"send","name":id,"text":"ping"})).await.unwrap();
        assert_eq!(first["reply"], "PONG");
        // A restart/model change ends the CLI; the next turn resumes the same
        // agy conversation. Its history replay reaches the new prompt's stream.
        drop(server);
        let reopened = Arc::new(Server::new(paths).unwrap());
        let second = h(&reopened, json!({"op":"send","name":id,"text":"continue"})).await.unwrap();
        assert_eq!(second["reply"], "PONG", "replayed history was returned as the new reply: {:?}", second["reply"]);
        std::env::remove_var("KURISU_AGY_BIN");
        std::env::remove_var("AGY_FIXTURE_HISTORY");
    }

    fn opencode_fixture(name: &str) -> PathBuf {
        let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/fixtures").join(name);
        assert!(p.is_file(), "fixture missing at {}", p.display());
        p
    }

    /// The OpenCode list is whatever its CLI prints, including an id that is
    /// hardcoded nowhere in Kurisu, and it refreshes without a restart.
    #[tokio::test]
    async fn opencode_models_come_from_the_cli_and_refresh_without_restart() {
        let _env = OPENCODE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("fixture-models.txt"),
                       "fixture/alpha-model\nfixture/novel-cli-only-7\n").unwrap();
        std::env::set_var("KURISU_OPENCODE_BIN", opencode_fixture("opencode-models.py"));
        std::env::set_var("KURISU_FIXTURE_HOME", dir.path());
        std::env::set_var("KURISU_OPENCODE_CONFIG", dir.path().join("no-opencode.json"));
        std::env::set_var("KURISU_OPENCODE_MODELS_TTL_SECS", "0");
        let s = Arc::new(Server::new(Paths { home: dir.path().join("home") }).unwrap());
        let m = h(&s, json!({"op": "models", "harness": "opencode"})).await.unwrap();
        let ids: Vec<&str> = m["models"].as_array().unwrap().iter().filter_map(|x| x["id"].as_str()).collect();
        assert_eq!(ids, vec!["fixture/alpha-model", "fixture/novel-cli-only-7"], "{m}");
        assert!(m["note"].as_str().unwrap().contains("opencode"), "{m}");
        // A provider/model added later (opencode.json + its CLI) appears with no restart.
        std::fs::write(dir.path().join("fixture-models.txt"),
                       "fixture/alpha-model\nfixture/beta-model\n").unwrap();
        let m = h(&s, json!({"op": "models", "harness": "opencode"})).await.unwrap();
        let ids: Vec<&str> = m["models"].as_array().unwrap().iter().filter_map(|x| x["id"].as_str()).collect();
        assert!(ids.contains(&"fixture/beta-model") && !ids.contains(&"fixture/novel-cli-only-7"), "{m}");
    }

    /// A broken CLI must never produce an empty or invented list: use the real
    /// opencode.json models, else OpenCode's own default, always with a note.
    #[tokio::test]
    async fn opencode_models_cli_failure_falls_back_with_a_note() {
        let _env = OPENCODE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("KURISU_OPENCODE_BIN", opencode_fixture("opencode-models-fail.py"));
        std::env::set_var("KURISU_FIXTURE_HOME", dir.path());
        std::env::set_var("KURISU_OPENCODE_MODELS_TTL_SECS", "0");
        let config = dir.path().join("opencode.json");
        std::env::set_var("KURISU_OPENCODE_CONFIG", &config);
        let s = Arc::new(Server::new(Paths { home: dir.path().join("home") }).unwrap());
        // Nothing known yet: OpenCode's own default, not an empty list.
        let m = h(&s, json!({"op": "models", "harness": "opencode"})).await.unwrap();
        assert_eq!(m["models"][0]["id"], "default", "{m}");
        let note = m["note"].as_str().unwrap();
        assert!(note.contains("Could not list OpenCode models") && note.contains("falling back"), "{m}");
        // The user's opencode.json is still real data: show it instead.
        std::fs::write(&config, json!({
            "model": "acme/fast",
            "provider": {"acme": {"models": {"fast": {"name": "Fast"}, "deep": {}}}}
        }).to_string()).unwrap();
        let m = h(&s, json!({"op": "models", "harness": "opencode"})).await.unwrap();
        let mut ids: Vec<&str> = m["models"].as_array().unwrap().iter().filter_map(|x| x["id"].as_str()).collect();
        ids.sort();
        assert_eq!(ids, vec!["acme/deep", "acme/fast"], "{m}");
        assert_eq!(m["models"].as_array().unwrap().iter().filter(|x| x["default"] == true).count(), 1, "{m}");
        assert!(m["note"].as_str().unwrap().contains("configured in"), "{m}");
    }

    /// `/model` must not spawn `opencode models` on every keystroke.
    #[tokio::test]
    async fn opencode_models_are_cached_for_a_few_minutes() {
        let _env = OPENCODE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("fixture-models.txt"), "fixture/cached-model\n").unwrap();
        std::env::set_var("KURISU_OPENCODE_BIN", opencode_fixture("opencode-models.py"));
        std::env::set_var("KURISU_FIXTURE_HOME", dir.path());
        std::env::set_var("KURISU_OPENCODE_CONFIG", dir.path().join("no-opencode.json"));
        std::env::set_var("KURISU_OPENCODE_MODELS_TTL_SECS", "180");
        let s = Arc::new(Server::new(Paths { home: dir.path().join("home") }).unwrap());
        let calls = || std::fs::read_to_string(dir.path().join("fixture-models-calls")).unwrap_or_default().lines().count();
        for _ in 0..3 {
            let m = h(&s, json!({"op": "models", "harness": "opencode"})).await.unwrap();
            assert_eq!(m["models"][0]["id"], "fixture/cached-model", "{m}");
        }
        assert_eq!(calls(), 1, "a cached catalogue must not rerun the CLI");
        std::env::set_var("KURISU_OPENCODE_MODELS_TTL_SECS", "0");
        h(&s, json!({"op": "models", "harness": "opencode"})).await.unwrap();
        assert_eq!(calls(), 2, "an expired cache refreshes");
    }

    #[tokio::test]
    async fn default_model_reads_config_json_and_falls_back_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let s = Arc::new(Server::new(Paths { home: home.clone() }).unwrap());
        // (a) config without default_model -> store::default_model(harness)
        assert_eq!(s.default_model("opencode"), store::default_model("opencode"));
        assert_eq!(s.default_model("claude"), store::default_model("claude"));
        std::fs::write(home.join("config.json"), r#"{"default_harness":"opencode"}"#).unwrap();
        assert_eq!(s.default_model("opencode"), store::default_model("opencode"));
        // (b) config with default_model -> that id for harness opencode (and newly created sessions)
        std::fs::write(
            home.join("config.json"),
            r#"{"default_harness":"opencode","default_model":"opencode/deepseek-v4.1-flash-free"}"#,
        ).unwrap();
        assert_eq!(s.default_model("opencode"), "opencode/deepseek-v4.1-flash-free");
        let created = h(&s, json!({"op": "create", "name": "free-probe", "harness": "opencode"})).await.unwrap();
        assert_eq!(created["model"], "opencode/deepseek-v4.1-flash-free");
        // (c) bogus/unparseable config falls back, never errors
        std::fs::write(home.join("config.json"), b"{not valid json").unwrap();
        assert_eq!(s.default_model("opencode"), store::default_model("opencode"));
        let fallback = h(&s, json!({"op": "create", "name": "fallback-probe", "harness": "opencode"})).await.unwrap();
        assert_eq!(fallback["model"], store::default_model("opencode"));
    }

    struct MockOAuthClient {
        responses: std::sync::Mutex<HashMap<String, crate::oauth::HttpResponse>>,
        posts: std::sync::Mutex<Vec<(String, Value)>>,
        forms: std::sync::Mutex<Vec<(String, String)>>,
    }

    impl MockOAuthClient {
        fn new() -> Self {
            Self {
                responses: std::sync::Mutex::new(HashMap::new()),
                posts: std::sync::Mutex::new(Vec::new()),
                forms: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn set(&self, url: impl Into<String>, resp: crate::oauth::HttpResponse) {
            self.responses.lock().unwrap().insert(url.into(), resp);
        }
    }

    impl crate::oauth::HttpClient for MockOAuthClient {
        fn get_with_headers<'a>(&'a self, url: &'a str, _headers: &'a [(String, String)]) -> crate::oauth::BoxFuture<'a, Result<crate::oauth::HttpResponse>> {
            self.get(url)
        }

        fn get<'a>(&'a self, url: &'a str) -> crate::oauth::BoxFuture<'a, Result<crate::oauth::HttpResponse>> {
            Box::pin(async move {
                let guard = self.responses.lock().unwrap();
                match guard.get(url) {
                    Some(resp) => Ok(resp.clone()),
                    None => Ok(crate::oauth::HttpResponse::new(404, "not found")),
                }
            })
        }

        fn post_json<'a>(&'a self, url: &'a str, body: &'a Value) -> crate::oauth::BoxFuture<'a, Result<crate::oauth::HttpResponse>> {
            Box::pin(async move {
                self.posts.lock().unwrap().push((url.to_string(), body.clone()));
                let guard = self.responses.lock().unwrap();
                match guard.get(url) {
                    Some(resp) => Ok(resp.clone()),
                    None => Ok(crate::oauth::HttpResponse::new(200, json!({
                        "client_id": "mock-reg-id",
                        "access_token": "mock-access-token",
                        "expires_in": 3600
                    }).to_string())),
                }
            })
        }

        fn post_form<'a>(&'a self, url: &'a str, form: &'a str) -> crate::oauth::BoxFuture<'a, Result<crate::oauth::HttpResponse>> {
            Box::pin(async move {
                self.forms.lock().unwrap().push((url.to_string(), form.to_string()));
                let guard = self.responses.lock().unwrap();
                match guard.get(url) {
                    Some(resp) => Ok(resp.clone()),
                    None => Ok(crate::oauth::HttpResponse::new(200, json!({
                        "access_token": "mock-access-token",
                        "expires_in": 3600
                    }).to_string())),
                }
            })
        }
    }

    /// A fake MCP server + authorization server: RFC 9728 discovery, RFC 8414 metadata,
    /// DCR, and a token endpoint. Zero network.
    fn mock_oauth_as() -> Arc<MockOAuthClient> {
        let mock = Arc::new(MockOAuthClient::new());
        mock.set(
            "https://mcp.example.com/.well-known/oauth-protected-resource",
            crate::oauth::HttpResponse::new(200, json!({
                "resource": "https://mcp.example.com",
                "authorization_servers": ["https://auth.example.com"]
            }).to_string()),
        );
        mock.set(
            "https://auth.example.com/.well-known/oauth-authorization-server",
            crate::oauth::HttpResponse::new(200, json!({
                "issuer": "https://auth.example.com",
                "authorization_endpoint": "https://auth.example.com/oauth/authorize",
                "token_endpoint": "https://auth.example.com/oauth/token",
                "registration_endpoint": "https://auth.example.com/oauth/register",
                "code_challenge_methods_supported": ["S256"]
            }).to_string()),
        );
        mock.set(
            "https://auth.example.com/oauth/register",
            crate::oauth::HttpResponse::new(201, json!({
                "client_id": "registered-client-777"
            }).to_string()),
        );
        mock.set(
            "https://auth.example.com/oauth/token",
            crate::oauth::HttpResponse::new(200, json!({
                "access_token": "test-notion-access-token",
                "token_type": "Bearer",
                "expires_in": 7200
            }).to_string()),
        );
        mock
    }

    #[tokio::test]
    async fn mcp_oauth_connect_and_callback_flow() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths { home: dir.path().to_path_buf() };
        let s = Arc::new(Server::new(paths.clone()).unwrap());
        let id = h(&s, json!({"op": "create", "name": "dev"})).await.unwrap()["id"].as_str().unwrap().to_string();

        let mock = mock_oauth_as();
        s.set_http_client(mock.clone());

        // 1. Connect initiates OAuth flow, runs DCR, sets client_id, returns authorize_url
        let conn = h(&s, json!({
            "op": "mcp_connect",
            "session": "dev",
            "name": "notion",
            "url": "https://mcp.example.com",
            "caller": "dev"
        })).await.unwrap();

        let auth_url = conn["authorize_url"].as_str().unwrap();
        assert!(auth_url.starts_with("https://auth.example.com/oauth/authorize"));
        assert!(auth_url.contains("code_challenge="));
        assert!(auth_url.contains("code_challenge_method=S256"));
        assert!(auth_url.contains("client_id=registered-client-777"));
        assert!(auth_url.contains("state="));

        // Verify client_id persisted into config
        let rows = s.store.lock().await.mcp_list(&id).unwrap();
        let (_, cfg, _) = rows.iter().find(|(n, ..)| n == "notion").unwrap();
        assert_eq!(cfg["client_id"], "registered-client-777");

        // Pending file exists
        let pending_path = s.paths.mcp_oauth_pending(&id, "notion");
        assert!(pending_path.is_file());
        let pending_bytes = std::fs::read(&pending_path).unwrap();
        let pending_json: Value = serde_json::from_slice(&pending_bytes).unwrap();
        let state = pending_json["state"].as_str().unwrap().to_string();

        // 2. State mismatch is rejected
        let err = h(&s, json!({
            "op": "mcp_callback",
            "session": "dev",
            "name": "notion",
            "code": "code-xyz",
            "state": "wrong-state"
        })).await.unwrap_err();
        assert!(err.to_string().contains("mismatch"), "expected state mismatch, got: {err}");

        // 3. Expired pending flow is rejected
        let mut expired_json = pending_json.clone();
        expired_json["expires_at"] = json!(now() - 60);
        std::fs::write(&pending_path, serde_json::to_vec(&expired_json).unwrap()).unwrap();

        let err = h(&s, json!({
            "op": "mcp_callback",
            "session": "dev",
            "name": "notion",
            "code": "code-xyz",
            "state": state
        })).await.unwrap_err();
        assert!(err.to_string().contains("expired"), "expected expired flow error, got: {err}");

        // Regenerate valid flow
        let _conn2 = h(&s, json!({
            "op": "mcp_connect",
            "session": "dev",
            "name": "notion",
            "url": "https://mcp.example.com"
        })).await.unwrap();
        let pending_bytes2 = std::fs::read(&pending_path).unwrap();
        let pending_json2: Value = serde_json::from_slice(&pending_bytes2).unwrap();
        let state2 = pending_json2["state"].as_str().unwrap().to_string();

        // 4. Successful callback exchange writes the token file and mcp_list reports authorized
        let cb_res = h(&s, json!({
            "op": "mcp_callback",
            "session": "dev",
            "name": "notion",
            "code": "valid-auth-code-123",
            "state": state2
        })).await.unwrap();
        assert_eq!(cb_res["auth_state"], "authorized");

        // Token file written
        let tok_path = s.paths.mcp_oauth_token(&id, "notion");
        assert!(tok_path.is_file());
        let tok_json: Value = serde_json::from_slice(&std::fs::read(&tok_path).unwrap()).unwrap();
        assert_eq!(tok_json["access_token"], "test-notion-access-token");
        assert!(tok_json["expires_at"].as_i64().unwrap() > now());

        // Pending file removed
        assert!(!pending_path.exists());

        // mcp_list reports authorized
        let list_res = h(&s, json!({"op": "mcp_list", "name": "dev"})).await.unwrap();
        let notion_row = list_res["servers"].as_array().unwrap().iter().find(|s| s["name"] == "notion").unwrap();
        assert_eq!(notion_row["auth_state"], "authorized");
        assert_eq!(notion_row["auth"]["state"], "authorized");
        assert_eq!(notion_row["auth"]["expires_at"], tok_json["expires_at"]);
    }

    /// A dead port must never be handed out as the redirect: the preflight checks a
    /// real listener and takes the first candidate that answers, in preference order.
    #[tokio::test]
    async fn oauth_redirect_preflight_skips_dead_loopback_port() {
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let dead_port = dead.local_addr().unwrap().port();
        drop(dead);
        let live = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let live_port = live.local_addr().unwrap().port();
        let timeout = std::time::Duration::from_millis(300);

        // Dead first, live second: the listener is picked, the dead one skipped.
        assert_eq!(pick_listening_loopback([dead_port, live_port], timeout).await, Some(live_port));
        // Nothing listening anywhere: no redirect can complete, so none is claimed.
        assert_eq!(pick_listening_loopback([dead_port], timeout).await, None);
        assert!(callback_uri(live_port).starts_with("http://127.0.0.1:"), "redirect stays on loopback");
    }

    /// `mcp_connect` mints a redirect_uri on a loopback port that is actually listening
    /// (the standalone UI's port is preferred, a dead configured port is skipped).
    #[tokio::test]
    async fn mcp_connect_redirect_uri_uses_a_live_loopback_listener() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths { home: dir.path().to_path_buf() };
        let s = Arc::new(Server::new(paths.clone()).unwrap());
        h(&s, json!({"op": "create", "name": "dev"})).await.unwrap();
        s.set_http_client(mock_oauth_as());

        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let dead_port = dead.local_addr().unwrap().port();
        drop(dead);
        let live = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let live_port = live.local_addr().unwrap().port();
        std::fs::write(
            paths.home.join("web.json"),
            json!({"enabled": true, "listen": format!("127.0.0.1:{dead_port}")}).to_string(),
        ).unwrap();
        std::fs::write(
            crate::remote::web_config_path(&paths.home),
            json!({"enabled": true, "listen": format!("127.0.0.1:{live_port}")}).to_string(),
        ).unwrap();

        let conn = h(&s, json!({
            "op": "mcp_connect", "session": "dev", "name": "notion", "url": "https://mcp.example.com"
        })).await.unwrap();
        assert_eq!(
            conn["redirect_uri"].as_str(),
            Some(format!("http://127.0.0.1:{live_port}/api/v1/oauth/callback").as_str()),
            "redirect must target the live listener: {conn}"
        );
        assert_eq!(conn["auto_callback"], json!(true));
        let auth_url = conn["authorize_url"].as_str().unwrap();
        assert!(
            auth_url.contains(&format!("redirect_uri=http%3A%2F%2F127.0.0.1%3A{live_port}%2Fapi%2Fv1%2Foauth%2Fcallback")),
            "authorize URL carries the live redirect: {auth_url}"
        );
        let message = conn["message"].as_str().unwrap();
        assert!(message.contains("address bar"), "paste-back stays documented: {message}");
        assert!(!message.contains("Auto-callback is off"), "automatic callback is on here: {message}");
        drop(live);
    }

    /// With no listener answering, the flow still starts: authorize URL plus the
    /// paste-back wording, and the message says automatic callback is off.
    #[tokio::test]
    async fn mcp_connect_without_a_listener_says_auto_callback_is_off() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths { home: dir.path().to_path_buf() };
        let s = Arc::new(Server::new(paths.clone()).unwrap());
        h(&s, json!({"op": "create", "name": "dev"})).await.unwrap();
        s.set_http_client(mock_oauth_as());

        let mut dead_ports = Vec::new();
        for _ in 0..2 {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            dead_ports.push(l.local_addr().unwrap().port());
            drop(l);
        }
        std::fs::write(
            paths.home.join("web.json"),
            json!({"enabled": true, "listen": format!("127.0.0.1:{}", dead_ports[0])}).to_string(),
        ).unwrap();
        std::fs::write(
            crate::remote::web_config_path(&paths.home),
            json!({"enabled": true, "listen": format!("127.0.0.1:{}", dead_ports[1])}).to_string(),
        ).unwrap();

        let conn = h(&s, json!({
            "op": "mcp_connect", "session": "dev", "name": "notion", "url": "https://mcp.example.com"
        })).await.unwrap();
        assert!(conn["authorize_url"].as_str().unwrap().starts_with("https://auth.example.com/oauth/authorize"), "{conn}");
        let redirect = conn["redirect_uri"].as_str().unwrap();
        assert!(redirect.starts_with("http://127.0.0.1:"), "redirect stays loopback: {redirect}");
        let default_port = crate::remote::default_web_port(&paths.home);
        let default_live = crate::remote::loopback_listening(default_port, std::time::Duration::from_millis(300)).await;
        assert_eq!(conn["auto_callback"], json!(default_live), "only a listening port may claim auto-callback");
        let message = conn["message"].as_str().unwrap();
        assert!(message.contains("address bar"), "paste-back wording stays: {message}");
        if !default_live {
            assert_eq!(redirect, format!("http://127.0.0.1:{default_port}/api/v1/oauth/callback"));
            assert!(message.contains("Auto-callback is off"), "no listener means auto-callback is off: {message}");
        }
    }

    /// The phone paste-back path: an authenticated agent caller finishes its own flow by
    /// handing over the redirect URL it got in chat, and the state is consumed once.
    #[tokio::test]
    async fn mcp_oauth_pasteback_by_agent_session() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths { home: dir.path().to_path_buf() };
        let s = Arc::new(Server::new(paths.clone()).unwrap());
        let id = h(&s, json!({"op": "create", "name": "dev"})).await.unwrap()["id"].as_str().unwrap().to_string();
        h(&s, json!({"op": "create", "name": "other"})).await.unwrap();

        let mock = mock_oauth_as();
        s.set_http_client(mock.clone());

        // The connect reply is chat-ready: it carries the authorize URL and the paste-back step.
        let conn = h(&s, json!({
            "op": "mcp_connect",
            "session": "dev",
            "name": "notion",
            "url": "https://mcp.example.com",
            "caller": "dev"
        })).await.unwrap();
        let auth_url = conn["authorize_url"].as_str().unwrap();
        let message = conn["message"].as_str().unwrap();
        assert!(message.contains(auth_url), "connect message should name the authorize URL: {message}");
        assert!(message.contains("address bar"), "connect message should explain the paste-back: {message}");
        let parsed = reqwest::Url::parse(auth_url).unwrap();
        let redirect = parsed.query_pairs().find(|(k, _)| k == "redirect_uri").map(|(_, v)| v.into_owned());
        assert_eq!(
            redirect.as_deref(),
            Some(format!("http://127.0.0.1:{}/api/v1/oauth/callback", crate::remote::default_web_port(&paths.home)).as_str()),
            "redirect URI stays loopback: {auth_url}"
        );

        let pending_path = s.paths.mcp_oauth_pending(&id, "notion");
        let pending: Value = serde_json::from_slice(&std::fs::read(&pending_path).unwrap()).unwrap();
        let state = pending["state"].as_str().unwrap().to_string();
        let pasted = format!("http://127.0.0.1:8798/api/v1/oauth/callback?code=abc%2B123&state={state}&iss=extra%3Aparam");

        // Malformed paste is rejected before the pending flow is read.
        let err = h(&s, json!({
            "op": "mcp_callback", "session": "dev", "name": "notion", "url": "hello there", "caller": "dev"
        })).await.unwrap_err();
        assert!(err.to_string().contains("pasted"), "expected malformed-paste error, got: {err}");

        // Another session cannot complete a flow it did not start.
        let err = h(&s, json!({
            "op": "mcp_callback", "session": "dev", "name": "notion", "url": pasted, "caller": "other"
        })).await.unwrap_err();
        assert!(err.to_string().contains("permission denied"), "expected ownership check, got: {err}");
        assert!(pending_path.is_file(), "a refused caller must not consume the flow");

        // The owning agent pastes the redirect URL and the token exchange runs.
        let ok = h(&s, json!({
            "op": "mcp_callback", "session": "dev", "name": "notion", "url": pasted, "caller": "dev"
        })).await.unwrap();
        assert_eq!(ok["auth_state"], "authorized");
        assert!(s.paths.mcp_oauth_token(&id, "notion").is_file());

        // The token POST is an RFC 6749 §4.1.3 form, never JSON (Notion rejects JSON).
        let forms = mock.forms.lock().unwrap().clone();
        let (_, form) = forms
            .iter()
            .find(|(u, _)| u.ends_with("/oauth/token"))
            .expect("token endpoint called");
        assert!(!form.trim_start().starts_with('{'), "token endpoint must not receive JSON: {form}");
        let pairs: HashMap<String, String> = reqwest::Url::parse(&format!("http://x/?{form}"))
            .unwrap()
            .query_pairs()
            .into_owned()
            .collect();
        assert_eq!(pairs.get("code").map(String::as_str), Some("abc+123"), "percent-encoded code decoded once");
        assert_eq!(pairs.get("grant_type").map(String::as_str), Some("authorization_code"));
        assert_eq!(pairs.get("client_id").map(String::as_str), Some("registered-client-777"));
        assert_eq!(
            pairs.get("redirect_uri").map(String::as_str),
            Some(format!("http://127.0.0.1:{}/api/v1/oauth/callback", crate::remote::default_web_port(&paths.home)).as_str())
        );
        assert_eq!(
            pairs.get("code_verifier").map(String::as_str),
            pending["code_verifier"].as_str(),
            "the PKCE verifier travels in the form"
        );
        assert!(
            mock.posts.lock().unwrap().iter().all(|(u, _)| !u.ends_with("/oauth/token")),
            "no JSON request may hit the token endpoint"
        );

        // Single use: replaying the same pasted URL finds no pending flow.
        let err = h(&s, json!({
            "op": "mcp_callback", "session": "dev", "name": "notion", "url": pasted, "caller": "dev"
        })).await.unwrap_err();
        assert!(err.to_string().contains("no pending OAuth flow"), "expected single-use state, got: {err}");
    }

    /// A provider rejection reaches the caller with its own error text intact, so a 400
    /// (e.g. a wrong content type) is diagnosable from chat instead of a bare status.
    #[tokio::test]
    async fn mcp_oauth_token_error_surfaces_provider_description_verbatim() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths { home: dir.path().to_path_buf() };
        let s = Arc::new(Server::new(paths).unwrap());
        let id = h(&s, json!({"op": "create", "name": "dev"})).await.unwrap()["id"].as_str().unwrap().to_string();

        let mock = mock_oauth_as();
        mock.set(
            "https://auth.example.com/oauth/token",
            crate::oauth::HttpResponse::new(400, json!({
                "error": "invalid_request",
                "error_description": "Content-Type must be application/x-www-form-urlencoded"
            }).to_string()),
        );
        s.set_http_client(mock.clone());

        h(&s, json!({
            "op": "mcp_connect", "session": "dev", "name": "notion", "url": "https://mcp.example.com"
        })).await.unwrap();
        let pending_path = s.paths.mcp_oauth_pending(&id, "notion");
        let pending: Value = serde_json::from_slice(&std::fs::read(&pending_path).unwrap()).unwrap();
        let state = pending["state"].as_str().unwrap().to_string();

        let err = h(&s, json!({
            "op": "mcp_callback", "session": "dev", "name": "notion", "code": "abc", "state": state
        })).await.unwrap_err().to_string();
        assert!(err.contains("HTTP 400"), "status must be named: {err}");
        assert!(err.contains("invalid_request"), "provider error code must survive: {err}");
        assert!(
            err.contains("Content-Type must be application/x-www-form-urlencoded"),
            "provider error_description must be verbatim: {err}"
        );
        // A failed exchange must not mint a token file.
        assert!(!s.paths.mcp_oauth_token(&id, "notion").is_file());
    }
}
