# Kurisu HTTP API v1

`ku web --serve` serves this API and the embedded web UI. The web UI uses **only**
this API, so other clients (a mobile or Tauri app, a Telegram channel, scripts)
can do everything it does.

* Base URL: `http://127.0.0.1:<port>/api/v1` (port 8787 by default; `ku web --serve --port N`).
* Content type: JSON (`content-type: application/json` on POST).
* Envelope: success is `{"ok": <result>}`. An error is `{"error": "<message>"}`
  with HTTP 400 (bad request or failure), 401 (token), 403 (Host header) or
  404 (unknown route).
* Session identifiers: anywhere `{id}` appears, a session's **UUID**, a unique
  **UUID prefix** (6+ hex characters) or its live **name** is accepted.

## Authentication

Every request needs the token that `ku web --serve` prints (set `KURISU_WEB_TOKEN` to
fix it; 16+ characters). Send it as one of:

* `Authorization: Bearer <token>` (preferred);
* `x-kurisu-token: <token>`;
* `?token=<token>`, accepted **only** on `GET /stream`, for EventSource
  clients that cannot set headers.

There are no accounts. The token is the only credential, so treat it like a
password.

## Network and CORS

* By default the server binds `127.0.0.1` and rejects requests whose `Host`
  header is not that address (a DNS-rebinding guard).
* For a remote Kurisu (for example over Tailscale): run
  `ku web --serve --host 0.0.0.0` (or the tailnet IP). This prints a warning, and the
  Host check is off. Prefer `tailscale serve` for HTTPS: the PWA service worker
  needs a secure context.
* Browser clients on another origin (a UI hosted elsewhere, a Tauri webview)
  need `KURISU_WEB_CORS_ORIGINS=https://app.example,tauri://localhost`.
  Only the listed origins get CORS headers.

## Data types

```ts
Session {
  id: string (uuid); name: string; role: "lead" | "worker"; harness: "claude" | "codex" | "opencode";
  cwd: string; model: string; effort: string | null;
  status: "new" | "starting" | "working" | "idle" | "error" | "stopped" | "awaiting_approval";
  lifecycle: "active" | "paused" | "archived";
  description: string;  // the "topic"
  note: string;         // the session's own status note (set_my_status)
  created_by_session: string | null; created_by_name: string | null;  // null = created by the user
  children: { id, name }[];
  replies: number;      // non-user messages, for unread counts
  last?: { seq, role, text (preview), at };
  last_activity: number | null; harness_session_id: string | null;
  created_at: number;               // unix seconds
  last_user_at: number | null;      // the user's last message here (not peers'), for sorting
  replaces: uuid | null; replaced_by: uuid | null;   // harness switch links (see below)
  permission_mode: "auto" | "default" | "acceptEdits" | "plan" | "bypassPermissions" | null;  // set for this session; null = Kurisu's default
  permission_mode_effective: string;  // what it runs with (see Permission modes)
  // From the CLI's status line (null until the session's first turn):
  context_tokens: number | null;    // tokens in the context now
  context_window: number | null;    // the model's window (e.g. 200000 or 1000000)
  context_used_pct: number | null;
  cost_usd: number | null;          // Claude Code's own API-equivalent estimate, summed over the session's CLI runs
  cost_usd_run: number | null;      // the current CLI run only
  context_pending?: true;           // after a compact: the size is unknown (null) until the next turn
  metrics_at: number | null;        // when those were captured
  // While a turn runs: the tool it is running now (or ran last); null between turns.
  activity: { tool: string, title: string ("Bash: npm test"), state: "running" | "done", at } | null;
  // From the transcript (all turns, also across restarts):
  usage: { input, output, cache_read, cache_write, messages } | null;
}
Message  { seq: number (global, increasing); session: uuid; role: "user" | "agent" | "error" | "peer:<name>" | "approval" | "compaction" | "tool"; text: string; at: unix seconds;
           data?: { tool_call_id, tool, status: "running" | "completed" | "failed", input, output? } }  // role "tool" only
         // role "tool": one tool call (text is its title, e.g. "Bash: ls"); only in /history and /handoff, never in
         //   /messages, GET /messages or the stream, so chat clients do not see them. input/output over 8000 characters
         //   are kept as a shortened string.
         // role "compaction": the summary the CLI kept after a compact
         // role "approval": text is "[approval:<id>] <title>", the place of a permission request in the channel
AuditRow { seq; actor: "user" | uuid | "rule#<n>" | "kurisu"; actor_name; target: uuid; target_name;
           field: "<field>" | "event:<created|started|resumed|stopped|failed|approval_requested|approval_approved|
                   approval_denied|approval_expired|approval_cancelled|rule_added|rule_removed|question_answered|steered|
                   compacted|compact_failed|wake_scheduled|wake_fired|wake_failed|wake_cancelled|reharnessed|replaces|refreshed|tools_updated|conversation_restarted>"; old; new; at }
Approval { id: string (8 hex; any unique prefix of 4+ works); session: uuid; session_name; request_id;
           tool: string; title: string ("Bash: ls -la"); input: object (the tool input); cwd: string;
           status: "pending" | "approved" | "denied" | "expired" | "cancelled";
           decided_by: "user" | "rule#<n>" | "kurisu" | null; reason: string | null; rule_id: number | null;
           created_at; expires_at; decided_at: number | null;
           kind: "permission" | "question";
           preview: string;       // short display-safe summary: the command, path or URL, or "Which color? [Red / Blue]"
           questions: [{question, header?, options: [{label, description?}], multiSelect}] | null;  // kind "question"
           answer: {<question>: string} | null }                                                  // once answered
Wake     { id: number; session: uuid; session_name; created_by: "user" | uuid; created_by_name;
           at: number (next due time, unix seconds); at_local: string ("2026-11-08 06:00 -05:00");
           message: string; repeat: string | null (5-field cron, local time);
           status: "pending" | "fired" | "failed" | "cancelled"; fired_count: number;
           last_fired_at: number | null; last_late: boolean; last_error: string | null; created_at }
Rule     { id: number; scope: "role" | "session"; target: string (role name or session uuid); target_name;
           tool: string ("Bash", "mcp__github__*", "*"); in_cwd: boolean; command_prefix: string | null;
           decision: "allow" | "deny"; created_by; created_at }
```

## Endpoints

| Method and path | Body | Result |
| --- | --- | --- |
| `GET /status` | | `{api, version, clis: string[] (display text), cli_status: [{name, installed, path, version, logged_in, login_method}], server: {pid, rss_kb, home}, home}` |
| `GET /status/clis?fresh=false` | | `{clis: [claude, codex, opencode, stt], checked_at, note}`. claude/codex/opencode: `{name, installed, path, version, logged_in, login_method}` (login only from `claude auth status --json` / `codex login status`; `null` when unknown; credentials are never read). stt: `{name, installed, path, version ("model …"), set_up, kurisu_stt}`. Cached 60 s unless `fresh=true`. |
| `GET /sessions?all=true` | | `Session[]`. `all=false` hides archived sessions; the default is `true`. |
| `POST /sessions` | `{name, description?, model?, cwd?, harness?}` (harness defaults to the installed, logged-in CLI, Claude first; `KURISU_DEFAULT_HARNESS` or `default_harness` in `config.json` overrides; a missing CLI is refused at once) | the created worker `Session`, plus `warning?` for an unverified model. `cwd` must be absolute (default: Kurisu's workspace for that name); `harness` is `claude`, `codex` or `opencode`. A Codex/OpenCode session without `model` uses its configured default (shown as `default`). |
| `GET /sessions/{id}` | | `Session` |
| `GET /sessions/{id}/messages?after=<seq>` | | `Message[]` with `seq > after` (up to 500) |
| `GET /sessions/{id}/messages?tail=<n>` | | the last `n` messages (1–200), oldest first |
| `GET /sessions/{id}/history?limit=&before=&after=&search=&roles=&tools=&max_chars=` | | One page of the session's full history (see [Session history](#session-history-and-continuing-a-session)): `{session: {id, name, harness, model}, total, messages: Message[], has_more, direction, first_seq, last_seq}` |
| `GET /sessions/{id}/handoff?max_chars=` | | What an agent needs to continue that session: `{session, compaction_summary, messages, omitted_older, total_messages, how_to_continue}` |
| `GET /messages?after=<seq>&limit=<n>` | | `{messages: (Message & {session_name})[], msg_seq}`: new messages in **all** sessions (limit 1–1000, default 200). One cursor for a whole-team poll; `msg_seq` is the newest seq. |
| `POST /sessions/{id}/messages` | `{text, wait?, mode?, images?, files?}` | `wait` true (default): `{id, name, reply, stop_reason, notice}` when the turn ends. `wait: false`: `{id, name, seq, queued, steered}` at once; the turn runs in the background (after the running one, if any; `queued: true`), and its reply arrives as a `message` event. `mode: "steer"`: if a turn is running, the text is typed into it (Claude folds it into that turn, or runs it right after; the reply comes with that turn's reply; `steered: true`); if no turn is running it behaves like `wait: false`. `notice`: see below. |
| `POST /sessions/{id}/files` | `{name, data_b64}` | `{path, size, transcript?, transcript_error?}` (`transcript` only for audio when speech-to-text is on): the file is saved in the session's folder under `.kurisu-uploads/<unix time>-<safe name>` (mode 0600), so the session can read it without a permission prompt. `data_b64` may be a `data:<mime>;base64,` URL. At most 25 MB decoded per file; request bodies up to about 35 MB. |
| `POST /sessions/{id}/compact` | | Runs the CLI's own `/compact` and waits (up to 10 min): `{id, name, pre_tokens, post_tokens_estimate, summary, trigger}`. `pre_tokens` comes from the transcript's compact boundary; `post_tokens_estimate` is the summary's size only (length / 4); the real context also holds the system prompt and tools, so session rows show `context_tokens: null, context_pending: true` until the next turn reports it. Refused while a turn is running, before the session's first turn, and for Codex sessions. Also recorded as a `compaction` message and an `event:compacted` audit row. |
| `GET /sessions/{id}/mcp` | | `{builtin: ["kurisu-mcp"], servers: [{name, enabled, config}], note}`; `env` and `headers` values show as `"<set>"` |
| `POST /sessions/{id}/mcp` | `{name, config?, enabled?}` | add a server (needs `config`), change its config, or turn it on or off: `{name, server, changed, note}`. `config` is an `mcpServers` entry: `{command, args?, env?}` (stdio) or `{type: "http" \| "sse", url, headers?}`. |
| `POST /sessions/{id}/mcp/{server}/delete` | | remove it |
| `GET /sessions/{id}/events` | | `AuditRow[]` for that session (lifecycle events and field changes) |
| `POST /sessions/{id}/update` | any of `{description, model, effort, permission_mode, name, lifecycle, role, task}`, or `{harness, model?}` alone | `{name, changes: [{field, new}], unchanged: [field], warning?}`. Unchanged values are not written or audited. Model and effort apply from the next turn. With `harness`: a harness switch (below), returning `{old, new: Session, moved: {children, wakes, mcp_servers, rules}, seed, note}`. |
| `POST /sessions/{id}/stop` | | `{name, was_running, turn_cancelled}`. Closes the CLI; a running turn ends at once (its blocking send fails with "the turn was stopped", a `turn stopped: …` error message is recorded, and pending approvals are cancelled). Status becomes `stopped`; the next message resumes the CLI and is told not to answer the stopped one. |
| `POST /lead` | `{text, model?}` | Starts or reuses the Lead and sends it the task; same result as a blocking send |
| `GET /lead?create=false` | | the Lead's `Session` (the active session named `lead`, else the first active lead). 400 if there is none, unless `create=true`. This is the default target for a chat channel (e.g. Telegram): send to it with `POST /sessions/{id}/messages`. |
| `GET /models?harness=claude` | | `{harness, models: [{id, label, verified, default}], note}`. Other well-formed `claude-*` ids are accepted with a warning; Codex has a curated catalogue; OpenCode discovers models and effort options through ACP. |
| `GET /audit` | | all `AuditRow[]` |
| `GET /usage` | | `{rate_limits, sessions: {<name>: {messages, input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens, models, context_tokens, context_window, context_used_pct, cost_usd, metrics_at}}, total, note}`. `rate_limits` is `{five_hour: {used_percentage, resets_at}, seven_day: {…}, captured_at, source}` or `null` (see below). |
| `GET /approvals?all=false&session=<id>` | | pending `Approval[]`, oldest first; `all=true` returns the last 100 of any status, newest first |
| `POST /approvals/{id}/approve` | `{always?: bool, session_wide?: bool}` | `{id, status, session, session_name, title, rule}`. Single use. `always` also adds a session rule: that tool is allowed inside the session's folder. With `session_wide` the rule covers any path in the session (the Telegram "Always allow (this session)" button; Codex also gets `acceptForSession`). |
| `POST /approvals/{id}/deny` | `{reason?}` | as above; the session sees "The user denied this tool call: <reason>". For a question this dismisses it. |
| `POST /approvals/{id}/answer` | `{answers: {<key>: string \| string[]}}` | answers a `kind: "question"` approval. `<key>` is the question text, its `header`, or its index (`"0"`); the value is an option label or free text, or a list of labels for `multiSelect`. Returns the approval summary plus `answer`. Every question needs an answer. |
| `GET /wakes?session=<id>&all=false` | | pending `Wake[]`, soonest first; `all=true` adds the newest 200 past ones |
| `POST /wakes` | `{session, message, at?, repeat?}` | the new `Wake` plus `in_seconds`. See [Scheduled wakes](#scheduled-wakes). |
| `POST /wakes/{id}/cancel` | | `{cancelled: id, session, session_name}` (only pending wakes) |
| `GET /stt` | | `{enabled, config: {backend: "command", command} \| {backend: "mcp", command, tool} \| null, audio_extensions}` |
| `POST /transcribe` | `{path}` | `{path, text, seconds}`; 400 when speech-to-text is off |
| `GET /rules` | | `Rule[]` |
| `POST /rules` | `{scope, target, tool, in_cwd?, command_prefix?, decision?}` | the new `Rule`. `target` is a role name, or a session id or name for `scope: "session"`. An allow rule for `*` must set `in_cwd` or `command_prefix`. |
| `POST /rules/{id}/delete` | | `{removed: id}` |
| `GET /stream` | | **Server-sent events** (see below) |

## Live events: `GET /stream`

A `text/event-stream` response. Each event has a name and a JSON `data`
payload, and a keep-alive comment is sent every 20 s.

| Event | Data | When |
| --- | --- | --- |
| `hello` | `{sessions: Session[], msg_seq, audit_seq, approvals: Approval[]}` | first event on connect (a snapshot) |
| `sessions` | `Session[]` | the session list changed: status, new session, rename, archive, last message, tool activity, context, … |
| `message` | `Message` | a message was recorded in any session (user, agent, peer or error) |
| `audit` | `AuditRow` | a lifecycle event or field change |
| `approvals` | `Approval[]` (pending) | the pending list changed (a request arrived, was answered, expired or was cancelled) |
| `resync` | `{}` | the client fell behind and missed events: reload `/sessions` and the open channel |

Client pattern (this is what the web UI does): on connect, load
`/sessions`; when a channel opens, load `/sessions/{id}/messages` and
`/events`; then apply `message` and `audit` events for that channel and
replace the list on `sessions`. Reconnect with backoff, and resync after a
reconnect. Events come from a poller in `ku web` that checks the server about
every 0.7 s, so the latency is under a second.

### Notifications (designed, not built)

The `message` / `audit` / `sessions` events are the hook for notifications. A
notifier subscribes to `/stream` (or runs inside `ku web`) and maps events
such as an `agent` message in a session you aren't viewing, an `error`
message, or status `awaiting_approval` to Web Push or ntfy. No API change is
needed; a later version may add `POST /notify/subscriptions` for Web Push
endpoints.

## Sending, queueing and steering

* Every send records the message first (`seq` in the reply, and a `message`
  event), then runs one turn per message, in order.
* Only one turn runs per session. A blocking send while a turn is running
  waits for it; `wait: false` returns at once with `queued: true`.
* `mode: "steer"` types the message into the running turn instead of waiting
  (Telegram's `/send_now`). It is recorded like any message, and an audit row
  `event:steered` names its `seq`.
* **Callbacks.** A `wait: false` send from a session (`session_send`) asks for
  a callback unless it passes `notify: false`: when that turn ends, the sender
  gets a `peer:<worker>` message starting `[Kurisu callback]` with the outcome
  (done, error, stopped, or interrupted by a restart) and the reply or error
  (first 1500 characters). It is queued like any message if the sender is busy.
  The callback asks for no callback, and steered or blocking sends get none.
  Pending callbacks are stored (`callbacks` table), so they survive a restart.
* **Resume after a restart.** At startup, a session that was mid-turn when the
  previous server stopped (or that still has a callback pending) gets one
  `peer:kurisu` message starting `[Kurisu: resume after restart]` asking it to
  check what is done and finish; its callbacks fire when that turn ends. A
  session cut off again while resuming is not resumed twice; its senders get an
  "interrupted" callback. `KURISU_NO_RESUME=1` turns resuming off (senders are
  still told).
* If the previous turn failed (for example a rate limit), the failed message
  is still in the CLI's context. The next turn tells the session not to answer
  it, and the blocking send returns `notice` saying so.

### Attachments

A send may carry `images: [{mime?, name?, data_b64}]` and
`files: [{name, data_b64}]`. Each one is saved as with
`POST /sessions/{id}/files`, and the message text gets a list of the saved
paths ("[Attached (in your folder; read them with the Read tool): …]"). The
recorded message includes that list. Claude reads images by path.

## Usage, context and plan limits

Kurisu sets a status-line command in the `--settings` it passes to each
Claude CLI. Claude Code feeds that command a JSON object (model, context
window, cost, and for subscription logins `rate_limits`), and Kurisu keeps
the latest one per session. No credential is read: this is the same data
Claude Code shows in its own status line.

* `rate_limits` (the official 5-hour and 7-day plan limits) is account-wide,
  so `GET /usage` reports the newest capture from any session. It is `null`
  until a session has finished a turn, and with an API-key login, which has
  no plan limits.
* `context_*` on a session comes from its latest capture, which lands about
  2 seconds after a turn ends. After a compact, `context_tokens` is `null`
  with `context_pending: true` until the next turn (the summary alone would
  understate it: the system prompt and tools stay in the context).
* `cost_usd` is Claude Code's estimate summed over the session's CLI runs:
  when Kurisu starts or resumes a CLI, the previous run's cost is added to
  the session's total (`cost_usd_run` is the current run).
* `usage` token counts come from the session's transcript and cover every
  turn.

## Session history and continuing a session

Kurisu keeps every session's history in its own database, the same way for
every harness (Claude Code, Codex and OpenCode ACP), apart from the
CLI's own transcript:

| Stored by Kurisu | Role |
| --- | --- |
| each message sent to the session (from the user or a peer) | `user` / the sender |
| the agent's final reply of each turn | `agent` |
| each tool call: tool, title, input, output, status (Claude: from the hooks; Codex: from the app-server items) | `tool` |
| compaction summaries: from `/compact`, and the CLI's own auto-compacts (picked up after the turn) | `compaction` |
| failures, stops, approval markers | `error`, `approval` |

Not stored: the agent's thinking, the text it writes between tool calls
(only the turn's final message), system prompts, and image contents (an
attachment is stored as the path it was saved to).

A session's live context is never moved to another CLI. Instead, any session
can read another session's history and continue from it. Ask it to, e.g.
"look at session hist-a and continue from there":

- **`session_handoff {id, max_chars?}`** (kurisu-mcp; `GET /sessions/{id}/handoff`;
  `ku handoff <name>`): the session's name, description, status note, folder,
  harness and model, its latest compaction summary, and the messages and tool
  calls after it, newest kept first within `max_chars` (default 24000, at
  most 200000; long messages are shortened, tool calls are shown as title,
  status and a short output). `omitted_older` counts what did not fit.
- **`session_history {id, limit?, before?, after?, search?, roles?, tools?, max_chars?}`**
  (kurisu-mcp; `GET /sessions/{id}/history`; `ku history <name> [--tools]
  [--search T] [--limit N] [--before SEQ]`): one page, oldest first. With no
  bounds it is the newest page; page back with `before=<first_seq>` while
  `has_more`, or forward with `after=<last_seq>`. `search` matches text and
  tool input/output, case-insensitive. `roles` is a comma list (`peer` matches
  every message from another session). Tool rows only with `tools=true` or
  `roles=tool`. `total` counts every match. `limit` 1–200 (default 30).

The session prompts tell agents to call `session_handoff` first. Any session
can read any other session's history (as with `peers_list`).

**Codex sessions** get `kurisu-mcp` and Kurisu's session prompt through the
app-server's thread options (`config` overrides and `developerInstructions`);
`~/.codex/config.toml` is not edited. kurisu-mcp tools run without approval
there, as they do for Claude.

## Switching a session's harness

A CLI's live context cannot move between harnesses, so a switch replaces the
session: `session_update {id, harness, model?}` (kurisu-mcp), `POST
/sessions/{id}/update {"harness": "codex"}`, or `ku update <name> --harness
codex [--model M]`. Change the harness on its own (only `model` may go with
it). Same authority as other updates: the user, an ancestor of the session, or
the session itself (a self-switch is scheduled until its current turn ends).
The session must be idle for an ancestor/user switch.

1. The old session is archived (its CLI is closed; its history stays
   readable by id) and gets `replaced_by`.
2. A new session is created with the same name, role, description, status
   note, folder and parent (`created_by_session`), on the target harness
   (model: the given one, else Haiku for Claude or the configured Codex/OpenCode default), with
   `replaces` pointing at the old one.
3. The old session's children (lineage), pending wakes, MCP servers and
   session rules move to the new one.
4. The new session is sent a first message from the caller (`[Kurisu:
   harness switch] …`) asking it to call `session_handoff` on the old
   session and say what it picked up; its reply arrives like any other.
   (`{"op": "reharness", …, "seed": false}` skips it.)

Audited as `event:reharnessed` on the old session and `event:replaces` on
the new one.

**Switching yourself.** A session may also switch its own harness
(`session_update {id: <its own id>, harness, model?}`), the Lead included.
Its turn is still running, so the call returns `{scheduled: true, note}` and
Kurisu performs the switch right after that turn ends (`event:reharness_scheduled`,
then the steps above). The replacement keeps `created_by_session` (NULL for
a user-created Lead, so it is the Lead and `GET /lead` returns it). The
built-in Telegram channel follows the replacement automatically.

## Importing sessions from Omnigent

`ku import omnigent [--session ID]... [--all] [--dry-run] [--model M] [--effort E] [--roster FILE] [--db FILE]`
(CLI only; the server op `import_session` is user-only).

- **Source:** a snapshot of `~/.omnigent/chat.db` taken with SQLite's online
  backup from a read-only connection (the live database is never written and
  Omnigent keeps running), plus each Claude session's transcript under
  `~/.claude/projects/` (found by its Omnigent `external_session_id`).
- **Selection:** by default the roster table in `~/omni-lead/AGENTS.md`;
  `--all` takes every unarchived session; `--session` takes ids or prefixes.
- **Per session:** a Kurisu session named after the roster name or title
  (slug), in the same folder (Omnigent's workspace, else the roster folder),
  with its model and effort (`--model`/`--effort` override), harness
  (`claude-native` → `claude`, `codex*` → `codex`; others are skipped), and
  parent: the roster's Omni Lead becomes the Kurisu Lead (skipped when Kurisu
  already has one), and every other session becomes the Lead's worker (or the
  worker of its imported Omnigent parent).
- **History:** messages, tool calls with their input and output, errors and
  compaction summaries, with their original times, so `session_history` and
  `session_handoff` work. Reasoning items, runtime events and slash commands are
  not imported.
- **Live context (Claude):** the transcript is copied under a **new** session
  id into the Claude project folder for the session's folder (the id inside is
  rewritten), and Kurisu resumes that copy. Omnigent keeps writing its own
  file, so two CLIs never write one transcript; the copy holds the context as of
  the import. Codex sessions get their history only (their threads live in
  Omnigent's private Codex home) and start a fresh thread.
- **Idempotent:** an `imports` table maps each Omnigent id to its Kurisu
  session; a re-run reports "already imported". `--dry-run` prints the plan
  (name, harness, model/effort, role, items, resume or history, folder) and
  changes nothing.

## Telegram channel (optional, built in)

Off until `ku telegram setup` (`--from-env FILE` reads `TG_BOT_TOKEN` or
`TELEGRAM_BOT_TOKEN`, `TG_CHAT_ID`, `TG_OWNER_USER_ID`, `KURISU_CWD`; otherwise
it prompts). The token and chat id are stored in `<KURISU_HOME>/telegram.json`
(mode 600) and never logged. The channel runs inside the ku server (it starts
with it; `ku telegram off|on|status`), calls the server's ops in-process, and
keeps its state in `<KURISU_HOME>/telegram/` (offset, current session per
chat, pending updates, cards, broadcast cursors, and the ids of replies
already sent so a restart never re-broadcasts them).

Voice notes are transcribed when a working `stt` CLI is found (`stt status`
exits 0; checked every 60 s), with no separate opt-in; `ku stt use`/`setup`
picks another backend and `ku stt off` turns it off. If transcription fails
the audio is passed through as a file without a message. Approval and
question cards show their expiry time and close at once when the request
expires, is decided by a rule or is answered in another client; a late tap gets
an alert. `ku telegram notify "<text>"` sends a one-off notice (used before
and after restarts).

It is a port of the Python Telegram bridge's Kurisu backend: the same
commands (`/list`, `/status`, `/catch_up`, `/model`, `/mcp`, `/usage`,
`/send_now`, `/compact`, `/stop`, `/id`, `/new`, `/use`, `/help`), texts,
buttons, queue and acknowledgements, approval and question cards, photos and
files, voice notes through the optional speech-to-text (`🎙 Transcribing…`
then `🎙 Heard: …`), broadcasts of final replies from every session, and the
retry and outage handling. Its default session is the Lead.


## Speech-to-text (optional)

The Telegram channel uses a working `stt` CLI automatically (see the Telegram
section). Everything below (transcripts on send, `POST /files`, the
`transcribe` tool) is off until the user opts in: until then voice and audio files are attachments
like any other (saved in the session's folder and listed in the message).
Kurisu bundles no model; it uses an external tool:

- `ku stt setup [stt setup options]` uses the `stt` CLI on `PATH` (local
  faster-whisper with a warm daemon; `--bin PATH` picks another binary). If
  `stt status` says it is not set up yet, Kurisu runs `stt setup` once (venv
  plus model download; slow), then turns speech-to-text on.
- `ku stt use --command "CMD [{file}]"` uses any command that prints a
  transcript (`{file}` is replaced by the path, else the path is appended).
- `ku stt use --mcp "CMD ARGS" [--tool transcribe]` uses any stdio MCP server
  whose tool takes `{path}` (e.g. `--mcp "stt mcp"`).
- `ku stt status`, `ku stt off`, `ku stt <file>` (transcribe one file).

The choice is stored in `<KURISU_HOME>/stt.json` and read on each use (no
restart). When it is on:

- **On send**, audio attachments (`files` or `images` with an audio
  extension: ogg, oga, opus, mp3, m4a, wav, flac, aac, webm, amr, mp4) are
  transcribed and the transcript is added after the attachment list:
  `[Voice/audio <path>, local speech-to-text transcript (may contain mistakes):]`
  then the text. The file itself is still attached. If transcription fails,
  the message says so and the file passes through unchanged.
- **`POST /sessions/{id}/files`** also returns `transcript` for an audio
  upload (or `transcript_error`), so a client that uploads first and sends a
  message later can include it.
- **kurisu-mcp `transcribe {path}`** lets a session transcribe an audio file
  (absolute, or relative to its folder); it answers "not set up" when off.
- **kurisu-mcp `send_file {path, caption?}`** sends a file from the session's
  own workspace folder to the paired Telegram chat: a jpg/png goes as a photo
  (10 MB), anything else as a document (50 MB), with a 1024-character caption
  at most. `..` and paths that resolve outside the folder (symlinks included)
  are refused. This is how a session shows the user what it saw (e.g. an
  `lb shot` JPEG) without a live-view link; when Telegram is not set up it
  says so instead of failing silently.

One transcription may take up to 300 seconds.

## Remote terminal from Telegram (optional)

Off until turned on, which can be done from the phone: when it is off,
`/terminal` replies with an approval card ("🖥 Enable the remote terminal? It
will listen on <address>", Approve / Deny, 2 minutes, owner's chat only);
Approve turns it on (persisted) and sends the link at once. `/terminal disable`
turns it off; `/terminal close` closes every session and kills every link. On
the computer, `ku terminal enable [--listen IP:PORT] [--url URL]` and
`ku terminal disable` do the same.

**Address.** Links never use 127.0.0.1 or localhost. Kurisu listens on the
Tailscale address (found with the `tailscale` CLI, also on the host from a
distrobox, else the `tailscale0` / 100.64.0.0/10 interface) and puts the
MagicDNS name in links when MagicDNS is on; without Tailscale it uses the LAN
address. With neither, it replies "No Tailscale or LAN address found; install
Tailscale on this machine and your phone." instead of a link. An older
local-only setup is moved to that address on the next `/terminal`
(`remote_listen_moved`). `0.0.0.0` or a public address only with `--force`.

- **The link alone grants nothing.** It is valid for 5 minutes and up to 3
  attempts. Opening it shows a 6-character pairing code and "Waiting for
  approval in Telegram…", and the chat gets an approval card: "🖥 Terminal
  access requested from <ip> / <browser>. Code: …" with Approve / Deny and the
  expiry time. Approve unlocks that browser (an HttpOnly, SameSite=Strict
  cookie) and uses the link up; Deny, or no answer within 2 minutes, kills the
  link (the card closes at once; a late tap gets an alert). A refresh starts a
  new attempt with a new code and closes the old card.
- **The terminal** is a real PTY running your shell (`$SHELL -l`) in
  `<KURISU_HOME>/workspaces`, shown with xterm.js 5.5.0 (embedded in the
  binary) over a same-origin websocket, with a key bar (Esc, Tab, Ctrl,
  arrows). The approved card gets a ⏹ Close button. Sessions close after 30
  minutes idle.
- Links and tokens are never logged. Every step is audited (target
  `terminal`: `remote_link_issued`, `remote_attempt`, `remote_approved`,
  `remote_denied`, `remote_expired`, `remote_refused`, `remote_shell_started`,
  `remote_closed`, `remote_enabled`, `remote_enable_declined`,
  `remote_enable_failed`, `remote_disabled`, `remote_listen_moved`).

## Live Browser add-on (optional) and `/browser`

[Live Browser](https://github.com/anslwy/live-browser) (`lb`) is a real Chrome
that agents drive through MCP tools and humans watch and take over in a web
UI. Kurisu does not include its code; nothing happens until
`ku browser setup [--listen IP:PORT] [--port N] [--url URL] [--lb PATH]`:

- **Setup** targets lb v0.4.22 (live-browser PR #39: per-session tabs, and the
  Kurisu skin for the live view, opt-in via `?theme=kurisu`, which `/browser` adds). It uses an installed `lb` that is at
  least v0.4.22 with per-session tabs (`lb mcp --session`), else downloads the
  v0.4.22 release into
  `<KURISU_HOME>/lb/`, else builds that commit from source (needs git, cargo,
  npm, and on Linux pkg-config and the OpenSSL headers), else falls back to an
  installed older `lb` with a warning. `--lb PATH` picks a
  binary. It checks that lb finds a Chrome (`lb browsers`) and writes
  `<KURISU_HOME>/browser.json` (mode 600) with a random 64-character passcode.
  `ku browser off|on|status`.
- **Off by default, per session.** lb is a normal per-session MCP server
  (`lb` in `ku mcp list <session>`), off for every session until turned on with
  `ku mcp enable <session> lb`, the Telegram /mcp toggle, or `session_update
  {id, browser: "on"|"off"}` (the session itself, an ancestor, or the user).
  Only kurisu-mcp is built in. Upgrading switched existing lb rows off once.
- **Per-session tabs.** Each session's `lb mcp` runs with `LB_SESSION=<session
  name>`, so the live view's owner chips show the Kurisu session; a session only
  acts on its own tabs (`open` reuses its tab, `close_tab` closes one), and lb
  closes all of them when that session's CLI exits. Optional settings in
  `browser.json`: `max_tabs` (`LB_MAX_TABS`, lb's default 5 per session),
  `tab_idle_secs` (`LB_TAB_IDLE_SECS`, default 1800, 0 = never) and `isolated`
  (`LB_ISOLATED`: a private browser context per session, so no shared logins).
- **Every session** (Claude, Codex and OpenCode) then gets the `lb` MCP server through
  Kurisu's own per-session MCP config (Claude: the generated `--mcp-config`
  file, now mode 600, and `--allowedTools mcp__lb`; Codex: a thread config
  override with `default_tools_approval_mode = approve`). `~/.claude.json` and
  `~/.codex/config.toml` are never edited. Sessions are told about it in their
  prompt, and running sessions restart with it before their next turn.
- **`lb web`** runs on Kurisu's own port (default 8792, not lb's 8765) with that
  passcode. lb listens on every interface; the passcode is its guard (401
  without it). Kurisu starts it on demand (a session starting, or `/browser`),
  in its own process group, and stops the whole group (lb and its Chrome) after
  30 minutes with no live session and no viewer, and on `ku shutdown`. Its log
  and browser profile are in `<KURISU_HOME>/browser/`.
- **Setup from the phone:** when it is not set up, `/browser` replies with a
  card ("🌐 Set up the Live Browser?"); Approve runs the same setup (download
  the pinned lb if needed, check Chrome, Tailscale/LAN address) and sends the
  link. `/browser disable` turns it off. Only a failed setup mentions the
  computer.
- **`/browser`** in Telegram works like `/terminal` (one-time link, pairing
  code, approval card "🌐 Browser access requested from …", Deny or 2 minutes
  kills the link, 3 attempts, `/browser close`, audited) and listens on the
  `--listen` address (default `127.0.0.1:8793`; loopback, LAN, Tailscale). After
  approval, Kurisu proxies the lb web UI to that browser and adds the passcode
  itself, so the phone never sees it. Without setup, `/browser` says how to set
  it up.
- `ku status` and Telegram `/status` show it as an optional line; `/mcp` lists
  `lb` as built in.

## Web chat from Telegram (optional) and `/web`

A chat view of the sessions, meant for a phone: the same one-time link and
approval flow as `/terminal`, but the page is a chat UI rather than a shell.

- **Turning it on.** `/web` in Telegram: while it is off the reply is the
  approval card "💬 Enable the web chat?" (Approve / Deny, 2 minutes, owner's
  chat); Approve writes `<KURISU_HOME>/weblink.json` and sends the link.
  `/web close` closes every open page and kills every link, `/web disable`
  turns it off. On the computer: `ku web-link enable [--listen IP:PORT]
  [--url URL]`, `ku web-link disable`, `ku web-link status`. The live instance
  uses port 8791; a test home (no `telegram.json`, or `KURISU_TELEGRAM_API`
  pointing at a mock) defaults to 8797 and refuses 8791 in `--listen` or
  `--url` unless `--prod` is passed. Loopback moves to the Tailscale/LAN
  address like `/terminal`.
  `enable` binds the address before it writes `weblink.json`, so it fails
  honestly (with the pid holding the port) instead of claiming "on".
  `enable` and `disable` refuse a real Telegram-connected home unless `--prod`
  is passed; `status` is read-only.
- **The link** is `/terminal`'s: 5 minutes, 3 attempts, a pairing code shown on
  the page and in the card, single use, Deny or 2 minutes of silence kills it.
  Approving sets an HttpOnly `ku_web` cookie and uses the link up. A link only
  opens on the listener it was issued for: a `/web` token opened on the
  terminal address is refused before a pairing card is sent, so it can never
  render the terminal app or grant a shell. The pairing page is the light
  lavender chat page, and the approval card/outcome say "web chat" (never
  "terminal").
- **What `/_web/` serves** is the bundled chat Svelte app (`chat.html` and its
  hashed `/_web/assets/*`), not `ku web`'s terminal-style index.
- **Endpoints** (only with an approved cookie; they call the server over its
  socket as the user, so there is no second token):

  | endpoint | |
  |---|---|
  | `GET /_web/` | the app: sidebar, the selected session, the composer |
  | `GET /_web/api/sessions?archived=true` | sessions, plus the one to open by default |
  | `GET /_web/api/sessions/{id}/messages?after=&before=&limit=` | history, tool rows included |
  | `POST /_web/api/sessions/{id}/send` `{"text":"…"}` | records the message and returns at once (`wait:false`) |
  | `POST /_web/api/sessions/{id}/interrupt` | cancels that session's running turn server-side |
  | `GET /_web/assets/{file}` | the hashed Svelte bundle (immutable cache) |
  | `GET /_web/stream` | SSE: `hello`, `sessions`, `message`, `delta` (assistant text), `tool` (start/update), `turn` (working/idle/stopped/error), `resync`, `closed` |

  The composer's button becomes **Stop** while `turn` is `working`. `interrupt`
  uses the harness's own cancel path (ACP session cancel for Claude/OpenCode,
  `turn/interrupt` for Codex), waits for the turn to release, and closes any
  tool row that never got a completion update as `cancelled`.

  Without the cookie every one of them answers 401 (pages answer with "Not
  paired"). The UI is a chat: bubbles per role (user, agent, peer, tool,
  approval, error), live streaming, archived sessions behind a sidebar
  toggle, and a sidebar that collapses behind ☰ on a narrow screen.
- **Source and build.** The page is Svelte 5 + TypeScript (`crates/kurisu/web/src/chat/`,
  entry `crates/kurisu/web/chat.html`), built by Vite into `web/dist/chat.html` and
  `web/dist/assets/*` and embedded in the binary. No CDN: everything is same-origin.
  After editing run `cd crates/kurisu/web && npm run check && npm run build` and
  commit `dist/`; `cargo build` then embeds it.
- Approved pages live in memory, so a `ku shutdown`/restart ends them: send
  `/web` again for a new link. Every step is audited with the `terminal`
  target and `kind` "web" (`remote_link_issued`, `remote_attempt`,
  `remote_approved`, `remote_denied`, `remote_expired`, `remote_closed`, …).

## Rebuilding Kurisu from source (`ku source`)

Every binary carries its repo URL and exact commit (`ku --version`,
`ku status`, `ku --help` and every session's prompt say so). `ku source
check [--install]` lists the prerequisites (git, Rust, a C compiler, tmux) and
installs rustup with your consent; `get` clones that commit into
`~/.kurisu/src` on a `self/<date>` branch; `build`; `install` checks the new
binary, keeps `~/.kurisu/bin/ku.prev` (and the first replaced binary as
`ku.orig`), swaps it in, restarts the server, and puts the previous one back if
it does not start; `rollback`; `reset` (the original binary and commit; edits
stay on their branch). The kept copies can restore the install even when the
new `ku` is broken: `~/.kurisu/bin/ku.prev source rollback`.

## Scheduled wakes

A wake delivers a message to a session at a future time, as a normal turn
(queued behind a running turn, like `wait: false`). Sessions use the
kurisu-mcp tools `wake_at {session?, at?, repeat?, message}`, `wake_list
{session?, all?}` and `wake_cancel {id}`; the user uses `POST /wakes` or
`ku wake add <name> "<message>" --at WHEN [--repeat CRON]`, `ku wake ls
[<name>] [--all]`, `ku wake rm <id>`.

- **When.** `at` is `in 30m` / `in 2h 15m` / `in 1d` / `90s`, a local clock
  time `18:30` (the next time it comes round), a local date and time
  `2026-11-08 06:00`, or RFC 3339 with an offset. Times are in the server's
  local time zone. `repeat` is a 5-field cron in local time (`0 6 * * *`
  daily at 06:00, `30 9 * * 1-5` weekdays); with `repeat` alone the first
  wake is its next match. A time in the past or more than 5 years away is
  refused; at most 50 pending wakes per session; messages up to 8000
  characters.
- **Who.** The user may wake any session. A session may wake itself (the
  default in `wake_at`) and the sessions it created, directly or indirectly
  (the `session_update` rule). A session lists and cancels wakes for itself
  and its descendants, and the ones it set.
- **Delivery.** Kurisu checks every 2 seconds. The message arrives from its
  creator (`user`, or the session that set it) with a header such as
  `[Scheduled wake #12 set by lead for 2026-11-08 06:00 -05:00; repeats '0 6 * * *', next 2026-11-09 06:00 -05:00]`.
  A repeating wake then moves to its next time; a one-off is `fired`. If the
  session cannot take the message (paused, archived) the wake is `failed`
  with `last_error` (a repeating one still moves on).
- **Restarts.** Wakes are stored in SQLite. A wake due while the server was
  down fires once when the server starts, with `LATE: delivered at …` in the
  header and `last_late: true`; a repeating wake does not replay every missed
  time. Any delivery more than 60 seconds after the due time counts as late.
  The server must be running for wakes to fire: it runs while `ku web` (or
  the Telegram bridge's Kurisu) is up, and starts on demand with any `ku`
  command.
- **Audit.** `event:wake_scheduled`, `event:wake_fired` (`(late)`),
  `event:wake_failed`, `event:wake_cancelled`.

## MCP servers per session

Every session gets `kurisu-mcp`. Other MCP servers are stored per session in
Kurisu and merged into the `--mcp-config` file Kurisu generates when the
session's CLI starts (still with `--strict-mcp-config`). Kurisu never reads
or edits `~/.claude.json`, `.mcp.json` or the Codex config. A change marks the
session for a restart (resume) before its next turn. Tools from these servers
are not pre-allowed: their first use is an approval (or add a rule, e.g.
`{"scope": "session", "target": "<id>", "tool": "mcp__github__*"}`). Only the
user changes MCP servers; sessions cannot.

**Upgrades.** kurisu-mcp is configured with `alwaysLoad`, so its tools are
always in the CLI's prompt with the live schemas (never deferred behind
ToolSearch, where a schema fetched before an upgrade could linger in the
conversation). Kurisu fingerprints each session's tool list, prompt and `ku`
binary: a live CLI launched with an older fingerprint is relaunched (resumed)
before its next turn (`event:refreshed`), and the first turn after the tools
or prompt changed starts with a short note telling the session to rely on its
current tool definitions (`event:tools_updated`). Claude Code also freezes the schema of every tool a session loaded through
ToolSearch into its transcript and replays it on resume; a session whose
transcript holds kurisu-mcp schemas that differ from the current ones is
therefore not resumed but started on a fresh CLI conversation
(`event:conversation_restarted`, new `harness_session_id`), and its first
prompt tells it to call `session_handoff` on itself to catch up from Kurisu's
history. Restart the server after replacing the binary (`ku shutdown`, or the
service's own restart).

**Startup wait.** Each time Kurisu starts or resumes a Claude CLI, it waits
until every server in the session's MCP config (kurisu-mcp and the ones
above) has connected before it delivers the first message, so the agent
never starts a turn without its tools. It reads the CLI's MCP debug log
(`--debug=mcp --debug-file <state>/<session>/mcp-debug.log`). If a server
fails, or one has not connected within `KURISU_MCP_TIMEOUT` seconds (default
45; Claude Code gives each server 30), the CLI is closed, nothing is
delivered, the session's status becomes `error`, and the send fails with
`could not start the session: MCP servers did not connect: <server> (<reason>); …`.
Fix or disable that server (`ku mcp disable <session> <server>`) and send
again.

## Permission modes

Each session has a permission mode (`ku new|update <name> --mode M`,
`session_create`/`session_update {permission_mode}`, `POST /sessions` and
`/update`, Telegram `/mode` and the `permissions:` line in `/status`). It
applies from the session's next CLI start; a change, or a new default, makes
the next turn restart the CLI (resume, context kept).

| Mode | Claude Code | Codex (approval policy, sandbox) |
| --- | --- | --- |
| `auto` | `--permission-mode auto`: its safety classifier runs routine actions and escalates risky ones | `on-request`, `workspace-write` |
| `default` | asks for most tools (`default`, or `manual` on newer CLIs) | `untrusted`, `workspace-write` |
| `acceptEdits` | file edits run, commands ask | `on-request`, `workspace-write` |
| `plan` | read and plan only | `on-request`, `read-only` |
| `bypassPermissions` | never asks (only the user can set it, never a session) | `never`, `danger-full-access` |

Without a mode set, sessions (the Lead included) run in `auto` when the
installed `claude` offers it (read from `claude --help`), else `default`; Codex
sessions without a mode keep `KURISU_CODEX_SANDBOX` if it is set. Whatever the
mode, these Claude commands always ask and so reach you as approval cards:
`sudo`, `doas`, `su`, `rm -r`/`rm -rf`/`rm -fr`, `git push --force|-f`,
`git reset --hard`, `git clean`, `dd`, `mkfs`, `shutdown`, `reboot` (Claude's
`permissions.ask` in Kurisu's `--settings`). Anything auto mode's classifier
escalates also arrives as an approval card. Auto mode treats what the user
asked for in the conversation as authorized, so an explicit request ("delete
that folder") can run without a card unless it matches the list above.

## Approvals

When a session's Claude CLI wants a tool that is not pre-allowed (in its
`default` permission mode), the CLI's `PermissionRequest` hook hands the
request to Kurisu instead of showing a dialog in the terminal:

1. Rules are checked first. Deny rules win over allow rules. A rule applies to
   a role or to one session, names a tool (or a `prefix*`, or `*`), and can
   require `in_cwd` (every path inside the session's folder; for Bash: the
   shell's cwd, and no `..`, `~` or outside absolute path in the command) or
   a `command_prefix` (Bash only; no `;`, `&&`, `|`, redirects or `$(`).
   A match is settled at once and audited as `rule#<n>`.
2. Otherwise an `Approval` is created, a `role: "approval"` message marks its
   place in the channel, and the session's status becomes
   `awaiting_approval`. Answer it here, in `ku web` (a card in the channel), or
   with `ku approve <id>` / `ku deny <id>`.
3. Unanswered requests expire after `KURISU_APPROVAL_TTL` seconds (default
   600) and are denied. Requests still pending when the turn ends, the session
   is stopped, or the server restarts are cancelled.

**Questions.** Claude's `AskUserQuestion` tool arrives the same way, as an
approval with `kind: "question"` and `questions[]`. Rules never settle a
question. Answer it with `POST /approvals/{id}/answer` (`ku answer <id>
"<label>"`); the CLI receives the answers as the tool's result. Deny it to
dismiss the question. `approve` is refused for questions.

Every request and decision is in the audit trail. Only the user answers
approvals and edits rules; sessions cannot (their MCP calls are refused).
The standalone `claude-cli-acp` agent maps the same requests to ACP
`session/request_permission` for its client.

## Versioning

Breaking changes go to `/api/v2`; `/api/v1` stays as documented. Adding fields
to results or new events is not breaking, so clients must ignore unknown
fields and events.

## Examples

```bash
T=<token>; B=http://127.0.0.1:8787/api/v1
curl -H "Authorization: Bearer $T" $B/sessions
curl -H "Authorization: Bearer $T" -H content-type:application/json -d '{"text":"hi"}' $B/sessions/worker-a/messages
curl -N "$B/stream?token=$T"
```

OpenCode uses installed ACP model/config discovery. `GET /models?harness=opencode&model=provider/model` returns `models` and `config_options` (including model-specific effort values). See [OpenCode capabilities and Telegram matrix](opencode.md).

For OpenCode, the permission modes in the table above narrow to `default` (upstream Build permissions) and `plan` (upstream Plan agent). Other modes fail explicitly; see [OpenCode capabilities](opencode.md).

### Docked terminal

Both web UIs have a **Terminal** panel icon at the right edge of the chat header.
It opens a pane to the right of chat, or a full-width overlay below the chat
header on narrow screens, so the same toggle remains reachable. Close (or Escape) disconnects
and reaps that pane's shell. The directory defaults to the selected session's
workspace; enter another existing directory and press Open to start a fresh shell.

The pane uses `remote.rs`'s PTY spawn, xterm assets, resize/input protocol and
30-minute idle timeout. WebSocket connections stay on the web origin, avoiding a
second listener, one-time link or approval hop. Each pane owns a separate shell;
opening it alongside `/terminal` or another web pane cannot replace either shell.
A second connection to the same standalone `/terminal` session gets HTTP 409 and
a visible already-attached message; the first connection keeps working.

- `GET /api/v1/terminal?session=<id>&dir=<optional directory>` upgrades the
  `ku web` connection. Reuse the existing owner token with WebSocket protocols
  `["kurisu", "<token>"]` (or an Authorization header for non-browser clients).
  Only `kurisu` is negotiated; the token stays out of URLs.
- `GET /_web/terminal?session=<id>&dir=<optional directory>` upgrades the
  Telegram-paired web connection using its existing HttpOnly `ku_web` cookie.
  Revoking the web session closes its terminal panes too.

Both routes reject missing/wrong credentials and cross-origin upgrades before
spawning a shell. An explicit directory overrides the selected session. A missing
session uses the workspaces root; an invalid session or directory returns 400.
WebSocket binary output and text input/resize frames match `/terminal`'s transport.

Standalone `/terminal` and the terminal/browser/web pairing pages embed the same
light/dark `palette.css` as the app. Both terminal views use the same xterm theme
helper and reapply it when the system colour scheme changes.

### Local diagnostics

`ku doctor` prints an aligned table; `ku doctor --json` returns an array of
`{check, status, expected, actual, detail}` records. Status is `OK`, `WARN` or
`FAIL`; the exit code is 1 if any check fails, otherwise 0. Neither form starts
the daemon, installs anything, contacts Telegram, or migrates the database.
Absent processes are reported as `not running` and do not fail the command.

The checks compare the running daemon's socket-reported build against this
binary, negotiate diagnostic protocol version 1, compare the served index's
main bundle hash with the served service worker's cache name, check the Live
Browser pin against the installed executable (and the supervised running
executable on Linux), probe the configured web listener, and open the database
read-only using the same pre-UUID guard as startup. Dirty or unavailable build
identities are warnings because a commit alone cannot identify local edits.
An unknown server operation explains how to restart an older daemon.

The standalone `ku web --serve` records its actual bound address in
`$KURISU_HOME/web.json` and marks it disabled on graceful exit. The paired
`/web` listener retains its existing `weblink.json`; it has no standalone
service worker, so an unavailable worker is reported as a warning. If both
listeners are configured, both are probed. Older standalone processes without
a discovery record must be restarted once before they can be discovered.
HTTP probes use literal configured IP addresses with proxies and redirects
disabled; they never use a public URL. Diagnostics do not repair stale browser
caches or restart processes.

Enabling or disabling the paired web listener notifies the running daemon to
re-read `weblink.json` before reporting success. The daemon also restores an
enabled paired listener at startup; a bind failure is logged and remains a
`doctor` failure. With no daemon running, enable saves the configuration and
checks address availability; the listener starts with the next daemon.

Doctor checks also appear at common failure points: `ku web` includes the paired
listener check when it refuses a link, and unknown socket operations include the
binary and running-daemon SHAs plus the restart command. The paired web page
checks its initial bundle hash and build SHA against the running server on load,
focus, and every 30 seconds, showing a one-line reload banner on drift. Vite
hashes and git SHAs are compared through their build provenance, not directly.
After a successful deployment health check, the deploy script runs the installed
doctor as an advisory assertion; FAIL is printed loudly but never rolls back a
healthy deployment.

## Read-only session workspace

These GET routes inspect the selected session's `cwd`; each requires the same
owner authentication as the session routes. The paired chat UI exposes matching
routes under `/_web/api/sessions/{id}/workspace/` using its existing `ku_web` cookie.
Responses use the `{"ok": ...}` envelope and `Cache-Control: no-store`.

| Route suffix under `/sessions/{id}/workspace/` | Result |
| --- | --- |
| `list-dir?path=docs&hidden=false` | `{entries: [{name, path, directory}]}`; one directory at a time |
| `read-file?path=README.md` | `{kind: "text", text, size}` or `{kind: "binary" | "large", size}` |
| `git-status` | `{entries: [{path, status}]}` from porcelain Git status |
| `git-diff?path=README.md` | File-view result containing the diff against HEAD, including staged changes |

Paths must be relative, contain no `..`, and resolve inside the canonical session
workspace. Escaping symlinks are refused. Listing hides `.git`, `node_modules`,
`target`, `dist`, and `.svelte-kit` unless `hidden=true`. Text previews cap at
512 KiB; reads and Git output have a 1 MiB hard cap. Binary files return metadata.
Untracked files show their content; deleted files show a deletion note. Git runs
without a shell, external diff, textconv, pager, or optional index writes.

The shared panel in both UIs provides Terminal, Files, and Git changes tabs.
Terminal is the initial default; `ku.workspaceTab` remembers the last selection.
Left/right arrows and Home/End work in the tab strip. Ctrl+1/2/3 switches tabs
outside xterm; terminal keystrokes retain their shell behavior.
