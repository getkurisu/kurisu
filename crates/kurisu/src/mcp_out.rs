//! `ku mcp serve`: Kurisu as an OUTWARD MCP server.
//!
//! `mcp.rs` is the stdio server Kurisu injects into every session it starts (bare
//! `ku mcp`); this is the other direction: a streamable-HTTP MCP endpoint that an
//! app on another device (Vicuna, an editor, a second agent) calls over Tailscale,
//! shaped like any remote MCP server: a URL plus a token. `ku mcp serve` runs the
//! listener in the foreground and talks to the daemon over the same 0600 unix
//! socket as `ku` itself.
//!
//!   ku mcp serve [--host IP] [--port N]   default 100.118.224.35:8793 (Tailscale)
//!   ku mcp token                          create if missing, print
//!   ku mcp url [--host IP] [--port N]     http://<host>:<port>/mcp/<token>
//!
//! The daemon can host the same router in-process (the remote Hub's "mcp" kind),
//! which is what the Telegram `/mcp_server` card enables: `<KURISU_HOME>/mcp-server.json`
//! holds `{enabled, host, port}`, a start with it enabled re-arms the listener, and
//! `ku mcp serve` in the foreground keeps working unchanged.
//!
//!   POST /mcp/<token>    JSON-RPC 2.0, answered as application/json
//!   POST /mcp            same, with `Authorization: Bearer <token>`
//!   GET  /mcp[...]       405: no server-to-client SSE stream (POST only)
//!
//! Bind: an explicit IP address only. A hostname or an unspecified address
//! (0.0.0.0, ::) is refused, so the listener never lands on every interface by
//! accident. Auth is the token file `<KURISU_HOME>/mcp-server-token` (0600), in the
//! URL for clients that cannot set headers or as a bearer header. On a loopback
//! bind a non-loopback Host header is refused (DNS rebinding).
//!
//! Safety. Tools, deliberately minimal: `sessions`, `send`, `read`, `peers`.
//! An outward call has no caller session, so `send` is recorded as the OWNER's own
//! chat message (like text typed in Telegram): the session treats it as the user
//! and runs a normal turn. Nothing else is reachable: no session create/stop, no
//! lifecycle, harness, model or MCP-server changes, and no approvals. The tool set
//! is a whitelist in `to_request`.
//!
//! Blocking: a real MCP client gives up at 120 s, so no call blocks longer than
//! about 55 s. `send` queues and returns a seq at once; `read` polls for new
//! messages, with `wait_secs` capped at 60 and every call cut off at 55 s.

use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::body::Bytes;
use axum::extract::{Path as AxPath, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub const SERVER_NAME: &str = "kurisu-mcp";
/// This box's Tailscale address: the outward server is for another device.
pub const DEFAULT_HOST: &str = "100.118.224.35";
pub const DEFAULT_PORT: u16 = 8793;
pub const TOKEN_FILE: &str = "mcp-server-token";
/// The stored switch, the way `terminal.json` / `browser.json` hold theirs.
pub const CONFIG_FILE: &str = "mcp-server.json";
/// The client-side hard limit is 120 s; stay well inside it.
pub const MAX_WAIT_SECS: u64 = 60;
/// A tool call is answered (or failed) before this, whatever the client sends.
const CALL_BUDGET: Duration = Duration::from_secs(55);
/// How often `read` asks the daemon for new messages while it waits.
const POLL: Duration = Duration::from_millis(400);
/// Per-message text cap in `read` results, so one huge reply cannot blow up a client.
const READ_MAX_CHARS: i64 = 6000;

#[derive(Clone)]
struct Ctx {
    home: PathBuf,
    token: Arc<String>,
    allowed_hosts: Arc<Vec<String>>,
    /// `http://<bound addr>`: issuer, resource and endpoint URLs hang off it.
    base: Arc<String>,
}

impl Ctx {
    fn socket(&self) -> PathBuf {
        crate::paths::Paths { home: self.home.clone() }.socket()
    }
}

// ------------------------------------------------------------------ token

pub fn token_path(home: &Path) -> PathBuf {
    home.join(TOKEN_FILE)
}

/// The token, generated (0600) on first start; a hand-edited token is kept.
pub fn ensure_token(home: &Path) -> Result<String> {
    let path = token_path(home);
    if let Ok(raw) = std::fs::read_to_string(&path) {
        let token = raw.trim().to_string();
        // A short or non-hex token is a mistake; replace it rather than serve it.
        if token.len() >= 32 && token.chars().all(|c| c.is_ascii_hexdigit()) {
            tighten(&path);
            return Ok(token);
        }
    }
    let token = format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple());
    crate::telegram::write_private(&path, token.as_bytes())?;
    Ok(token)
}

/// Repair the mode of a token file that lost its 0600 (a hand-made copy).
fn tighten(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
}

/// First 8 characters then an ellipsis: enough to recognise the token, never
/// enough to use it. The status card only ever carries this form.
pub fn mask(token: &str) -> String {
    format!("{}…", token.chars().take(8).collect::<String>())
}

/// The URL with the token masked, for the status card.
pub fn masked_url(addr: SocketAddr, token: Option<&str>) -> String {
    match token {
        Some(t) if !t.is_empty() => format!("http://{addr}/mcp/{}", mask(t)),
        _ => format!("http://{addr}/mcp/<token created on enable>"),
    }
}

/// The mode of the token file as `0600`, or None when there is no file yet.
pub fn token_mode(home: &Path) -> Option<String> {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::metadata(token_path(home)).ok()?;
    Some(format!("{:04o}", meta.permissions().mode() & 0o777))
}

// ------------------------------------------------------------------ switch

/// The persisted switch, next to the token. The listener itself is owned by the
/// daemon's remote Hub (`ku mcp serve` in the foreground stays a separate command).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub enabled: bool,
    /// The address to listen on, kept between restarts so the URL stays stable.
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_host() -> String { DEFAULT_HOST.to_string() }
fn default_port() -> u16 { DEFAULT_PORT }

impl Default for Config {
    fn default() -> Self { Config { enabled: false, host: default_host(), port: default_port() } }
}

pub fn config_path(home: &Path) -> PathBuf {
    home.join(CONFIG_FILE)
}

/// The stored switch; None when the file is missing or unreadable (treated as off).
pub fn load_config(home: &Path) -> Option<Config> {
    serde_json::from_slice(&std::fs::read(config_path(home)).ok()?).ok()
}

/// 0600 like the token: the file says where a listener sits, which is not public.
pub fn save_config(home: &Path, cfg: &Config) -> Result<()> {
    crate::telegram::write_private(&config_path(home), &serde_json::to_vec_pretty(cfg)?)
}

/// The configured address, refusing an unspecified or hostname host as `--host` does.
pub fn socket_addr(cfg: &Config) -> Result<SocketAddr> {
    let (ip, port) = host_port(Some(&cfg.host), Some(&cfg.port.to_string()))?;
    Ok(SocketAddr::new(ip, port))
}

/// Turn the stored switch off; answers whether it had been on.
pub fn disable(home: &Path) -> Result<bool> {
    match load_config(home).filter(|c| c.enabled) {
        Some(mut c) => {
            c.enabled = false;
            save_config(home, &c)?;
            Ok(true)
        }
        None => Ok(false),
    }
}

// ------------------------------------------------------------- status card

/// "1h 02m", "3d 4h", "5m 09s": the uptime line the owner reads in Telegram.
pub fn human_uptime(secs: u64) -> String {
    let (d, h, m, s) = (secs / 86400, secs % 86400 / 3600, secs % 3600 / 60, secs % 60);
    if d > 0 { format!("{d}d {h}h") }
    else if h > 0 { format!("{h}h {m:02}m") }
    else if m > 0 { format!("{m}m {s:02}s") }
    else { format!("{s}s") }
}

/// The `/mcp_server` card text: plain lines, no markdown (Telegram shows it raw),
/// the token always masked. `st` is the `mcp_server_status` reply.
pub fn status_text(st: &Value) -> String {
    let listening = st["listening"] == json!(true);
    let mut lines = vec![
        "🔌 Outward MCP server".to_string(),
        String::new(),
        format!("state: {}", if listening { "running" } else { "stopped" }),
        format!("address: {}", st["listen"].as_str().filter(|s| !s.is_empty()).unwrap_or("unknown")),
    ];
    let token = match (st["token_path"].as_str(), st["token_mode"].as_str()) {
        (Some(path), Some(mode)) => format!("{path} ({mode})"),
        (Some(path), None) => format!("{path} (not created yet)"),
        _ => "unknown".into(),
    };
    if let Some(u) = st["sign_in_url"].as_str().filter(|u| !u.is_empty()) {
        lines.push(format!("url: {u}"));
        lines.push("  paste it into any MCP client; it opens a sign-in you approve here".into());
    }
    let clients = st["oauth_clients"].as_array().cloned().unwrap_or_default();
    let names: Vec<String> = clients.iter().map(|c| c["client_name"].as_str().unwrap_or("?").to_string()).collect();
    lines.push(format!("signed-in clients: {}", if names.is_empty() { "none".to_string() } else { names.join(", ") }));
    lines.push(format!("legacy secret file: {token}"));
    if listening {
        let pid = st["pid"].as_u64().unwrap_or(0);
        lines.push(format!("pid: {pid} (the daemon)"));
        if let Some(secs) = st["uptime_seconds"].as_u64() {
            lines.push(format!("uptime: {}", human_uptime(secs)));
        }
    }
    if let Some(url) = st["url"].as_str().filter(|u| !u.is_empty()) {
        lines.push(format!("legacy URL (no sign-in): {url}"));
    }
    if let Some(note) = st["note"].as_str().filter(|n| !n.trim().is_empty()) {
        lines.push(format!("note: {note}"));
    }
    lines.extend([
        String::new(),
        if listening {
            "The legacy URL is masked; Show URL sends it in full. Sign out removes a client's sign-in.".into()
        } else {
            "Enable starts it inside the daemon; a restart with it enabled brings it back.".into()
        },
    ]);
    lines.join("\n")
}

/// Enable (when stopped) or Disable (when running), plus Show URL and one
/// Sign out button per signed-in OAuth client. `msv:` is
/// this card's callback prefix. `st` is the `mcp_server_status` reply.
pub fn keyboard(st: &Value) -> Value {
    let toggle = if st["listening"] == json!(true) {
        json!({"text": "⏹ Disable", "callback_data": "msv:off"})
    } else {
        json!({"text": "▶️ Enable", "callback_data": "msv:on"})
    };
    let mut rows = vec![json!([toggle, {"text": "🔗 Show URL", "callback_data": "msv:url"}])];
    for c in st["oauth_clients"].as_array().into_iter().flatten().take(4) {
        let (Some(id), name) = (c["client_id"].as_str(), c["client_name"].as_str().unwrap_or("client")) else { continue };
        let short: String = name.chars().take(24).collect();
        rows.push(json!([{"text": format!("🚫 Sign out {short}"), "callback_data": format!("msv:rv:{id}")}]));
    }
    json!({"inline_keyboard": rows})
}

// ------------------------------------------------------------------ auth

fn ct_eq(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn bearer(h: &HeaderMap) -> Option<&str> {
    h.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "))
}

fn host_ok(allowed: &[String], h: &HeaderMap) -> bool {
    // No Host at all is not a browser (browsers always send one), so it is no rebinding risk.
    let Some(host) = h.get(header::HOST) else { return true };
    let host = host.to_str().unwrap_or("");
    allowed.is_empty() || allowed.iter().any(|a| a == host)
}

/// The static secret in the URL or in `Authorization: Bearer` (unchanged), or an
/// unexpired OAuth access token as a bearer. Anything else is 401 with the
/// `WWW-Authenticate` header that starts a stock client's sign-in.
fn authorize(ctx: &Ctx, h: &HeaderMap, path_token: Option<&str>) -> Result<(), Response> {
    if !host_ok(&ctx.allowed_hosts, h) {
        return Err(refused(StatusCode::FORBIDDEN, "unexpected Host header"));
    }
    let header_ok = bearer(h).is_some_and(|t| ct_eq(t, &ctx.token) || oauth_token_ok(&ctx.home, t));
    let path_ok = path_token.is_some_and(|t| ct_eq(t, &ctx.token));
    if !header_ok && !path_ok {
        return Err(unauthorized(ctx, "sign-in required (missing, wrong or expired token)"));
    }
    Ok(())
}

fn refused(status: StatusCode, why: &str) -> Response {
    (status, Json(json!({"error": why}))).into_response()
}

// ------------------------------------------------------------------ OAuth (MCP auth 2025-06-18)
//
// A stock MCP client pastes the bare `/mcp` URL, gets 401 + `resource_metadata`,
// discovers this server as its own authorization server, registers itself
// (RFC 7591, public client), and opens `/authorize` in the owner's browser with
// PKCE S256. The owner approves on a Telegram card; the waiting page polls and
// then navigates to the client's (loopback) redirect_uri with the code, so the
// code never passes through Telegram. `/token` swaps it for an opaque bearer.
//
// All state is in 0600 JSON files under the home, so the Telegram decision (in
// the daemon) reaches a foreground `ku mcp serve` listener too.

pub const CLIENTS_FILE: &str = "mcp-server-clients.json";
pub const TOKENS_FILE: &str = "mcp-server-oauth-tokens.json";
pub const PENDING_FILE: &str = "mcp-server-oauth-pending.json";
pub const OAUTH_SCOPE: &str = "mcp";
/// How long the owner has to tap Approve.
const PENDING_SECS: i64 = 600;
/// Life of an authorization code once approved.
const CODE_SECS: i64 = 300;
const ACCESS_SECS: i64 = 3600;
const REFRESH_SECS: i64 = 30 * 86400;
/// Open sign-in requests at once: more is someone spamming the owner's chat.
const MAX_PENDING: usize = 5;
const MAX_CLIENTS: usize = 200;

/// Serialises read-modify-write of the OAuth files inside this process.
static OAUTH_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn now_s() -> i64 {
    chrono::Utc::now().timestamp()
}

fn random_hex(bytes: usize) -> String {
    use rand::RngCore;
    let mut b = vec![0u8; bytes];
    rand::rng().fill_bytes(&mut b);
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Tokens are stored hashed: the file says who is signed in, never lets anyone sign in.
fn token_hash(t: &str) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(t.as_bytes()).iter().map(|x| format!("{x:02x}")).collect()
}

fn read_obj(home: &Path, file: &str) -> serde_json::Map<String, Value> {
    std::fs::read(home.join(file)).ok()
        .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default()
}

fn write_obj(home: &Path, file: &str, v: &serde_json::Map<String, Value>) -> Result<()> {
    crate::telegram::write_private(&home.join(file), &serde_json::to_vec_pretty(v)?)
}

/// Run `f` on one OAuth file under the lock and write it back.
fn with_file<T>(home: &Path, file: &str, f: impl FnOnce(&mut serde_json::Map<String, Value>) -> T) -> Result<T> {
    let _g = OAUTH_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mut m = read_obj(home, file);
    let out = f(&mut m);
    write_obj(home, file, &m)?;
    Ok(out)
}

fn base_url(ctx: &Ctx) -> String {
    ctx.base.to_string()
}

fn resource_url(ctx: &Ctx) -> String {
    format!("{}/mcp", ctx.base)
}

fn www_authenticate(ctx: &Ctx) -> HeaderValue {
    HeaderValue::from_str(&format!(
        "Bearer resource_metadata=\"{}/.well-known/oauth-protected-resource\"", ctx.base))
        .unwrap_or_else(|_| HeaderValue::from_static("Bearer"))
}

/// 401 for /mcp: the header tells a stock client where to start signing in.
fn unauthorized(ctx: &Ctx, why: &str) -> Response {
    let mut r = refused(StatusCode::UNAUTHORIZED, why);
    r.headers_mut().insert(header::WWW_AUTHENTICATE, www_authenticate(ctx));
    r
}

/// Is this an unexpired OAuth access token?
fn oauth_token_ok(home: &Path, token: &str) -> bool {
    let h = token_hash(token);
    let now = now_s();
    read_obj(home, TOKENS_FILE).values()
        .any(|t| t["access_hash"].as_str() == Some(h.as_str()) && t["expires_at"].as_i64().unwrap_or(0) > now)
}

fn oauth_error(status: StatusCode, err: &str, desc: &str) -> Response {
    let mut r = (status, Json(json!({"error": err, "error_description": desc}))).into_response();
    r.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

/// `application/x-www-form-urlencoded` (or a query string) into pairs; the last value wins.
fn form_decode(raw: &str) -> std::collections::HashMap<String, String> {
    fn dec(s: &str) -> String {
        let b = s.as_bytes();
        let mut out = Vec::with_capacity(b.len());
        let mut i = 0;
        while i < b.len() {
            match b[i] {
                b'+' => out.push(b' '),
                b'%' if i + 2 < b.len() => {
                    match u8::from_str_radix(std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or("zz"), 16) {
                        Ok(v) => { out.push(v); i += 2; }
                        Err(_) => out.push(b'%'),
                    }
                }
                c => out.push(c),
            }
            i += 1;
        }
        String::from_utf8_lossy(&out).into_owned()
    }
    raw.split('&').filter(|p| !p.is_empty()).map(|p| {
        let (k, v) = p.split_once('=').unwrap_or((p, ""));
        (dec(k), dec(v))
    }).collect()
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;").replace('\'', "&#39;")
}

/// (scheme, host, port, path) of an http(s) URL, without pulling in a URL crate.
fn split_url(u: &str) -> Option<(String, String, Option<u16>, String)> {
    let (scheme, rest) = u.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return None;
    }
    let (auth, path) = match rest.find(['/', '?', '#']) {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    if auth.is_empty() || auth.contains('@') {
        return None;
    }
    let (host, port) = if let Some(r) = auth.strip_prefix('[') {
        let (h, tail) = r.split_once(']')?;
        (format!("[{h}]"), tail.strip_prefix(':'))
    } else {
        match auth.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), Some(p)),
            None => (auth.to_string(), None),
        }
    };
    let port = match port {
        Some(p) => Some(p.parse::<u16>().ok()?),
        None => None,
    };
    Some((scheme, host.to_ascii_lowercase(), port, path.to_string()))
}

fn is_loopback_host(h: &str) -> bool {
    matches!(h, "127.0.0.1" | "localhost" | "[::1]")
}

/// A redirect a public client may register: https anywhere, http only on loopback.
fn redirect_allowed(u: &str) -> bool {
    match split_url(u) {
        Some((s, h, _, _)) => !u.contains('#') && (s == "https" || is_loopback_host(&h)),
        None => false,
    }
}

/// The requested redirect against the registered ones: exact, or for loopback
/// the same host and path on any port (RFC 8252 §7.3: native apps pick a free port).
fn redirect_matches(registered: &[String], asked: &str) -> bool {
    if registered.iter().any(|r| r == asked) {
        return true;
    }
    let Some((s, h, _, p)) = split_url(asked) else { return false };
    if s != "http" || !is_loopback_host(&h) {
        return false;
    }
    registered.iter().any(|r| matches!(split_url(r), Some((rs, rh, _, rp)) if rs == s && rh == h && rp == p))
}

fn redirect_host(u: &str) -> String {
    match split_url(u) {
        Some((_, h, Some(p), _)) => format!("{h}:{p}"),
        Some((_, h, None, _)) => h,
        None => "?".into(),
    }
}

fn with_query(uri: &str, fields: &[(&str, &str)]) -> String {
    let fields: Vec<(&str, &str)> = fields.iter().filter(|(_, v)| !v.is_empty()).copied().collect();
    let sep = if uri.contains('?') { '&' } else { '?' };
    format!("{uri}{sep}{}", crate::oauth::form_encode(&fields))
}

fn protected_resource_doc(ctx: &Ctx) -> Value {
    json!({
        "resource": resource_url(ctx),
        "authorization_servers": [base_url(ctx)],
        "scopes_supported": [OAUTH_SCOPE],
        "bearer_methods_supported": ["header"],
        "resource_name": "Kurisu",
    })
}

fn authorization_server_doc(ctx: &Ctx) -> Value {
    let b = base_url(ctx);
    json!({
        "issuer": b,
        "authorization_endpoint": format!("{b}/authorize"),
        "token_endpoint": format!("{b}/token"),
        "registration_endpoint": format!("{b}/register"),
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none"],
        "scopes_supported": [OAUTH_SCOPE],
    })
}

async fn well_known_resource(State(ctx): State<Ctx>, h: HeaderMap) -> Response {
    if !host_ok(&ctx.allowed_hosts, &h) {
        return refused(StatusCode::FORBIDDEN, "unexpected Host header");
    }
    json_response(protected_resource_doc(&ctx))
}

async fn well_known_as(State(ctx): State<Ctx>, h: HeaderMap) -> Response {
    if !host_ok(&ctx.allowed_hosts, &h) {
        return refused(StatusCode::FORBIDDEN, "unexpected Host header");
    }
    json_response(authorization_server_doc(&ctx))
}

/// RFC 7591 dynamic registration, public clients only (no secret is issued).
async fn register(State(ctx): State<Ctx>, h: HeaderMap, body: Bytes) -> Response {
    if !host_ok(&ctx.allowed_hosts, &h) {
        return refused(StatusCode::FORBIDDEN, "unexpected Host header");
    }
    let bad = |d: &str| (StatusCode::BAD_REQUEST, Json(json!({"error": "invalid_client_metadata", "error_description": d}))).into_response();
    let Ok(meta) = serde_json::from_slice::<Value>(&body) else { return bad("body must be a JSON object") };
    let uris: Vec<String> = meta["redirect_uris"].as_array().map(|a| a.iter().filter_map(Value::as_str).map(String::from).collect()).unwrap_or_default();
    if uris.is_empty() || uris.len() > 10 {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "invalid_redirect_uri", "error_description": "1 to 10 redirect_uris are required"}))).into_response();
    }
    if let Some(u) = uris.iter().find(|u| !redirect_allowed(u)) {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "invalid_redirect_uri",
            "error_description": format!("{u}: only https, or http on 127.0.0.1/localhost/[::1], is allowed")}))).into_response();
    }
    if let Some(m) = meta["token_endpoint_auth_method"].as_str() {
        if m != "none" {
            return bad("only public clients (token_endpoint_auth_method none) are supported");
        }
    }
    let name: String = meta["client_name"].as_str().map(str::trim).filter(|s| !s.is_empty())
        .unwrap_or("MCP client").chars().filter(|c| !c.is_control()).take(80).collect();
    let client_id = random_hex(16);
    let rec = json!({"client_name": name, "redirect_uris": uris, "created_at": now_s()});
    let saved = with_file(&ctx.home, CLIENTS_FILE, |m| {
        if m.len() >= MAX_CLIENTS {
            // Drop the oldest registration; a real client simply registers again.
            if let Some(old) = m.iter().min_by_key(|(_, v)| v["created_at"].as_i64().unwrap_or(0)).map(|(k, _)| k.clone()) {
                m.remove(&old);
            }
        }
        m.insert(client_id.clone(), rec);
    });
    if let Err(e) = saved {
        return refused(StatusCode::INTERNAL_SERVER_ERROR, &format!("could not save the registration: {e}"));
    }
    let mut r = (StatusCode::CREATED, Json(json!({
        "client_id": client_id,
        "client_id_issued_at": now_s(),
        "client_name": name,
        "redirect_uris": uris,
        "token_endpoint_auth_method": "none",
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "scope": OAUTH_SCOPE,
    }))).into_response();
    r.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

fn html_page(status: StatusCode, title: &str, body: &str) -> Response {
    let page = format!("<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
<title>{t}</title><style>body{{font-family:system-ui,sans-serif;max-width:32rem;margin:3rem auto;padding:0 1rem;line-height:1.5}}</style></head>\
<body>{body}</body></html>", t = html_escape(title));
    let mut r = (status, page).into_response();
    let hd = r.headers_mut();
    hd.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));
    hd.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    hd.insert("x-frame-options", HeaderValue::from_static("DENY"));
    r
}

fn redirect_to(uri: &str) -> Response {
    let mut r = StatusCode::FOUND.into_response();
    if let Ok(v) = HeaderValue::from_str(uri) {
        r.headers_mut().insert(header::LOCATION, v);
    }
    r
}

/// GET /authorize: check the request, open a pending sign-in, ask the owner on
/// Telegram (in the background), and answer a page that waits for the decision.
async fn authorize_get(State(ctx): State<Ctx>, h: HeaderMap, uri: axum::http::Uri) -> Response {
    if !host_ok(&ctx.allowed_hosts, &h) {
        return refused(StatusCode::FORBIDDEN, "unexpected Host header");
    }
    let q = form_decode(uri.query().unwrap_or(""));
    let get = |k: &str| q.get(k).cloned().unwrap_or_default();
    let client_id = get("client_id");
    let clients = read_obj(&ctx.home, CLIENTS_FILE);
    // Unknown client or redirect: never redirect (RFC 6749 §4.1.2.1), show the error here.
    let Some(client) = clients.get(&client_id) else {
        return html_page(StatusCode::BAD_REQUEST, "Sign-in failed", "<p>Unknown client_id. The client must register first.</p>");
    };
    let registered: Vec<String> = client["redirect_uris"].as_array().map(|a| a.iter().filter_map(Value::as_str).map(String::from).collect()).unwrap_or_default();
    let redirect_uri = match q.get("redirect_uri") {
        Some(r) => r.clone(),
        None if registered.len() == 1 => registered[0].clone(),
        None => return html_page(StatusCode::BAD_REQUEST, "Sign-in failed", "<p>redirect_uri is required.</p>"),
    };
    if !redirect_matches(&registered, &redirect_uri) {
        return html_page(StatusCode::BAD_REQUEST, "Sign-in failed", "<p>redirect_uri is not registered for this client.</p>");
    }
    let state = get("state");
    let fail = |err: &str, desc: &str| redirect_to(&with_query(&redirect_uri, &[("error", err), ("error_description", desc), ("state", &state)]));
    if get("response_type") != "code" {
        return fail("unsupported_response_type", "only response_type=code is supported");
    }
    let challenge = get("code_challenge");
    if challenge.len() < 43 || challenge.len() > 128 || get("code_challenge_method") != "S256" {
        return fail("invalid_request", "PKCE with code_challenge_method=S256 is required");
    }
    if let Some(res) = q.get("resource") {
        let want = resource_url(&ctx);
        if res.trim_end_matches('/') != want && res.trim_end_matches('/') != base_url(&ctx) {
            return fail("invalid_target", "resource must be this server's /mcp URL");
        }
    }
    let scope = match q.get("scope").map(|s| s.trim()).filter(|s| !s.is_empty()) {
        None => OAUTH_SCOPE.to_string(),
        Some(s) if s.split_whitespace().all(|x| x == OAUTH_SCOPE) => OAUTH_SCOPE.to_string(),
        Some(_) => return fail("invalid_scope", "the only scope is mcp"),
    };
    let name = client["client_name"].as_str().unwrap_or("MCP client").to_string();
    let id = random_hex(16);
    let now = now_s();
    let rec = json!({"client_id": client_id, "client_name": name, "redirect_uri": redirect_uri,
                     "redirect_given": q.contains_key("redirect_uri"), "state": state,
                     "code_challenge": challenge, "scope": scope, "created_at": now,
                     "expires_at": now + PENDING_SECS, "decision": "pending"});
    let opened = with_file(&ctx.home, PENDING_FILE, |m| {
        prune_pending(m, now);
        if m.values().filter(|p| p["decision"] == "pending").count() >= MAX_PENDING {
            return false;
        }
        m.insert(id.clone(), rec);
        true
    });
    match opened {
        Ok(true) => {}
        Ok(false) => return html_page(StatusCode::TOO_MANY_REQUESTS, "Too many sign-ins",
            "<p>Too many sign-in requests are waiting. Answer or let them expire, then try again.</p>"),
        Err(e) => return refused(StatusCode::INTERNAL_SERVER_ERROR, &format!("could not save the request: {e}")),
    }
    // The card goes out in the background: the page and the daemon never wait on Telegram.
    let card = json!({"op": "mcp_oauth_card", "id": id, "client_name": name,
                      "redirect_host": redirect_host(&redirect_uri), "scope": scope, "expires_at": now + PENDING_SECS});
    let c2 = ctx.clone();
    let id2 = id.clone();
    tokio::spawn(async move {
        if let Err(e) = call_daemon(&c2, card).await {
            let _ = with_file(&c2.home, PENDING_FILE, |m| {
                if let Some(p) = m.get_mut(&id2) { p["card_error"] = json!(e); }
            });
        }
    });
    let body = format!(
        "<h2>Approve in Telegram</h2><p>Kurisu is asking you to approve <b>{n}</b>. Tap Approve in Telegram.</p>\
<p id=\"s\">Waiting for your answer… (expires in 10 minutes)</p>\
<script>const id={id:?};async function t(){{try{{const r=await fetch('/authorize/poll/'+id,{{cache:'no-store'}});const j=await r.json();\
if(j.redirect){{document.getElementById('s').textContent=j.state==='approved'?'Approved. Returning to the app…':'Denied.';location.replace(j.redirect);return}}\
if(j.state==='expired'){{document.getElementById('s').textContent='This request expired. Start the sign-in again.';return}}\
if(j.error){{document.getElementById('s').textContent='Could not ask Telegram: '+j.error}}}}catch(e){{}}setTimeout(t,1000)}}t();</script>",
        n = html_escape(&name));
    html_page(StatusCode::OK, "Approve in Telegram", &body)
}

/// Drop expired requests and used/expired codes.
fn prune_pending(m: &mut serde_json::Map<String, Value>, now: i64) {
    m.retain(|_, p| match p["decision"].as_str() {
        Some("approved") => p["code_expires_at"].as_i64().unwrap_or(0) > now,
        _ => p["expires_at"].as_i64().unwrap_or(0) > now,
    });
}

/// GET /authorize/poll/<id>: where the waiting page goes next.
async fn authorize_poll(State(ctx): State<Ctx>, h: HeaderMap, AxPath(id): AxPath<String>) -> Response {
    if !host_ok(&ctx.allowed_hosts, &h) {
        return refused(StatusCode::FORBIDDEN, "unexpected Host header");
    }
    let now = now_s();
    let p = read_obj(&ctx.home, PENDING_FILE).get(&id).cloned();
    let Some(p) = p else { return json_response(json!({"state": "expired", "redirect": null})) };
    let uri = p["redirect_uri"].as_str().unwrap_or("");
    let state = p["state"].as_str().unwrap_or("");
    let out = match p["decision"].as_str() {
        Some("approved") if p["code_expires_at"].as_i64().unwrap_or(0) > now => json!({"state": "approved",
            "redirect": with_query(uri, &[("code", p["code"].as_str().unwrap_or("")), ("state", state)])}),
        Some("denied") => json!({"state": "denied",
            "redirect": with_query(uri, &[("error", "access_denied"), ("error_description", "the owner denied the sign-in"), ("state", state)])}),
        Some("pending") if p["expires_at"].as_i64().unwrap_or(0) > now => {
            let mut v = json!({"state": "pending", "redirect": null});
            if let Some(e) = p["card_error"].as_str() { v["error"] = json!(e); }
            v
        }
        _ => json!({"state": "expired", "redirect": null}),
    };
    json_response(out)
}

/// The owner's answer from Telegram. Returns the client name, or an error when
/// the request is unknown, expired or already answered.
pub fn oauth_decide(home: &Path, id: &str, approve: bool) -> Result<String> {
    let now = now_s();
    with_file(home, PENDING_FILE, |m| {
        prune_pending(m, now);
        let p = m.get_mut(id).ok_or_else(|| anyhow::anyhow!("this sign-in request expired"))?;
        if p["decision"] != "pending" {
            anyhow::bail!("this sign-in request was already answered");
        }
        if approve {
            p["decision"] = json!("approved");
            p["code"] = json!(random_hex(32));
            p["code_expires_at"] = json!(now + CODE_SECS);
        } else {
            p["decision"] = json!("denied");
        }
        Ok(p["client_name"].as_str().unwrap_or("MCP client").to_string())
    })?
}

fn issue_tokens(home: &Path, client_id: &str, client_name: &str, scope: &str) -> Result<Value> {
    let access = random_hex(32);
    let refresh = random_hex(32);
    let now = now_s();
    with_file(home, TOKENS_FILE, |m| {
        m.retain(|_, t| t["refresh_expires_at"].as_i64().unwrap_or(0) > now);
        m.insert(random_hex(8), json!({"client_id": client_id, "client_name": client_name, "scope": scope,
            "access_hash": token_hash(&access), "refresh_hash": token_hash(&refresh),
            "issued_at": now, "expires_at": now + ACCESS_SECS, "refresh_expires_at": now + REFRESH_SECS}));
    })?;
    Ok(json!({"access_token": access, "token_type": "Bearer", "expires_in": ACCESS_SECS,
              "refresh_token": refresh, "scope": scope}))
}

/// POST /token: authorization_code (PKCE S256) and refresh_token grants.
async fn token_post(State(ctx): State<Ctx>, h: HeaderMap, body: Bytes) -> Response {
    if !host_ok(&ctx.allowed_hosts, &h) {
        return refused(StatusCode::FORBIDDEN, "unexpected Host header");
    }
    let f = form_decode(&String::from_utf8_lossy(&body));
    let get = |k: &str| f.get(k).cloned().unwrap_or_default();
    let client_id = get("client_id");
    let clients = read_obj(&ctx.home, CLIENTS_FILE);
    let Some(client) = clients.get(&client_id) else {
        return oauth_error(StatusCode::UNAUTHORIZED, "invalid_client", "unknown client_id");
    };
    let name = client["client_name"].as_str().unwrap_or("MCP client").to_string();
    let now = now_s();
    match get("grant_type").as_str() {
        "authorization_code" => {
            let code = get("code");
            if code.is_empty() || get("code_verifier").is_empty() {
                return oauth_error(StatusCode::BAD_REQUEST, "invalid_request", "code and code_verifier are required");
            }
            // Single use: the code is taken out whatever happens next.
            let taken = with_file(&ctx.home, PENDING_FILE, |m| {
                prune_pending(m, now);
                let key = m.iter().find(|(_, p)| p["decision"] == "approved"
                    && p["code"].as_str().is_some_and(|c| ct_eq(c, &code))).map(|(k, _)| k.clone())?;
                m.remove(&key)
            });
            let p = match taken {
                Ok(Some(p)) => p,
                Ok(None) => return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant", "unknown, used or expired code"),
                Err(e) => return refused(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")),
            };
            if p["client_id"].as_str() != Some(client_id.as_str()) {
                return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant", "the code was issued to another client");
            }
            let asked = get("redirect_uri");
            let want = p["redirect_uri"].as_str().unwrap_or("");
            if (p["redirect_given"] == json!(true) || !asked.is_empty()) && asked != want {
                return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant", "redirect_uri does not match the authorization request");
            }
            let challenge = crate::oauth::pkce_challenge(&get("code_verifier"));
            if !ct_eq(&challenge, p["code_challenge"].as_str().unwrap_or("")) {
                return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant", "code_verifier does not match the code_challenge");
            }
            match issue_tokens(&ctx.home, &client_id, &name, p["scope"].as_str().unwrap_or(OAUTH_SCOPE)) {
                Ok(v) => json_response(v),
                Err(e) => refused(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")),
            }
        }
        "refresh_token" => {
            let rh = token_hash(&get("refresh_token"));
            // Rotation: the old pair is gone once the new one is issued.
            let old = with_file(&ctx.home, TOKENS_FILE, |m| {
                let key = m.iter().find(|(_, t)| t["refresh_hash"].as_str() == Some(rh.as_str())
                    && t["refresh_expires_at"].as_i64().unwrap_or(0) > now).map(|(k, _)| k.clone())?;
                m.remove(&key)
            });
            let t = match old {
                Ok(Some(t)) => t,
                Ok(None) => return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant", "unknown or expired refresh_token"),
                Err(e) => return refused(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")),
            };
            if t["client_id"].as_str() != Some(client_id.as_str()) {
                return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant", "the refresh_token belongs to another client");
            }
            match issue_tokens(&ctx.home, &client_id, &name, t["scope"].as_str().unwrap_or(OAUTH_SCOPE)) {
                Ok(v) => json_response(v),
                Err(e) => refused(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")),
            }
        }
        "" => oauth_error(StatusCode::BAD_REQUEST, "invalid_request", "grant_type is required"),
        _ => oauth_error(StatusCode::BAD_REQUEST, "unsupported_grant_type", "authorization_code or refresh_token"),
    }
}

/// Signed-in clients for the status card: one row per client with a live refresh token.
pub fn oauth_clients(home: &Path) -> Value {
    let now = now_s();
    let mut by: std::collections::BTreeMap<String, (String, i64)> = Default::default();
    for t in read_obj(home, TOKENS_FILE).values() {
        if t["refresh_expires_at"].as_i64().unwrap_or(0) <= now {
            continue;
        }
        let id = t["client_id"].as_str().unwrap_or("").to_string();
        let e = by.entry(id).or_insert((t["client_name"].as_str().unwrap_or("").to_string(), 0));
        e.1 = e.1.max(t["issued_at"].as_i64().unwrap_or(0));
    }
    Value::Array(by.into_iter().map(|(id, (name, at))| json!({"client_id": id, "client_name": name, "last_sign_in": at})).collect())
}

/// Sign a client out: its tokens are deleted (it must be approved again).
/// `None` signs every client out. Answers how many token pairs were removed.
pub fn oauth_revoke(home: &Path, client_id: Option<&str>) -> Result<usize> {
    with_file(home, TOKENS_FILE, |m| {
        let before = m.len();
        m.retain(|_, t| client_id.is_some_and(|c| t["client_id"].as_str() != Some(c)));
        before - m.len()
    })
}

// ------------------------------------------------------------------ tools

fn str_(d: &str) -> Value {
    json!({"type": "string", "description": d})
}

fn int_(d: &str) -> Value {
    json!({"type": "integer", "description": d})
}

fn schema(props: Value, required: &[&str]) -> Value {
    json!({"type": "object", "properties": props, "required": required})
}

/// The outward tool list. Deliberately small: read the team, talk to it.
pub fn tools() -> Value {
    json!([
        {"name": "sessions",
         "description": "Every Kurisu session: name, harness, status (and role, description, last activity). \
Names are what send and read take.",
         "inputSchema": schema(json!({}), &[])},
        {"name": "peers",
         "description": "Alias for sessions, for callers that think in peers: the same list of session names, harnesses and statuses.",
         "inputSchema": schema(json!({}), &[])},
        {"name": "send",
         "description": "Send a message to a session and return at once. It is recorded as the OWNER's own chat message \
(an outward call has no caller session), and the session runs a normal turn on it. Returns {seq}; it never waits for the reply, \
so read with that seq to collect it. No approvals, no lifecycle/harness/model changes.",
         "inputSchema": schema(json!({"to": str_("session name or UUID"),
                                       "text": str_("the message")}), &["to", "text"])},
        {"name": "read",
         "description": "Messages of a session newer than a seq (ascending). With wait_secs it polls up to that many seconds \
(capped at 60) for something new, so a caller can wait on the seq send returned; then it answers, possibly empty. \
Text per message is capped at 6000 characters.",
         "inputSchema": schema(json!({"session": str_("session name or UUID"),
                                       "after": int_("only messages newer than this seq (default 0: the whole start of history, so pass the seq send gave you)"),
                                       "limit": int_("max messages to return (default 50, max 200)"),
                                       "wait_secs": int_("poll up to this long for new messages (0 default, max 60)")}), &["session"])}
    ])
}

/// Numbers arrive as numbers or as strings from LLM clients; accept both.
fn num(args: &Value, key: &str) -> Option<i64> {
    args.get(key).and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.trim().parse().ok())))
}

/// Map an outward tool call to the one daemon request it makes.
///
/// Deliberately a whitelist: no create/stop/update/approval op can be reached from
/// here, and nothing carries a `caller`, so the daemon treats `send` as the user's.
pub fn to_request(tool: &str, args: &Value) -> Option<Value> {
    let s = |k: &str| args.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    match tool {
        "sessions" | "peers" => Some(json!({"op": "peers"})),
        "send" => Some(json!({"op": "send", "name": s("to"), "text": s("text"), "wait": false, "notify": false})),
        "read" => Some(json!({"op": "history_page", "name": s("session"), "after": num(args, "after").unwrap_or(0).max(0),
                              "limit": num(args, "limit").unwrap_or(50).clamp(1, 200), "tools": false,
                              "max_chars": READ_MAX_CHARS})),
        _ => None,
    }
}

/// The compact rows `sessions` / `peers` answer with.
fn session_rows(peers: &Value) -> Value {
    json!(peers.as_array().into_iter().flatten().map(|p| json!({
        "name": p["name"],
        "harness": p["harness"],
        "status": p["status"],
        "role": p["role"],
        "description": p["description"],
        "last_activity": p["last_activity"],
    })).collect::<Vec<_>>())
}

fn compact(page: &Value) -> (Value, Option<i64>, bool) {
    let rows: Vec<Value> = page["messages"].as_array().into_iter().flatten().map(|m| json!({
        "seq": m["seq"], "role": m["role"], "text": m["text"], "at": m["at"],
    })).collect();
    (json!(rows), page["last_seq"].as_i64(), page["has_more"].as_bool().unwrap_or(false))
}

async fn call_daemon(ctx: &Ctx, req: Value) -> Result<Value, String> {
    crate::server::call(&ctx.socket(), req).await.map_err(|e| format!("{e:#}"))
}

/// One tool call. `read` polls; everything else is a single daemon request.
async fn call_tool(ctx: &Ctx, name: &str, args: &Value) -> Result<Value, String> {
    if name == "read" {
        return read(ctx, args).await;
    }
    let req = to_request(name, args).ok_or_else(|| format!("unknown tool {name}"))?;
    let v = call_daemon(ctx, req).await?;
    Ok(match name {
        "send" => json!({"seq": v["seq"], "session": v["name"], "queued": v["queued"] == true}),
        "sessions" | "peers" => json!({"sessions": session_rows(&v)}),
        _ => v,
    })
}

async fn read(ctx: &Ctx, args: &Value) -> Result<Value, String> {
    let session = args.get("session").and_then(Value::as_str).filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "missing session".to_string())?;
    let after = num(args, "after").unwrap_or(0).max(0);
    let wait = num(args, "wait_secs").unwrap_or(0).clamp(0, MAX_WAIT_SECS as i64) as u64;
    let started = Instant::now();
    let deadline = started + Duration::from_secs(wait);
    loop {
        let mut req = to_request("read", args).unwrap_or(Value::Null);
        req["session"] = json!(session);
        req["after"] = json!(after);
        let page = call_daemon(ctx, req).await?;
        let (messages, last_seq, has_more) = compact(&page);
        let empty = messages.as_array().is_some_and(Vec::is_empty);
        if !empty || wait == 0 || Instant::now() >= deadline {
            return Ok(json!({
                "session": page["session"]["name"].as_str().unwrap_or(session),
                "after": after,
                "messages": messages,
                "last_seq": last_seq.or(if after > 0 { Some(after) } else { None }),
                "has_more": has_more,
                "waited_ms": started.elapsed().as_millis() as u64,
            }));
        }
        let left = deadline.saturating_duration_since(Instant::now());
        tokio::time::sleep(POLL.min(left)).await;
    }
}

// -------------------------------------------------------------------- JSON-RPC

fn json_response(v: Value) -> Response {
    let mut r = Json(v).into_response();
    let h = r.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn initialize_result(msg: &Value) -> Value {
    json!({
        "protocolVersion": msg.pointer("/params/protocolVersion").and_then(Value::as_str)
            .filter(|v| !v.is_empty()).unwrap_or("2025-06-18"),
        "capabilities": {"tools": {"listChanged": false}},
        "serverInfo": {"name": SERVER_NAME, "version": env!("CARGO_PKG_VERSION")},
        "instructions": "Kurisu's outward MCP endpoint: sessions, send, read, peers. \
An outward call has no caller session, so send is recorded as the OWNER's own chat message and the session answers it as the user. \
send returns a seq at once; read polls for the reply (wait_secs, max 60). \
No approvals, no session lifecycle, harness, model or MCP-server changes are reachable here."
    })
}

fn tool_result(v: &Value) -> Value {
    json!({
        "content": [{"type": "text", "text": serde_json::to_string_pretty(v).unwrap_or_default()}],
        "structuredContent": v,
        "isError": false,
    })
}

fn tool_failure(why: &str) -> Value {
    json!({"content": [{"type": "text", "text": why}], "isError": true})
}

async fn handle(ctx: &Ctx, msg: &Value) -> Option<Value> {
    let id = msg.get("id")?.clone(); // a notification (no id) needs no reply
    let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
    let reply = match method {
        "initialize" => json!({"jsonrpc": "2.0", "id": id, "result": initialize_result(msg)}),
        "tools/list" => json!({"jsonrpc": "2.0", "id": id, "result": {"tools": tools()}}),
        "ping" => json!({"jsonrpc": "2.0", "id": id, "result": {}}),
        "tools/call" => {
            let name = msg.pointer("/params/name").and_then(Value::as_str).unwrap_or("");
            let args = msg.pointer("/params/arguments").cloned().unwrap_or(json!({}));
            // The client's own timeout (120 s) is never reached: this one fires first.
            let result = match tokio::time::timeout(CALL_BUDGET, call_tool(ctx, name, &args)).await {
                Ok(Ok(v)) => tool_result(&v),
                Ok(Err(e)) => tool_failure(&e),
                Err(_) => tool_failure(&format!("tool call took longer than {} s and was stopped", CALL_BUDGET.as_secs())),
            };
            json!({"jsonrpc": "2.0", "id": id, "result": result})
        }
        _ => rpc_error(id, -32601, &format!("method not found: {method}")),
    };
    Some(reply)
}

async fn dispatch(ctx: Ctx, body: Bytes) -> Response {
    let msg: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json_response(rpc_error(Value::Null, -32700, &format!("parse error: {e}"))),
    };
    if let Value::Array(items) = msg {
        let mut out = Vec::new();
        for item in &items {
            if let Some(r) = handle(&ctx, item).await {
                out.push(r);
            }
        }
        return if out.is_empty() { StatusCode::ACCEPTED.into_response() } else { json_response(Value::Array(out)) };
    }
    match handle(&ctx, &msg).await {
        Some(r) => json_response(r),
        None => StatusCode::ACCEPTED.into_response(),
    }
}

async fn rpc_path(State(ctx): State<Ctx>, AxPath(token): AxPath<String>, h: HeaderMap, body: Bytes) -> Response {
    if let Err(r) = authorize(&ctx, &h, Some(&token)) {
        return r;
    }
    dispatch(ctx, body).await
}

async fn rpc_header(State(ctx): State<Ctx>, h: HeaderMap, body: Bytes) -> Response {
    if let Err(r) = authorize(&ctx, &h, None) {
        return r;
    }
    dispatch(ctx, body).await
}

/// No server-to-client SSE stream: POST answers JSON, GET says so plainly.
fn no_stream_body() -> Response {
    refused(StatusCode::METHOD_NOT_ALLOWED, "POST JSON-RPC here; this endpoint does not offer GET/SSE streaming")
}

async fn no_stream_path(State(ctx): State<Ctx>, AxPath(token): AxPath<String>, h: HeaderMap) -> Response {
    match authorize(&ctx, &h, Some(&token)) {
        Ok(()) => no_stream_body(),
        Err(r) => r,
    }
}

async fn no_stream_header(State(ctx): State<Ctx>, h: HeaderMap) -> Response {
    match authorize(&ctx, &h, None) {
        Ok(()) => no_stream_body(),
        Err(r) => r,
    }
}

fn allowed_hosts(bound: SocketAddr) -> Vec<String> {
    if bound.ip().is_loopback() {
        vec![format!("127.0.0.1:{}", bound.port()), format!("localhost:{}", bound.port()), format!("[::1]:{}", bound.port())]
    } else {
        vec![]
    }
}

pub fn router(home: PathBuf, token: String, bound: SocketAddr) -> Router {
    let ctx = Ctx { home, token: Arc::new(token), allowed_hosts: Arc::new(allowed_hosts(bound)),
                    base: Arc::new(format!("http://{bound}")) };
    Router::new()
        .route("/mcp", post(rpc_header).get(no_stream_header))
        .route("/mcp/", post(rpc_header))
        .route("/mcp/{token}", post(rpc_path).get(no_stream_path))
        .route("/.well-known/oauth-protected-resource", get(well_known_resource))
        .route("/.well-known/oauth-protected-resource/mcp", get(well_known_resource))
        .route("/.well-known/oauth-authorization-server", get(well_known_as))
        .route("/.well-known/oauth-authorization-server/mcp", get(well_known_as))
        .route("/register", post(register))
        .route("/authorize", get(authorize_get))
        .route("/authorize/poll/{id}", get(authorize_poll))
        .route("/token", post(token_post))
        .with_state(ctx)
}

// ------------------------------------------------------------------ listener

/// `--host` / `--port` with their defaults. The host must be an explicit IP
/// address: no hostname, and never the unspecified 0.0.0.0 / ::.
pub fn host_port(host: Option<&str>, port: Option<&str>) -> Result<(IpAddr, u16)> {
    let raw = host.map(str::trim).unwrap_or(DEFAULT_HOST);
    let ip: IpAddr = raw.trim_matches(|c| c == '[' || c == ']').parse()
        .with_context(|| format!("--host takes an explicit IP address (e.g. {DEFAULT_HOST} or 127.0.0.1), not {raw:?}"))?;
    if ip.is_unspecified() {
        anyhow::bail!("refusing to bind {ip}: that listens on every interface; pass the explicit address (e.g. {DEFAULT_HOST})");
    }
    let port = match port {
        Some(p) => p.trim().parse::<u16>().context("--port takes a number, e.g. --port 8793")?,
        None => DEFAULT_PORT,
    };
    Ok((ip, port))
}

pub fn url_for(addr: SocketAddr, token: &str) -> String {
    format!("http://{addr}/mcp/{token}")
}

/// `ku mcp url`: the full URL, creating the token if needed.
pub fn url(home: &Path, host: Option<&str>, port: Option<&str>) -> Result<String> {
    let (ip, port) = host_port(host, port)?;
    Ok(url_for(SocketAddr::new(ip, port), &ensure_token(home)?))
}

/// `ku mcp serve`: run the listener in the foreground until Ctrl-C / SIGTERM.
pub async fn serve(paths: crate::paths::Paths, host: IpAddr, port: u16) -> Result<()> {
    let token = ensure_token(&paths.home)?;
    let listener = tokio::net::TcpListener::bind((host, port)).await
        .with_context(|| format!("bind {}", SocketAddr::new(host, port)))?;
    let addr = listener.local_addr()?;
    println!("Kurisu MCP server (KURISU_HOME={})", paths.home.display());
    println!("  url:   http://{addr}/mcp (OAuth sign-in, approved in Telegram by the running daemon)");
    println!("  legacy: {}", url_for(addr, &token));
    println!("  token: {} (0600)", token_path(&paths.home).display());
    println!("  tools: sessions, send, read, peers (send acts as the owner's own message)");
    let app = router(paths.home.clone(), token, addr);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = async { match term.as_mut() { Some(t) => { t.recv().await; } None => std::future::pending().await } } => {}
            }
        })
        .await
        .context("mcp server listener")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_tmp() -> String {
        std::env::var("KURISU_TEST_TMPDIR").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned())
    }

    fn home() -> tempfile::TempDir {
        tempfile::Builder::new().prefix("mcp-out-").tempdir_in(test_tmp()).unwrap()
    }

    /// An obviously fake, low-entropy token (secret scanners flag random-looking ones).
    fn fake_token() -> String {
        "0".repeat(32)
    }

    fn ctx(home: &Path) -> Ctx {
        Ctx {
            home: home.to_path_buf(),
            token: Arc::new(fake_token()),
            allowed_hosts: Arc::new(vec!["127.0.0.1:8793".into(), "localhost:8793".into()]),
            base: Arc::new("http://127.0.0.1:8793".into()),
        }
    }

    fn headers() -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("host", "127.0.0.1:8793".parse().unwrap());
        h
    }

    #[test]
    fn token_is_generated_private_and_stable() {
        use std::os::unix::fs::PermissionsExt;
        let dir = home();
        let token = ensure_token(dir.path()).unwrap();
        assert_eq!(token.len(), 64, "two uuid simple forms");
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
        let path = token_path(dir.path());
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(ensure_token(dir.path()).unwrap(), token, "the same token is reused");
        // A hand-edited token is kept; a malformed one is replaced.
        std::fs::write(&path, "deadbeefdeadbeefdeadbeefdeadbeef\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(ensure_token(dir.path()).unwrap(), "deadbeefdeadbeefdeadbeefdeadbeef");
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600, "mode repaired");
        std::fs::write(&path, "short\n").unwrap();
        assert_ne!(ensure_token(dir.path()).unwrap(), "short");
    }

    #[test]
    fn token_check_accepts_url_or_bearer_and_refuses_the_rest() {
        let dir = home();
        let c = ctx(dir.path());
        let good = fake_token();
        let good = good.as_str();
        assert!(authorize(&c, &headers(), None).is_err(), "no token at all");
        assert!(authorize(&c, &headers(), Some("wrong-token-wrong-token")).is_err(), "wrong path token");
        assert!(authorize(&c, &headers(), Some(good)).is_ok(), "plain URL token");
        let mut h = headers();
        h.insert(header::AUTHORIZATION, "Bearer wrong-token-wrong-token".parse().unwrap());
        assert!(authorize(&c, &h, None).is_err(), "wrong bearer");
        h.insert(header::AUTHORIZATION, format!("Bearer {good}").parse().unwrap());
        assert!(authorize(&c, &h, None).is_ok(), "bearer token");
        // A wrong Host is refused even with the right token (DNS rebinding).
        h.insert("host", "evil.example:8793".parse().unwrap());
        assert!(authorize(&c, &h, None).is_err());
        assert!(authorize(&c, &h, Some(good)).is_err());
        // A host-less request on a loopback bind is allowed (some clients omit it).
        let bare = HeaderMap::new();
        assert!(authorize(&c, &bare, Some(good)).is_ok());
        assert!(ct_eq("abc", "abc") && !ct_eq("abc", "abd") && !ct_eq("abc", "ab"));
    }

    #[test]
    fn tools_are_the_minimal_outward_set_and_map_to_owner_requests() {
        let names: Vec<String> = tools().as_array().unwrap().iter()
            .map(|t| t["name"].as_str().unwrap().to_string()).collect();
        assert_eq!(names, vec!["sessions", "peers", "send", "read"]);
        for n in &names {
            assert!(to_request(n, &json!({})).is_some(), "{n}");
        }
        // send queues as the owner: wait:false, no notify, and no caller tag at all.
        let r = to_request("send", &json!({"to": "dev", "text": "hi"})).unwrap();
        assert_eq!(r, json!({"op": "send", "name": "dev", "text": "hi", "wait": false, "notify": false}));
        assert!(r.get("caller").is_none(), "an outward call has no caller: the owner sent it");
        // read pages forward from a seq, caps the limit and the text size.
        let r = to_request("read", &json!({"session": "dev", "after": "12", "limit": 9000})).unwrap();
        assert_eq!((r["op"].as_str(), r["limit"].as_i64(), r["after"].as_i64()), (Some("history_page"), Some(200), Some(12)));
        assert_eq!(r["tools"], json!(false));
        let r = to_request("read", &json!({"session": "dev"})).unwrap();
        assert_eq!((r["limit"].as_i64(), r["after"].as_i64()), (Some(50), Some(0)));
        // sessions and peers are the same read.
        assert_eq!(to_request("sessions", &json!({})).unwrap(), to_request("peers", &json!({})).unwrap());
        // Nothing that changes a session or answers an approval is reachable.
        for n in ["session_create", "session_update", "session_send", "session_status", "approve", "deny", "stop", "mcp_set",
                  "set_my_status", "wake_at", "send_file", "ask_user", "rm_rf", ""] {
            assert!(to_request(n, &json!({})).is_none(), "{n} must not be reachable outward");
        }
        // The owner semantics are documented where a client reads them.
        let send = tools().as_array().unwrap().iter().find(|t| t["name"] == "send").unwrap().clone();
        assert!(send["description"].as_str().unwrap().contains("OWNER"));
    }

    #[test]
    fn host_must_be_an_explicit_address() {
        let (ip, port) = host_port(None, None).unwrap();
        assert_eq!((ip.to_string().as_str(), port), (DEFAULT_HOST, DEFAULT_PORT));
        assert_eq!(host_port(Some("127.0.0.1"), Some("18793")).unwrap().1, 18793);
        assert!(host_port(Some("[::1]"), None).is_ok());
        for bad in ["0.0.0.0", "::", "[::]", "localhost", "example.com", ""] {
            assert!(host_port(Some(bad), None).is_err(), "{bad} must be refused");
        }
        assert!(host_port(None, Some("nope")).is_err());
        let dir = home();
        let u = url(dir.path(), Some("127.0.0.1"), Some("18793")).unwrap();
        assert_eq!(u, format!("http://127.0.0.1:18793/mcp/{}", ensure_token(dir.path()).unwrap()));
    }

    #[test]
    fn wait_is_capped_and_session_rows_are_compact() {
        assert_eq!(num(&json!({"wait_secs": "900"}), "wait_secs").unwrap().clamp(0, MAX_WAIT_SECS as i64), 60);
        let peers = json!([{"id": "s1", "name": "dev", "harness": "claude", "status": "working", "role": "worker",
                            "description": "d", "last_activity": 5, "cwd": "/x", "children": []}]);
        let rows = session_rows(&peers);
        assert_eq!(rows[0]["name"], "dev");
        assert_eq!((rows[0]["harness"].as_str(), rows[0]["status"].as_str()), (Some("claude"), Some("working")));
        assert!(rows[0].get("cwd").is_none(), "only the fields a client needs");
        assert_eq!(session_rows(&json!(null)), json!([]));
    }

    #[test]
    fn config_round_trips_and_the_switch_turns_off() {
        use std::os::unix::fs::PermissionsExt;
        let dir = home();
        // Missing file: nothing is enabled and the defaults are the Tailscale address.
        assert!(load_config(dir.path()).is_none());
        assert!(!Config::default().enabled);
        assert_eq!((Config::default().host.as_str(), Config::default().port), (DEFAULT_HOST, DEFAULT_PORT));
        assert!(!disable(dir.path()).unwrap(), "nothing was on");
        assert_eq!(socket_addr(&Config::default()).unwrap(), SocketAddr::new(DEFAULT_HOST.parse().unwrap(), DEFAULT_PORT));
        let cfg = Config { enabled: true, host: "127.0.0.1".into(), port: 18793 };
        save_config(dir.path(), &cfg).unwrap();
        assert_eq!(std::fs::metadata(config_path(dir.path())).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(load_config(dir.path()).unwrap(), cfg);
        assert_eq!(socket_addr(&cfg).unwrap().to_string(), "127.0.0.1:18793");
        assert!(disable(dir.path()).unwrap(), "it was on");
        assert_eq!(load_config(dir.path()).unwrap(), Config { enabled: false, ..cfg.clone() }, "host and port are kept");
        assert!(!disable(dir.path()).unwrap(), "turning it off twice changes nothing");
        // A host-edited file must not smuggle in 0.0.0.0 through the config.
        save_config(dir.path(), &Config { enabled: true, host: "0.0.0.0".into(), port: 8793 }).unwrap();
        assert!(socket_addr(&load_config(dir.path()).unwrap()).is_err());
    }

    #[test]
    fn status_text_is_plain_masked_and_says_state_and_address() {
        let token = fake_token();
        let addr: SocketAddr = "100.118.224.35:8793".parse().unwrap();
        let running = json!({
            "enabled": true, "listening": true, "listen": addr.to_string(),
            "token_path": "/srv/kurisu/mcp-server-token", "token_mode": "0600",
            "pid": 4242, "uptime_seconds": 3725,
            "url": masked_url(addr, Some(&token)), "note": "",
        });
        let text = status_text(&running);
        assert!(text.contains("state: running"), "{text}");
        assert!(text.contains("100.118.224.35:8793"), "{text}");
        assert!(text.contains("legacy secret file: /srv/kurisu/mcp-server-token (0600)"), "{text}");
        assert!(text.contains("pid: 4242"), "{text}");
        assert!(text.contains("uptime: 1h 02m"), "{text}");
        // Masked: the first 8 characters, then an ellipsis. The token itself never appears.
        assert!(text.contains(&format!("/mcp/{}…", &token[..8])), "{text}");
        assert!(!text.contains(&token), "the status line must never carry the full token");
        assert!(!text.contains("**") && !text.contains('`'), "plain text: Telegram shows markdown raw");
        // Stopped: no pid/uptime, and the reason is shown.
        let stopped = json!({
            "enabled": false, "listening": false, "listen": "100.118.224.35:8793",
            "token_path": "/srv/kurisu/mcp-server-token", "token_mode": Value::Null,
            "url": "http://100.118.224.35:8793/mcp/<token created on enable>", "note": "port in use by pid 2407275 (ku)",
        });
        let text = status_text(&stopped);
        assert!(text.contains("state: stopped"), "{text}");
        assert!(!text.contains("pid:") && !text.contains("uptime:"), "{text}");
        assert!(text.contains("(not created yet)"), "{text}");
        assert!(text.contains("note: port in use by pid 2407275 (ku)"), "{text}");
    }

    #[test]
    fn keyboard_shows_exactly_one_toggle_for_the_state() {
        fn data(st: Value) -> Vec<String> {
            let kb = keyboard(&st);
            let rows = kb["inline_keyboard"].as_array().unwrap();
            rows.iter().flat_map(|r| r.as_array().unwrap().iter())
                .inspect(|b| assert!(b["text"].as_str().unwrap().len() <= 64))
                .map(|b| b["callback_data"].as_str().unwrap().to_string()).collect()
        }
        assert_eq!(data(json!({"listening": false})), vec!["msv:on", "msv:url"]);
        assert_eq!(data(json!({})), vec!["msv:on", "msv:url"], "unknown state offers Enable");
        assert_eq!(data(json!({"listening": true})), vec!["msv:off", "msv:url"]);
    }

    #[test]
    fn mask_uptime_and_url_helpers() {
        assert_eq!(mask("0123456789abcdef"), "01234567…");
        assert_eq!(mask("short"), "short…");
        let addr: SocketAddr = "127.0.0.1:8793".parse().unwrap();
        assert_eq!(masked_url(addr, Some("0123456789abcdef")), "http://127.0.0.1:8793/mcp/01234567…");
        assert!(masked_url(addr, None).contains("created on enable"));
        assert_eq!(human_uptime(0), "0s");
        assert_eq!(human_uptime(75), "1m 15s");
        assert_eq!(human_uptime(3725), "1h 02m");
        assert_eq!(human_uptime(90061), "1d 1h");
    }

    /// A stand-in daemon: answers each unix-socket line with one `{"ok": ...}` line.
    fn fake_daemon(socket: PathBuf) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            while let Ok((stream, _)) = listener.accept().await {
                let (r, mut w) = stream.into_split();
                let mut lines = BufReader::new(r).lines();
                let Ok(Some(line)) = lines.next_line().await else { continue };
                let req: Value = serde_json::from_str(&line).unwrap_or_default();
                let ok = match req["op"].as_str() {
                    Some("peers") => json!([
                        {"id": "s1", "name": "tester", "harness": "claude", "status": "idle", "role": "worker",
                         "description": "the test session", "last_activity": 7, "lifecycle": "active"},
                        {"id": "s2", "name": "other", "harness": "codex", "status": "stopped", "role": "worker",
                         "description": "", "last_activity": 3, "lifecycle": "active"}
                    ]),
                    Some("send") => json!({"id": "s1", "name": "tester", "seq": 41, "queued": false}),
                    Some("history_page") => json!({"session": {"name": req["name"]}, "messages": [], "last_seq": null}),
                    _ => json!({}),
                };
                let _ = w.write_all(format!("{}\n", json!({"ok": ok})).as_bytes()).await;
            }
        })
    }

    /// The acceptance shape, in-tree: a real HTTP client drives initialize ->
    /// tools/list -> tools/call over an ephemeral port against a throwaway home.
    #[tokio::test]
    async fn initialize_list_and_sessions_over_http_on_an_ephemeral_port() {
        let dir = home();
        let paths = crate::paths::Paths { home: dir.path().to_path_buf() };
        let daemon = fake_daemon(paths.socket());
        let token = fake_token();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(dir.path().to_path_buf(), token.clone(), addr);
        let served = tokio::spawn(async move { let _ = axum::serve(listener, app).await; });
        let client = reqwest::Client::new();
        let url = format!("http://{addr}/mcp/{token}");
        let rpc = |id: i64, method: &str, params: Value| json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});

        let init: Value = client.post(&url)
            .json(&rpc(1, "initialize", json!({"protocolVersion": "2025-06-18", "capabilities": {},
                                               "clientInfo": {"name": "test", "version": "1"}})))
            .send().await.unwrap().json().await.unwrap();
        assert_eq!(init["result"]["serverInfo"]["name"], SERVER_NAME);
        assert_eq!(init["result"]["protocolVersion"], "2025-06-18");
        assert!(init["result"]["capabilities"]["tools"].is_object());

        let list: Value = client.post(&url).json(&rpc(2, "tools/list", json!({})))
            .send().await.unwrap().json().await.unwrap();
        let names: Vec<&str> = list["result"]["tools"].as_array().unwrap().iter()
            .map(|t| t["name"].as_str().unwrap()).collect();
        assert_eq!(names, vec!["sessions", "peers", "send", "read"]);

        let call: Value = client.post(&url)
            .json(&rpc(3, "tools/call", json!({"name": "sessions", "arguments": {}})))
            .send().await.unwrap().json().await.unwrap();
        assert_eq!(call["result"]["isError"], false);
        let sessions = &call["result"]["structuredContent"]["sessions"];
        assert_eq!(sessions[0]["name"], "tester");
        assert_eq!(sessions[0]["harness"], "claude");
        assert_eq!(sessions[1]["status"], "stopped");

        // send answers a seq at once instead of waiting for the turn.
        let sent: Value = client.post(&url)
            .json(&rpc(4, "tools/call", json!({"name": "send", "arguments": {"to": "tester", "text": "hello"}})))
            .send().await.unwrap().json().await.unwrap();
        assert_eq!(sent["result"]["structuredContent"]["seq"], 41);
        assert_eq!(sent["result"]["structuredContent"]["session"], "tester");

        // A notification gets 202 with no body; a bad token is 401; GET has no SSE.
        let resp = client.post(&url).json(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
            .send().await.unwrap();
        assert_eq!(resp.status(), 202);
        let resp = client.post(format!("http://{addr}/mcp/wrong-token-wrong-token-wrong"))
            .json(&rpc(5, "tools/list", json!({}))).send().await.unwrap();
        assert_eq!(resp.status(), 401);
        let resp = client.post(format!("http://{addr}/mcp"))
            .header("authorization", format!("Bearer {token}"))
            .json(&rpc(6, "ping", json!({}))).send().await.unwrap();
        assert_eq!(resp.status(), 200, "bearer header works without a URL token");
        let resp = client.get(&url).send().await.unwrap();
        assert_eq!(resp.status(), 405);

        served.abort();
        daemon.abort();
    }

    // ------------------------------------------------------------ OAuth

    struct Srv {
        addr: SocketAddr,
        home: tempfile::TempDir,
        served: tokio::task::JoinHandle<()>,
        daemon: tokio::task::JoinHandle<()>,
        client: reqwest::Client,
    }

    impl Drop for Srv {
        fn drop(&mut self) {
            self.served.abort();
            self.daemon.abort();
        }
    }

    async fn srv() -> Srv {
        let home = home();
        let paths = crate::paths::Paths { home: home.path().to_path_buf() };
        let daemon = fake_daemon(paths.socket());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(home.path().to_path_buf(), fake_token(), addr);
        let served = tokio::spawn(async move { let _ = axum::serve(listener, app).await; });
        let client = reqwest::Client::builder().no_proxy().redirect(reqwest::redirect::Policy::none()).build().unwrap();
        Srv { addr, home, served, daemon, client }
    }

    const VERIFIER: &str = "a-test-verifier-that-is-long-enough-for-pkce-0123456789";
    const REDIRECT: &str = "http://127.0.0.1:33418/callback";

    impl Srv {
        fn url(&self, p: &str) -> String { format!("http://{}{p}", self.addr) }

        async fn register(&self) -> String {
            let r = self.client.post(self.url("/register"))
                .json(&json!({"client_name": "Test Client", "redirect_uris": [REDIRECT]})).send().await.unwrap();
            assert_eq!(r.status(), 201);
            let v: Value = r.json().await.unwrap();
            assert_eq!(v["token_endpoint_auth_method"], "none");
            assert!(v.get("client_secret").is_none());
            v["client_id"].as_str().unwrap().to_string()
        }

        /// GET /authorize; returns (status, pending id scraped from the page).
        async fn authorize(&self, client_id: &str, redirect: &str) -> (u16, Option<String>) {
            let q = crate::oauth::form_encode(&[("response_type", "code"), ("client_id", client_id), ("redirect_uri", redirect),
                ("state", "st8"), ("code_challenge", &crate::oauth::pkce_challenge(VERIFIER)), ("code_challenge_method", "S256"),
                ("scope", "mcp"), ("resource", &self.url("/mcp"))]);
            let r = self.client.get(self.url(&format!("/authorize?{q}"))).send().await.unwrap();
            let status = r.status().as_u16();
            let body = r.text().await.unwrap();
            let id = body.split("const id=\"").nth(1).map(|t| t.split('"').next().unwrap().to_string());
            (status, id)
        }

        async fn poll(&self, id: &str) -> Value {
            self.client.get(self.url(&format!("/authorize/poll/{id}"))).send().await.unwrap().json().await.unwrap()
        }

        async fn token(&self, form: &[(&str, &str)]) -> (u16, Value) {
            let r = self.client.post(self.url("/token")).header("content-type", "application/x-www-form-urlencoded")
                .body(crate::oauth::form_encode(form)).send().await.unwrap();
            (r.status().as_u16(), r.json().await.unwrap())
        }

        /// register -> authorize -> approve -> poll; returns (client_id, code).
        async fn approved_code(&self) -> (String, String) {
            let cid = self.register().await;
            let (st, id) = self.authorize(&cid, REDIRECT).await;
            assert_eq!(st, 200);
            let id = id.expect("pending id in the page");
            assert_eq!(self.poll(&id).await["state"], "pending");
            assert_eq!(oauth_decide(self.home.path(), &id, true).unwrap(), "Test Client");
            assert!(oauth_decide(self.home.path(), &id, true).is_err(), "answered once only");
            let p = self.poll(&id).await;
            assert_eq!(p["state"], "approved");
            let redirect = p["redirect"].as_str().unwrap();
            assert!(redirect.starts_with(&format!("{REDIRECT}?code=")), "{redirect}");
            assert!(redirect.ends_with("&state=st8"), "{redirect}");
            let code = redirect.split("code=").nth(1).unwrap().split('&').next().unwrap().to_string();
            (cid, code)
        }

        async fn list_tools(&self, bearer: &str) -> reqwest::Response {
            self.client.post(self.url("/mcp")).header("authorization", format!("Bearer {bearer}"))
                .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})).send().await.unwrap()
        }
    }

    #[tokio::test]
    async fn oauth_discovery_is_public_and_well_formed() {
        let s = srv().await;
        let base = s.url("");
        let r = s.client.post(s.url("/mcp")).json(&json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})).send().await.unwrap();
        assert_eq!(r.status(), 401);
        let www = r.headers()["www-authenticate"].to_str().unwrap().to_string();
        assert_eq!(www, format!("Bearer resource_metadata=\"{base}/.well-known/oauth-protected-resource\""));
        assert_eq!(s.client.get(s.url("/mcp")).send().await.unwrap().status(), 401);
        for p in ["/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/mcp"] {
            let v: Value = s.client.get(s.url(p)).send().await.unwrap().json().await.unwrap();
            assert_eq!(v["resource"], format!("{base}/mcp"), "{p}");
            assert_eq!(v["authorization_servers"], json!([base]));
            assert_eq!(v["scopes_supported"], json!(["mcp"]));
            assert_eq!(v["bearer_methods_supported"], json!(["header"]));
        }
        for p in ["/.well-known/oauth-authorization-server", "/.well-known/oauth-authorization-server/mcp"] {
            let v: Value = s.client.get(s.url(p)).send().await.unwrap().json().await.unwrap();
            assert_eq!(v["issuer"], base, "{p}");
            assert_eq!(v["authorization_endpoint"], format!("{base}/authorize"));
            assert_eq!(v["token_endpoint"], format!("{base}/token"));
            assert_eq!(v["registration_endpoint"], format!("{base}/register"));
            assert_eq!(v["response_types_supported"], json!(["code"]));
            assert_eq!(v["grant_types_supported"], json!(["authorization_code", "refresh_token"]));
            assert_eq!(v["code_challenge_methods_supported"], json!(["S256"]));
            assert_eq!(v["token_endpoint_auth_methods_supported"], json!(["none"]));
        }
    }

    #[tokio::test]
    async fn oauth_happy_path_issues_a_token_that_lists_tools_and_refreshes() {
        use std::os::unix::fs::PermissionsExt;
        let s = srv().await;
        let (cid, code) = s.approved_code().await;
        let (st, tok) = s.token(&[("grant_type", "authorization_code"), ("code", &code), ("redirect_uri", REDIRECT),
                                  ("client_id", &cid), ("code_verifier", VERIFIER)]).await;
        assert_eq!(st, 200, "{tok}");
        assert_eq!(tok["token_type"], "Bearer");
        assert_eq!(tok["scope"], "mcp");
        assert!(tok["expires_in"].as_i64().unwrap() > 0);
        let access = tok["access_token"].as_str().unwrap().to_string();
        assert_eq!(access.len(), 64);
        let r = s.list_tools(&access).await;
        assert_eq!(r.status(), 200);
        let v: Value = r.json().await.unwrap();
        let names: Vec<&str> = v["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert_eq!(names, vec!["sessions", "peers", "send", "read"]);
        for f in [CLIENTS_FILE, TOKENS_FILE] {
            let mode = std::fs::metadata(s.home.path().join(f)).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "{f}");
        }
        let stored = std::fs::read_to_string(s.home.path().join(TOKENS_FILE)).unwrap();
        assert!(!stored.contains(&access), "tokens are stored hashed");
        // Status card lists the client; refresh rotates; revoke signs it out.
        assert_eq!(oauth_clients(s.home.path())[0]["client_name"], "Test Client");
        let (st, re) = s.token(&[("grant_type", "refresh_token"), ("refresh_token", tok["refresh_token"].as_str().unwrap()), ("client_id", &cid)]).await;
        assert_eq!(st, 200, "{re}");
        assert_eq!(s.list_tools(&access).await.status(), 401, "the old access token is rotated out");
        let fresh = re["access_token"].as_str().unwrap().to_string();
        assert_eq!(s.list_tools(&fresh).await.status(), 200);
        let (st, _) = s.token(&[("grant_type", "refresh_token"), ("refresh_token", tok["refresh_token"].as_str().unwrap()), ("client_id", &cid)]).await;
        assert_eq!(st, 400, "a refresh token is single-use");
        assert_eq!(oauth_revoke(s.home.path(), Some(&cid)).unwrap(), 1);
        assert_eq!(s.list_tools(&fresh).await.status(), 401);
        assert_eq!(oauth_clients(s.home.path()), json!([]));
    }

    #[tokio::test]
    async fn oauth_refuses_bad_verifier_reused_code_unknown_client_and_wrong_redirect() {
        let s = srv().await;
        // Wrong verifier: invalid_grant, and the code is spent.
        let (cid, code) = s.approved_code().await;
        let base = |c: &str| vec![("grant_type", "authorization_code".to_string()), ("code", c.to_string()),
                                  ("redirect_uri", REDIRECT.to_string()), ("client_id", cid.clone())];
        let mut f = base(&code);
        f.push(("code_verifier", "wrong-verifier-wrong-verifier-wrong-verifier-000".into()));
        let ff: Vec<(&str, &str)> = f.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let (st, v) = s.token(&ff).await;
        assert_eq!((st, v["error"].as_str()), (400, Some("invalid_grant")));
        // Reused code: the first exchange works, the second is invalid_grant.
        let (cid2, code2) = s.approved_code().await;
        let good = [("grant_type", "authorization_code"), ("code", code2.as_str()), ("redirect_uri", REDIRECT),
                    ("client_id", cid2.as_str()), ("code_verifier", VERIFIER)];
        assert_eq!(s.token(&good).await.0, 200);
        let (st, v) = s.token(&good).await;
        assert_eq!((st, v["error"].as_str()), (400, Some("invalid_grant")));
        // Unknown client at /token: invalid_client 401; at /authorize: an error page, no redirect.
        let (st, v) = s.token(&[("grant_type", "authorization_code"), ("code", "x"), ("client_id", "nope"), ("code_verifier", VERIFIER)]).await;
        assert_eq!((st, v["error"].as_str()), (401, Some("invalid_client")));
        assert_eq!(s.authorize("nope", REDIRECT).await, (400, None));
        // Mismatched redirect: unregistered host is refused at /authorize; a loopback
        // port change is allowed (RFC 8252), but /token must then repeat it exactly.
        let cid3 = s.register().await;
        assert_eq!(s.authorize(&cid3, "http://evil.example/callback").await.0, 400);
        assert_eq!(s.authorize(&cid3, "http://127.0.0.1:33418/other").await.0, 400);
        let (st, id) = s.authorize(&cid3, "http://127.0.0.1:40000/callback").await;
        assert_eq!(st, 200);
        let id = id.unwrap();
        oauth_decide(s.home.path(), &id, true).unwrap();
        let p = s.poll(&id).await;
        let code3 = p["redirect"].as_str().unwrap().split("code=").nth(1).unwrap().split('&').next().unwrap().to_string();
        let (st, v) = s.token(&[("grant_type", "authorization_code"), ("code", &code3), ("redirect_uri", REDIRECT),
                                ("client_id", &cid3), ("code_verifier", VERIFIER)]).await;
        assert_eq!((st, v["error"].as_str()), (400, Some("invalid_grant")));
        // Registration refuses plain http off loopback.
        let r = s.client.post(s.url("/register")).json(&json!({"redirect_uris": ["http://evil.example/cb"]})).send().await.unwrap();
        assert_eq!(r.status(), 400);
    }

    #[tokio::test]
    async fn oauth_denied_expired_token_and_legacy_path_token() {
        let s = srv().await;
        // Denied: the page is sent back to the client with access_denied.
        let cid = s.register().await;
        let (_, id) = s.authorize(&cid, REDIRECT).await;
        let id = id.unwrap();
        oauth_decide(s.home.path(), &id, false).unwrap();
        let p = s.poll(&id).await;
        assert_eq!(p["state"], "denied");
        assert_eq!(p["redirect"], format!("{REDIRECT}?error=access_denied&error_description=the+owner+denied+the+sign-in&state=st8"));
        assert_eq!(s.poll("unknown-id").await["state"], "expired");
        // Expired access token: 401 with the same WWW-Authenticate.
        let (cid, code) = s.approved_code().await;
        let (_, tok) = s.token(&[("grant_type", "authorization_code"), ("code", &code), ("redirect_uri", REDIRECT),
                                 ("client_id", &cid), ("code_verifier", VERIFIER)]).await;
        let access = tok["access_token"].as_str().unwrap();
        assert_eq!(s.list_tools(access).await.status(), 200);
        with_file(s.home.path(), TOKENS_FILE, |m| for t in m.values_mut() { t["expires_at"] = json!(now_s() - 1); }).unwrap();
        let r = s.list_tools(access).await;
        assert_eq!(r.status(), 401);
        assert!(r.headers()["www-authenticate"].to_str().unwrap().contains("resource_metadata="));
        assert_eq!(s.list_tools("f".repeat(64).as_str()).await.status(), 401, "unknown token");
        // Back-compat: the static secret still works in the path and as a bearer.
        let r = s.client.post(s.url(&format!("/mcp/{}", fake_token())))
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})).send().await.unwrap();
        assert_eq!(r.status(), 200);
        assert_eq!(s.list_tools(&fake_token()).await.status(), 200);
    }

    #[test]
    fn oauth_helpers_parse_forms_and_urls() {
        let f = form_decode("a=1&b=x%20y+z&c=%2Fp%3F&d");
        assert_eq!(f["a"], "1");
        assert_eq!(f["b"], "x y z");
        assert_eq!(f["c"], "/p?");
        assert_eq!(f["d"], "");
        assert!(redirect_allowed("http://localhost:1234/cb"));
        assert!(redirect_allowed("http://[::1]:1/cb"));
        assert!(redirect_allowed("https://example.com/cb"));
        assert!(!redirect_allowed("http://example.com/cb"));
        assert!(!redirect_allowed("javascript:alert(1)"));
        assert!(!redirect_allowed("http://127.0.0.1@evil.com/"));
        assert_eq!(redirect_host("http://127.0.0.1:33418/callback"), "127.0.0.1:33418");
        assert_eq!(with_query("http://x/cb?a=1", &[("code", "c"), ("state", "")]), "http://x/cb?a=1&code=c");
    }

    #[test]
    fn status_card_offers_the_plain_url_and_sign_out_buttons() {
        let st = json!({"listening": true, "listen": "127.0.0.1:8793", "sign_in_url": "http://127.0.0.1:8793/mcp",
                        "url": "http://127.0.0.1:8793/mcp/00000000…",
                        "oauth_clients": [{"client_id": "abc123", "client_name": "Claude Code"}]});
        let text = status_text(&st);
        assert!(text.contains("url: http://127.0.0.1:8793/mcp\n"), "{text}");
        assert!(text.contains("sign-in"), "{text}");
        assert!(text.contains("signed-in clients: Claude Code"), "{text}");
        let kb = keyboard(&st);
        assert_eq!(kb["inline_keyboard"][1][0]["callback_data"], "msv:rv:abc123");
    }
}
