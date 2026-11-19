//! Remote access from the phone: `/terminal` (a PTY shell in the browser), `/browser`
//! (the live browser) and `/web` (a chat UI, see `chat.rs`). Off until
//! `ku terminal enable` / `ku web-link enable` (or the Telegram enable card).
//!
//! A link alone grants nothing:
//!   1. `/terminal` in Telegram returns a link with a random token (5 min, at most 3 attempts).
//!   2. Opening it starts an *attempt*: the page shows a 6-character code and waits; the
//!      Telegram chat gets an approval card with the requester's IP, user agent and code.
//!   3. Approve unlocks that browser (an HttpOnly cookie) and starts the shell; the link is
//!      then used up. Deny, or no answer in 2 min, kills the link. A refresh is a new attempt.
//! Idle sessions close after 30 min; `/terminal close` closes everything. Every step is
//! audited. Links and tokens are never logged.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Path as AxPath, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::server::Server;

pub const LINK_TTL: Duration = Duration::from_secs(5 * 60);
pub const ATTEMPT_TTL: Duration = Duration::from_secs(2 * 60);
pub const MAX_ATTEMPTS: u32 = 3;
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// `/web` listens here by default (terminal 8790, browser 8792/8793).
pub const WEB_PORT: u16 = 8791;
/// A home without a real Telegram channel (a test/staging instance) uses this
/// instead, so it can never take the live `/web` endpoint by binding first.
pub const WEB_TEST_PORT: u16 = 8797;

/// A real Telegram-connected home. `KURISU_TELEGRAM_API` points the bridge at a
/// mock, which is how the staging/QA instance is run, so it counts as a test.
pub fn is_test_home(home: &Path) -> bool {
    std::env::var_os("KURISU_TELEGRAM_API").is_some() || !home.join("telegram.json").is_file()
}

pub fn default_web_port(home: &Path) -> u16 {
    if is_test_home(home) { WEB_TEST_PORT } else { WEB_PORT }
}
/// Cookie per kind (cookies ignore the port, and both listeners may share a host).
fn cookie_name(kind: &str) -> &'static str {
    match kind {
        "browser" => "ku_browser",
        "web" => "ku_web",
        _ => "ku_terminal",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub enabled: bool,
    /// Address to listen on: loopback, a LAN or Tailscale address (never 0.0.0.0 unless forced).
    pub listen: String,
    /// The URL in links (e.g. http://my-box.tailnet.ts.net:8790); default http://<listen>.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_url: Option<String>,
}

pub fn config_path(home: &Path) -> PathBuf {
    home.join("terminal.json")
}

pub fn load(home: &Path) -> Option<Config> {
    serde_json::from_slice(&std::fs::read(config_path(home)).ok()?).ok()
}

/// Loopback, private LAN (RFC 1918 / ULA) or Tailscale (100.64/10, fd7a:115c:a1e0::/48).
pub fn address_allowed(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => v.is_loopback() || v.is_private() || (v.octets()[0] == 100 && (v.octets()[1] & 0xC0) == 64),
        IpAddr::V6(v) => v.is_loopback() || (v.segments()[0] & 0xfe00) == 0xfc00,
    }
}

pub fn check_listen(listen: &str, force: bool) -> Result<SocketAddr> {
    let addr: SocketAddr = listen.parse().with_context(|| format!("listen address {listen:?} must be IP:PORT"))?;
    if addr.ip().is_unspecified() && !force {
        bail!("{listen} listens on every interface; use 127.0.0.1, a LAN or a Tailscale address (or --force)");
    }
    if !addr.ip().is_unspecified() && !address_allowed(addr.ip()) && !force {
        bail!("{} is a public address; use 127.0.0.1, a LAN or a Tailscale address (or --force)", addr.ip());
    }
    Ok(addr)
}

/// Where a phone can reach this machine: the Tailscale address (with its MagicDNS
/// name when enabled), else a LAN address. Never loopback or 0.0.0.0.
#[derive(Debug, Clone, PartialEq)]
pub struct Detected {
    pub ip: IpAddr,
    pub host: Option<String>,
    pub via: &'static str,
}

impl Detected {
    pub fn url(&self, port: u16) -> String {
        match &self.host {
            Some(h) => format!("http://{h}:{port}"),
            None => match self.ip {
                IpAddr::V6(v) => format!("http://[{v}]:{port}"),
                ip => format!("http://{ip}:{port}"),
            },
        }
    }

    pub fn listen(&self, port: u16) -> String {
        SocketAddr::new(self.ip, port).to_string()
    }

    pub fn describe(&self, port: u16) -> String {
        match &self.host {
            Some(h) => format!("{h}:{port} ({}, {})", self.ip, self.via),
            None => format!("{}:{port} ({})", self.ip, self.via),
        }
    }
}

pub const NO_ADDRESS: &str = "No Tailscale or LAN address found; install Tailscale on this machine and your phone.";

fn tailscale_ip(ip: IpAddr) -> bool {
    matches!(ip, IpAddr::V4(v) if v.octets()[0] == 100 && (v.octets()[1] & 0xC0) == 64)
}

/// `tailscale status --json` from the CLI, directly or (in a distrobox) on the host.
fn tailscale_status() -> Option<Value> {
    let cands = ["tailscale", "/opt/tailscale/tailscale", "/usr/bin/tailscale", "/usr/local/bin/tailscale",
                 "/Applications/Tailscale.app/Contents/MacOS/Tailscale"];
    let run = |argv: &[&str]| -> Option<Value> {
        let out = std::process::Command::new(argv[0]).args(&argv[1..]).stderr(std::process::Stdio::null()).output().ok()?;
        out.status.success().then(|| serde_json::from_slice::<Value>(&out.stdout).ok()).flatten()
    };
    for c in cands {
        if let Some(v) = run(&[c, "status", "--json"]) {
            return Some(v);
        }
    }
    if crate::stt::which_bin("distrobox-host-exec").is_some() {
        for c in cands {
            if let Some(v) = run(&["distrobox-host-exec", c, "status", "--json"]) {
                return Some(v);
            }
        }
    }
    None
}

/// IPv4 addresses per interface (`ip -o -4 addr`, else `ifconfig`).
fn interface_addrs() -> Vec<(String, IpAddr)> {
    let mut out = vec![];
    if let Ok(o) = std::process::Command::new("ip").args(["-o", "-4", "addr", "show"]).output() {
        for line in String::from_utf8_lossy(&o.stdout).lines() {
            let f: Vec<&str> = line.split_whitespace().collect();
            if let (Some(name), Some(i)) = (f.get(1), f.iter().position(|w| *w == "inet")) {
                if let Some(ip) = f.get(i + 1).and_then(|a| a.split('/').next()).and_then(|a| a.parse().ok()) {
                    out.push((name.to_string(), ip));
                }
            }
        }
    }
    if out.is_empty() {
        if let Ok(o) = std::process::Command::new("ifconfig").output() {
            let mut name = String::new();
            for line in String::from_utf8_lossy(&o.stdout).lines() {
                if !line.starts_with(char::is_whitespace) {
                    name = line.split(':').next().unwrap_or("").to_string();
                } else if let Some(rest) = line.trim().strip_prefix("inet ") {
                    if let Some(ip) = rest.split_whitespace().next().and_then(|a| a.parse().ok()) {
                        out.push((name.clone(), ip));
                    }
                }
            }
        }
    }
    out
}

pub async fn detect_address() -> Option<Detected> {
    tokio::task::spawn_blocking(|| {
        if let Some(v) = tailscale_status() {
            let ip = v["Self"]["TailscaleIPs"].as_array().into_iter().flatten().filter_map(|x| x.as_str()?.parse::<IpAddr>().ok()).find(|ip| ip.is_ipv4());
            if let Some(ip) = ip {
                let magic = v["CurrentTailnet"]["MagicDNSEnabled"] == json!(true);
                let host = v["Self"]["DNSName"].as_str().map(|h| h.trim_end_matches('.').to_string()).filter(|h| magic && !h.is_empty());
                return Some(Detected { ip, host, via: "Tailscale" });
            }
        }
        let addrs = interface_addrs();
        if let Some((_, ip)) = addrs.iter().find(|(n, ip)| n.starts_with("tailscale") || tailscale_ip(*ip)) {
            return Some(Detected { ip: *ip, host: None, via: "Tailscale" });
        }
        let virtual_if = |n: &str| ["docker", "br-", "veth", "virbr", "podman", "cni", "lo"].iter().any(|p| n.starts_with(p));
        addrs.iter().filter(|(n, ip)| !virtual_if(n) && matches!(ip, IpAddr::V4(v) if v.is_private()))
            .max_by_key(|(_, ip)| matches!(ip, IpAddr::V4(v) if v.octets()[0] == 192)) // prefer 192.168 home LANs
            .map(|(_, ip)| Detected { ip: *ip, host: None, via: "LAN" })
    }).await.ok().flatten()
}

fn is_local_only(listen: &str) -> bool {
    listen.parse::<SocketAddr>().map(|a| a.ip().is_loopback() || a.ip().is_unspecified()).unwrap_or(true)
}

/// Base URL for a freshly issued link. Telegram uses the public (MagicDNS) URL
/// so a phone can open it; `ku web` asks for `direct`, the listener address
/// itself (no tunnel).
pub fn link_base(listen: &str, public_url: Option<&str>, direct: bool) -> String {
    if direct {
        // 0.0.0.0 / [::] is not openable; the local machine reaches loopback.
        let host = match listen.parse::<SocketAddr>() {
            Ok(a) if a.ip().is_unspecified() => {
                let ip = if a.is_ipv6() {
                    IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
                } else {
                    IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
                };
                SocketAddr::new(ip, a.port()).to_string()
            }
            _ => listen.to_string(),
        };
        return format!("http://{host}");
    }
    public_url.map(str::to_string).unwrap_or_else(|| format!("http://{listen}"))
}

/// True when the listener is on a Tailscale address, so the direct link is also
/// openable from a phone.
pub(crate) fn is_tailscale_listen(listen: &str) -> bool {
    listen.parse::<SocketAddr>().is_ok_and(|a| tailscale_ip(a.ip()))
}

/// Turn on `/terminal` (from Telegram after approval, or `ku terminal enable`).
pub fn enable_terminal(home: &Path, d: &Detected, port: u16) -> Result<Config> {
    let cfg = Config { enabled: true, listen: d.listen(port), public_url: d.host.as_ref().map(|_| d.url(port)) };
    crate::telegram::write_private(&config_path(home), &serde_json::to_vec_pretty(&cfg)?)?;
    Ok(cfg)
}

pub fn disable_terminal(home: &Path) -> Result<bool> {
    let Some(mut c) = load(home) else { return Ok(false) };
    let was = c.enabled;
    c.enabled = false;
    crate::telegram::write_private(&config_path(home), &serde_json::to_vec_pretty(&c)?)?;
    Ok(was)
}

/// `/web` (the chat UI). Same shape as the terminal config, its own file so the
/// two can be turned on and off independently.
pub fn web_config_path(home: &Path) -> PathBuf {
    home.join("weblink.json")
}

pub fn load_web(home: &Path) -> Option<Config> {
    serde_json::from_slice(&std::fs::read(web_config_path(home)).ok()?).ok()
}

/// The standalone `/web` UI records its bound address in `web.json`.
pub fn load_standalone_web(home: &Path) -> Option<Config> {
    serde_json::from_slice(&std::fs::read(home.join("web.json")).ok()?).ok()
}

/// Loopback ports a browser running on this host could use to reach the OAuth
/// callback, best first: the standalone UI (it is the surface handed to people),
/// then the paired `/web` listener, then the conventional default. Only ports are
/// returned; every one of them is still preflighted before it is handed out.
pub fn loopback_callback_ports(home: &Path) -> Vec<u16> {
    let mut ports: Vec<u16> = Vec::new();
    let mut push = |p: u16| {
        if p != 0 && !ports.contains(&p) {
            ports.push(p);
        }
    };
    for cfg in [load_standalone_web(home), load_web(home)].into_iter().flatten() {
        if cfg.enabled {
            if let Some(p) = endpoint_port(&cfg.listen) {
                push(p);
            }
        }
    }
    push(default_web_port(home));
    ports
}

/// Is anything accepting TCP connections on `127.0.0.1:port` right now? A port
/// that no listener holds cannot serve the redirect, so it must not be offered.
pub async fn loopback_listening(port: u16, timeout: Duration) -> bool {
    matches!(
        tokio::time::timeout(timeout, tokio::net::TcpStream::connect(("127.0.0.1", port))).await,
        Ok(Ok(_))
    )
}

pub fn enable_web(home: &Path, d: &Detected, port: u16) -> Result<Config> {
    let cfg = Config { enabled: true, listen: d.listen(port), public_url: d.host.as_ref().map(|_| d.url(port)) };
    crate::telegram::write_private(&web_config_path(home), &serde_json::to_vec_pretty(&cfg)?)?;
    Ok(cfg)
}

pub fn disable_web(home: &Path) -> Result<bool> {
    let Some(mut c) = load_web(home) else { return Ok(false) };
    let was = c.enabled;
    c.enabled = false;
    crate::telegram::write_private(&web_config_path(home), &serde_json::to_vec_pretty(&c)?)?;
    Ok(was)
}

fn random_hex(n: usize) -> String {
    let mut s = String::new();
    while s.len() < n {
        s.push_str(&uuid::Uuid::new_v4().simple().to_string());
    }
    s.truncate(n);
    s
}

/// 6 characters without look-alikes (0/O, 1/I/L).
pub fn pairing_code() -> String {
    const A: &[u8] = b"ABCDEFGHJKMNPQRSTUVWXYZ23456789";
    let bytes = uuid::Uuid::new_v4().into_bytes();
    bytes.iter().take(6).map(|b| A[*b as usize % A.len()] as char).collect()
}

struct Link {
    kind: &'static str,
    expires: Instant,
    attempts: u32,
    dead: bool,
    /// /terminal only: the directory the approved shell starts in.
    dir: Option<String>,
    channel: Option<crate::channels::Address>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum AttemptState {
    Pending,
    Approved,
    Denied,
    Expired,
    Replaced,
}

/// The live pending attempt this browser already has for this link, if any.
/// A duplicate request (prefetch, retried GET) must not replace it, or the
/// page that is actually on screen would be left showing "replaced".
fn pending_for<'a>(
    attempts: &'a std::collections::HashMap<String, Attempt>,
    token: &str,
    cookie: &str,
    now: Instant,
) -> Option<&'a Attempt> {
    attempts.values().find(|a| a.cookie == cookie && a.token == token && a.state == AttemptState::Pending && now <= a.expires)
}

struct Attempt {
    token: String,
    kind: &'static str,
    cookie: String,
    code: String,
    ip: String,
    ua: String,
    state: AttemptState,
    expires: Instant,
    expires_unix: i64,
    dir: Option<String>,
}

struct Session {
    kind: &'static str,
    ip: String,
    attempt: String,
    last: Instant,
    shell: Option<Arc<Shell>>,
    dir: Option<String>,
}

pub struct Shell {
    writer: std::sync::Mutex<Box<dyn std::io::Write + Send>>,
    master: std::sync::Mutex<Box<dyn portable_pty::MasterPty + Send>>,
    child: std::sync::Mutex<Option<Box<dyn portable_pty::Child + Send + Sync>>>,
    out: tokio::sync::broadcast::Sender<Vec<u8>>,
}

impl Shell {
    fn kill(&self) {
        // Take ownership once: disconnect, Hub shutdown and Drop may all run.
        // Never signal an already reaped PID that the OS could have reused.
        let child = self.child.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(mut child) = child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for Shell {
    fn drop(&mut self) { self.kill(); }
}

#[derive(Default)]
struct Inner {
    links: HashMap<String, Link>,
    attempts: HashMap<String, Attempt>,
    sessions: HashMap<String, Session>,
    listeners: HashMap<&'static str, (String, tokio::task::JoinHandle<()>, std::net::TcpListener, Arc<tokio::sync::Notify>)>,
}

/// Remote access state, owned by the server.
#[derive(Default)]
pub struct Hub {
    inner: Mutex<Inner>,
    inherited_web: std::sync::Mutex<Option<std::net::TcpListener>>,
    /// Why the last MCP-server start failed, so the card and doctor can say it.
    mcp_error: Mutex<Option<String>>,
}

#[derive(Clone)]
pub(crate) struct Web {
    pub(crate) server: Weak<Server>,
    pub(crate) kind: &'static str,
}

/// Best-effort "who is listening on this port", for an actionable error instead
/// of the bare `os error 98` that would otherwise reach Telegram.
pub(crate) fn port_owner(port: u16) -> Option<String> {
    let out = std::process::Command::new("ss")
        .args(["-ltnpH", &format!("sport = :{port}")])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string());
    if let Some(s) = out {
        if let Some(pid) = s.split("pid=").nth(1).and_then(|r| r.split(|c: char| !c.is_ascii_digit()).next()).filter(|p| !p.is_empty()) {
            let cmd = s.split("users:((\"").nth(1).and_then(|r| r.split('"').next()).filter(|c| !c.is_empty());
            return Some(match cmd {
                Some(c) => format!("pid {pid} ({c})"),
                None => format!("pid {pid}"),
            });
        }
    }
    let out = std::process::Command::new("lsof")
        .args(["-nP", &format!("-iTCP:{port}"), "-sTCP:LISTEN", "-Fpc"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())?;
    let pid = out.lines().find_map(|l| l.strip_prefix('p'));
    let cmd = out.lines().find_map(|l| l.strip_prefix('c'));
    pid.map(|p| match cmd {
        Some(c) => format!("pid {p} ({c})"),
        None => format!("pid {p}"),
    })
}

async fn bind_listener(listen: &str, addr: SocketAddr) -> Result<tokio::net::TcpListener> {
    match tokio::net::TcpListener::bind(addr).await {
        Ok(t) => Ok(t),
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            let who = port_owner(addr.port()).map(|p| format!(" by {p}")).unwrap_or_default();
            bail!("{listen} is already in use{who}. Stop that process or choose another port.")
        }
        Err(e) => Err(e).with_context(|| format!("listen on {listen}")),
    }
}

/// A link only opens on the listener it was issued for. Otherwise a /web token
/// could be redeemed on the terminal address and render the terminal app.
fn link_kind_matches(kind: &str, expect: &str) -> std::result::Result<(), &'static str> {
    if kind == expect {
        return Ok(());
    }
    Err(match kind {
        "web" => "This is a /web link. Open it on the /web address exactly as Telegram sent it.",
        "browser" => "This is a /browser link. Open it on the Live Browser address exactly as Telegram sent it.",
        _ => "This is a /terminal link. Open it on the terminal address exactly as Telegram sent it.",
    })
}

impl Hub {
    /// `/terminal`: a new link (starts the listener if needed). `direct` skips
    /// the public URL and the loopback-to-Tailscale move, for `ku web` on the
    /// machine itself.
    pub async fn link(&self, server: &Arc<Server>, kind: &'static str, direct: bool, dir: Option<&str>) -> Result<Value> {
        self.link_for_channel(server,kind,direct,dir,None).await
    }
    pub async fn link_for_channel(&self, server: &Arc<Server>, kind: &'static str, direct: bool, dir: Option<&str>, channel:Option<crate::channels::Address>) -> Result<Value> {
        let home = server.home();
        let (mut listen, mut public_url) = match kind {
            "browser" => {
                let b = crate::browser::active(&home).context("Live Browser is not set up")?;
                // Start lb web now, so the live view is there right after approval.
                server.browser.ensure(&home, &b).await?;
                (b.listen, b.public_url)
            }
            "web" => {
                let c = load_web(&home).filter(|c| c.enabled).context("the /web chat UI is off")?;
                (c.listen, c.public_url)
            }
            _ => {
                let c = load(&home).filter(|c| c.enabled).context("the remote terminal is off")?;
                (c.listen, c.public_url)
            }
        };
        if kind == "web"
            && is_test_home(&home)
            && (endpoint_port(&listen) == Some(WEB_PORT)
                || public_url.as_deref().and_then(endpoint_port) == Some(WEB_PORT))
        {
            bail!("weblink.json points at {WEB_PORT}, the live /web port. Run `ku web-link enable` to move this test home to {WEB_TEST_PORT}.");
        }
        // A phone cannot open 127.0.0.1: move a local-only setup to the Tailscale/LAN address.
        // `ku web` wants the direct listener address instead, so it does not move.
        if !direct && is_local_only(&listen) {
            let default_port = match kind { "browser" => 8793, "web" => default_web_port(&home), _ => 8790 };
            let port = listen.parse::<SocketAddr>().map(|a| a.port()).unwrap_or(default_port);
            let d = detect_address().await.context(NO_ADDRESS)?;
            listen = d.listen(port);
            public_url = d.host.as_ref().map(|_| d.url(port));
            match kind {
                "browser" => {
                    if let Some(mut b) = crate::browser::load(&home) {
                        b.listen = listen.clone();
                        b.public_url = public_url.clone();
                        crate::telegram::write_private(&crate::browser::config_path(&home), &serde_json::to_vec_pretty(&b)?)?;
                    }
                }
                "web" => { enable_web(&home, &d, port)?; }
                _ => { enable_terminal(&home, &d, port)?; }
            }
            server.audit("kurisu", "terminal", "remote_listen_moved", &format!("{kind}: from a local-only address to {}", d.describe(port))).await;
        }
        struct Cfg { listen: String, public_url: Option<String> }
        let cfg = Cfg { listen, public_url };
        let addr = check_listen(&cfg.listen, true)?;
        self.ensure_listener(server, kind, &cfg.listen, addr).await?;
        let token = random_hex(40);
        let mut g = self.inner.lock().await;
        g.links.insert(token.clone(), Link { kind, expires: Instant::now() + LINK_TTL, attempts: 0, dead: false, dir: dir.map(String::from), channel });
        drop(g);
        let base = link_base(&cfg.listen, cfg.public_url.as_deref(), direct);
        let base = base.trim_end_matches('/');
        server.audit("user", "terminal", "remote_link_issued", &format!("{kind} link, expires in 5 min")).await;
        let path = match kind { "browser" => "/_ku/r/", "web" => "/_web/r/", _ => "/r/" };
        Ok(json!({"url": format!("{base}{path}{token}"), "expires_in": LINK_TTL.as_secs(),
                  "listen": cfg.listen, "tailscale": is_tailscale_listen(&cfg.listen)}))
    }

    /// Bind the listener for `kind` and keep it in the server. Used by the CLI
    /// so `web-link enable` can prove the address before it says "on".
    pub async fn bind(&self, server: &Arc<Server>, kind: &'static str, listen: &str) -> Result<Value> {
        let addr = check_listen(listen, true)?;
        self.ensure_listener(server, kind, listen, addr).await?;
        Ok(json!({"listen": listen}))
    }

    pub fn with_inherited_web() -> Result<Self> {
        use std::os::fd::FromRawFd;
        let inherited = match std::env::var("KURISU_DEPLOY_WEB_FD") {
            Ok(value) => {
                std::env::remove_var("KURISU_DEPLOY_WEB_FD");
                let fd: i32 = value.parse()?;
                Some(unsafe { std::net::TcpListener::from_raw_fd(fd) })
            }
            Err(_) => None,
        };
        Ok(Self { inner: Mutex::default(), inherited_web: std::sync::Mutex::new(inherited), mcp_error: Mutex::default() })
    }

    pub async fn export_web_fd(&self, receiver: &Path) -> Result<bool> {
        use std::os::fd::AsRawFd;
        let g = self.inner.lock().await;
        let Some((_, _, socket, _)) = g.listeners.get("web") else { return Ok(false) };
        crate::fdpass::send(receiver, socket.as_raw_fd())?;
        Ok(true)
    }

    pub async fn web_sessions(&self) -> Value {
        let g = self.inner.lock().await;
        json!(g.sessions.iter().filter(|(_, s)| s.kind == "web").map(|(cookie, s)| {
            json!({"cookie":cookie,"ip":s.ip,"attempt":s.attempt,"idle_ms":s.last.elapsed().as_millis() as u64,"dir":s.dir})
        }).collect::<Vec<_>>())
    }

    pub async fn import_web_sessions(&self, rows: &Value) -> Result<()> {
        let mut g = self.inner.lock().await;
        for row in rows.as_array().context("invalid web sessions")? {
            let cookie = row["cookie"].as_str().context("missing web cookie")?;
            let idle = Duration::from_millis(row["idle_ms"].as_u64().unwrap_or(0));
            g.sessions.insert(cookie.to_string(), Session { kind:"web", ip:row["ip"].as_str().unwrap_or_default().into(),
                attempt:row["attempt"].as_str().unwrap_or_default().into(), last:Instant::now().checked_sub(idle).unwrap_or_else(Instant::now),
                shell:None, dir:row["dir"].as_str().map(String::from) });
        }
        Ok(())
    }

    /// Re-read persisted /web state and reconcile the daemon before acknowledging it.
    pub async fn reload_web(&self, server: &Arc<Server>) -> Result<Value> {
        let cfg = match std::fs::read(web_config_path(&server.home())) {
            Ok(bytes) => Some(serde_json::from_slice::<Config>(&bytes)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        if let Some(c) = cfg.filter(|c| c.enabled) {
            self.bind(server, "web", &c.listen).await?;
            return Ok(json!({"enabled":true,"listen":c.listen}));
        }
        self.close_all(server, Some("web"), "web config disabled").await;
        self.stop_listener("web").await;
        Ok(json!({"enabled":false}))
    }

    async fn stop_listener(&self, kind: &str) {
        let listener = self.inner.lock().await.listeners.remove(kind);
        if let Some((_, mut task, _socket, stop)) = listener {
            stop.notify_one();
            // Wait until the bound socket is dropped before acknowledging disable.
            if tokio::time::timeout(Duration::from_secs(3), &mut task).await.is_err() {
                task.abort();
                let _ = task.await;
            }
        }
    }

    async fn ensure_listener(&self, server: &Arc<Server>, kind: &'static str, listen: &str, addr: SocketAddr) -> Result<()> {
        let mut g = self.inner.lock().await;
        if let Some((l, h, _, _)) = g.listeners.get(kind) {
            if l == listen && !h.is_finished() {
                return Ok(());
            }
        }
        // Bind before aborting the old listener: a failed move must not leave
        // the instance with no listener at all.
        let inherited = if kind == "web" { self.inherited_web.lock().unwrap().take() } else { None };
        let tcp = match inherited {
            Some(socket) => { socket.set_nonblocking(true)?; tokio::net::TcpListener::from_std(socket)? }
            None => bind_listener(listen, addr).await?,
        };
        let retained = tcp.into_std()?;
        let tcp = tokio::net::TcpListener::from_std(retained.try_clone()?)?;
        if let Some((_, h, _, _)) = g.listeners.get(kind) {
            h.abort();
        }
        let web = Web { server: Arc::downgrade(server), kind };
        let app = match kind {
            "browser" => browser_router(web),
            "web" => crate::chat::router(web),
            // The outward MCP endpoint, hosted here instead of as a child process:
            // same router, same host:port and token file as `ku mcp serve`.
            "mcp" => {
                let home = server.home();
                let token = crate::mcp_out::ensure_token(&home)?;
                crate::mcp_out::router(home, token, addr)
            }
            _ => router(web),
        };
        let stop = Arc::new(tokio::sync::Notify::new());
        let shutdown = stop.clone();
        let h = tokio::spawn(async move {
            let _ = axum::serve(tcp, app.into_make_service_with_connect_info::<SocketAddr>())
                .with_graceful_shutdown(async move { shutdown.notified().await }).await;
        });
        g.listeners.insert(kind, (listen.to_string(), h, retained, stop));
        Ok(())
    }

    /// Any approved browser viewer (keeps lb web alive).
    pub async fn browser_viewers(&self) -> bool {
        self.inner.lock().await.sessions.values().any(|s| s.kind == "browser")
    }

    // ------------------------------------------------------------ outward MCP server

    /// The bound address of a live listener, or None when nothing is listening.
    pub async fn listening(&self, kind: &str) -> Option<String> {
        let g = self.inner.lock().await;
        g.listeners.get(kind).filter(|(_, h, _, _)| !h.is_finished()).map(|(l, _, _, _)| l.clone())
    }

    /// Why the last start failed (empty once it works again).
    pub async fn mcp_error(&self) -> Option<String> {
        self.mcp_error.lock().await.clone()
    }

    async fn set_mcp_error(&self, why: Option<String>) {
        *self.mcp_error.lock().await = why;
    }

    /// Bring the stored switch up: bind, then persist enabled=true. On a failure
    /// nothing this daemon did not start is touched, the stored switch goes back
    /// to false and the reason is kept for the status card and `ku doctor`.
    pub async fn enable_mcp(&self, server: &Arc<Server>) -> Result<Value> {
        let home = server.home();
        let cfg = crate::mcp_out::load_config(&home).unwrap_or_default();
        let addr = crate::mcp_out::socket_addr(&cfg)?;
        let listen = addr.to_string();
        if let Err(e) = self.ensure_listener(server, "mcp", &listen, addr).await {
            let mut off = cfg.clone();
            if off.enabled {
                off.enabled = false;
                if let Err(werr) = crate::mcp_out::save_config(&home, &off) {
                    eprintln!("could not clear the mcp-server switch: {werr:#}");
                }
            }
            self.set_mcp_error(Some(format!("{e:#}"))).await;
            return Err(e);
        }
        let mut on = cfg;
        on.enabled = true;
        crate::mcp_out::save_config(&home, &on)?;
        self.set_mcp_error(None).await;
        Ok(json!({"enabled": true, "listen": listen}))
    }

    /// Stop the listener and persist enabled=false; answers whether it had been on.
    pub async fn disable_mcp(&self, server: &Arc<Server>) -> Result<bool> {
        let was = crate::mcp_out::disable(&server.home())?;
        self.stop_listener("mcp").await;
        self.set_mcp_error(None).await;
        Ok(was)
    }

    /// At daemon start: bring the listener back when the switch is on.
    pub async fn reload_mcp(&self, server: &Arc<Server>) -> Result<Value> {
        let home = server.home();
        let cfg = crate::mcp_out::load_config(&home).unwrap_or_default();
        if !cfg.enabled {
            self.stop_listener("mcp").await;
            return Ok(json!({"enabled": false}));
        }
        self.enable_mcp(server).await
    }

    /// Close every remote session and kill every link (`/terminal close`, `ku terminal disable`).
    pub async fn close_all(&self, server: &Server, kind: Option<&str>, why: &str) -> usize {
        let mut g = self.inner.lock().await;
        let keys: Vec<String> = g.sessions.iter().filter(|(_, s)| kind.is_none_or(|k| s.kind == k)).map(|(k, _)| k.clone()).collect();
        let n = keys.len();
        for k in keys {
            if let Some(s) = g.sessions.remove(&k) {
                if let Some(sh) = s.shell {
                    sh.kill();
                }
            }
        }
        for l in g.links.values_mut().filter(|l| kind.is_none_or(|k| l.kind == k)) {
            l.dead = true;
        }
        let pending: Vec<String> = g.attempts.iter().filter(|(_, a)| a.state == AttemptState::Pending && kind.is_none_or(|k| a.kind == k)).map(|(k, _)| k.clone()).collect();
        for id in &pending {
            if let Some(a) = g.attempts.get_mut(id) {
                a.state = AttemptState::Expired;
            }
        }
        drop(g);
        for id in pending {
            server.telegram.pair_close(&id, "⌛ Closed — the link is dead.").await;
            server.discord.remote_close(&id, "⌛ Closed — the link is dead.").await;
        }
        if n > 0 {
            server.audit("user", "terminal", "remote_closed", &format!("{n} session(s): {why}")).await;
        }
        n
    }

    pub async fn stop(&self) {
        // Finish accepted HTTP requests before clearing their pairing state.
        let kinds: Vec<_> = self.inner.lock().await.listeners.keys().copied().collect();
        for kind in kinds { self.stop_listener(kind).await; }
        let mut g = self.inner.lock().await;
        for (_, s) in g.sessions.drain() {
            if let Some(sh) = s.shell {
                sh.kill();
            }
        }
        for (_, (_, h, _, _)) in g.listeners.drain() {
            h.abort();
        }
    }

    /// Approve or deny an attempt (the Telegram card). Err when it is no longer pending.
    pub async fn decide(&self, server: &Server, attempt: &str, approve: bool) -> Result<String> {
        let mut g = self.inner.lock().await;
        let Some(a) = g.attempts.get_mut(attempt) else { bail!("expired") };
        if a.state != AttemptState::Pending || Instant::now() > a.expires {
            bail!("expired");
        }
        let (token, cookie, ip, kind, ua, dir) = (a.token.clone(), a.cookie.clone(), a.ip.clone(), a.kind, short_ua(&a.ua), a.dir.clone());
        if approve {
            a.state = AttemptState::Approved;
            if let Some(l) = g.links.get_mut(&token) {
                l.dead = true; // used up
            }
            g.sessions.insert(cookie, Session { kind, ip: ip.clone(), attempt: attempt.to_string(), last: Instant::now(), shell: None, dir });
            drop(g);
            server.audit("user", "terminal", "remote_approved", &format!("{kind} from {ip} ({ua})")).await;
            Ok(ip)
        } else {
            a.state = AttemptState::Denied;
            if let Some(l) = g.links.get_mut(&token) {
                l.dead = true;
            }
            drop(g);
            server.audit("user", "terminal", "remote_denied", &format!("{kind} from {ip}; the link is dead")).await;
            Ok(ip)
        }
    }

    /// Close one approved session (the card's Close button).
    pub async fn close_attempt(&self, server: &Server, attempt: &str) -> bool {
        let mut g = self.inner.lock().await;
        let key = g.sessions.iter().find(|(_, s)| s.attempt == attempt).map(|(k, _)| k.clone());
        let Some(k) = key else { return false };
        if let Some(s) = g.sessions.remove(&k) {
            if let Some(sh) = s.shell {
                sh.kill();
            }
            drop(g);
            server.audit("user", "terminal", "remote_closed", &format!("{} from {} (Close button)", s.kind, s.ip)).await;
        }
        true
    }

    /// Server tick: expire attempts (killing their link) and idle sessions.
    pub async fn tick(&self, server: &Server) {
        let now = Instant::now();
        let mut g = self.inner.lock().await;
        let expired: Vec<(String, String)> = g.attempts.iter()
            .filter(|(_, a)| a.state == AttemptState::Pending && now > a.expires).map(|(k, a)| (k.clone(), a.token.clone())).collect();
        for (id, tok) in &expired {
            if let Some(a) = g.attempts.get_mut(id) {
                a.state = AttemptState::Expired;
            }
            if let Some(l) = g.links.get_mut(tok) {
                l.dead = true;
            }
        }
        let idle: Vec<String> = g.sessions.iter().filter(|(_, s)| now.duration_since(s.last) > IDLE_TIMEOUT).map(|(k, _)| k.clone()).collect();
        let mut closed = vec![];
        for k in idle {
            if let Some(s) = g.sessions.remove(&k) {
                if let Some(sh) = &s.shell {
                    sh.kill();
                }
                closed.push(s);
            }
        }
        g.links.retain(|_, l| now < l.expires + Duration::from_secs(600));
        g.attempts.retain(|_, a| a.state == AttemptState::Pending || now < a.expires + Duration::from_secs(3600));
        drop(g);
        for (id, _) in expired {
            server.audit("kurisu", "terminal", "remote_expired", "no answer in 2 min; the link is dead").await;
            server.telegram.pair_close(&id, "⌛ Expired — no answer in 2 min; the link is dead.").await;
            server.discord.remote_close(&id, "⌛ Expired — no answer in 2 min; the link is dead.").await;
        }
        for s in closed {
            server.audit("kurisu", "terminal", "remote_closed", &format!("{} from {} (idle 30 min)", s.kind, s.ip)).await;
            server.telegram.pair_close(&s.attempt, "⏹ Closed after 30 min idle.").await;
            server.discord.remote_close(&s.attempt, "⏹ Closed after 30 min idle.").await;
        }
    }

    /// Opening a link: a new attempt for that browser, or why not (audited either way).
    async fn open(&self, server: &Server, token: &str, ip: &str, ua: &str, cookie: Option<String>, expect: &'static str) -> std::result::Result<(String, String, i64), &'static str> {
        let r = self.open_inner(server, token, ip, ua, cookie, expect).await;
        if let Err(why) = &r {
            server.audit("kurisu", "terminal", "remote_refused", &format!("from {ip} ({}): {why}", short_ua(ua))).await;
        }
        r
    }

    async fn open_inner(&self, server: &Server, token: &str, ip: &str, ua: &str, cookie: Option<String>, expect: &'static str) -> std::result::Result<(String, String, i64), &'static str> {
        let mut g = self.inner.lock().await;
        let now = Instant::now();
        let (kind, dir) = {
            let Some(l) = g.links.get_mut(token) else { return Err("This link is not valid.") };
            if l.dead || now > l.expires {
                return Err("This link has expired or was already used. Send the command again in Telegram.");
            }
            if l.attempts >= MAX_ATTEMPTS {
                l.dead = true;
                return Err("Too many attempts with this link. Send the command again in Telegram.");
            }
            (l.kind, l.dir.clone())
        };
        // A link is only valid on the listener it was issued for. Letting a
        // /web token through here used to render the terminal pairing page
        // (and, after approval, grant a shell) when the URL named the terminal
        // address. Refuse before creating an attempt or a Telegram card.
        link_kind_matches(kind, expect)?;
        let cookie = cookie.filter(|c| c.len() == 48 && !g.sessions.contains_key(c)).unwrap_or_else(|| random_hex(48));
        // Browsers sometimes request the link twice (Chrome prefetches a typed
        // URL). The second request used to replace the first attempt, killing
        // the page that was actually on screen. Reuse a live pending attempt
        // for the same browser and link: same code, same card, both requests
        // land on a page that can still be approved.
        if let Some(a) = pending_for(&g.attempts, token, &cookie, now) {
            return Ok((cookie, a.code.clone(), a.expires_unix));
        }
        let n = match g.links.get_mut(token) {
            Some(l) => {
                l.attempts += 1;
                l.attempts
            }
            None => return Err("This link is not valid."),
        };
        // A refresh replaces the previous pending attempt of this link.
        let replaced: Vec<String> = g.attempts.iter().filter(|(_, a)| a.token == token && a.state == AttemptState::Pending).map(|(k, _)| k.clone()).collect();
        for r in &replaced {
            if let Some(a) = g.attempts.get_mut(r) {
                a.state = AttemptState::Replaced;
            }
        }
        let channel=g.links.get(token).and_then(|l|l.channel.clone());
        let id = random_hex(12);
        let code = pairing_code();
        let expires_unix = chrono::Utc::now().timestamp() + ATTEMPT_TTL.as_secs() as i64;
        g.attempts.insert(id.clone(), Attempt { token: token.into(), kind, cookie: cookie.clone(), code: code.clone(), ip: ip.into(),
                                                 ua: ua.into(), state: AttemptState::Pending, expires: now + ATTEMPT_TTL, expires_unix, dir });
        drop(g);
        for r in replaced {
            server.telegram.pair_close(&r, "↻ Replaced by a new attempt (page refreshed).").await;
            server.discord.remote_close(&r, "↻ Replaced by a new attempt (page refreshed).").await;
        }
        server.audit("kurisu", "terminal", "remote_attempt", &format!("{kind} attempt {n}/{MAX_ATTEMPTS} from {ip} ({})", short_ua(ua))).await;
        let discord_origin=channel.is_some();
        let posted = match channel {
            Some(to)=>server.discord.remote_card(&to,&id,kind,ip,&short_ua(ua),&code,expires_unix).await,
            None=>server.telegram.pair_card(&id, kind, ip, &short_ua(ua), &code, expires_unix).await,
        };
        if let Err(e) = posted {
            eprintln!("remote: approval card not sent: {e:#}");
            let mut g = self.inner.lock().await;
            if let Some(a) = g.attempts.get_mut(&id) {
                a.state = AttemptState::Expired;
            }
            return Err(if discord_origin {"Could not ask for approval in Discord (is the Discord channel on?)."}else{"Could not ask for approval in Telegram (is the Telegram channel on?)."});
        }
        Ok((cookie, code, expires_unix))
    }

    pub(crate) async fn state_of(&self, cookie: &str) -> (&'static str, Option<String>) {
        let g = self.inner.lock().await;
        if g.sessions.contains_key(cookie) {
            return ("approved", None);
        }
        match g.attempts.values().filter(|a| a.cookie == cookie).max_by_key(|a| a.expires_unix) {
            Some(a) => (match a.state {
                AttemptState::Pending => "pending",
                AttemptState::Approved => "closed",
                AttemptState::Denied => "denied",
                AttemptState::Expired => "expired",
                AttemptState::Replaced => "replaced",
            }, Some(a.code.clone())),
            None => ("none", None),
        }
    }

    /// The approved session's shell (started on first use).
    async fn shell(&self, server: &Server, cookie: &str) -> Result<Arc<Shell>> {
        let mut g = self.inner.lock().await;
        let s = g.sessions.get_mut(cookie).filter(|s| s.kind == "terminal").context("not approved")?;
        s.last = Instant::now();
        if s.shell.is_some() {
            bail!("Already attached. Close the other terminal tab first.");
        }
        let sh = Arc::new(spawn_shell(&server.home(), s.dir.as_deref())?);
        s.shell = Some(sh.clone());
        let ip = s.ip.clone();
        drop(g);
        server.audit("user", "terminal", "remote_shell_started", &format!("from {ip}")).await;
        Ok(sh)
    }

    pub(crate) async fn touch_kind(&self, cookie: &str, kind: &str) -> bool {
        let mut g = self.inner.lock().await;
        match g.sessions.get_mut(cookie).filter(|s| s.kind == kind) {
            Some(s) => {
                s.last = Instant::now();
                true
            }
            None => false,
        }
    }

    async fn touch(&self, cookie: &str) -> bool {
        let mut g = self.inner.lock().await;
        match g.sessions.get_mut(cookie) {
            Some(s) => {
                s.last = Instant::now();
                true
            }
            None => false,
        }
    }
}

fn short_ua(ua: &str) -> String {
    let ua = ua.trim();
    if ua.is_empty() {
        return "unknown browser".into();
    }
    let mut parts = vec![];
    for (needle, name) in [("Android", "Android"), ("iPhone", "iPhone"), ("iPad", "iPad"), ("Mac OS X", "macOS"), ("Windows", "Windows"), ("Linux", "Linux")] {
        if ua.contains(needle) {
            parts.push(name);
            break;
        }
    }
    for (needle, name) in [("Edg/", "Edge"), ("Firefox/", "Firefox"), ("Chrome/", "Chrome"), ("Safari/", "Safari")] {
        if ua.contains(needle) {
            parts.push(name);
            break;
        }
    }
    if parts.is_empty() { ua.chars().take(60).collect() } else { parts.join(" · ") }
}

pub(crate) fn spawn_shell(home: &Path, dir: Option<&str>) -> Result<Shell> {
    use portable_pty::{CommandBuilder, NativePtySystem, PtySize, PtySystem};
    let pty = NativePtySystem::default().openpty(PtySize { rows: 30, cols: 100, pixel_width: 0, pixel_height: 0 })?;
    let shell = std::env::var("SHELL").ok().filter(|s| !s.is_empty()).unwrap_or_else(|| "/bin/sh".into());
    // The directory chosen in /terminal; the workspaces root when there is none.
    let chosen = dir.map(PathBuf::from).filter(|d| d.is_dir());
    let cwd = chosen.unwrap_or_else(|| home.join("workspaces"));
    std::fs::create_dir_all(&cwd)?;
    let mut cmd = CommandBuilder::new(&shell);
    cmd.arg("-l");
    cmd.cwd(&cwd);
    cmd.env("TERM", "xterm-256color");
    // Prepare fallible handles before spawning so an error cannot orphan a shell.
    let mut reader = pty.master.try_clone_reader()?;
    let writer = pty.master.take_writer()?;
    let child = pty.slave.spawn_command(cmd)?;
    drop(pty.slave);
    let (tx, _) = tokio::sync::broadcast::channel::<Vec<u8>>(256);
    let out = tx.clone();
    std::thread::spawn(move || {
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let _ = out.send(buf[..n].to_vec());
                }
            }
        }
        let _ = out.send(vec![]); // end of output
    });
    Ok(Shell { writer: std::sync::Mutex::new(writer), master: std::sync::Mutex::new(pty.master), child: std::sync::Mutex::new(Some(child)), out: tx })
}

use std::io::Read as _;

// ------------------------------------------------------------------ HTTP

fn router(web: Web) -> Router {
    Router::new()
        .route("/r/{token}", get(open_link))
        .route("/state", get(state))
        .route("/ws", get(ws))
        .route("/xterm.js", get(|| async { asset("application/javascript", include_str!("../terminal/xterm-5.5.0.js")) }))
        .route("/xterm.css", get(|| async { asset("text/css", include_str!("../terminal/xterm-5.5.0.css")) }))
        .route("/fit.js", get(|| async { asset("application/javascript", include_str!("../terminal/addon-fit-0.10.0.js")) }))
        .with_state(web)
}

/// `/browser`: pairing under /_ku/, everything else is lb web's UI, proxied for an approved
/// browser with the passcode added here (the phone never gets it).
fn browser_router(web: Web) -> Router {
    Router::new()
        .route("/_ku/r/{token}", get(open_link))
        .route("/_ku/state", get(state))
        .fallback(proxy)
        .with_state(web)
}

async fn proxy(State(web): State<Web>, req: axum::extract::Request) -> Response {
    let Some(server) = web.server.upgrade() else { return StatusCode::SERVICE_UNAVAILABLE.into_response() };
    let Some(cookie) = cookie_of(req.headers(), "browser") else {
        return page("Kurisu", "<h2>Kurisu</h2><p>Not paired. Send /browser in Telegram.</p>");
    };
    if !server.remote.touch_kind(&cookie, "browser").await {
        return page("Kurisu", "<h2>Kurisu</h2><p>Not paired, or closed. Send /browser in Telegram.</p>");
    }
    let Some(cfg) = crate::browser::active(&server.home()) else {
        return page("Kurisu", "<h2>Kurisu</h2><p>Live Browser is off on the computer.</p>");
    };
    server.browser.touch().await;
    let path = req.uri().path_and_query().map(|p| p.as_str().to_string()).unwrap_or_else(|| "/".into());
    // lb's Kurisu skin is opt-in (v0.4.22+): a bare page load gets ?theme=kurisu.
    if path == "/" && req.method() == axum::http::Method::GET {
        return axum::response::Redirect::to("/?theme=kurisu").into_response();
    }
    let method = reqwest::Method::from_bytes(req.method().as_str().as_bytes()).unwrap_or(reqwest::Method::GET);
    let mut out = reqwest::Client::new().request(method, format!("{}{path}", cfg.server_url())).timeout(Duration::from_secs(60));
    for (k, v) in req.headers() {
        let n = k.as_str();
        if matches!(n, "host" | "cookie" | "authorization" | "x-lb-passcode" | "connection" | "content-length" | "origin" | "referer") {
            continue;
        }
        out = out.header(n, v.as_bytes());
    }
    out = out.header("x-lb-passcode", &cfg.passcode);
    let body = match axum::body::to_bytes(req.into_body(), 32 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };
    let resp = match out.body(body.to_vec()).send().await {
        Ok(r) => r,
        Err(_) => {
            // lb web may have been stopped as idle: start it again.
            let _ = server.browser.ensure(&server.home(), &cfg).await;
            return (StatusCode::BAD_GATEWAY, "Live Browser is starting; retry in a moment").into_response();
        }
    };
    let mut r = Response::builder().status(resp.status().as_u16());
    for (k, v) in resp.headers() {
        if matches!(k.as_str(), "set-cookie" | "transfer-encoding" | "connection" | "content-length") {
            continue;
        }
        r = r.header(k.as_str(), v.as_bytes());
    }
    let bytes = resp.bytes().await.unwrap_or_default();
    let mut r = r.body(axum::body::Body::from(bytes)).unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response());
    r.headers_mut().insert("referrer-policy", "no-referrer".parse().unwrap());
    r.headers_mut().insert("x-frame-options", "DENY".parse().unwrap());
    r
}

// Shared with both bundled web entry points. Inline pages have no asset build
// or extra theme route; include the same source tokens and xterm mapping.
const PALETTE: &str = include_str!("../web/src/palette.css");
const TERMINAL_THEME: &str = include_str!("../web/src/lib/terminal-theme.js");

fn themed(html: &str) -> String {
    // Keep the existing classic inline script; only remove the shared module's
    // export marker, leaving its exact theme mapping available to this page.
    let theme = TERMINAL_THEME.replace("export function terminalTheme", "function terminalTheme");
    html.replace("__PALETTE__", PALETTE).replace("__TERMINAL_THEME__", &theme)
}

fn browser_wait_page(code: &str) -> Response {
    let html = r##"<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>Kurisu browser</title><style>__PALETTE__
body{font:16px system-ui;background:var(--bg2);color:var(--fg);margin:0;padding:24px}
#code{font:700 40px ui-monospace,monospace;letter-spacing:.2em;margin:12px 0;color:var(--accent)}p{color:var(--muted)}a{color:var(--accent)}</style></head><body>
<h2>Kurisu live browser</h2><p>Pairing code</p><div id=code>__CODE__</div>
<p id=msg>Waiting for approval in Telegram… Check that the code on the card matches, then tap Approve.</p>
<script>const texts={denied:'Denied in Telegram. This link is dead.',expired:'No approval in time. This link is dead; send /browser again.',
replaced:'This page was replaced by a newer attempt.',closed:'Closed.',none:'Not paired.'};
async function poll(){try{const s=(await (await fetch('/_ku/state',{cache:'no-store'})).json()).state;
if(s==='approved'){location.replace('/?theme=kurisu');return}if(s!=='pending'){document.getElementById('msg').textContent=texts[s]||s;return}}catch(e){}setTimeout(poll,1500)}
poll();</script></body></html>"##;
    secure_headers(Html(themed(&html.replace("__CODE__", code))).into_response())
}

fn web_wait_page(code: &str) -> Response {
    let html = r##"<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>Kurisu web chat</title><style>__PALETTE__
body{font:16px system-ui;background:var(--bg2);color:var(--fg);margin:0;padding:24px;min-height:100vh;box-sizing:border-box}
#code{font:700 40px ui-monospace,monospace;letter-spacing:.2em;margin:12px 0;color:var(--accent)}
p{color:var(--muted)}</style></head><body>
<h2>Kurisu web chat</h2><p>Pairing code</p><div id=code>__CODE__</div>
<p id=msg>Waiting for approval in Telegram… Check that the code on the card matches, then tap Approve.</p>
<script>const texts={denied:'Denied in Telegram. This link is dead.',expired:'No approval in time. This link is dead; send /web again.',
replaced:'This page was replaced by a newer attempt.',closed:'Closed.',none:'Not paired.'};
async function poll(){try{const s=(await (await fetch('/_web/state',{cache:'no-store'})).json()).state;
if(s==='approved'){location.replace('/_web/');return}if(s!=='pending'){document.getElementById('msg').textContent=texts[s]||s;return}}catch(e){}setTimeout(poll,1500)}
poll();</script></body></html>"##;
    secure_headers(Html(themed(&html.replace("__CODE__", code))).into_response())
}

fn asset(ct: &'static str, body: &'static str) -> Response {
    ([(header::CONTENT_TYPE, ct), (header::CACHE_CONTROL, "public, max-age=86400")], body).into_response()
}

pub(crate) fn cookie_of(h: &HeaderMap, kind: &str) -> Option<String> {
    let name = cookie_name(kind);
    h.get_all(header::COOKIE).iter().filter_map(|v| v.to_str().ok()).flat_map(|v| v.split(';'))
        .find_map(|c| c.trim().strip_prefix(&format!("{name}=")).map(String::from))
}

fn secure_headers(mut r: Response) -> Response {
    let h = r.headers_mut();
    h.insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    h.insert("referrer-policy", "no-referrer".parse().unwrap());
    h.insert("x-frame-options", "DENY".parse().unwrap());
    h.insert("content-security-policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'".parse().unwrap());
    r
}

fn page(title: &str, body: &str) -> Response {
    secure_headers(Html(themed(&format!("<!doctype html><html><head><meta charset=utf-8><meta name=viewport content=\"width=device-width,initial-scale=1\">\
<title>{title}</title><style>__PALETTE__\nbody{{font:16px system-ui;background:var(--bg2);color:var(--fg);margin:0;padding:24px}}.code{{font:700 40px ui-monospace,monospace;letter-spacing:.2em;margin:16px 0;color:var(--accent)}}p{{color:var(--muted)}}a{{color:var(--accent)}}</style>\
</head><body>{body}</body></html>"))).into_response())
}

pub(crate) async fn open_link(State(web): State<Web>, AxPath(token): AxPath<String>, ConnectInfo(peer): ConnectInfo<SocketAddr>, h: HeaderMap) -> Response {
    let Some(server) = web.server.upgrade() else { return StatusCode::SERVICE_UNAVAILABLE.into_response() };
    let ua = h.get(header::USER_AGENT).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    match server.remote.open(&server, &token, &peer.ip().to_string(), &ua, cookie_of(&h, web.kind), web.kind).await {
        Err(why) => page("Kurisu", &format!("<h2>Kurisu</h2><p>{why}</p>")),
        Ok((cookie, code, _)) => {
            let mut r = match web.kind {
                "browser" => browser_wait_page(&code),
                "web" => web_wait_page(&code),
                _ => terminal_page(&code),
            };
            r.headers_mut().insert(header::SET_COOKIE, format!("{}={cookie}; HttpOnly; SameSite=Strict; Path=/; Max-Age=86400", cookie_name(web.kind)).parse().unwrap());
            r
        }
    }
}

pub(crate) async fn state(State(web): State<Web>, h: HeaderMap) -> Response {
    let Some(server) = web.server.upgrade() else { return StatusCode::SERVICE_UNAVAILABLE.into_response() };
    let Some(c) = cookie_of(&h, web.kind) else { return axum::Json(json!({"state": "none"})).into_response() };
    let (st, _) = server.remote.state_of(&c).await;
    let attached = web.kind == "terminal" && server.remote.inner.lock().await.sessions
        .get(&c).is_some_and(|s| s.kind == "terminal" && s.shell.is_some());
    secure_headers(axum::Json(json!({"state": st, "attached": attached})).into_response())
}

pub(crate) fn same_origin(h: &HeaderMap) -> bool {
    let host = h.get(header::HOST).and_then(|v| v.to_str().ok()).unwrap_or("");
    let origin = h.get(header::ORIGIN).and_then(|v| v.to_str().ok()).unwrap_or("");
    !host.is_empty() && origin.split("://").nth(1) == Some(host)
}

async fn ws(State(web): State<Web>, h: HeaderMap, upgrade: WebSocketUpgrade) -> Response {
    let Some(server) = web.server.upgrade() else { return StatusCode::SERVICE_UNAVAILABLE.into_response() };
    // Same-origin only.
    if !same_origin(&h) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(cookie) = cookie_of(&h, "terminal") else { return StatusCode::UNAUTHORIZED.into_response() };
    if !server.remote.touch_kind(&cookie, "terminal").await { return StatusCode::UNAUTHORIZED.into_response(); }
    let shell = match server.remote.shell(&server, &cookie).await {
        Ok(s) => s,
        Err(e) => return (StatusCode::CONFLICT, e.to_string()).into_response(),
    };
    upgrade.on_upgrade(move |sock| pump(sock, shell, server, cookie))
}

async fn pump(sock: WebSocket, shell: Arc<Shell>, server: Arc<Server>, cookie: String) {
    pump_owned(sock, shell, Some((server.clone(), cookie.clone()))).await;
    if let Some(s) = server.remote.inner.lock().await.sessions.get_mut(&cookie) { s.shell = None; }
}

/// A connection owns its shell; disconnect, cancellation and idle kill it.
pub(crate) async fn pump_owned(mut sock: WebSocket, shell: Arc<Shell>, lease: Option<(Arc<Server>, String)>) {
    struct Cleanup(Arc<Shell>);
    impl Drop for Cleanup { fn drop(&mut self) { self.0.kill(); } }
    let _cleanup = Cleanup(shell.clone());
    let idle = tokio::time::sleep(IDLE_TIMEOUT);
    tokio::pin!(idle);
    let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
    let mut rx = shell.out.subscribe();
    loop {
        tokio::select! {
            _ = &mut idle => { let _ = sock.send(Message::Close(None)).await; break; }
            _ = heartbeat.tick() => {
                if let Some((server, cookie)) = &lease {
                    if !server.remote.touch(cookie).await { let _ = sock.send(Message::Close(None)).await; break; }
                }
            }
            out = rx.recv() => match out {
                Ok(b) if b.is_empty() => {
                    let _ = sock.send(Message::Close(None)).await;
                    break;
                }
                Ok(b) => {
                    if sock.send(Message::Binary(b.into())).await.is_err() {
                        break;
                    }
                    idle.as_mut().reset(tokio::time::Instant::now() + IDLE_TIMEOUT);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            },
            msg = sock.recv() => {
                let Some(Ok(msg)) = msg else { break };
                idle.as_mut().reset(tokio::time::Instant::now() + IDLE_TIMEOUT);
                match msg {
                    Message::Text(t) => {
                        let t = t.to_string();
                        if let Some(ctl) = t.strip_prefix('\u{0}') {
                            if let Ok(v) = serde_json::from_str::<Value>(ctl) {
                                if let (Some(c), Some(r)) = (v["cols"].as_u64(), v["rows"].as_u64()) {
                                    let _ = shell.master.lock().unwrap_or_else(|e| e.into_inner()).resize(portable_pty::PtySize { rows: r.clamp(5, 500) as u16, cols: c.clamp(10, 500) as u16, pixel_width: 0, pixel_height: 0 });
                                }
                            }
                        } else {
                            let _ = shell.writer.lock().unwrap_or_else(|e| e.into_inner()).write_all(t.as_bytes());
                        }
                    }
                    Message::Binary(b) => {
                        let _ = shell.writer.lock().unwrap_or_else(|e| e.into_inner()).write_all(&b);
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        }
    }
}

use std::io::Write as _;

fn terminal_page(code: &str) -> Response {
    let html = r##"<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1,interactive-widget=resizes-content">
<title>Kurisu terminal</title><link rel=stylesheet href="/xterm.css">
<style>__PALETTE__
html,body{margin:0;height:100%;background:var(--bg2);color:var(--fg);font:16px system-ui}
#wait{padding:24px}#code{font:700 40px ui-monospace,monospace;letter-spacing:.2em;margin:12px 0;color:var(--accent)}p{color:var(--muted)}
#app{display:none;flex-direction:column;height:100%}#t{flex:1;min-height:0;padding:4px}
#keys{display:flex;gap:4px;padding:4px;background:var(--bg2);border-top:1px solid var(--line);overflow-x:auto}
#keys button{flex:1;min-width:44px;padding:10px 6px;background:var(--bg);color:var(--fg);border:1px solid var(--line);border-radius:6px;font:15px system-ui}
#keys button:hover{background:var(--hover)}#keys button.on{background:var(--accent);color:var(--accent-fg)}
.xterm-viewport{scrollbar-color:var(--muted) var(--bg2);scrollbar-width:thin}
.xterm-viewport::-webkit-scrollbar{width:8px}.xterm-viewport::-webkit-scrollbar-track{background:var(--bg2)}
.xterm-viewport::-webkit-scrollbar-thumb{background:var(--muted);border:2px solid var(--bg2);border-radius:6px}</style></head><body>
<div id=wait><h2>Kurisu terminal</h2><p>Pairing code</p><div id=code>__CODE__</div>
<p id=msg>Waiting for approval in Telegram… Check that the code on the card matches, then tap Approve.</p></div>
<div id=app><div id=t></div><div id=keys>
<button data-k="\x1b">Esc</button><button data-k="\t">Tab</button><button id=ctrl>Ctrl</button>
<button data-k="\x1b[D">←</button><button data-k="\x1b[A">↑</button><button data-k="\x1b[B">↓</button><button data-k="\x1b[C">→</button>
</div></div>
<script src="/xterm.js"></script><script src="/fit.js"></script><script>
__TERMINAL_THEME__
const msg=document.getElementById('msg');let ctrl=false,ws=null;
const texts={denied:'Denied in Telegram. This link is dead.',expired:'No approval in time. This link is dead; send /terminal again.',
 replaced:'This page was replaced by a newer attempt.',closed:'Closed.',none:'Not paired.'};
async function poll(){try{const r=await fetch('/state',{cache:'no-store'});const s=(await r.json()).state;
 if(s==='approved')return start();if(s!=='pending'){msg.textContent=texts[s]||s;return}}catch(e){}setTimeout(poll,1500)}
function start(){document.getElementById('wait').style.display='none';document.getElementById('app').style.display='flex';
 const theme=()=>terminalTheme(name=>getComputedStyle(document.documentElement).getPropertyValue(name).trim());
 const term=new Terminal({fontSize:14,cursorBlink:true,scrollback:5000,theme:theme()});
 const scheme=matchMedia('(prefers-color-scheme: dark)');const recolor=()=>{term.options.theme=theme()};
 scheme.addEventListener('change',recolor);addEventListener('pagehide',()=>scheme.removeEventListener('change',recolor),{once:true});const fit=new FitAddon.FitAddon();term.loadAddon(fit);
 term.open(document.getElementById('t'));fit.fit();window.term=term;
 ws=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws');ws.binaryType='arraybuffer';
 const size=()=>{fit.fit();if(ws.readyState===1)ws.send('\0'+JSON.stringify({cols:term.cols,rows:term.rows}))};
 ws.onopen=size;ws.onmessage=e=>term.write(new Uint8Array(e.data));
 ws.onclose=async()=>{let text='Terminal closed.';try{const s=await(await fetch('/state',{cache:'no-store'})).json();
 if(s.attached)text='Already attached. Close the other terminal tab, then refresh this page.';}catch(e){}
 term.write('\r\n\x1b[33m['+text+']\x1b[0m\r\n')};
 const send=d=>{if(ws.readyState!==1)return;if(ctrl&&d.length===1){const c=d.toUpperCase().charCodeAt(0);if(c>=64&&c<96)d=String.fromCharCode(c-64);ctrl=false;cb.classList.remove('on')}ws.send(d)};
 term.onData(send);addEventListener('resize',size);const cb=document.getElementById('ctrl');
 cb.onclick=()=>{ctrl=!ctrl;cb.classList.toggle('on',ctrl);term.focus()};
 document.querySelectorAll('#keys [data-k]').forEach(b=>b.onclick=()=>{ws.send(JSON.parse('"'+b.dataset.k+'"'));term.focus()});
 term.focus()}
poll();</script></body></html>"##;
    let mut r = Html(themed(&html.replace("__CODE__", code))).into_response();
    let h = r.headers_mut();
    h.insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    h.insert("referrer-policy", "no-referrer".parse().unwrap());
    h.insert("x-frame-options", "DENY".parse().unwrap());
    h.insert("content-security-policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'".parse().unwrap());
    r
}

// ------------------------------------------------------------------ CLI

pub async fn cli(paths: &crate::paths::Paths, args: &[String]) -> Result<()> {
    let home = &paths.home;
    let force = args.iter().any(|a| a == "--force");
    let flag = |n: &str| args.iter().position(|a| a == n).and_then(|i| args.get(i + 1)).cloned();
    match args.get(2).map(String::as_str).unwrap_or("status") {
        "enable" => {
            let cfg = match flag("--listen") {
                Some(listen) => {
                    check_listen(&listen, force)?;
                    let cfg = Config { enabled: true, listen, public_url: flag("--url") };
                    crate::telegram::write_private(&config_path(home), &serde_json::to_vec_pretty(&cfg)?)?;
                    cfg
                }
                None => {
                    let d = detect_address().await.context(NO_ADDRESS)?;
                    enable_terminal(home, &d, 8790)?
                }
            };
            println!("remote terminal: on, listening on {} when a link is used{}. In Telegram, send /terminal.",
                     cfg.listen, cfg.public_url.as_ref().map(|u| format!(" (links: {u})")).unwrap_or_default());
            Ok(())
        }
        "disable" => {
            disable_terminal(home)?;
            let _ = crate::server::call(&paths.socket(), json!({"op": "remote_close", "why": "ku terminal disable"})).await;
            println!("remote terminal: off (open sessions closed)");
            Ok(())
        }
        "status" => {
            match load(home) {
                Some(c) if c.enabled => println!("remote terminal: on (listen {}{})", c.listen, c.public_url.map(|u| format!(", links {u}")).unwrap_or_default()),
                _ => println!("remote terminal: off (optional). Turn it on with `ku terminal enable [--listen IP:PORT]`."),
            }
            Ok(())
        }
        _ => bail!("usage: kurisu terminal enable [--listen IP:PORT] [--url URL] [--force] | disable | status"),
    }
}

/// Refuse to change a Telegram-connected home (one with `telegram.json`) unless
/// the caller says `--prod`. A test that points `KURISU_HOME` at the live home by
/// mistake would otherwise issue real links or close real pages.
fn require_prod(home: &Path, prod: bool) -> Result<()> {
    if !prod && !is_test_home(home) {
        bail!("{} has telegram.json: this is a Telegram-connected instance. Test with a fresh KURISU_HOME under /tmp (no Telegram), or pass --prod if you mean the live instance.", home.display());
    }
    Ok(())
}

/// A test home must not claim the live `/web` port. `--prod` is the explicit
/// override for the one real instance.
fn check_web_port(home: &Path, port: u16, prod: bool) -> Result<()> {
    if is_test_home(home) && port == WEB_PORT && !prod {
        bail!("port {WEB_PORT} is the live /web endpoint; test homes use {WEB_TEST_PORT}. Pass --prod only for the real instance.");
    }
    Ok(())
}

/// The port named by an `IP:PORT` listen address or an http(s) URL, if any.
fn endpoint_port(endpoint: &str) -> Option<u16> {
    let endpoint = endpoint.trim();
    let hostport = endpoint.split("://").nth(1).unwrap_or(endpoint);
    let hostport = hostport.split('/').next().unwrap_or(hostport);
    let hostport = hostport.rsplit('@').next().unwrap_or(hostport);
    if let Some(rest) = hostport.strip_prefix('[') {
        return rest.split_once("]:").and_then(|(_, p)| p.parse().ok());
    }
    hostport.rsplit_once(':').and_then(|(_, p)| p.parse().ok())
}

fn check_web_endpoint(home: &Path, endpoint: &str, prod: bool) -> Result<()> {
    if let Some(port) = endpoint_port(endpoint) {
        check_web_port(home, port, prod)?;
    }
    Ok(())
}

/// Bind once now: through the server when it is running (so the listener is
/// actually held), otherwise locally just to prove the address is free.
async fn preflight_web_bind(paths: &crate::paths::Paths, listen: &str) -> Result<bool> {
    let sock = paths.socket();
    match tokio::net::UnixStream::connect(&sock).await {
        Ok(stream) => {
            drop(stream);
            crate::server::call(&sock, json!({"op": "remote_bind", "kind": "web", "listen": listen})).await?;
            return Ok(true);
        }
        Err(e) if matches!(e.kind(), std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused) => {},
        Err(e) => return Err(e).context("connect to daemon before enabling /web"),
    }
    let addr = check_listen(listen, true)?;
    drop(bind_listener(listen, addr).await?);
    Ok(false)
}

/// `ku web-link enable|disable|status`: the chat UI behind `/web`.
pub async fn web_cli(paths: &crate::paths::Paths, args: &[String]) -> Result<()> {
    let home = &paths.home;
    let force = args.iter().any(|a| a == "--force");
    let prod = args.iter().any(|a| a == "--prod");
    let flag = |n: &str| args.iter().position(|a| a == n).and_then(|i| args.get(i + 1)).cloned();
    match args.get(2).map(String::as_str).unwrap_or("status") {
        "enable" => {
            require_prod(home, prod)?;
            let cfg = match flag("--listen") {
                Some(listen) => {
                    check_listen(&listen, force)?;
                    Config { enabled: true, listen, public_url: flag("--url") }
                }
                None => {
                    let d = detect_address().await.context(NO_ADDRESS)?;
                    let port = default_web_port(home);
                    Config { enabled: true, listen: d.listen(port), public_url: flag("--url").or_else(|| d.host.as_ref().map(|_| d.url(port))) }
                }
            };
            check_web_endpoint(home, &cfg.listen, prod)?;
            if let Some(url) = &cfg.public_url {
                check_web_endpoint(home, url, prod)?;
            }
            let running = preflight_web_bind(paths, &cfg.listen).await?;
            crate::telegram::write_private(&web_config_path(home), &serde_json::to_vec_pretty(&cfg)?)?;
            if running {
                crate::server::call(&paths.socket(), json!({"op":"remote_reload", "kind":"web"})).await?;
            }
            println!("web chat: on{} {}{}. In Telegram, send /web.",
                     if running { " and bound to" } else { "; starts with the daemon at" },
                     cfg.listen, cfg.public_url.as_ref().map(|u| format!(" (links: {u})")).unwrap_or_default());
            Ok(())
        }
        "disable" => {
            require_prod(home, prod)?;
            disable_web(home)?;
            match crate::server::call(&paths.socket(), json!({"op":"remote_reload", "kind":"web"})).await {
                Ok(_) => {},
                Err(e) if e.chain().filter_map(|c| c.downcast_ref::<std::io::Error>()).any(|e| matches!(e.kind(), std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused)) => {},
                Err(e) => return Err(e),
            }
            println!("web chat: off (open pages closed)");
            Ok(())
        }
        "status" => {
            match load_web(home) {
                Some(c) if c.enabled => println!("web chat: on (listen {}{})", c.listen, c.public_url.map(|u| format!(", links {u}")).unwrap_or_default()),
                _ => println!("web chat: off (optional). Turn it on with `ku web-link enable [--listen IP:PORT]` or send /web in Telegram."),
            }
            Ok(())
        }
        _ => bail!("usage: kurisu web-link enable [--listen IP:PORT] [--url URL] [--force] [--prod] | disable [--prod] | status"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn served_terminal_page_uses_shared_palette_and_theme() {
        // Exercise the rendered HTTP response, including the inline CSS/JS, not
        // just the template source. This route is private to this isolated test.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().route("/terminal", get(|| async { terminal_page("ABC234") }));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let response = reqwest::get(format!("http://{addr}/terminal")).await.unwrap();
        assert!(response.status().is_success());
        assert_eq!(response.headers()["cache-control"], "no-store");
        let html = response.text().await.unwrap();
        assert!(html.contains(PALETTE), "must embed the canonical palette verbatim");
        assert!(html.contains("background:var(--bg2)"));
        assert!(html.contains("color:var(--accent)"));
        assert!(html.contains("getComputedStyle(document.documentElement)"));
        assert!(html.contains(&TERMINAL_THEME.replace("export function terminalTheme", "function terminalTheme")));
        assert!(html.contains("scheme.addEventListener('change',recolor)"));
        assert!(html.contains("ABC234"));
        assert!(!html.contains("#111"), "the standalone dark palette must not return");
        assert!(!html.contains("__PALETTE__") && !html.contains("__TERMINAL_THEME__"));
        task.abort();
        for response in [browser_wait_page("ABC234"), web_wait_page("ABC234"), page("Kurisu", "Pairing")] {
            let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let html = std::str::from_utf8(&body).unwrap();
            assert!(html.contains(PALETTE));
            assert!(html.contains("background:var(--bg2)"));
            assert!(!html.contains("#111") && !html.contains("linear-gradient"));
            assert!(!html.contains("__PALETTE__"));
        }
    }

    // Real HTTP/WebSocket regression: simultaneous /web and /terminal clients
    // must keep independent shells; a duplicate legacy attach is a clean 409.
    async fn attach(addr: SocketAddr, path: &str, auth: &str) -> (tokio::net::TcpStream, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nOrigin: http://{addr}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n{auth}\r\n");
        tcp.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        while !response.ends_with(b"\r\n\r\n") {
            response.push(tokio::time::timeout(Duration::from_secs(10), tcp.read_u8()).await.unwrap().unwrap());
        }
        (tcp, String::from_utf8(response).unwrap())
    }

    async fn shell_pid(tcp: &mut tokio::net::TcpStream) -> u32 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // Client frames must be masked. A zero mask is valid for this test.
        let command = b"echo __KU_PID_$$\r";
        let mut frame = vec![0x81, 0x80 | command.len() as u8, 0, 0, 0, 0];
        frame.extend_from_slice(command);
        let mut seen = String::new();
        let mut retry = tokio::time::interval(Duration::from_millis(300));
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            let mut buf = [0; 8192];
            loop {
                let n = tokio::select! {
                    _ = retry.tick() => { tcp.write_all(&frame).await.unwrap(); continue; }
                    n = tcp.read(&mut buf) => n.unwrap(),
                };
                assert!(n > 0, "terminal disconnected: {seen}");
                seen.push_str(&String::from_utf8_lossy(&buf[..n]));
                for piece in seen.split("__KU_PID_").skip(1) {
                    let digits: String = piece.chars().take_while(char::is_ascii_digit).collect();
                    if !digits.is_empty() && piece.len() > digits.len() { return digits.parse().unwrap(); }
                }
            }
        }).await;
        result.unwrap_or_else(|e| panic!("shell must answer: {e}: {seen}"))
    }

    #[tokio::test]
    async fn authenticated_web_terminal_is_independent_and_second_attach_is_safe() {
        let dir = tempfile::tempdir().unwrap();
        let server = Arc::new(Server::new(crate::paths::Paths { home: dir.path().into() }).unwrap());
        for (cookie, kind) in [("owner-web", "web"), ("owner-terminal", "terminal")] {
            server.remote.inner.lock().await.sessions.insert(cookie.into(), Session {
                kind, ip: "127.0.0.1".into(), attempt: "test".into(), last: Instant::now(), shell: None, dir: None,
            });
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(Web { server: Arc::downgrade(&server), kind: "terminal" })
            .merge(crate::chat::router(Web { server: Arc::downgrade(&server), kind: "web" }));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (_, denied) = attach(addr, "/_web/terminal", "").await;
        assert!(denied.starts_with("HTTP/1.1 401"), "{denied}");
        let (mut legacy, ok) = attach(addr, "/ws", "Cookie: ku_terminal=owner-terminal\r\n").await;
        assert!(ok.starts_with("HTTP/1.1 101"), "{ok}");
        let legacy_pid = shell_pid(&mut legacy).await;
        let (_, refused) = attach(addr, "/ws", "Cookie: ku_terminal=owner-terminal\r\n").await;
        assert!(refused.starts_with("HTTP/1.1 409"), "{refused}");
        let state: Value = reqwest::Client::new().get(format!("http://{addr}/state"))
            .header("Cookie", "ku_terminal=owner-terminal").send().await.unwrap().json().await.unwrap();
        assert_eq!(state["attached"], true, "UI can explain the duplicate refusal");
        let (mut web, ok) = attach(addr, "/_web/terminal", "Cookie: ku_web=owner-web\r\n").await;
        assert!(ok.starts_with("HTTP/1.1 101"), "{ok}");
        let web_pid = shell_pid(&mut web).await;
        let (mut web2, ok) = attach(addr, "/_web/terminal", "Cookie: ku_web=owner-web\r\n").await;
        assert!(ok.starts_with("HTTP/1.1 101"), "{ok}");
        let web2_pid = shell_pid(&mut web2).await;
        assert_ne!(legacy_pid, web_pid);
        assert_ne!(web2_pid, web_pid);
        assert_ne!(web2_pid, legacy_pid);
        assert_eq!(shell_pid(&mut legacy).await, legacy_pid, "first shell remains responsive");
        let response = reqwest::get(format!("http://{addr}/_web/state")).await.unwrap();
        assert!(response.status().is_success(), "server still answers after second attach");
        // Revocation also closes owned panes, without touching the legacy shell.
        server.remote.inner.lock().await.sessions.remove("owner-web");
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(shell_pid(&mut legacy).await, legacy_pid);
        drop(web); drop(web2); drop(legacy);
        tokio::time::timeout(Duration::from_secs(10), async {
            while [legacy_pid, web_pid, web2_pid].iter().any(|pid| Path::new(&format!("/proc/{pid}")).exists()) {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }).await.expect("disconnect must reap every shell");
        task.abort();
    }

    #[tokio::test]
    async fn ku_web_terminal_requires_the_existing_owner_token() {
        let dir = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = crate::web::app(crate::paths::Paths { home: dir.path().into() }, addr, "owner-token".into());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (_, denied) = attach(addr, "/api/v1/terminal", "").await;
        assert!(denied.starts_with("HTTP/1.1 401"), "{denied}");
        let (mut tcp, ok) = attach(addr, "/api/v1/terminal", "Sec-WebSocket-Protocol: kurisu, owner-token\r\n").await;
        assert!(ok.starts_with("HTTP/1.1 101"), "{ok}");
        let pid = shell_pid(&mut tcp).await;
        drop(tcp);
        tokio::time::timeout(Duration::from_secs(10), async {
            while Path::new(&format!("/proc/{pid}")).exists() { tokio::time::sleep(Duration::from_millis(25)).await; }
        }).await.expect("close reaps shell");
        task.abort();
    }

    #[test]
    fn a_duplicate_request_reuses_the_pending_attempt() {
        let now = Instant::now();
        let attempt = |cookie: &str, token: &str, state| Attempt {
            token: token.into(), kind: "web", cookie: cookie.into(), code: "AB12CD".into(),
            ip: "127.0.0.1".into(), ua: "test".into(), state, expires: now + std::time::Duration::from_secs(60), expires_unix: 1,
            dir: None,
        };
        let mut attempts = std::collections::HashMap::new();
        attempts.insert("a".to_string(), attempt("cookie-1", "link-1", AttemptState::Pending));
        assert_eq!(pending_for(&attempts, "link-1", "cookie-1", now).map(|a| a.code.as_str()), Some("AB12CD"));
        // Another link or another browser starts fresh; a decided attempt is not reused.
        assert!(pending_for(&attempts, "link-2", "cookie-1", now).is_none());
        assert!(pending_for(&attempts, "link-1", "cookie-2", now).is_none());
        attempts.get_mut("a").unwrap().state = AttemptState::Approved;
        assert!(pending_for(&attempts, "link-1", "cookie-1", now).is_none());
    }

    #[test]
    fn detected_urls_and_local_only() {
        let d = Detected { ip: "100.118.224.35".parse().unwrap(), host: Some("mybox.tail.ts.net".into()), via: "Tailscale" };
        assert_eq!(d.url(8790), "http://mybox.tail.ts.net:8790");
        assert_eq!(d.listen(8790), "100.118.224.35:8790");
        let lan = Detected { ip: "192.168.0.58".parse().unwrap(), host: None, via: "LAN" };
        assert_eq!(lan.url(8793), "http://192.168.0.58:8793");
        assert!(is_local_only("127.0.0.1:8790") && is_local_only("0.0.0.0:8790") && !is_local_only("100.1.2.3:8790"));
        assert!(tailscale_ip("100.118.224.35".parse().unwrap()) && !tailscale_ip("100.200.0.1".parse().unwrap()));
    }

    #[test]
    fn direct_links_skip_the_public_tunnel_url() {
        // Telegram keeps the public (MagicDNS) URL; `ku web` gets the listener itself.
        assert_eq!(link_base("100.118.224.35:8791", Some("http://box.tail.ts.net:8791"), false), "http://box.tail.ts.net:8791");
        assert_eq!(link_base("100.118.224.35:8791", Some("http://box.tail.ts.net:8791"), true), "http://100.118.224.35:8791");
        assert_eq!(link_base("127.0.0.1:8791", None, true), "http://127.0.0.1:8791");
        assert_eq!(link_base("0.0.0.0:8791", None, true), "http://127.0.0.1:8791", "0.0.0.0 is not openable");
        assert_eq!(link_base("192.168.0.5:8791", Some("http://box.lan:8791"), true), "http://192.168.0.5:8791");
        assert!(is_tailscale_listen("100.118.224.35:8791"));
        assert!(!is_tailscale_listen("127.0.0.1:8791"));
    }

    #[test]
    fn web_link_config_roundtrip() {
        let dir = std::env::temp_dir().join(format!("ku-web-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        assert!(load_web(&dir).is_none());
        assert!(!disable_web(&dir).unwrap(), "nothing to disable");
        let d = Detected { ip: "100.118.224.35".parse().unwrap(), host: Some("mybox.tail.ts.net".into()), via: "Tailscale" };
        let c = enable_web(&dir, &d, WEB_PORT).unwrap();
        assert_eq!(c.listen, "100.118.224.35:8791");
        assert_eq!(c.public_url.as_deref(), Some("http://mybox.tail.ts.net:8791"));
        let back = load_web(&dir).unwrap();
        assert!(back.enabled);
        assert_eq!(back.listen, c.listen);
        assert!(disable_web(&dir).unwrap());
        assert!(!load_web(&dir).unwrap().enabled);
        assert_eq!(cookie_name("web"), "ku_web");
        assert_eq!(cookie_name("terminal"), "ku_terminal");
        assert_ne!(cookie_name("web"), cookie_name("terminal"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_homes_default_off_the_live_web_port() {
        let dir = std::env::temp_dir().join(format!("ku-web-port-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        assert_eq!(default_web_port(&dir), WEB_TEST_PORT);
        assert!(check_web_port(&dir, WEB_PORT, false).is_err(), "8791 is the live port");
        assert!(check_web_port(&dir, WEB_TEST_PORT, false).is_ok());
        assert!(check_web_port(&dir, WEB_PORT, true).is_ok(), "--prod is the explicit override");
        assert_eq!(endpoint_port("100.118.224.35:8791"), Some(8791));
        assert_eq!(endpoint_port("http://host:8791/_web/r/x"), Some(8791));
        assert_eq!(endpoint_port("http://[::1]:8791/x"), Some(8791));
        assert_eq!(endpoint_port("http://host/x"), None);
        assert!(check_web_endpoint(&dir, "http://host:8791/x", false).is_err());
        std::fs::write(dir.join("telegram.json"), b"{}").unwrap();
        if std::env::var_os("KURISU_TELEGRAM_API").is_none() {
            assert_eq!(default_web_port(&dir), WEB_PORT);
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn web_changes_refuse_a_telegram_home_without_prod() {
        let dir = std::env::temp_dir().join(format!("ku-web-prod-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        assert!(require_prod(&dir, false).is_ok(), "no telegram.json: a test home");
        std::fs::write(dir.join("telegram.json"), b"{}").unwrap();
        let err = require_prod(&dir, false).unwrap_err().to_string();
        assert!(err.contains("telegram.json") && err.contains("--prod"), "{err}");
        assert!(require_prod(&dir, true).is_ok(), "--prod is the explicit override");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_link_only_opens_on_its_own_listener() {
        assert!(link_kind_matches("web", "web").is_ok());
        assert!(link_kind_matches("terminal", "terminal").is_ok());
        let err = link_kind_matches("web", "terminal").unwrap_err();
        assert!(err.contains("/web"), "{err}");
        let err = link_kind_matches("terminal", "web").unwrap_err();
        assert!(err.contains("/terminal"), "{err}");
    }

    #[test]
    fn link_paths_per_kind() {
        let path = |kind: &str| match kind { "browser" => "/_ku/r/", "web" => "/_web/r/", _ => "/r/" };
        assert_eq!(path("web"), "/_web/r/");
        assert_eq!(path("browser"), "/_ku/r/");
        assert_eq!(path("terminal"), "/r/");
    }

    #[tokio::test]
    async fn a_terminal_shell_starts_in_the_chosen_directory() {
        use std::io::Write as _;
        let dir = std::env::temp_dir().join(format!("ku-term-cwd-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let dir = std::fs::canonicalize(&dir).unwrap();
        let home = std::env::temp_dir().join(format!("ku-term-home-{}", uuid::Uuid::new_v4()));
        let sh = spawn_shell(&home, Some(dir.to_str().unwrap())).unwrap();
        let mut rx = sh.out.subscribe();
        let recv_until = |rx: &mut tokio::sync::broadcast::Receiver<Vec<u8>>, seen: &mut String, needle: &str, secs: u64| {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
            while std::time::Instant::now() < deadline && !seen.contains(needle) {
                match rx.try_recv() {
                    Ok(b) if b.is_empty() => break,
                    Ok(b) => seen.push_str(&String::from_utf8_lossy(&b)),
                    Err(tokio::sync::broadcast::error::TryRecvError::Empty) => std::thread::sleep(std::time::Duration::from_millis(50)),
                    Err(_) => break,
                }
            }
        };
        // The login shell must be ready (first prompt) before the command is typed;
        // ask it for $PWD so an abbreviated prompt cannot hide the real directory.
        let mut seen = String::new();
        recv_until(&mut rx, &mut seen, "$ ", 20);
        sh.writer.lock().unwrap().write_all(b"echo $PWD\r").unwrap();
        recv_until(&mut rx, &mut seen, dir.to_str().unwrap(), 10);
        assert!(seen.contains(dir.to_str().unwrap()), "shell did not start in the chosen directory: {seen}");
        sh.kill();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&home).ok();
    }

    #[test]
    fn listen_addresses_and_codes() {
        assert!(check_listen("127.0.0.1:8790", false).is_ok());
        assert!(check_listen("100.101.102.103:8790", false).is_ok());
        assert!(check_listen("192.168.1.5:8790", false).is_ok());
        assert!(check_listen("0.0.0.0:8790", false).unwrap_err().to_string().contains("every interface"));
        assert!(check_listen("0.0.0.0:8790", true).is_ok());
        assert!(check_listen("8.8.8.8:8790", false).unwrap_err().to_string().contains("public"));
        assert!(check_listen("[fd7a:115c:a1e0::1]:8790", false).is_ok());
        assert!(check_listen("nope", false).is_err());
        let c = pairing_code();
        assert_eq!(c.len(), 6);
        assert!(c.chars().all(|ch| "ABCDEFGHJKMNPQRSTUVWXYZ23456789".contains(ch)));
        assert_eq!(short_ua("Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 Chrome/131.0 Mobile Safari/537.36"), "Android · Chrome");
    }
}
