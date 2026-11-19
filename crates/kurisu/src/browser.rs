//! Optional Live Browser add-on (github.com/anslwy/live-browser, binary `lb`).
//! Nothing happens until `ku browser setup`. Then:
//!   - every session (Claude and Codex) gets the `lb` MCP server through Kurisu's own
//!     per-session MCP config (never ~/.claude.json or ~/.codex/config.toml), pre-approved;
//!   - Kurisu runs `lb web` on its own port with a random passcode (lb listens on every
//!     interface and the passcode is its guard), starts it on demand and stops it after
//!     30 min without live sessions or viewers, and on shutdown;
//!   - `/browser` in Telegram opens the live view through Kurisu's paired, proxied link
//!     (the phone never sees the passcode).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

/// The lb release Kurisu targets: per-session tabs (live-browser PR #39) plus the
/// Kurisu-themed live view (`/browser` opens it with `?theme=kurisu`, which since
/// v0.4.22 is opt-in; lb's default look is its own). `ku browser
/// setup` uses an installed lb at least this new, else downloads this release.
pub const LB_PINNED: &str = "v0.4.22";
/// The commit of LB_PINNED, built from source when the release cannot be downloaded
/// (e.g. no release asset for this OS/arch).
pub const LB_PINNED_COMMIT: &str = "1a51fb8dcbaa";
pub const LB_REPO: &str = "https://github.com/anslwy/live-browser";
pub const IDLE: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub enabled: bool,
    pub lb: String,
    /// Port of Kurisu's own `lb web` (not lb's default 8765).
    pub port: u16,
    pub passcode: String,
    /// Where the `/browser` link listens (loopback, LAN or Tailscale).
    pub listen: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_url: Option<String>,
    /// lb web: max open tabs per agent session (LB_MAX_TABS; lb's default 5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tabs: Option<u32>,
    /// lb web: close tabs untouched this long (LB_TAB_IDLE_SECS; lb's default 1800, 0 = never).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tab_idle_secs: Option<u64>,
    /// Each session gets a private browser context (LB_ISOLATED): no shared logins.
    #[serde(default)]
    pub isolated: bool,
}

pub fn config_path(home: &Path) -> PathBuf {
    home.join("browser.json")
}

pub fn load(home: &Path) -> Option<Config> {
    serde_json::from_slice(&std::fs::read(config_path(home)).ok()?).ok()
}

pub fn active(home: &Path) -> Option<Config> {
    load(home).filter(|c| c.enabled && Path::new(&c.lb).is_file())
}

impl Config {
    pub fn server_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// The env for a session's `lb mcp`. Since lb d009bf7 (in v0.4.19) each `lb mcp` process is its own
    /// agent session (it owns its tabs and closes them when the CLI exits); LB_SESSION is
    /// the label the live view shows, so it is the Kurisu session's name.
    pub fn mcp_env(&self, session: &str) -> Value {
        let mut env = json!({"LB_SERVER": self.server_url(), "LB_PASSCODE": self.passcode, "LB_SESSION": session_label(session)});
        if self.isolated {
            env["LB_ISOLATED"] = json!("1");
        }
        env
    }

    /// The `mcpServers` entry a session gets.
    pub fn mcp_entry(&self, session: &str) -> Value {
        json!({"type": "stdio", "command": self.lb, "args": ["mcp"], "env": self.mcp_env(session)})
    }

    /// Env a session's CLI needs to run `lb` itself, not just the MCP server:
    /// `lb shot --tab N --out shot.jpg` reads LB_SERVER and LB_PASSCODE. These are
    /// the same two values the MCP entry gets, and the lb directory is put on PATH
    /// so the documented "page -> file -> send_file" sequence needs no guessing.
    pub fn shell_env(&self) -> Vec<(String, String)> {
        let dir = Path::new(&self.lb).parent().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default();
        let path = match std::env::var("PATH") {
            Ok(p) if !p.is_empty() && !dir.is_empty() => format!("{dir}:{p}"),
            Ok(p) => p,
            Err(_) => dir,
        };
        vec![
            ("LB_SERVER".into(), self.server_url()),
            ("LB_PASSCODE".into(), self.passcode.clone()),
            ("PATH".into(), path),
        ]
    }
}

/// A label lb accepts (it appends -<pid> for uniqueness): letters, digits, '-' and '_'.
pub fn session_label(name: &str) -> String {
    let l: String = name.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' }).take(40).collect();
    if l.trim_matches('-').is_empty() { "kurisu".into() } else { l }
}

/// Whether this lb has per-session tabs (`lb mcp --session`, live-browser PR #39).
pub fn supports_sessions(lb: &Path) -> bool {
    std::process::Command::new(lb).args(["mcp", "--help"]).output()
        .map(|o| String::from_utf8_lossy(&o.stdout).contains("LB_SESSION")).unwrap_or(false)
}

/// Build the pinned commit from source into `<home>/lb` (lb serves its UI from a
/// `web/dist` next to the binary). Needs git, cargo and npm.
async fn build_lb(home: &Path) -> Result<PathBuf> {
    for tool in ["git", "cargo", "npm"] {
        if crate::stt::which_bin(tool).is_none() {
            bail!("{tool} not found");
        }
    }
    // lb links OpenSSL on Linux (reqwest's default native-tls), found via pkg-config.
    if cfg!(target_os = "linux") {
        let ok = std::process::Command::new("pkg-config").args(["--exists", "openssl"]).status().is_ok_and(|s| s.success());
        if !ok {
            bail!("building lb needs pkg-config and the OpenSSL headers (Debian/Ubuntu: sudo apt install pkg-config libssl-dev; Fedora: pkg-config openssl-devel)");
        }
    }
    let dir = home.join("lb").join(format!("src-{LB_PINNED_COMMIT}"));
    let run = |cmd: &mut std::process::Command, what: &str| -> Result<()> {
        let st = cmd.status().with_context(|| what.to_string())?;
        if !st.success() {
            bail!("{what} failed");
        }
        Ok(())
    };
    if !dir.join(".git").exists() {
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.parent().unwrap())?;
        run(std::process::Command::new("git").args(["clone", "--quiet", LB_REPO]).arg(&dir), "git clone live-browser")?;
    }
    run(std::process::Command::new("git").arg("-C").arg(&dir).args(["checkout", "--quiet", LB_PINNED_COMMIT]), "git checkout")?;
    run(std::process::Command::new("npm").current_dir(dir.join("web")).args(["ci", "--silent", "--no-audit", "--no-fund"]), "npm ci (lb web UI)")?;
    run(std::process::Command::new("npm").current_dir(dir.join("web")).args(["run", "--silent", "build"]), "npm run build (lb web UI)")?;
    run(std::process::Command::new("cargo").current_dir(&dir).args(["build", "--release", "--quiet", "--locked"]), "cargo build (lb)")?;
    let bin = dir.join("target/release/lb");
    if !bin.is_file() {
        bail!("the build produced no lb binary");
    }
    Ok(bin)
}

pub(crate) fn find_lb() -> Option<PathBuf> {
    crate::stt::which_bin("lb").or_else(|| {
        let h = PathBuf::from(std::env::var("HOME").ok()?);
        [".local/bin/lb", ".local/share/live-browser/lb", ".kurisu/lb/lb"].iter().map(|p| h.join(p)).find(|p| p.is_file())
    })
}

fn lb_version(lb: &Path) -> Option<String> {
    let out = std::process::Command::new(lb).arg("--version").output().ok()?;
    String::from_utf8_lossy(&out.stdout).split_whitespace().nth(1).map(String::from)
}

/// Chrome engines lb can use (from `lb browsers`), e.g. "System Chrome: /usr/bin/chromium".
fn chrome_engines(lb: &Path) -> Vec<String> {
    let out = std::process::Command::new(lb).arg("browsers").output().map(|o| String::from_utf8_lossy(&o.stdout).into_owned()).unwrap_or_default();
    out.lines().filter(|l| l.contains(": /")).map(|l| l.trim().to_string()).collect()
}

/// The release asset for this OS/arch, or why there is none.
fn asset_for_this_host() -> Result<&'static str> {
    Ok(match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => "linux-x86_64",
        ("macos", "aarch64") => "darwin-arm64",
        ("macos", "x86_64") => "darwin-x86_64",
        (os, arch) => bail!("no Live Browser release for {os}/{arch}; install lb yourself (github.com/anslwy/live-browser) and run this again"),
    })
}

/// The pinned release download URL for an asset (e.g. "linux-x86_64").
fn release_url(asset: &str) -> String {
    format!("https://github.com/anslwy/live-browser/releases/download/{LB_PINNED}/live-browser-{LB_PINNED}-{asset}.tar.gz")
}

/// An lb already unpacked into `<home>/lb` for LB_PINNED (a previous download).
/// Deliberately exact: `<home>/lb` may also hold older releases.
fn local_pinned(home: &Path) -> Option<PathBuf> {
    let asset = asset_for_this_host().ok()?;
    let p = home.join("lb").join(format!("live-browser-{LB_PINNED}-{asset}")).join("lb");
    (p.is_file() && pin_ok(lb_version(&p).as_deref())).then_some(p)
}

async fn download_lb(home: &Path) -> Result<PathBuf> {
    let asset = asset_for_this_host()?;
    let url = release_url(asset);
    let bytes = reqwest::Client::new().get(&url).timeout(Duration::from_secs(120)).send().await?
        .error_for_status()?.bytes().await?;
    let dir = home.join("lb");
    std::fs::create_dir_all(&dir)?;
    let tgz = dir.join("lb.tar.gz");
    std::fs::write(&tgz, &bytes)?;
    let st = std::process::Command::new("tar").arg("-xzf").arg(&tgz).arg("-C").arg(&dir).status()?;
    let _ = std::fs::remove_file(&tgz);
    if !st.success() {
        bail!("could not unpack {url}");
    }
    // The archive holds `live-browser-<tag>-<asset>/lb` and its `web/dist`.
    let exact = dir.join(format!("live-browser-{LB_PINNED}-{asset}")).join("lb");
    if exact.is_file() {
        return Ok(exact);
    }
    // Layout changed: fall back to a search, but only inside this release's folder.
    let tag_dir = dir.join(format!("live-browser-{LB_PINNED}-{asset}"));
    walk_find(&tag_dir, "lb").context("the release archive has no lb binary")
}

/// lb at the pin: an install that already qualifies, else the copy under
/// `<home>/lb`, else the release download, else a build of LB_PINNED_COMMIT.
async fn obtain_pinned(home: &Path, existing: Option<&Path>) -> Result<PathBuf> {
    if let Some(p) = existing.filter(|p| pin_ok(lb_version(p).as_deref())) {
        return Ok(p.to_path_buf());
    }
    if let Some(p) = local_pinned(home) {
        return Ok(p);
    }
    match download_lb(home).await {
        Ok(p) if pin_ok(lb_version(&p).as_deref()) => Ok(p),
        Ok(p) => bail!("the downloaded lb at {} does not report {LB_PINNED}", p.display()),
        Err(de) => match build_lb(home).await {
            // A source build of LB_PINNED_COMMIT must report the pin too.
            Ok(p) if pin_ok(lb_version(&p).as_deref()) => Ok(p),
            Ok(p) => bail!("the source build at {} does not report {LB_PINNED} (download failed: {de:#})", p.display()),
            Err(e) => bail!("download failed ({de:#}) and the source build failed ({e:#})"),
        },
    }
}

fn walk_find(dir: &Path, name: &str) -> Option<PathBuf> {
    for e in std::fs::read_dir(dir).ok()?.flatten() {
        let p = e.path();
        if p.is_dir() {
            if let Some(f) = walk_find(&p, name) {
                return Some(f);
            }
        } else if p.file_name().and_then(|n| n.to_str()) == Some(name) {
            return Some(p);
        }
    }
    None
}

/// `ver` ("0.4.19", or "lb 0.4.19 (…)") is at least `min` ("v0.4.19"), numerically.
pub fn at_least(ver: &str, min: &str) -> bool {
    let nums = |s: &str| -> Vec<u64> {
        let s = s.split_whitespace().find(|w| w.trim_start_matches('v').starts_with(|c: char| c.is_ascii_digit())).unwrap_or("");
        s.trim_start_matches('v').split(|c: char| !c.is_ascii_digit()).take(3).map(|n| n.parse().unwrap_or(0)).collect()
    };
    nums(ver) >= nums(min)
}

/// An lb version Kurisu accepts as the pin: inside the 0.4.x/0.5.x allowlist and
/// at or above LB_PINNED. An older binary (or none) must not be mistaken for the
/// pin — that mismatch is how a stale 0.4.19 kept serving the old UI.
pub fn pin_ok(ver: Option<&str>) -> bool {
    ver.is_some_and(|v| (v.starts_with("0.4") || v.starts_with("0.5")) && at_least(v, LB_PINNED))
}

fn random_passcode() -> String {
    format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple())
}

/// Set up the add-on: find or download lb, check for Chrome, pick the /browser address
/// (the Tailscale/LAN address unless given) and write browser.json. Returns a summary.
pub async fn setup(home: &Path, listen: Option<String>, url: Option<String>, port: u16, lb: Option<PathBuf>, verbose: bool) -> Result<String> {
    let explicit = lb.filter(|p| p.is_file());
    let found = explicit.clone().or_else(find_lb);
    let current = |p: &PathBuf| supports_sessions(p) && pin_ok(lb_version(p).as_deref());
    let lb = match found {
        // --lb PATH is the user's choice; an auto-found lb must also be at least LB_PINNED.
        Some(p) if (explicit.is_some() && supports_sessions(&p)) || current(&p) => p,
        other => {
            if verbose {
                println!("downloading Live Browser {LB_PINNED} ...");
            }
            match obtain_pinned(home, None).await {
                Ok(p) => p,
                Err(e) => {
                    let p = other.ok_or_else(|| anyhow::anyhow!("could not get lb {LB_PINNED} ({e:#})"))?;
                    eprintln!("warning: could not get lb {LB_PINNED} ({e:#}); using the older {}{}.", p.display(),
                              if supports_sessions(&p) { "" } else { " without per-session tabs (sessions share tabs)" });
                    p
                }
            }
        }
    };
    let ver = lb_version(&lb).context("that lb does not run")?;
    if !ver.starts_with("0.4") && !ver.starts_with("0.5") {
        bail!("lb {ver} is not a version Kurisu knows (0.4.x/0.5.x)");
    }
    let engines = chrome_engines(&lb);
    if engines.is_empty() {
        bail!("Live Browser found no Chrome on this machine. Install Google Chrome or Chromium there, or run `{} browsers download` on it, then try again.", lb.display());
    }
    let (listen, public_url) = match listen {
        Some(l) => (l, url),
        None => {
            let d = crate::remote::detect_address().await.context(crate::remote::NO_ADDRESS)?;
            (d.listen(8793), url.or_else(|| d.host.as_ref().map(|_| d.url(8793))))
        }
    };
    let old = load(home);
    let (max_tabs, tab_idle_secs, isolated) = old.as_ref().map(|o| (o.max_tabs, o.tab_idle_secs, o.isolated)).unwrap_or((None, None, false));
    let cfg = Config { enabled: true, lb: lb.to_string_lossy().into_owned(), port,
                       passcode: old.map(|o| o.passcode).filter(|p| p.len() >= 32).unwrap_or_else(random_passcode),
                       listen: listen.clone(), public_url: public_url.clone(), max_tabs, tab_idle_secs, isolated };
    crate::telegram::write_private(&config_path(home), &serde_json::to_vec_pretty(&cfg)?)?;
    Ok(format!("Live Browser: on (lb {ver}; {}). /browser links: {}.", engines.first().cloned().unwrap_or_default(),
               public_url.unwrap_or_else(|| format!("http://{listen}"))))
}

pub async fn cli(paths: &crate::paths::Paths, args: &[String]) -> Result<()> {
    let home = &paths.home;
    let force = args.iter().any(|a| a == "--force");
    let flag = |n: &str| args.iter().position(|a| a == n).and_then(|i| args.get(i + 1)).cloned();
    match args.get(2).map(String::as_str).unwrap_or("status") {
        "setup" => {
            let listen = flag("--listen");
            if let Some(l) = &listen {
                crate::remote::check_listen(l, force)?;
            }
            let port: u16 = flag("--port").map(|p| p.parse()).transpose().context("--port")?.unwrap_or(8792);
            let summary = setup(home, listen, flag("--url"), port, flag("--lb").map(PathBuf::from), true).await?;
            println!("{summary}");
            println!("Every session gets the lb MCP tools (from its next start). In Telegram, /browser opens the live view (approve the pairing code on the card).");
            let _ = crate::server::call(&paths.socket(), json!({"op": "browser_reload"})).await;
            Ok(())
        }
        "off" | "on" => {
            let mut c = load(home).context("Live Browser is not set up: ku browser setup")?;
            c.enabled = sub_on(args);
            crate::telegram::write_private(&config_path(home), &serde_json::to_vec_pretty(&c)?)?;
            let _ = crate::server::call(&paths.socket(), json!({"op": "browser_reload"})).await;
            println!("Live Browser: {} (sessions pick it up from their next start)", if c.enabled { "on" } else { "off" });
            Ok(())
        }
        "status" => {
            println!("{}", status_line(home, crate::server::call(&paths.socket(), json!({"op": "browser_status"})).await.ok().map(|v| v["running"] == json!(true))));
            Ok(())
        }
        _ => bail!("usage: kurisu browser setup [--listen IP:PORT] [--port N] [--url URL] [--lb PATH] | status | off | on"),
    }
}

fn sub_on(args: &[String]) -> bool {
    args.get(2).map(String::as_str) == Some("on")
}

/// "browser: on (lb 0.4.19, running)" or the optional hint.
pub fn status_line(home: &Path, running: Option<bool>) -> String {
    match load(home) {
        Some(c) if c.enabled => {
            let ver = lb_version(Path::new(&c.lb)).unwrap_or_else(|| "missing".into());
            // Name the mismatch instead of hiding it: the binary on disk can be
            // older than the pin (set up before the pin moved).
            let pin = if pin_ok(Some(&ver)) { String::new() } else { format!(" (pinned {LB_PINNED}; upgrades on next start)") };
            format!("browser: on (lb {ver}{pin}, {}; /browser link on {})", match running { Some(true) => "running", Some(false) => "stopped until needed", None => "server not running" }, c.listen)
        }
        Some(_) => "browser: off (ku browser on)".into(),
        None => "browser: not set up (optional: ku browser setup gives every session a real Chrome, and /browser the live view)".into(),
    }
}

/// A process's command line (Linux /proc, macOS `ps`). Used only to refuse to
/// touch a listener that is not lb.
fn process_args(pid: u32) -> String {
    #[cfg(target_os = "linux")]
    {
        return std::fs::read(format!("/proc/{pid}/cmdline")).map(|b| String::from_utf8_lossy(&b).replace('\0', " ")).unwrap_or_default();
    }
    #[cfg(target_os = "macos")]
    {
        return std::process::Command::new("ps").args(["-p", &pid.to_string(), "-o", "command="])
            .output().map(|o| String::from_utf8_lossy(&o.stdout).into_owned()).unwrap_or_default();
    }
    #[allow(unreachable_code)]
    String::new()
}

/// PIDs listening on a local TCP port (Linux /proc, macOS `lsof`).
fn port_listeners(port: u16) -> Vec<u32> {
    #[cfg(target_os = "linux")]
    {
        let mut inodes = std::collections::HashSet::new();
        for f in ["/proc/net/tcp", "/proc/net/tcp6"] {
            let Ok(txt) = std::fs::read_to_string(f) else { continue };
            for line in txt.lines().skip(1) {
                let c: Vec<&str> = line.split_whitespace().collect();
                // st "0A" = LISTEN; local address is hex "ADDR:PORT"; inode is the 10th field.
                if c.len() < 10 || c[3] != "0A" { continue; }
                if u16::from_str_radix(c[1].rsplit(':').next().unwrap_or(""), 16) != Ok(port) { continue; }
                if let Ok(i) = c[9].parse::<u64>() { inodes.insert(i); }
            }
        }
        if inodes.is_empty() {
            return vec![];
        }
        let mut pids = vec![];
        for e in std::fs::read_dir("/proc").into_iter().flatten().flatten() {
            let Ok(pid) = e.file_name().to_string_lossy().parse::<u32>() else { continue };
            let Ok(fds) = std::fs::read_dir(e.path().join("fd")) else { continue };
            for fd in fds.flatten() {
                let Ok(t) = std::fs::read_link(fd.path()) else { continue };
                let Some(n) = t.to_string_lossy().strip_prefix("socket:[").and_then(|s| s.strip_suffix(']')).map(String::from) else { continue };
                if n.parse::<u64>().is_ok_and(|i| inodes.contains(&i)) {
                    pids.push(pid);
                    break;
                }
            }
        }
        return pids;
    }
    #[cfg(target_os = "macos")]
    {
        let out = std::process::Command::new("lsof").args(["-nP", "-t", &format!("-iTCP:{port}"), "-sTCP:LISTEN"]).output();
        return out.map(|o| String::from_utf8_lossy(&o.stdout).lines().filter_map(|l| l.trim().parse::<u32>().ok()).collect()).unwrap_or_default();
    }
    #[allow(unreachable_code)]
    {
        let _ = port;
        vec![]
    }
}

/// Stop an `lb web` that still serves this port from the previous binary. On a
/// pin upgrade the config changes but the answering process does not — the exact
/// "new binary on disk, old one running" mismatch that kept the old UI alive.
async fn retire_daemon(port: u16) {
    for pid in port_listeners(port) {
        if !process_args(pid).contains("lb") {
            continue; // never touch something that is not lb, even on our port
        }
        let _ = std::process::Command::new("kill").args(["-TERM", &pid.to_string()]).stderr(Stdio::null()).status();
        for _ in 0..20 {
            std::thread::sleep(Duration::from_millis(100));
            let alive = std::process::Command::new("kill").args(["-0", &pid.to_string()]).stderr(Stdio::null()).status().is_ok_and(|s| s.success());
            if !alive {
                break;
            }
        }
        let _ = std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).stderr(Stdio::null()).status();
        eprintln!("browser: stopped the older lb web still serving the live view (pid {pid})");
    }
}

/// Kurisu's own `lb web`: started on demand, stopped when idle or on shutdown.
#[derive(Default)]
pub struct Supervisor {
    child: Mutex<Option<Child>>,
    last_use: Mutex<Option<Instant>>,
    /// One attempt per process to bring the install up to LB_PINNED.
    synced: Mutex<bool>,
}

impl Supervisor {
    pub async fn healthy(cfg: &Config) -> bool {
        reqwest::Client::new().get(format!("{}/api/health", cfg.server_url())).header("x-lb-passcode", &cfg.passcode)
            .timeout(Duration::from_secs(3)).send().await.is_ok_and(|r| r.status().is_success())
    }

    pub fn pid(&self) -> Option<u32> {
        self.child.try_lock().ok()?.as_ref().and_then(Child::id)
    }

    pub async fn running(&self) -> bool {
        self.child.lock().await.as_mut().is_some_and(|c| c.try_wait().ok().flatten().is_none())
    }

    /// Bring the installed lb up to LB_PINNED and point browser.json at it.
    ///
    /// `ku browser setup` only version-checks what it finds *at setup time*, and
    /// the config stores an absolute path. So on a box set up before the pin
    /// moved, Kurisu kept spawning that older binary — which is exactly how the
    /// live view served 0.4.19 while LB_PINNED was v0.4.20. Called at server
    /// start and before every lb start; download, else source build; keeps the
    /// old binary (with a warning) when both fail.
    pub async fn sync_pin(&self, home: &Path) {
        let mut synced = self.synced.lock().await;
        if *synced {
            return;
        }
        *synced = true;
        let Some(mut cfg) = load(home) else { return };
        if !cfg.enabled {
            return;
        }
        let have = lb_version(Path::new(&cfg.lb));
        if pin_ok(have.as_deref()) {
            return;
        }
        eprintln!("browser: lb {} is older than the pinned {LB_PINNED}; getting the pinned build ...",
                  have.as_deref().unwrap_or("missing"));
        match obtain_pinned(home, None).await {
            Ok(p) => {
                let ver = lb_version(&p).unwrap_or_default();
                if p != Path::new(&cfg.lb) {
                    cfg.lb = p.to_string_lossy().into_owned();
                    if let Ok(bytes) = serde_json::to_vec_pretty(&cfg) {
                        if let Err(e) = crate::telegram::write_private(&config_path(home), &bytes) {
                            eprintln!("browser: could not save the new lb path: {e:#}");
                        }
                    }
                }
                eprintln!("browser: using lb {ver} ({})", p.display());
                // The config alone does not change the process already answering:
                // stop ours (if any) and any older daemon on this port.
                self.stop().await;
                retire_daemon(cfg.port).await;
            }
            Err(e) => eprintln!("browser: could not get lb {LB_PINNED} ({e:#}); keeping {} (the live view stays unthemed)", cfg.lb),
        }
    }

    /// Make sure `lb web` answers; start it (own process group, so it and its Chrome are
    /// stopped with Kurisu and never left behind) if it does not.
    pub async fn ensure(&self, home: &Path, cfg: &Config) -> Result<()> {
        *self.last_use.lock().await = Some(Instant::now());
        self.sync_pin(home).await;
        // sync_pin may have rewritten browser.json to the pinned binary.
        let current = load(home).filter(|c| c.enabled);
        let cfg = current.as_ref().unwrap_or(cfg);
        if Self::healthy(cfg).await {
            return Ok(());
        }
        let mut g = self.child.lock().await;
        if g.as_mut().is_some_and(|c| c.try_wait().ok().flatten().is_none()) {
            drop(g);
        } else {
            let dir = home.join("browser");
            std::fs::create_dir_all(&dir)?;
            let log = std::fs::OpenOptions::new().create(true).append(true).open(dir.join("lb-web.log"))?;
            let mut cmd = Command::new(&cfg.lb);
            cmd.args(["web", "--port", &cfg.port.to_string(), "--foreground", "--no-open", "--profile"]).arg(dir.join("profile"))
                .env("LB_PASSCODE", &cfg.passcode).stdin(Stdio::null()).stdout(log.try_clone()?).stderr(log).kill_on_drop(true);
            if let Some(n) = cfg.max_tabs {
                cmd.env("LB_MAX_TABS", n.to_string());
            }
            if let Some(n) = cfg.tab_idle_secs {
                cmd.env("LB_TAB_IDLE_SECS", n.to_string());
            }
            #[cfg(unix)]
            cmd.process_group(0);
            *g = Some(cmd.spawn().with_context(|| format!("start {} web", cfg.lb))?);
            drop(g);
        }
        for _ in 0..60 {
            if Self::healthy(cfg).await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        bail!("lb web did not start (see {}/browser/lb-web.log)", home.display())
    }

    pub async fn touch(&self) {
        *self.last_use.lock().await = Some(Instant::now());
    }

    /// Stop it: SIGTERM to its process group (lb reaps its browsers), then SIGKILL.
    pub async fn stop(&self) {
        let Some(mut c) = self.child.lock().await.take() else { return };
        #[cfg(unix)]
        if let Some(pid) = c.id() {
            // The whole group (lb and its Chrome) must go, not just lb.
            let group = format!("-{pid}");
            let alive = || std::process::Command::new("kill").args(["-0", "--", &group]).stderr(Stdio::null()).status().is_ok_and(|s| s.success());
            let _ = std::process::Command::new("kill").args(["-TERM", "--", &group]).stderr(Stdio::null()).status();
            for _ in 0..30 {
                let _ = c.try_wait();
                if !alive() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let _ = std::process::Command::new("kill").args(["-KILL", "--", &group]).stderr(Stdio::null()).status();
        }
        let _ = c.kill().await;
    }

    /// Stop after `IDLE` with no live sessions and no viewers.
    pub async fn tick(&self, busy: bool) {
        if busy {
            self.touch().await;
            return;
        }
        let idle = self.last_use.lock().await.is_some_and(|t| t.elapsed() > IDLE);
        if idle && self.running().await {
            eprintln!("browser: stopping lb web (idle 30 min)");
            self.stop().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lb_version_floor() {
        assert!(at_least("0.4.22", LB_PINNED) && at_least("lb 0.4.21", "v0.4.20") && at_least("0.5.0", LB_PINNED));
        assert!(!at_least("0.4.21", LB_PINNED) && !at_least("lb 0.4.9", "v0.4.20") && !at_least("", "v0.4.20"));
    }

    #[test]
    fn mcp_entry_and_status() {
        assert_eq!(session_label("kurisu dev/2"), "kurisu-dev-2");
        assert_eq!(session_label("✨"), "kurisu");
        let d = tempfile::tempdir().unwrap();
        assert!(status_line(d.path(), None).contains("not set up"));
        let c = Config { enabled: true, lb: "/x/lb".into(), port: 8792, passcode: random_passcode(), listen: "127.0.0.1:8793".into(), public_url: None,
                            max_tabs: None, tab_idle_secs: None, isolated: false };
        assert_eq!(c.passcode.len(), 64);
        let e = c.mcp_entry("kurisu dev/2");
        assert_eq!(e["args"], json!(["mcp"]));
        assert_eq!(e["env"]["LB_SERVER"], "http://127.0.0.1:8792");
        assert_eq!(e["env"]["LB_SESSION"], "kurisu-dev-2");
        assert!(e["env"].get("LB_ISOLATED").is_none());
        assert!(active(d.path()).is_none());
    }

    /// A session's shell must be able to run `lb shot --out` itself: the two
    /// values the CLI reads, plus the lb directory on PATH so the documented
    /// "page -> file -> send_file" sequence needs no guessing.
    #[test]
    fn shell_env_carries_the_lb_cli_contract() {
        let c = Config { enabled: true, lb: "/opt/live-browser/bin/lb".into(), port: 8792, passcode: "p".repeat(64),
                           listen: "127.0.0.1:8793".into(), public_url: None, max_tabs: None, tab_idle_secs: None, isolated: false };
        let env: std::collections::HashMap<String, String> = c.shell_env().into_iter().collect();
        assert_eq!(env["LB_SERVER"], "http://127.0.0.1:8792");
        assert_eq!(env["LB_PASSCODE"], c.passcode);
        assert!(env["PATH"].starts_with("/opt/live-browser/bin:"), "PATH must put lb first: {}", env["PATH"]);
        // The MCP entry and a shell command must agree on server and passcode.
        assert_eq!(c.mcp_entry("w")["env"]["LB_SERVER"], json!(env["LB_SERVER"]));
        assert_eq!(c.mcp_entry("w")["env"]["LB_PASSCODE"], json!(env["LB_PASSCODE"]));
    }

    /// The regression guard: the binary on disk must report (at least) LB_PINNED,
    /// and when it does not, status names the pin instead of calling it current.
    #[cfg(unix)]
    #[test]
    fn installed_lb_must_match_the_pin() {
        use std::os::unix::fs::PermissionsExt;
        let d = tempfile::tempdir().unwrap();
        let fake = |name: &str, ver: &str| {
            let p = d.path().join(name);
            std::fs::write(&p, format!("#!/bin/sh\n[ \"$1\" = --version ] && echo 'lb {ver}'\nexit 0\n")).unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
            p
        };
        let old = fake("lb-old", "0.4.21");
        let pinned = fake("lb-pin", &LB_PINNED[1..]);
        assert_eq!(lb_version(&old).as_deref(), Some("0.4.21"));
        assert!(!pin_ok(lb_version(&old).as_deref()), "0.4.21 must not count as the v0.4.22 pin");
        assert!(pin_ok(lb_version(&pinned).as_deref()));
        assert!(pin_ok(Some("0.5.0")) && !pin_ok(Some("0.3.9")) && !pin_ok(None));

        let cfg = Config { enabled: true, lb: old.to_string_lossy().into(), port: 8792, passcode: random_passcode(),
                           listen: "127.0.0.1:8793".into(), public_url: None, max_tabs: None, tab_idle_secs: None, isolated: false };
        crate::telegram::write_private(&config_path(d.path()), &serde_json::to_vec(&cfg).unwrap()).unwrap();
        let line = status_line(d.path(), None);
        assert!(line.contains("lb 0.4.21") && line.contains(LB_PINNED), "status must name the pin: {line}");
    }

    /// Only `<home>/lb/live-browser-<pin>-<asset>/lb` counts as an already
    /// unpacked pinned build; older releases under `<home>/lb` are ignored.
    #[cfg(unix)]
    #[test]
    fn local_pinned_finds_the_tagged_build_only() {
        use std::os::unix::fs::PermissionsExt;
        let d = tempfile::tempdir().unwrap();
        let asset = asset_for_this_host().unwrap();
        let make = |tag: &str, ver: &str| {
            let dir = d.path().join("lb").join(format!("live-browser-{tag}-{asset}"));
            std::fs::create_dir_all(&dir).unwrap();
            let p = dir.join("lb");
            std::fs::write(&p, format!("#!/bin/sh\necho 'lb {ver}'\n")).unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
            p
        };
        make("v0.4.19", "0.4.19");
        assert_eq!(local_pinned(d.path()), None, "an older release dir must not satisfy the pin");
        make(&LB_PINNED.to_string(), &LB_PINNED[1..]);
        assert_eq!(local_pinned(d.path()), Some(d.path().join("lb").join(format!("live-browser-{LB_PINNED}-{asset}")).join("lb")));
    }

    /// The fix itself: an install older than the pin is replaced by the pinned
    /// build already under `<home>/lb`, and browser.json is rewritten to point at
    /// it (no network: the pinned copy is present).
    #[cfg(unix)]
    #[tokio::test]
    async fn sync_pin_rewrites_config_to_the_pinned_build() {
        use std::os::unix::fs::PermissionsExt;
        let d = tempfile::tempdir().unwrap();
        let script = |p: &Path, ver: &str| {
            std::fs::write(p, format!("#!/bin/sh\n[ \"$1\" = --version ] && echo 'lb {ver}'\nexit 0\n")).unwrap();
            std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o755)).unwrap();
        };
        let old = d.path().join("lb-old");
        script(&old, "0.4.19");
        let asset = asset_for_this_host().unwrap();
        let dir = d.path().join("lb").join(format!("live-browser-{LB_PINNED}-{asset}"));
        std::fs::create_dir_all(&dir).unwrap();
        let pinned = dir.join("lb");
        script(&pinned, &LB_PINNED[1..]);

        let cfg = Config { enabled: true, lb: old.to_string_lossy().into(), port: 39123, passcode: random_passcode(),
                           listen: "127.0.0.1:39124".into(), public_url: None, max_tabs: None, tab_idle_secs: None, isolated: false };
        crate::telegram::write_private(&config_path(d.path()), &serde_json::to_vec(&cfg).unwrap()).unwrap();

        Supervisor::default().sync_pin(d.path()).await;

        let now = load(d.path()).unwrap();
        assert_eq!(std::path::Path::new(&now.lb), pinned, "browser.json must point at the pinned build");
        assert!(pin_ok(lb_version(std::path::Path::new(&now.lb)).as_deref()));
        assert!(old.is_file(), "the old install is left alone (manual rollback)");
    }

    #[test]
    fn release_urls_follow_the_pin() {
        let u = release_url("linux-x86_64");
        assert_eq!(u, format!("https://github.com/anslwy/live-browser/releases/download/{LB_PINNED}/live-browser-{LB_PINNED}-linux-x86_64.tar.gz"));
        assert!(u.contains(&LB_PINNED));
        // The source-build fallback checks out this short commit; both must name
        // the same release, so a pin bump touches both.
        assert_eq!(LB_PINNED_COMMIT.len(), 12, "LB_PINNED_COMMIT is a 12-char short hash");
        assert!(LB_PINNED_COMMIT.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(LB_PINNED.starts_with('v') && at_least(&LB_PINNED[1..], "0.4.20"));
    }
}
