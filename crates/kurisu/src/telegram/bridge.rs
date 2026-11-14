//! Turn relay, queue, commands, attachments and the update loop (Python bridge,
//! Kurisu backend, function for function).

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio::time::Instant;

use super::backend::{disabled_model, item_text};
use super::tg::{failed_before_delivery, is_network_error, outage_notice_text, quoted_message_text, MAX_TG_CHARS, NET_BACKOFF_CAP_S};
use super::{clen, log, now_f, now_i, squash, take, token, vstr, Bridge, Ctx, Job, JobKind};

pub const TURN_TIMEOUT_S: u64 = 15 * 60;
pub const LIST_PAGE_SIZE: usize = 5;
/// Models shown per page in /model (a phone fits about eight model buttons).
pub const MODEL_PAGE_SIZE: usize = 8;
pub const MODEL_CARD_TTL_S: f64 = 24.0 * 60.0 * 60.0;
pub const MODEL_SEARCH_TTL_S: i64 = 10 * 60;
pub const MODEL_CARD_LIMIT: usize = 20;
pub const MCP_CARD_LIMIT: usize = 10;
/// /terminal's directory picker lives as long as the links it issues (5 min),
/// so a stale card can never mint a link long after it was sent.
pub const TERMINAL_CARD_TTL_S: f64 = 5.0 * 60.0;
pub const TERMINAL_CARD_LIMIT: usize = 10;
pub const USAGE_STALE_AFTER_S: i64 = 900;
const JOB_RETRY_BUDGET_S: u64 = 7200;
/// Kurisu's own per-attachment limit (the bridge's 7 MB was Omnigent's event limit).
const MAX_IMAGE_BYTES: usize = 25 * 1024 * 1024;

/// The periodic progress update, bounded to a single Telegram message. The
/// activity title carries a full command now, so keep the whole line under
/// `MAX_TG_CHARS` instead of letting `send_text` split or drop it.
#[cfg(test)]
fn progress_line(activity: &str, elapsed: u64) -> String {
    crate::channels::dispatch::format_progress(
        activity,
        elapsed,
        crate::channels::dispatch::ProgressLimit::Chars(MAX_TG_CHARS),
    )
}

pub const CATCH_UP_PROMPT: &str = "The user sent /catch_up. Generate a concise, self-contained operational overview of the Kurisu sessions you created or delegated on the user's behalf.

Use the existing conversation context and available read-only session tools (the kurisu-mcp tools peers_list, peer_get, session_status, session_history and board_read) to identify and inspect those sessions. Do not assume that an idle session is unfinished or that a completed session is still running.

Report:
1. Anything requiring the user's attention first: blockers, questions, pending approvals, failed work, and the exact decision or action needed.
2. Completed work and its outcome, including relevant PR/issue links or commits when available.
3. Work still in progress, its current state, and the next expected milestone.

Exclude this catch-up request itself and omit irrelevant old idle sessions. If session attribution or status is uncertain, say so rather than guessing. If nothing requires attention, state that explicitly.

This is a read-only review: do not create, message, resume, stop, or otherwise modify sessions, and do not change code, services, credentials, or settings. Return the overview directly for Telegram.";

const HELP: &str = "Kurisu Telegram bridge

/list — five most recent sessions, with paging
/status — current session status and context usage
/catch-up — summarize delegated sessions and blockers
/model — choose or switch the current session model
/mcp — list MCP servers and switch them on or off
/schedule — pending wakes (scheduled messages), with Cancel buttons
/version — Running Kurisu server build
/usage — Installed CLI plan usage and reset times (Claude, Codex; OpenCode quota unavailable)
/send-now — send queued messages to the active run
/compact — pick a session to compact (/compact this = current)
/stop — interrupt the run in the current session
/undo — take back the last turn: its file changes, its history, and its Telegram messages
/mode — permission mode of the current session (auto, default, …)
/terminal — one-time link to a shell in your browser (/terminal close, /terminal disable)
/browser — one-time link to the agents' live browser (/browser close, /browser disable)
/web — one-time link to the chat UI: sessions, history and replies (/web close, /web disable)
/id — current session ID
/new — start a fresh session
/use <id|prefix> — switch to an existing session

Final replies from every Kurisu session are broadcast here.
Approval prompts appear with Approve / Reject buttons.
Questions appear with one button per option; while one is
pending, your next text message answers it instead.
Send text, photos, or files (PDF, text, code, zip, and
most other types). A caption is sent with the attachment.";

#[derive(Clone, Copy, PartialEq)]
pub enum Announce {
    No,
    Yes,
    Busy,
}

fn busy(status: &str) -> bool {
    matches!(status, "running" | "starting" | "waiting")
}

fn fmt_int(n: i64) -> String {
    let s = n.abs().to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    if n < 0 { format!("-{out}") } else { out }
}

fn fmt_money(x: f64) -> String {
    let cents = (x * 100.0).round() as i64;
    format!("{}.{:02}", fmt_int(cents / 100), (cents % 100).abs())
}

/// Python "{:g}".format(round(x, 1)).
fn fmt_g(x: f64) -> String {
    let r = format!("{x:.1}");
    r.strip_suffix(".0").map(String::from).unwrap_or(r)
}

fn local_fmt(ts: i64, f: &str) -> String {
    chrono::DateTime::from_timestamp(ts, 0).map(|d| d.with_timezone(&chrono::Local).format(f).to_string()).unwrap_or_default()
}

/// Text already accumulated for the turn, split into what Telegram has and
/// what is still waiting for the next batching flush.
#[derive(Default)]
pub(crate) struct TurnStream {
    sent: String,
    pending: String,
    /// The Telegram message that currently holds the end of this turn's text:
    /// its id, the exact text it was sent with, and the assistant message the
    /// text came from (when the harness reports one). A short tail is appended
    /// to it with editMessageText, so the word a flush split off never lands
    /// as a bubble of its own -- but never across two assistant messages.
    last: Option<(i64, String, Option<String>)>,
    /// The assistant message the pending text belongs to, when reported.
    msg: Option<String>,
    /// The finished row the relay already delivered is the user's copy of
    /// that message. The drain of the event watch lags the item poll, so its
    /// last delta can arrive *after* the row; that text is already on screen
    /// and must not be streamed again (live 2026-11-09: a late " merged or
    /// deployed." was flushed and re-appended as " merged or").
    settled: bool,
    settled_msg: Option<String>,
}

/// A reconciled tail at most this long is the fragment a timed flush held
/// back, not a message in its own right.
pub(crate) const APPEND_TAIL_MAX: usize = 120;

impl TurnStream {
    fn push(&mut self, text: &str, msg: Option<&str>) {
        if self.settled {
            // Only a delta from a different assistant message is new text;
            // everything else is the row's lagging tail.
            match (self.settled_msg.as_deref(), msg) {
                (Some(done), Some(next)) if next != done => {
                    self.settled = false;
                    self.settled_msg = None;
                }
                _ => return,
            }
        }
        if let Some(msg) = msg {
            self.msg = Some(msg.to_string());
        }
        self.pending.push_str(text);
    }

    /// The next text belongs to a new assistant message: whatever bubble was
    /// last sent must not be its append target.
    fn close(&mut self) {
        self.last = None;
    }

    fn has_text(&self) -> bool {
        !self.sent.is_empty() || !self.pending.is_empty()
    }

    fn has_pending_text(&self) -> bool {
        !self.pending.trim().is_empty()
    }

    /// A flush is due on a sentence/paragraph boundary, after ~800 chars,
    /// or after the batching interval. Whitespace-only buffers never send.
    fn ready(&self, since: Duration) -> bool {
        let trimmed = self.pending.trim();
        if trimmed.is_empty() {
            return false;
        }
        since >= Duration::from_secs(2)
            || self.pending.chars().count() >= 800
            || matches!(trimmed.chars().next_back(), Some('.' | '!' | '?' | '\n' | '。' | '！' | '？' | '…'))
    }

    fn flush(&mut self) -> Option<String> {
        if self.pending.trim().is_empty() {
            return None;
        }
        let out = std::mem::take(&mut self.pending);
        self.sent.push_str(&out);
        Some(out)
    }

    /// Hold back the unfinished atomic token, like EventWatch holds the next
    /// message while earlier prose flushes. Only real message boundaries may
    /// force the complete pending buffer out.
    fn flush_partial(&mut self) -> Option<String> {
        let at = super::tg::text_boundaries(&self.pending).last().map(|(i, _)| *i)?;
        let end = at + self.pending[at..].chars().next()?.len_utf8();
        if self.pending[..end].trim().is_empty() { return None; }
        let tail = self.pending.split_off(end);
        let out = std::mem::replace(&mut self.pending, tail);
        self.sent.push_str(&out);
        Some(out)
    }

    /// State before a reconcile, so a send that definitely failed can be
    /// retried without believing the text was delivered.
    fn snapshot(&self) -> (String, String) {
        (self.sent.clone(), self.pending.clone())
    }

    /// Remember which message the text just flushed into, so a short tail can
    /// be appended to that bubble instead of becoming a lone word. A send
    /// that was split (or suppressed) is not a safe append target.
    fn note_sent(&mut self, mid: Option<i64>, text: &str) {
        let Some(mid) = mid else {
            self.last = None;
            return;
        };
        // Store exactly what Telegram holds (split_text trims each chunk), so
        // the append reproduces the bubble the user sees.
        let chunks = super::tg::split_text(text, MAX_TG_CHARS);
        match chunks.as_slice() {
            [only] => self.last = Some((mid, only.clone(), self.msg.clone())),
            _ => self.last = None,
        }
    }

    fn last_message(&self) -> Option<(i64, String)> {
        self.last.as_ref().map(|(mid, held, _)| (*mid, held.clone()))
    }

    /// The bubble a flushed stream chunk may be appended to: the turn's last
    /// message, unless the chunk starts a different assistant message.
    fn append_target(&self) -> Option<(i64, String)> {
        let (mid, held, sent_msg) = self.last.as_ref()?;
        if let (Some(pending), Some(sent)) = (self.msg.as_deref(), sent_msg.as_deref()) {
            if pending != sent {
                return None;
            }
        }
        Some((*mid, held.clone()))
    }

    /// The tail was edited into the last bubble; keep its text in step so a
    /// later tail appends to the whole sentence.
    fn note_appended(&mut self, tail: &str) {
        let Some((_, held, _)) = self.last.as_mut() else { return };
        let tail = tail.trim();
        if tail.is_empty() {
            return;
        }
        let sep = if held.ends_with('\n') || tail.starts_with('\n') { "" } else { " " };
        held.push_str(sep);
        held.push_str(tail);
    }

    fn restore(&mut self, (sent, pending): (String, String)) {
        self.sent = sent;
        self.pending = pending;
    }

    /// Reconcile a finished assistant row with the streamed deltas. Returns
    /// only text the user has not seen yet: the row is normally the exact
    /// concatenation of the deltas, so nothing is sent at all, and a watch
    /// that died mid-turn is completed from exactly where it stopped.
    fn reconcile(&mut self, row: &str) -> Option<String> {
        let row = row.trim();
        if row.is_empty() {
            return None;
        }
        // The row is the whole message: from here on the watch's deltas for it
        // are a lagging copy of what the user already has.
        self.settled = true;
        self.settled_msg = self.msg.clone();
        let sent: Vec<char> = self.sent.chars().collect();
        let pending: Vec<char> = self.pending.chars().collect();
        let row: Vec<char> = row.chars().collect();
        let mut full = sent.clone();
        full.extend_from_slice(&pending);
        let found = if row.len() <= full.len() {
            (0..=full.len() - row.len()).rev().find(|&i| full[i..i + row.len()] == row[..])
        } else {
            None
        };
        if let Some(start) = found {
            let end = start + row.len();
            if end <= sent.len() {
                // Fully delivered by the stream; drop only the trailing
                // whitespace that a row trim removed.
                self.pending.clear();
                return None;
            }
            if start >= sent.len() {
                // Entirely pending: send the row (trimmed), not any leading
                // whitespace that preceded it in the delta stream.
                let out: String = row.iter().collect();
                self.pending.clear();
                self.sent.push_str(&out);
                return Some(out);
            }
            // The row bridges what was sent and what is still pending:
            // send only the unsent tail, then keep the rest pending.
            let take = end - sent.len();
            let out: String = pending[..take].iter().collect();
            self.pending = pending[take..].iter().collect();
            self.sent.push_str(&out);
            return (!out.trim().is_empty()).then_some(out);
        }
        // The watch missed part (or all) of the row: send only what the
        // user has not seen, using the longest sent/row overlap.
        //
        // A streamed prefix with trailing whitespace the row trim removed
        // makes that overlap 0 and would resend the whole row. Check the
        // trimmed prefix first: the row continues from where the stream
        // stopped, so only its remainder is new.
        let sent_trim: Vec<char> = self.sent.trim_end().chars().collect();
        if !sent_trim.is_empty() && row.len() > sent_trim.len() && row[..sent_trim.len()] == sent_trim[..] {
            let out: String = row[sent_trim.len()..].iter().collect();
            self.pending.clear();
            // The row (trimmed) is now the complete delivered prefix.
            self.sent = row.iter().collect();
            return (!out.trim().is_empty()).then_some(out);
        }
        let overlap = (0..=sent.len().min(row.len()))
            .rev()
            .find(|&k| sent[sent.len() - k..] == row[..k])
            .unwrap_or(0);
        let out: String = row[overlap..].iter().collect();
        self.pending.clear();
        self.sent.push_str(&out);
        (!out.trim().is_empty()).then_some(out)
    }
}

/// The turn's event subscription. The socket `watch` op is the same stream
/// for out-of-process clients; the bridge runs in the server, so it subscribes
/// directly and sees deltas before the first tool call.
pub(crate) struct EventWatch {
    rx: broadcast::Receiver<Value>,
    sid: String,
    closed: bool,
    /// The assistant message the current deltas belong to, when reported.
    message: Option<String>,
    /// A chunk held back because the previous message's prose must flush first.
    held: Option<String>,
}

impl EventWatch {
    pub(crate) fn new(sid: impl Into<String>, rx: broadcast::Receiver<Value>) -> Self {
        Self { rx, sid: sid.into(), closed: false, message: None, held: None }
    }

    /// Drain queued events into `stream`. Returns true when a new assistant
    /// message starts and any pending prose must be sent before the next text.
    fn drain(&mut self, stream: &mut TurnStream) -> bool {
        if self.closed {
            return false;
        }
        loop {
            if let Some(text) = self.held.take() {
                stream.push(&text, self.message.as_deref());
                continue;
            }
            match self.rx.try_recv() {
                Ok(ev) => {
                    if ev["session"].as_str() != Some(self.sid.as_str()) {
                        continue;
                    }
                    // Only assistant prose joins the text stream. Reasoning
                    // events are ignored entirely; tool events are not text but
                    // mark the boundary between two assistant messages.
                    match ev["type"].as_str() {
                        Some("delta") if ev["kind"] == "text" => {
                            let Some(text) = ev["text"].as_str().filter(|t| !t.is_empty()) else {
                                continue;
                            };
                            let mid = ev["message_id"].as_str().map(str::to_string);
                            if mid.is_some() && mid != self.message && self.message.is_some() && stream.has_text() {
                                // A new message id: flush the previous message
                                // before appending this chunk, so the two never
                                // share a Telegram message.
                                if stream.has_pending_text() {
                                    self.message = mid;
                                    self.held = Some(text.to_string());
                                    return true;
                                }
                                // Everything the previous message had is
                                // already on screen: this text is a new bubble,
                                // never a tail of that one.
                                stream.close();
                            }
                            if let Some(mid) = mid {
                                self.message = Some(mid);
                            }
                            stream.push(text, self.message.as_deref());
                        }
                        Some("tool") => {
                            // A tool call between prose starts a new assistant
                            // message. Flush what precedes it now.
                            if stream.has_pending_text() {
                                return true;
                            }
                            stream.close();
                        }
                        _ => {}
                    }
                }
                Err(broadcast::error::TryRecvError::Empty) => return false,
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(broadcast::error::TryRecvError::Closed) => {
                    self.closed = true;
                    return false;
                }
            }
        }
    }

    fn closed(&self) -> bool {
        self.closed
    }
}

impl Bridge {
    fn reply(&self, ctx: &Ctx) -> Value {
        ctx.chat_id.clone()
    }

    pub(crate) async fn say(&self, ctx: &Ctx, text: &str) -> Result<Option<i64>> {
        self.send_text(ctx, &ctx.chat_id, text).await
    }

    /// Send assistant text to the chat and mark the update it answers as replied:
    /// this is what `telegram_history` uses to flag messages the agent never answered.
    ///
    /// The second belt lives here: a bubble identical to the previous one for
    /// this session, moments later, is the same reply seen twice (two items
    /// with the same text, or a retried send), not something the user wants
    /// twice. Suppressing it is logged and treated as delivered.
    async fn relay_text(&self, ctx: &Ctx, chat: &Value, sid: &str, text: &str) -> Result<Option<i64>> {
        if self.repeated_bubble(sid, text) {
            log(format!("suppressed a repeated bubble ({} chars) for {}", clen(text), take(sid, 8)));
            return Ok(None);
        }
        let mid = self.send_text(ctx, chat, text).await?;
        self.note_bubble(sid, text);
        if let Some(u) = ctx.update_id.filter(|_| self.is_owner_dm(chat)) {
            super::history::replied(&self.server, &self.cfg.chat_id, u, now_i()).await;
        }
        Ok(mid)
    }

    /// Flush one streamed chunk to Telegram. A chunk short enough to be the
    /// fragment a flush split off is appended to the bubble it belongs to with
    /// editMessageText; everything else is sent as its own message as before.
    /// This is the streamed counterpart of the reconcile path's tail handling:
    /// a word must never reach the user as a bubble of its own.
    async fn stream_text(&self, ctx: &Ctx, chat: &Value, sid: &str,
                         stream: &mut TurnStream, chunk: &str) -> Result<()> {
        if clen(chunk.trim()) <= APPEND_TAIL_MAX {
            if let Some((mid, held)) = stream.append_target() {
                if self.append_to_last(ctx, chat, mid, &held, chunk).await {
                    stream.note_appended(chunk);
                    return Ok(());
                }
            }
        }
        let mid = self.relay_text(ctx, chat, sid, chunk).await?;
        stream.note_sent(mid, chunk);
        Ok(())
    }

    /// Append a short reconciled tail to the message the turn last sent, so
    /// the word a timed flush held back does not arrive as its own bubble.
    /// Returns false when the edit is not possible (no target, too long,
    /// Telegram refused it) and the caller must send normally.
    async fn append_to_last(&self, ctx: &Ctx, chat: &Value, mid: i64, held: &str, tail: &str) -> bool {
        let tail = tail.trim();
        let held = held.trim_end();
        if tail.is_empty() {
            return true;
        }
        if held.is_empty() {
            return false;
        }
        let sep = if held.ends_with('\n') || tail.starts_with('\n') { "" } else { " " };
        let text = format!("{held}{sep}{tail}");
        if clen(&text) > MAX_TG_CHARS {
            return false;
        }
        match self.tg1("editMessageText", json!({"chat_id": chat, "message_id": mid, "text": text,
                                                  "disable_web_page_preview": true})).await {
            Ok(_) => true,
            Err(e) => {
                log(format!("append-to-last-message edit failed: {e}; sending the tail as its own message"));
                false
            }
        }
    }

    // ------------------------------------------------------------ active sets

    fn act<T>(&self, f: impl FnOnce(&mut super::Active) -> T) -> T {
        f(&mut self.act.lock().unwrap())
    }

    pub(crate) fn is_active(&self, sid: &str) -> bool {
        self.act(|a| a.active.contains(sid))
    }

    fn stop_requested(&self, sid: &str) -> bool {
        self.act(|a| a.stop.contains(sid))
    }

    fn end_active(&self, sid: &str) {
        self.act(|a| {
            a.active.remove(sid);
            a.relaying.remove(sid);
            a.stop.remove(sid);
        })
    }

    fn end_relay(&self, sid: &str) {
        self.act(|a| {
            a.relaying.remove(sid);
            a.stop.remove(sid);
        })
    }

    pub(crate) fn work_pending(&self) -> bool {
        self.queued.load(Ordering::SeqCst) > 0 || self.act(|a| !a.active.is_empty())
    }

    // ------------------------------------------------------------ pending updates

    fn record_pending(&self, update_id: i64, entry: Value) {
        self.with_state(|st| {
            let p = st.entry("pending_updates").or_insert(json!({}));
            if !p.is_object() {
                *p = json!({});
            }
            p[update_id.to_string()] = entry;
        });
        self.save_state();
    }

    pub(crate) fn clear_pending(&self, update_id: Option<i64>) {
        let Some(id) = update_id else { return };
        let removed = self.with_state(|st| st.get_mut("pending_updates").and_then(|p| p.as_object_mut()).and_then(|p| p.remove(&id.to_string())));
        if removed.is_some() {
            self.save_state();
        }
    }

    fn is_pending(&self, update_id: i64) -> bool {
        self.with_state(|st| st.get("pending_updates").and_then(|p| p.get(update_id.to_string())).is_some())
    }

    fn job_unposted(&self, ctx: &Ctx) -> bool {
        ctx.update_id.is_some_and(|id| self.is_pending(id))
    }

    // ------------------------------------------------------------ worker queue

    pub(crate) fn enqueue(&self, job: Job) {
        self.queued.fetch_add(1, Ordering::SeqCst);
        self.jobs.lock().unwrap().push_back(job);
        self.job_notify.notify_one();
    }

    fn drain_queued(&self, pred: impl Fn(&Job) -> bool) -> Vec<Job> {
        let mut q = self.jobs.lock().unwrap();
        let (sel, kept): (Vec<Job>, Vec<Job>) = q.drain(..).partition(|j| pred(j));
        q.extend(kept);
        self.queued.fetch_sub(sel.len(), Ordering::SeqCst);
        sel
    }

    pub(crate) async fn worker_loop(self: Arc<Self>) {
        loop {
            let job = self.jobs.lock().unwrap().pop_front();
            let Some(job) = job else {
                self.job_notify.notified().await;
                continue;
            };
            self.run_job(&job).await;
            self.queued.fetch_sub(1, Ordering::SeqCst);
        }
    }

    async fn run_job_once(self: &Arc<Self>, job: &Job, immediate: bool) -> Result<()> {
        let ov = job.override_session.clone();
        match &job.kind {
            JobKind::Text { text, ack } => Box::pin(self.handle_text(&job.ctx, text, ov, true, *ack && !immediate, immediate)).await,
            JobKind::Attachment { attachment, caption } => Box::pin(self.handle_attachment(&job.ctx, attachment, caption, ov, immediate)).await,
            JobKind::CatchUp => {
                let content = vec![json!({"type": "input_text", "text": CATCH_UP_PROMPT})];
                Box::pin(self.dispatch_content(&job.ctx, content, ov, true, immediate, Announce::No)).await
            }
        }
    }

    async fn on_job_error(&self, job: &Job, e: &anyhow::Error) {
        let text = match job.kind {
            JobKind::Text { .. } => format!("Bridge error: {e}"),
            JobKind::Attachment { .. } => format!("Attachment failed: {e}"),
            JobKind::CatchUp => format!("Catch-up failed: {e}"),
        };
        if self.say(&job.ctx, &text).await.is_err() {
            log(format!("worker error reply failed: {}", job.label));
        }
    }

    async fn run_job(self: &Arc<Self>, job: &Job) {
        log(format!("worker start: {}", job.label));
        let until = Instant::now() + Duration::from_secs(JOB_RETRY_BUDGET_S);
        let mut delay = 5;
        let err = loop {
            match self.run_job_once(job, false).await {
                Ok(()) => return,
                Err(e) => {
                    // A message that never reached its session is retried, not dropped.
                    if is_network_error(&e) && self.job_unposted(&job.ctx) && Instant::now() < until {
                        log(format!("retrying {} after network error {e} in {delay}s", job.label));
                        tokio::time::sleep(Duration::from_secs(delay)).await;
                        delay = (delay * 2).min(NET_BACKOFF_CAP_S);
                        continue;
                    }
                    break e;
                }
            }
        };
        log(format!("worker failed ({}): {err}", job.label));
        if !is_network_error(&err) {
            self.clear_pending(job.ctx.update_id);
        } else if !self.job_unposted(&job.ctx) {
            log("reply relay interrupted by network; leaving it to the broadcast watcher");
            return;
        }
        self.on_job_error(job, &err).await;
    }

    // ------------------------------------------------------------ relay

    /// Subscribe to a session's events. Called before the message is posted so
    /// no delta from the turn can be missed; the polling path remains the
    /// fallback if the subscription ends.
    pub(crate) fn watch_events(&self, sid: &str) -> EventWatch {
        EventWatch::new(sid, self.server.subscribe_events())
    }

    async fn relay_turn_inner(&self, ctx: &Ctx, sid: &str, baseline: Option<String>,
                              mut watch: Option<EventWatch>) -> Result<()> {
        let chat = self.reply(ctx);
        let mut cursor = baseline;
        let (mut sent_any, mut saw_work) = (false, false);
        let mut stream = TurnStream::default();
        let mut saw_final = false;
        let started = Instant::now();
        let mut last_item = started;
        let mut last_typing: Option<Instant> = None;
        let mut ticker = crate::channels::dispatch::ProgressTicker::with_start(
            crate::channels::dispatch::ProgressLimit::Chars(MAX_TG_CHARS),
            started,
        );
        let mut progress_mid: Option<i64> = None;
        let mut last_flush = started;
        loop {
            if self.stop_requested(sid) {
                if let Some(chunk) = stream.flush() {
                    self.stream_text(ctx, &chat, sid, &mut stream, &chunk).await?;
                }
                log("turn relay stopped on request");
                return Ok(());
            }
            if last_typing.is_none_or(|t| t.elapsed() >= Duration::from_secs(4)) {
                self.send_typing(ctx, &chat).await;
                last_typing = Some(Instant::now());
            }
            // Deltas arrive on the broadcast channel; only the text kind is
            // appended to the turn stream. A tool call or a new message id in
            // between forces a flush so separate assistant messages stay apart.
            let (boundary, watch_closed) = match watch.as_mut() {
                Some(w) => {
                    let boundary = w.drain(&mut stream);
                    (boundary, w.closed())
                }
                None => (false, false),
            };
            if watch_closed {
                log("event watch ended; falling back to item polling");
                watch = None;
            }
            if boundary {
                if let Some(chunk) = stream.flush() {
                    self.stream_text(ctx, &chat, sid, &mut stream, &chunk).await?;
                    log(format!("streamed {} chars of assistant text", clen(&chunk)));
                    sent_any = true;
                    saw_work = true;
                    ticker.reset();
                }
                // The bubble now holds the message that just ended; the text
                // after this boundary is a new assistant message.
                stream.close();
                last_flush = Instant::now();
            }
            if stream.ready(last_flush.elapsed()) {
                if let Some(chunk) = stream.flush_partial() {
                    self.stream_text(ctx, &chat, sid, &mut stream, &chunk).await?;
                    log(format!("streamed {} chars of assistant text", clen(&chunk)));
                    sent_any = true;
                    saw_work = true;
                    ticker.reset();
                }
                last_flush = Instant::now();
            }
            let poll: Result<()> = async {
                loop {
                    let page = self.list_items(sid, cursor.as_deref(), 100, false).await?;
                    let items = page["data"].as_array().cloned().unwrap_or_default();
                    for item in &items {
                        last_item = Instant::now();
                        if item["type"] != "message" || item["role"] != "assistant" {
                            continue;
                        }
                        let id = vstr(&item["id"]);
                        if self.was_relayed(&id) {
                            // Another path already sent this reply (the
                            // broadcast watcher, or an earlier relay), so the
                            // turn is answered; do not send it again.
                            sent_any = true;
                            saw_work = true;
                            saw_final = true;
                            ticker.reset();
                            continue;
                        }
                        let text = item_text(item);
                        if text.is_empty() {
                            continue;
                        }
                        // Claim before sending: the two relay paths race on
                        // this claim, never on a check-then-send.
                        if !self.claim_relayed(&id) {
                            log(format!("item {id} was claimed by another relay path"));
                            sent_any = true;
                            saw_work = true;
                            saw_final = true;
                            ticker.reset();
                            continue;
                        }
                        // Streamed deltas and the finished row are the same
                        // text: send only the tail, which is empty normally.
                        let before = stream.snapshot();
                        if let Some(chunk) = stream.reconcile(&text) {
                            // A short tail is the word a timed flush held
                            // back: append it to the bubble it belongs to
                            // instead of sending a one-word message.
                            let appended = clen(chunk.trim()) <= APPEND_TAIL_MAX
                                && match stream.last_message() {
                                    Some((mid, held)) => {
                                        let ok = self.append_to_last(ctx, &chat, mid, &held, &chunk).await;
                                        if ok {
                                            stream.note_appended(&chunk);
                                        }
                                        ok
                                    }
                                    None => false,
                                };
                            if !appended {
                                match self.relay_text(ctx, &chat, sid, &chunk).await {
                                    Ok(mid) => stream.note_sent(mid, &chunk),
                                    Err(e) => {
                                        // Release only when the whole chunk
                                        // (a single Telegram message)
                                        // definitely never arrived;
                                        // anything else may already be on
                                        // the user's screen and must stay
                                        // claimed.
                                        if failed_before_delivery(&e)
                                            && super::tg::split_text(&chunk, super::tg::MAX_TG_CHARS).len() <= 1 {
                                            stream.restore(before);
                                            self.release_relayed(&id);
                                        }
                                        return Err(e);
                                    }
                                }
                            }
                        }
                        log(format!("relayed assistant item {id} ({} chars)", clen(&text)));
                        sent_any = true;
                        saw_work = true;
                        saw_final = true;
                        ticker.reset();
                    }
                    if let Some(last) = items.last() {
                        cursor = Some(vstr(&last["id"])).filter(|c| !c.is_empty()).or(cursor.clone());
                    }
                    if page["has_more"] != json!(true) || items.is_empty() {
                        break;
                    }
                }
                Ok(())
            }.await;
            if let Err(e) = poll {
                if is_network_error(&e) {
                    return Err(e);
                }
                log(format!("item poll failed: {e}"));
            }
            let mut status: Option<String> = None;
            match self.session_snapshot(sid).await {
                Ok(snap) => {
                    status = snap["status"].as_str().map(String::from);
                    if ticker.observe_snapshot(&snap) {
                        saw_work = true;
                    }
                }
                Err(e) => log(format!("session status failed: {e}")),
            }
            if status.as_deref().is_some_and(busy) {
                saw_work = true;
            }
            let elapsed = started.elapsed().as_secs();
            if let Some(line) = ticker.poll(str::to_string) {
                if let Some(mid) = progress_mid {
                    if let Err(e) = self.tg1("editMessageText", json!({"chat_id": chat, "message_id": mid, "text": line, "disable_web_page_preview": true})).await {
                        log(format!("progress edit failed: {e}"));
                    }
                } else {
                    let mut p = json!({"chat_id": chat, "text": line, "disable_web_page_preview": true});
                    if ctx.scope != "dm" {
                        if let Some(t) = ctx.thread_id {
                            p["message_thread_id"] = json!(t);
                        }
                    }
                    match self.tg1("sendMessage", p).await {
                        Ok(r) => {
                            progress_mid = r["message_id"].as_i64();
                            if let Some(mid) = progress_mid { self.note_sent(ctx, &chat, mid).await; }
                        }
                        Err(e) => log(format!("progress send failed: {e}")),
                    }
                }
            }
            let quiet = last_item.elapsed().as_secs_f64();
            let idle = status.as_deref() == Some("idle");
            if sent_any && saw_final && quiet >= 2.5 && idle {
                log(format!("turn relay ended: status=idle quiet={quiet:.1}s sent=true"));
                return Ok(());
            }
            // Text streamed but the finished row has not been polled yet: give
            // it a moment so the next message's backlog cannot resend it.
            if sent_any && !saw_final && idle && quiet >= 10.0 {
                if let Some(chunk) = stream.flush() {
                    self.stream_text(ctx, &chat, sid, &mut stream, &chunk).await?;
                }
                log(format!("turn relay ended before the finished row: quiet={quiet:.1}s"));
                return Ok(());
            }
            if sent_any && quiet >= 15.0 && status.is_none() {
                if let Some(chunk) = stream.flush() {
                    self.stream_text(ctx, &chat, sid, &mut stream, &chunk).await?;
                }
                log(format!("turn relay ended without status: quiet={quiet:.1}s sent=true"));
                return Ok(());
            }
            if saw_work && idle && quiet >= 15.0 {
                if !sent_any {
                    self.send_text(ctx, &chat, "The agent finished without a text response.").await?;
                }
                return Ok(());
            }
            if !saw_work && elapsed >= 45 {
                self.send_text(ctx, &chat, "Message was accepted, but the session did not start a turn. Check Kurisu.").await?;
                return Ok(());
            }
            if elapsed >= TURN_TIMEOUT_S {
                self.send_text(ctx, &chat, "Timed out waiting for the agent response (15 minutes).").await?;
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(800)).await;
        }
    }

    async fn relay_turn(&self, ctx: &Ctx, sid: &str, baseline: Option<String>,
                        watch: Option<EventWatch>) -> Result<()> {
        self.act(|a| a.relaying.insert(sid.to_string()));
        let r = self.relay_turn_inner(ctx, sid, baseline, watch).await;
        self.end_relay(sid);
        r
    }

    /// Release a message that is waiting for another turn; true if one was waiting.
    fn request_send_now(&self, sid: &str) -> bool {
        self.act(|a| {
            if !a.waiting.contains(sid) {
                return false;
            }
            a.send_now.insert(sid.to_string());
            true
        })
    }

    fn take_send_now(&self, sid: &str) -> bool {
        self.act(|a| a.send_now.remove(sid))
    }

    fn end_waiting(&self, sid: &str) {
        self.act(|a| {
            a.waiting.remove(sid);
            a.send_now.remove(sid);
        })
    }

    /// Wait for a turn the bridge did not start; one acknowledgement ("On it" or "Queued").
    async fn wait_until_idle(&self, ctx: &Ctx, sid: &str, announce: Announce) -> Result<bool> {
        let chat = self.reply(ctx);
        let deadline = Instant::now() + Duration::from_secs(TURN_TIMEOUT_S);
        let mut announced = false;
        while Instant::now() < deadline {
            if self.stop_requested(sid) {
                return Ok(false);
            }
            if self.take_send_now(sid) {
                return Ok(true);
            }
            let status = match self.session_snapshot(sid).await {
                Ok(s) => s["status"].as_str().unwrap_or("").to_string(),
                Err(e) => {
                    log(format!("session status failed while queueing: {e}"));
                    if announce == Announce::Yes && !announced {
                        self.send_text(ctx, &chat, "⏳ On it.").await?;
                    }
                    return Ok(true);
                }
            };
            if status != "running" && status != "starting" {
                if announce == Announce::Yes && !announced {
                    self.send_text(ctx, &chat, "⏳ On it.").await?;
                }
                return Ok(true);
            }
            if !announced {
                if announce != Announce::No {
                    self.send_text(ctx, &chat, "⏳ Queued — the session is finishing another turn first. Tap /send_now to send it now.").await?;
                }
                announced = true;
            }
            for _ in 0..10 {
                if self.stop_requested(sid) {
                    return Ok(false);
                }
                if self.act(|a| a.send_now.contains(sid)) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        Ok(false)
    }

    /// Deliver final replies of turns that ended while a message was waiting.
    async fn flush_turn_backlog(&self, ctx: &Ctx, sid: &str, after: Option<String>) -> Result<()> {
        let items = match self.list_items(sid, after.as_deref(), 100, false).await {
            Ok(p) => p["data"].as_array().cloned().unwrap_or_default(),
            Err(e) => {
                log(format!("backlog poll failed: {e}"));
                return Ok(());
            }
        };
        let finals: Vec<(Value, String)> = super::cards::broadcast_final_messages(&items).into_iter().filter(|(i, _)| !self.was_relayed(&vstr(&i["id"]))).collect();
        if finals.is_empty() {
            return Ok(());
        }
        let session = self.session_snapshot(sid).await.unwrap_or(json!({"id": sid}));
        for (item, text) in finals {
            let id = vstr(&item["id"]);
            // Claim before sending: the turn relay and the broadcast watcher
            // may both want this item, and only one of them may send it.
            if !self.claim_relayed(&id) {
                continue;
            }
            if self.repeated_bubble(sid, &text) {
                log(format!("suppressed a repeated backlog bubble for {} (item {id})", take(sid, 8)));
                continue;
            }
            let body = format!("{}\n\n{text}", super::cards::broadcast_prefix(&session));
            if let Err(e) = self.send_text(ctx, &self.chat_id(), &body).await {
                if failed_before_delivery(&e) && super::tg::split_text(&body, MAX_TG_CHARS).len() <= 1 {
                    self.release_relayed(&id);
                }
                return Err(e);
            }
            self.note_bubble(sid, &text);
            log(format!("flushed backlog item {id} from {}", take(sid, 8)));
        }
        Ok(())
    }

    pub(crate) async fn dispatch_content(&self, ctx: &Ctx, content: Vec<Value>, override_session: Option<String>, persist: bool,
                                         send_immediately: bool, announce: Announce) -> Result<()> {
        let _ = persist;
        let sid = match override_session {
            Some(s) => s,
            None => self.ensure_session(ctx).await?,
        };
        if send_immediately {
            // Kurisu types it into the running turn (or runs it right after).
            self.post_message(&sid, &content, true, ctx.update_id).await?;
            self.clear_pending(ctx.update_id);
            return Ok(());
        }
        self.act(|a| {
            a.active.insert(sid.clone());
            a.waiting.insert(sid.clone());
        });
        let r = async {
            let backlog_after = match self.latest_item_id(&sid).await {
                Ok(x) => Some(x),
                Err(e) => {
                    log(format!("backlog baseline failed: {e}"));
                    None
                }
            };
            if !self.wait_until_idle(ctx, &sid, announce).await? {
                self.clear_pending(ctx.update_id);
                self.say(ctx, "🛑 Stopped before the queued message was sent.").await?;
                return Ok(());
            }
            if self.stop_requested(&sid) {
                self.clear_pending(ctx.update_id);
                return Ok(());
            }
            if let Some(after) = backlog_after {
                self.flush_turn_backlog(ctx, &sid, after).await?;
            }
            let baseline = self.latest_item_id(&sid).await?;
            // Subscribe before posting: a fast first tool call must not hide
            // the assistant text produced before it.
            let watch = Some(self.watch_events(&sid));
            self.post_message(&sid, &content, false, ctx.update_id).await?;
            self.clear_pending(ctx.update_id);
            self.end_waiting(&sid);
            if !self.stop_requested(&sid) {
                self.relay_turn(ctx, &sid, baseline, watch).await?;
            }
            Ok(())
        }.await;
        self.end_waiting(&sid);
        self.end_active(&sid);
        r
    }

    // ------------------------------------------------------------ attachments

    pub(crate) fn save_upload(&self, filename: &str, data: &[u8]) -> Result<std::path::PathBuf> {
        let dir = self.upload_dir();
        std::fs::create_dir_all(&dir)?;
        let base = std::path::Path::new(filename).file_name().and_then(|n| n.to_str()).unwrap_or("file");
        let mut safe: String = base.chars().map(|c| if c.is_alphanumeric() || "._-".contains(c) { c } else { '_' }).collect();
        safe = take(&safe, 120);
        if safe.is_empty() {
            safe = "file".into();
        }
        let path = dir.join(format!("{}-{}-{safe}", chrono::Local::now().format("%Y%m%d-%H%M%S"), token(6)));
        super::write_private(&path, data)?;
        Ok(path)
    }

    async fn handle_attachment(self: &Arc<Self>, ctx: &Ctx, attachment: &Value, caption: &str, override_session: Option<String>, send_immediately: bool) -> Result<()> {
        let fid = attachment["file_id"].as_str().unwrap_or("");
        let (data, file_path) = self.download_file(fid).await?;
        let name = attachment["name"].as_str().filter(|n| !n.is_empty()).map(String::from)
            .unwrap_or_else(|| std::path::Path::new(&file_path).file_name().and_then(|n| n.to_str()).unwrap_or("file").to_string());
        let mime = attachment["mime"].as_str().filter(|m| !m.is_empty()).map(String::from).unwrap_or_else(|| mime_for_path(&file_path));
        self.deliver_attachment(ctx, &name, &mime, data, caption, override_session, send_immediately).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn deliver_attachment(self: &Arc<Self>, ctx: &Ctx, name: &str, mime: &str, data: Vec<u8>, caption: &str,
                                override_session: Option<String>, send_immediately: bool) -> Result<()> {
        use base64::Engine;
        let caption = caption.trim();
        if mime.starts_with("image/") {
            if data.len() > MAX_IMAGE_BYTES {
                bail!("image is too large ({:.1} MB; Kurisu accepts at most 25 MB)", data.len() as f64 / 1048576.0);
            }
            let mut content = vec![json!({"type": "input_image",
                "image_url": format!("data:{mime};base64,{}", base64::engine::general_purpose::STANDARD.encode(&data))})];
            if !caption.is_empty() {
                content.push(json!({"type": "input_text", "text": caption}));
            }
            self.say(ctx, "🖼 Image received.").await?;
            return self.dispatch_content(ctx, content, override_session, true, send_immediately, Announce::Busy).await;
        }
        if is_audio_attachment(name, mime) {
            if let Some(stt) = self.stt_config().await {
                let path = self.save_upload(&audio_name(name, mime), &data)?;
                let started = Instant::now();
                let placeholder = match self.say(ctx, "🎙 Transcribing…").await {
                    Ok(id) => id,
                    Err(e) => {
                        log(format!("transcribing placeholder failed: {e}"));
                        None
                    }
                };
                let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();
                let keeper = {
                    let (b, c, chat) = (self.clone(), ctx.clone(), self.reply(ctx));
                    tokio::spawn(async move {
                        loop {
                            b.send_chat_action(&c, &chat, "record_voice").await;
                            if tokio::time::timeout(Duration::from_secs(4), &mut rx).await.is_ok() {
                                break;
                            }
                        }
                    })
                };
                let transcript = crate::stt::transcribe(&stt, &path).await;
                let _ = tx.send(());
                let _ = keeper.await;
                match transcript {
                    Err(e) => {
                        // Quietly pass the audio through as a file (spec 8.1): drop the placeholder.
                        log(format!("stt failed: {e:#}"));
                        if let Some(mid) = placeholder {
                            let _ = self.tg1("deleteMessage", json!({"chat_id": self.reply(ctx), "message_id": mid})).await;
                        }
                    }
                    Ok(t) => {
                        log(format!("voice transcribed in {:.1}s ({} chars)", started.elapsed().as_secs_f64(), clen(&t)));
                        let heard = if t.is_empty() { "(no speech detected)" } else { t.as_str() };
                        self.update_or_send(ctx, &self.reply(ctx), placeholder, &format!("🎙 Heard: {heard}")).await?;
                        let text = voice_transcript_text(name, &path.to_string_lossy(), &t, caption);
                        // The message was stored when it arrived (text unknown
                        // then): keep the transcript in its telegram_history row.
                        if let Some(u) = ctx.update_id.filter(|_| self.is_owner_dm(&ctx.chat_id)) {
                            super::history::transcript(&self.server, &self.cfg.chat_id, u, &t).await;
                        }
                        return self.dispatch_content(ctx, vec![json!({"type": "input_text", "text": text})], override_session, true, send_immediately, Announce::Busy).await;
                    }
                }
            }
        }
        let sid = match &override_session {
            Some(s) => s.clone(),
            None => self.ensure_session(ctx).await?,
        };
        let content = match self.upload_file(&sid, name, &data).await? {
            Some(path) => {
                let mut c = vec![json!({"type": "input_file", "file_id": path, "filename": name})];
                if !caption.is_empty() {
                    c.push(json!({"type": "input_text", "text": caption}));
                }
                self.say(ctx, "📎 File received.").await?;
                c
            }
            None => {
                let path = self.save_upload(name, &data)?;
                let mut text = format!("The user uploaded a file named \"{name}\" ({} bytes, {mime}). It is saved at {}.\nUse your tools to inspect it.",
                                       data.len(), path.display());
                if !caption.is_empty() {
                    text.push_str(&format!("\n\nUser note: {caption}"));
                }
                self.say(ctx, "📎 File received and saved to the workspace.").await?;
                vec![json!({"type": "input_text", "text": text})]
            }
        };
        self.dispatch_content(ctx, content, override_session, true, send_immediately, Announce::Busy).await
    }

    /// The speech-to-text to use, as the bridge's stt_command: `ku stt use/setup` wins,
    /// `ku stt off` disables it, otherwise a working `stt` CLI (checked every 60 s).
    pub(crate) async fn stt_config(&self) -> Option<crate::stt::Config> {
        match crate::stt::load(&self.home) {
            Some(crate::stt::Config::Off) => return None,
            Some(c) => return Some(c),
            None => {}
        }
        {
            let p = self.stt_probe.lock().unwrap();
            if now_f() - p.0 < 60.0 {
                return p.1.clone();
            }
        }
        let found = crate::stt::detect().await;
        *self.stt_probe.lock().unwrap() = (now_f(), found.clone());
        found
    }

    /// lb's `/api/status` says a human was asked to take over (`handover.active`) and
    /// nobody has claimed it yet. The key changes when the ask or the tab does, so the
    /// same request is announced once.
    pub(crate) fn handover_key(status: &Value) -> Option<String> {
        let h = &status["handover"];
        if h["active"] != json!(true) || h["claimed"] == json!(true) {
            return None;
        }
        let message = h["message"].as_str().unwrap_or("").trim();
        Some(format!("{}|{}", status["tab"], message))
    }

    /// The handover notice. It carries the link itself: "click Done in the browser"
    /// is useless on a phone that has no /browser link open.
    pub(crate) fn handover_text(why: &str, url: &str) -> String {
        let why = if why.is_empty() { "a login, a captcha or 2FA" } else { why };
        format!("🖐 The live browser needs you: {why}\n\nOpen this one-time link (5 min), do it, then press Done in the page:\n{url}\n\nSend /browser at any time for a fresh link.")
    }

    /// Every tab asking for a human, not just the active one: handover is per tab and
    /// another session switching tabs must not hide a blocked agent.
    async fn lb_handovers(&self) -> Vec<String> {
        let Some(cfg) = crate::browser::active(&self.home) else { return Vec::new() };
        let base = cfg.server_url();
        let pass = cfg.passcode.clone();
        let get = |path: String| {
            let http = self.http.clone();
            let (base, pass) = (base.clone(), pass.clone());
            async move {
                http.get(format!("{base}{path}"))
                    .header("X-LB-Passcode", pass)
                    .timeout(Duration::from_secs(3))
                    .send().await.ok()?
                    .json::<Value>().await.ok()
            }
        };
        let Some(tabs) = get("/api/tabs".into()).await else { return Vec::new() };
        let mut keys = Vec::new();
        for id in tabs["tabs"].as_array().into_iter().flatten()
            .filter(|t| t["handover"] == json!(true))
            .filter_map(|t| t["id"].as_u64())
        {
            if let Some(k) = get(format!("/api/status?tab={id}")).await.as_ref().and_then(Self::handover_key) {
                keys.push(k);
            }
        }
        keys
    }

    /// Which asks to announce now: new ones, plus a fresh link when the old one has
    /// expired (links live 5 min; 4 leaves time to act).
    pub(crate) fn handovers_due(keys: &[String], announced: &HashMap<String, Instant>, now: Instant) -> Vec<String> {
        keys.iter().filter(|k| announced.get(*k).map_or(true, |at| now.saturating_duration_since(*at) > Duration::from_secs(240))).cloned().collect()
    }

    async fn announce_handover(&self, key: &str) -> Result<()> {
        let why = key.split_once('|').map(|(_, m)| m).unwrap_or("");
        let ctx = self.dm();
        match self.k(json!({"op": "remote_link", "kind": "browser"})).await {
            Ok(v) => {
                let url = v["url"].as_str().unwrap_or("");
                self.say(&ctx, &Self::handover_text(why, url)).await?;
            }
            Err(e) => {
                self.say(&ctx, &format!("🖐 The live browser needs you ({why}), but the /browser link failed: {e}. Send /browser to retry.")).await?;
            }
        }
        Ok(())
    }

    /// Post one working /browser link per handover: the agent's `lb handover` blocks
    /// until the user clicks Done in the web UI, and on a phone that UI only exists
    /// behind Kurisu's one-time link.
    async fn handover_loop(self: Arc<Self>) {
        let mut announced: HashMap<String, Instant> = HashMap::new();
        loop {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let keys = self.lb_handovers().await;
            for k in Self::handovers_due(&keys, &announced, Instant::now()) {
                if let Err(e) = self.announce_handover(&k).await {
                    log(format!("handover notice failed: {e}"));
                }
                announced.insert(k, Instant::now());
            }
            // Drop asks that were answered or timed out, so a later one announces again.
            announced.retain(|k, _| keys.contains(k));
        }
    }

    /// /terminal and /browser: a one-time link when on; otherwise an approval card that
    /// turns it on (and then sends the link). `close` and `disable` too. Owner's DM only.
    /// Bare /terminal asks which directory first; `/terminal <path>` skips the picker.
    async fn remote_command(&self, ctx: &Ctx, kind: &str, arg: &str) -> Result<()> {
        let what = kind;
        if !self.is_owner_dm(&ctx.chat_id) {
            self.say(ctx, &format!("/{what} works only in the owner's private chat.")).await?;
            return Ok(());
        }
        let raw = arg.trim();
        match raw.to_lowercase().as_str() {
            "close" => {
                let n = self.k(json!({"op": "remote_close", "kind": kind, "why": format!("/{what} close")})).await.map(|v| v["closed"].as_u64().unwrap_or(0)).unwrap_or(0);
                let noun = match kind { "browser" => "browser view", "web" => "web page", _ => "terminal session" };
                self.say(ctx, &format!("⏹ Closed {n} {noun}{}; open links are dead.", if n == 1 { "" } else { "s" })).await?;
            }
            "disable" | "off" => {
                let r = self.k(json!({"op": "remote_disable", "kind": kind})).await?;
                self.say(ctx, &if r["was_on"] == json!(true) { format!("⏹ /{what} is off; links and sessions are closed. Send /{what} to turn it on again.") }
                              else { format!("/{what} was already off.") }).await?;
            }
            _ => {
                if kind == "terminal" && !raw.is_empty() {
                    return self.open_terminal_dir(ctx, raw).await;
                }
                let offer = self.k(json!({"op": "remote_offer", "kind": kind})).await?;
                if offer["enabled"] == json!(true) {
                    if kind == "terminal" {
                        return self.send_terminal_picker(ctx).await;
                    }
                    return self.send_remote_link(ctx, kind).await;
                }
                match offer["where"].as_str() {
                    None => {
                        self.say(ctx, offer["error"].as_str().unwrap_or(crate::remote::NO_ADDRESS)).await?;
                    }
                    Some(place) => self.post_enable_card(kind, place, offer["lb_installed"] == json!(true)).await?,
                }
            }
        }
        Ok(())
    }

    /// Bare /terminal: a button per directory before any link exists. The paths live
    /// in bridge state behind a token, so a callback never carries (or leaks) one.
    async fn send_terminal_picker(&self, ctx: &Ctx) -> Result<()> {
        let (rows, _) = self.sorted_session_rows().await?;
        let sessions: Vec<Value> = rows.into_iter().map(|(_, _, s)| s).collect();
        let entries = terminal_dir_entries(&self.home, &sessions);
        let tok = self.terminal_card(ctx, &entries);
        let mut buttons: Vec<Value> = entries.iter().enumerate()
            .map(|(i, (label, _))| json!([{"text": take(label, 64), "callback_data": format!("td:{tok}:{i}")}]))
            .collect();
        buttons.push(json!([{"text": "⌨️ Type a path", "callback_data": format!("td:{tok}:c")}]));
        self.send_markup(ctx, &ctx.chat_id, &terminal_picker_text(&entries), Some(json!({"inline_keyboard": buttons}))).await?;
        Ok(())
    }

    /// Store a /terminal picker (token -> chat + directories) with its short TTL.
    fn terminal_card(&self, ctx: &Ctx, entries: &[(String, String)]) -> String {
        let tok = token(16);
        let now = now_f();
        let dirs: Vec<Value> = entries.iter().map(|(_, path)| json!({"path": path})).collect();
        self.with_state(|st| {
            let cards = st.entry("terminal_cards").or_insert(json!({}));
            if !cards.is_object() {
                *cards = json!({});
            }
            let m = cards.as_object_mut().unwrap();
            m.retain(|_, c| now - c["created_at"].as_f64().unwrap_or(0.0) <= TERMINAL_CARD_TTL_S);
            m.insert(tok.clone(), json!({"created_at": now as i64, "chat": vstr(&ctx.chat_id), "dirs": dirs}));
            while m.len() > TERMINAL_CARD_LIMIT {
                let first = m.keys().next().cloned().unwrap();
                m.remove(&first);
            }
        });
        self.save_state();
        tok
    }

    /// td:<token>:<index|c>: pick a directory, or ask for one to be typed.
    async fn handle_terminal_callback(&self, ctx: &Ctx, cid: &Value, message: &Value, data: &str) -> Result<()> {
        let expired = "Terminal picker expired; run /terminal again";
        if !self.is_owner_dm(&ctx.chat_id) {
            self.answer_callback(cid, Some("Terminal is only available in the owner's DM")).await;
            return Ok(());
        }
        let parts: Vec<&str> = data.splitn(3, ':').collect();
        let tok = parts.get(1).copied().unwrap_or("");
        let arg = parts.get(2).copied().unwrap_or("");
        let now = now_f();
        let card = self.with_state(|st| st.get("terminal_cards").and_then(|c| c.get(tok)).cloned());
        let card = card.filter(|c| now - c["created_at"].as_f64().unwrap_or(0.0) <= TERMINAL_CARD_TTL_S
                                  && c["chat"].as_str() == Some(vstr(&ctx.chat_id).as_str()));
        let Some(card) = card else {
            self.with_state(|st| st.get_mut("terminal_cards").and_then(|c| c.as_object_mut()).map(|c| c.remove(tok)));
            self.save_state();
            self.answer_callback(cid, Some(expired)).await;
            return Ok(());
        };
        if arg == "c" {
            self.with_state(|st| {
                let waits = st.entry("terminal_path_wait").or_insert(json!({}));
                if !waits.is_object() {
                    *waits = json!({});
                }
                waits.as_object_mut().unwrap().insert(vstr(&ctx.chat_id), json!({"created_at": now as i64}));
            });
            self.save_state();
            self.answer_callback(cid, Some("Send the path")).await;
            return self.edit_or_send(ctx, message,
                "🖥 Send the directory to open the terminal in (an absolute path, or one starting with ~).\n/terminal <path> does the same.",
                Some(json!({"inline_keyboard": []}))).await;
        }
        let Some(path) = arg.parse::<usize>().ok().and_then(|i| card["dirs"].get(i)).and_then(|d| d["path"].as_str()).map(String::from) else {
            self.answer_callback(cid, Some(expired)).await;
            return Ok(());
        };
        self.with_state(|st| st.get_mut("terminal_cards").and_then(|c| c.as_object_mut()).map(|c| c.remove(tok)));
        self.save_state();
        let short = short_path(&path, std::env::var("HOME").ok().as_deref());
        self.answer_callback(cid, Some(&take(&format!("Opening {short}"), 180))).await;
        self.edit_or_send(ctx, message, &format!("🖥 Terminal in {short}"), Some(json!({"inline_keyboard": []}))).await?;
        self.send_remote_link_in(ctx, "terminal", Some(&path)).await
    }

    /// `/terminal <path>` (and the picker's typed path): validate, then link.
    async fn open_terminal_dir(&self, ctx: &Ctx, raw: &str) -> Result<()> {
        let dir = match terminal_dir(raw) {
            Ok(d) => d,
            Err(e) => {
                self.say(ctx, &format!("🖥 {e}")).await?;
                return Ok(());
            }
        };
        let offer = self.k(json!({"op": "remote_offer", "kind": "terminal"})).await?;
        if offer["enabled"] != json!(true) {
            match offer["where"].as_str() {
                None => {
                    self.say(ctx, offer["error"].as_str().unwrap_or(crate::remote::NO_ADDRESS)).await?;
                }
                Some(place) => self.post_enable_card("terminal", place, false).await?,
            }
            return Ok(());
        }
        self.send_remote_link_in(ctx, "terminal", Some(&dir.to_string_lossy())).await
    }

    /// A typed path from the picker's "Type a path" step (owner DM, same TTL).
    fn take_terminal_path_wait(&self, ctx: &Ctx) -> bool {
        if !self.is_owner_dm(&ctx.chat_id) {
            return false;
        }
        let chat = vstr(&ctx.chat_id);
        let removed = self.with_state(|st| {
            let now = now_f();
            let Some(waits) = st.get_mut("terminal_path_wait").and_then(|w| w.as_object_mut()) else { return false };
            waits.retain(|_, v| now - v["created_at"].as_f64().unwrap_or(0.0) <= TERMINAL_CARD_TTL_S);
            waits.remove(&chat).is_some()
        });
        if removed {
            self.save_state();
        }
        removed
    }

    pub(crate) async fn send_remote_link(&self, ctx: &Ctx, kind: &str) -> Result<()> {
        self.send_remote_link_in(ctx, kind, None).await
    }

    /// Mint and print a one-time link; `dir` (terminal only) is where the shell starts.
    pub(crate) async fn send_remote_link_in(&self, ctx: &Ctx, kind: &str, dir: Option<&str>) -> Result<()> {
        let mut req = json!({"op": "remote_link", "kind": kind});
        if let Some(d) = dir {
            req["dir"] = json!(d);
        }
        let home = std::env::var("HOME").ok();
        let where_ = dir.map(|d| format!(" (shell starts in {})", short_path(d, home.as_deref()))).unwrap_or_default();
        // The link is sent, never logged.
        let text = match self.k(req).await {
            Ok(v) => {
                let head = match kind { "browser" => "🌐 One-time link to the live browser", "web" => "💬 One-time link to the web chat", _ => "🖥 One-time terminal link" };
                format!("{head}{where_} (expires in 5 min). Opening it shows a code and asks for your approval here:\n{}", v["url"].as_str().unwrap_or(""))
            }
            Err(e) => {
                let what = match kind { "browser" => "Browser", "web" => "Web chat", _ => "Terminal" };
                format!("{what} unavailable{where_}: {}", e.to_string().trim_start_matches("HTTP 400: "))
            }
        };
        self.say(ctx, &text).await?;
        Ok(())
    }

    /// /mode [auto|default|acceptEdits|plan]: the current session's permission mode.
    async fn handle_mode_command(&self, ctx: &Ctx, arg: &str) -> Result<()> {
        let sid = self.ensure_session(ctx).await?;
        let want = arg.trim();
        if !want.is_empty() {
            return self.set_mode(ctx, &sid, want, None).await;
        }
        let snap = self.session_snapshot(&sid).await?;
        let cur = snap["permission_mode"].as_str().unwrap_or("default").to_string();
        let title = squash(snap["title"].as_str().filter(|t| !t.is_empty()).unwrap_or("(untitled)"));
        let opencode=snap["kurisu_harness"]=="opencode";
        let agy=snap["kurisu_harness"]=="agy";
        let fx=snap["kurisu_harness"]=="fx";
        let text = if fx { format!("fx permissions for {title}: {cur}\n\nauto — fx's automatic safety review; no prompts\ndefault — asks you here before changes and commands\n\nApplies from the session's next start.") } else if opencode { format!("OpenCode permissions for {title}: {cur}\n\ndefault — upstream Build permissions; requests appear here\nplan — upstream Plan agent\nOther Claude/Codex permission modes are unavailable through ACP.") } else if agy { format!("Antigravity permissions for {title}: {cur}\n\ndefault — asks before edits and commands\nacceptEdits — file edits run, commands ask\nplan — plan before executing\n\nApplies from the session's next start.") } else { format!("Permission mode for {title} ({}): {cur}\n\n\
auto — routine actions run; risky ones ask you here\ndefault — asks for most tools\nacceptEdits — file edits run, commands ask\n\
plan — read and plan only\n\nApplies from the session's next start.", take(&sid, 8)) };
        let modes: &[&str]=if fx { &["auto","default"] } else if opencode { &["default","plan"] } else if agy { &["default","acceptEdits","plan"] } else { &["auto","default","acceptEdits","plan"] };
        let buttons: Vec<Value> = modes.iter()
            .map(|m| json!([{"text": format!("{}{m}", if *m == cur { "✓ " } else { "" }), "callback_data": format!("mode:{}:{m}", take(&sid, 36))}])).collect();
        self.send_markup(ctx, &ctx.chat_id, &text, Some(json!({"inline_keyboard": buttons}))).await?;
        Ok(())
    }

    async fn set_mode(&self, ctx: &Ctx, sid: &str, mode: &str, message: Option<&Value>) -> Result<()> {
        if mode == "bypassPermissions" {
            self.say(ctx, "bypassPermissions can only be set on the computer (ku update <name> --mode bypassPermissions).").await?;
            return Ok(());
        }
        let text = match self.k(json!({"op": "update", "name": sid, "fields": {"permission_mode": mode}})).await {
            Ok(_) => format!("✅ Permission mode for {} is now {mode} (from its next start).", take(sid, 8)),
            Err(e) => format!("Mode change failed: {}", e.to_string().trim_start_matches("HTTP 400: ")),
        };
        match message.and_then(|m| m["message_id"].as_i64()) {
            Some(mid) => {
                let _ = self.tg1("editMessageText", json!({"chat_id": ctx.chat_id, "message_id": mid, "text": text, "reply_markup": {"inline_keyboard": []}})).await;
            }
            None => {
                self.say(ctx, &text).await?;
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------ /usage

    async fn handle_usage_command(&self, ctx: &Ctx) -> Result<()> {
        // The same report the kurisu-mcp `usage` tool returns (server op usage_report).
        let text = match self.k(json!({"op": "usage_report"})).await {
            Ok(r) => r["text"].as_str().unwrap_or("").to_string(),
            Err(e) => {
                log(format!("kurisu usage failed: {e}"));
                "Usage report unavailable; please try again.".into()
            }
        };
        self.say(ctx, &text).await?;
        Ok(())
    }

    /// "CLIs: claude 2.1.319 ✅ · codex 0.166.0 ✅ · stt ✅ (idle)" (Kurisu's own detection, cached 60 s).
    async fn cli_status_line(&self) -> String {
        let r = self.k(json!({"op": "clis"})).await;
        let mut entries = vec![];
        match &r {
            Ok(v) => {
                for name in ["claude", "codex", "opencode", "agy", "fx", "pi"] {
                    let c = v["clis"].as_array().into_iter().flatten().find(|c| c["name"] == name);
                    entries.push(match c {
                        Some(c) if c["installed"] == json!(true) => {
                            let ver = short_version(c["version"].as_str().unwrap_or(""));
                            let mut t = format!("{name} {ver}").trim().to_string() + " ✅";
                            if c["logged_in"] == json!(false) {
                                t.push_str(" (not logged in)");
                            }
                            t
                        }
                        _ => format!("{name} ❌"),
                    });
                }
            }
            Err(e) => {
                log(format!("kurisu status failed: {e}"));
                entries.push("CLI status unavailable".into());
            }
        }
        let stt = r.as_ref().ok().and_then(|v| v["clis"].as_array().and_then(|a| a.iter().find(|c| c["name"] == "stt").cloned()));
        let off = crate::stt::load(&self.home) == Some(crate::stt::Config::Off);
        entries.push(match stt {
            Some(s) if s["set_up"] == json!(true) && !off => format!("stt ✅ ({})", if s["daemon"] == "running" { "warm" } else { "idle" }),
            _ => "stt ❌".into(),
        });
        format!("CLIs: {}", entries.join(" · "))
    }

    // ------------------------------------------------------------ /list

    async fn sorted_session_rows(&self) -> Result<(Vec<(f64, Option<f64>, Value)>, HashMap<String, Value>)> {
        let sessions = self.fetch_sessions().await?;
        let by_id: HashMap<String, Value> = sessions.iter().filter_map(|s| s["id"].as_str().map(|i| (i.to_string(), s.clone()))).collect();
        let mut rows: Vec<(f64, Option<f64>, Value)> = sessions.into_iter().map(|s| {
            let last_user = s["last_user_at"].as_f64().filter(|x| *x != 0.0);
            let sort = last_user.or(s["updated_at"].as_f64().filter(|x| *x != 0.0)).or(s["created_at"].as_f64().filter(|x| *x != 0.0)).unwrap_or(0.0);
            (sort, last_user, s)
        }).collect();
        rows.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        Ok((rows, by_id))
    }

    async fn list_snapshot(&self, session: &Value) -> Value {
        let id = session["id"].as_str().unwrap_or("");
        match self.session_snapshot(id).await {
            Ok(s) => s,
            Err(e) => {
                log(format!("list snapshot failed for {}: {e}", take(id, 8)));
                session.clone()
            }
        }
    }

    async fn configured_codex_model(&self) -> Option<String> {
        {
            let c = self.codex_model.lock().unwrap();
            if now_f() - c.0 < 60.0 {
                return c.1.clone();
            }
        }
        let mut model = self.default_model("codex").await.filter(|m| m != "default");
        if model.is_none() {
            let path = std::env::var("CODEX_HOME").map(std::path::PathBuf::from)
                .unwrap_or_else(|_| std::path::PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(".codex")).join("config.toml");
            model = std::fs::read_to_string(path).ok().and_then(|t| toml_top_level_string(&t, "model"));
        }
        let model = model.map(|m| m.strip_suffix(":cloud").map(String::from).unwrap_or(m)).filter(|m| !m.is_empty());
        *self.codex_model.lock().unwrap() = (now_f(), model.clone());
        model
    }

    async fn resolved_model(&self, snap: &Value, harness: Option<&str>) -> (Option<String>, String) {
        let mut model = snap["model_override"].as_str().filter(|m| !m.is_empty()).or(snap["llm_model"].as_str().filter(|m| !m.is_empty())).map(String::from);
        if model.as_deref().is_some_and(|m| m.trim().to_lowercase() == "default") {
            model = None;
        }
        if let Some(m) = model {
            let short = short_model(&m);
            return (Some(m), short);
        }
        if harness.is_some_and(|h| h.starts_with("codex")) {
            let real = self.configured_codex_model().await;
            let label = real.as_deref().map(short_model).unwrap_or_else(|| "codex default".into());
            return (real, label);
        }
        (None, "default".into())
    }

    async fn model_effort_label(&self, session: &Value, snap: &Value) -> String {
        let harness = harness_label(snap, Some(session));
        let (_, model) = self.resolved_model(snap, harness.as_deref()).await;
        let effort = snap["reasoning_effort"].as_str().filter(|e| !e.is_empty()).or(session["reasoning_effort"].as_str().filter(|e| !e.is_empty())).unwrap_or("default");
        let label = format!("{model} / {effort}");
        match harness {
            Some(h) => format!("{h} · {label}"),
            None => label,
        }
    }

    fn session_skill_names(&self, workspace: Option<&str>) -> Vec<String> {
        let home = std::env::var("HOME").unwrap_or_default();
        let mut roots = vec![std::path::PathBuf::from(&home).join(".claude/skills")];
        if let Some(w) = workspace.filter(|w| !w.is_empty()) {
            roots.push(std::path::PathBuf::from(w).join(".claude/skills"));
        }
        let stamp: Vec<Option<std::time::SystemTime>> = roots.iter().map(|r| std::fs::metadata(r).and_then(|m| m.modified()).ok()).collect();
        let key = roots.iter().map(|r| r.to_string_lossy().into_owned()).collect::<Vec<_>>().join("|");
        if let Some((s, v)) = self.skills_cache.lock().unwrap().get(&key) {
            if *s == stamp {
                return v.clone();
            }
        }
        let mut names = std::collections::BTreeSet::new();
        for r in &roots {
            for e in std::fs::read_dir(r).into_iter().flatten().flatten() {
                if e.path().join("SKILL.md").is_file() {
                    names.insert(e.file_name().to_string_lossy().into_owned());
                }
            }
        }
        let v: Vec<String> = names.into_iter().collect();
        self.skills_cache.lock().unwrap().insert(key, (stamp, v.clone()));
        v
    }

    async fn session_extras_line(&self, snap: &Value) -> String {
        let mut parts = vec![];
        if let Some(c) = estimated_cost(snap) {
            parts.push(format!("💰 ~${} est", fmt_money(c)));
        }
        if let Some(c) = context_label(snap) {
            parts.push(format!("🧠 {c}"));
        }
        if let Some(id) = snap["id"].as_str() {
            parts.push(format!("🔌 {}", short_list(&self.kurisu_mcp_names(id).await, 3)));
        }
        if snap["harness"].as_str().unwrap_or("").starts_with("claude") {
            let skills = self.session_skill_names(snap["workspace"].as_str());
            if !skills.is_empty() {
                parts.push(format!("🛠 {}", short_list(&skills, 3)));
            }
        }
        parts.join(" · ")
    }

    /// /schedule: pending wakes, one Cancel button each.
    async fn schedule_card(&self) -> (String, Option<Value>) {
        match self.k(json!({"op": "wakes"})).await {
            Ok(w) => schedule_text(w.as_array().map(Vec::as_slice).unwrap_or(&[]), now_i()),
            Err(e) => (format!("Schedule failed: {e}"), None),
        }
    }

    pub(crate) async fn session_overview(&self, ctx: &Ctx, page: i64) -> Result<(String, Option<Value>)> {
        let (rows, by_id) = self.sorted_session_rows().await?;
        let current = self.current_session(ctx);
        let total = rows.len();
        let (page, start) = page_bounds(total, page);
        let visible: Vec<&(f64, Option<f64>, Value)> = rows.iter().skip(start).take(LIST_PAGE_SIZE).collect();
        let end = start + visible.len();
        let mut lines = if total > 0 { vec![format!("Recent sessions ({}-{end} of {total})", start + 1), String::new()] } else { vec!["Recent sessions (0)".to_string()] };
        let mut buttons: Vec<Value> = vec![];
        for (i, (_, last_user, session)) in visible.iter().enumerate() {
            let index = start + i + 1;
            let sid = session["id"].as_str().unwrap_or("?").to_string();
            let status = session["status"].as_str().filter(|s| !s.is_empty()).unwrap_or("?").to_string();
            let icon = match status.as_str() { "running" | "starting" => "🟢", "waiting" => "🟡", "failed" => "🔴", _ => "⚪" };
            let mut title = squash(session["title"].as_str().filter(|t| !t.is_empty()).unwrap_or("(untitled)"));
            if clen(&title) > 50 {
                title = format!("{}…", take(&title, 49));
            }
            let is_current = current.as_deref() == Some(sid.as_str());
            let parent = session["parent_session_id"].as_str().filter(|p| !p.is_empty());
            lines.push(format!("{index}. {icon} {}{title}{}", if parent.is_some() { "↳ " } else { "" }, if is_current { " ✓ current" } else { "" }));
            let age = last_user.map(duration_ago).unwrap_or_else(|| "no user message".into());
            let snap = self.list_snapshot(session).await;
            let mut detail = format!("last user: {age} · {status} · {}", self.model_effort_label(session, &snap).await);
            if let Some(p) = parent {
                let mut pt = squash(by_id.get(p).and_then(|x| x["title"].as_str()).unwrap_or(""));
                if clen(&pt) > 28 {
                    pt = format!("{}…", take(&pt, 27));
                }
                detail.push_str(&format!(" · child of {}", if pt.is_empty() { take(p, 8) } else { pt }));
            }
            if status == "running" || status == "starting" {
                let a = &snap["activity"];
                let what = if a.is_object() {
                    a["title"].as_str().filter(|s| !s.is_empty()).or(a["tool"].as_str()).unwrap_or("").to_string()
                } else {
                    "working".into()
                };
                detail.push_str(&format!(" · {what}"));
            }
            lines.push(format!("   {detail} · {}", take(&sid, 8)));
            if let Some(note) = crate::channels::note_line(session["note"].as_str().unwrap_or("")) {
                lines.push(format!("   {note}"));
            }
            if let Some(dir) = session["workspace"].as_str().filter(|d| !d.is_empty()) {
                lines.push(format!("   📁 {}", short_path(dir, std::env::var("HOME").ok().as_deref())));
            }
            let extras = self.session_extras_line(&snap).await;
            if !extras.is_empty() {
                lines.push(format!("   {extras}"));
            }
            buttons.push(json!([{"text": format!("{} {}", if is_current { "✓" } else { "Use" }, take(&title, 26)), "callback_data": format!("use:{sid}")}]));
        }
        let mut nav = vec![];
        if page > 0 {
            nav.push(json!({"text": "⬅️ Newer", "callback_data": format!("list:{}", page - 1)}));
        }
        if end < total {
            nav.push(json!({"text": format!("More ({} left) ➜", total - end), "callback_data": format!("list:{}", page + 1)}));
        }
        if !nav.is_empty() {
            buttons.push(json!(nav));
        }
        if total > 0 {
            buttons.push(json!([{"text": "🔄 Refresh", "callback_data": format!("list:{page}")}]));
        }
        Ok((lines.join("\n"), if buttons.is_empty() { None } else { Some(json!({"inline_keyboard": buttons})) }))
    }

    // ------------------------------------------------------------ /compact

    async fn compact_picker(&self, ctx: &Ctx, page: i64) -> Result<(String, Value)> {
        let (rows, _) = self.sorted_session_rows().await?;
        let current = self.current_session(ctx);
        let total = rows.len();
        let (page, start) = page_bounds(total, page);
        let visible: Vec<&(f64, Option<f64>, Value)> = rows.iter().skip(start).take(LIST_PAGE_SIZE).collect();
        let mut buttons = vec![];
        if let Some(c) = &current {
            buttons.push(json!([{"text": format!("🗜 This session ({})", take(c, 8)), "callback_data": format!("cmp:y:{c}")}]));
        }
        for (_, _, s) in &visible {
            let sid = s["id"].as_str().unwrap_or("");
            if sid.is_empty() {
                continue;
            }
            let title = take(&squash(s["title"].as_str().filter(|t| !t.is_empty()).unwrap_or("(untitled)")), 28);
            let mark = if current.as_deref() == Some(sid) { "✓ " } else { "" };
            buttons.push(json!([{"text": format!("{mark}{title} · {}", s["status"].as_str().filter(|x| !x.is_empty()).unwrap_or("?")), "callback_data": format!("cmp:a:{sid}")}]));
        }
        let mut nav = vec![];
        if page > 0 {
            nav.push(json!({"text": "⬅️ Newer", "callback_data": format!("cmp:p:{}", page - 1)}));
        }
        if start + visible.len() < total {
            nav.push(json!({"text": "More ➜", "callback_data": format!("cmp:p:{}", page + 1)}));
        }
        if !nav.is_empty() {
            buttons.push(json!(nav));
        }
        buttons.push(json!([{"text": "✖ Cancel", "callback_data": "cmp:n"}]));
        let text = format!("🗜 Compact which session? ({}-{} of {total})", if total > 0 { start + 1 } else { 0 }, start + visible.len());
        Ok((text, json!({"inline_keyboard": buttons})))
    }

    async fn compact_confirm_view(&self, sid: &str) -> (String, Value) {
        let snap = self.session_snapshot(sid).await.ok();
        let title = take(&squash(snap.as_ref().and_then(|s| s["title"].as_str()).filter(|t| !t.is_empty()).unwrap_or("(untitled)")), 40);
        let text = format!("🗜 Compact {title} ({})?\nstatus: {} · context: {}", take(sid, 8),
                           snap.as_ref().and_then(|s| s["status"].as_str()).filter(|s| !s.is_empty()).unwrap_or("?"),
                           snap.as_ref().map(format_context_usage).unwrap_or_else(|| "?".into()));
        (text, json!({"inline_keyboard": [[{"text": "✅ Compact", "callback_data": format!("cmp:y:{sid}")}, {"text": "⬅️ Back", "callback_data": "cmp:p:0"}]]}))
    }

    async fn edit_or_send(&self, ctx: &Ctx, message: &Value, text: &str, markup: Option<Value>) -> Result<()> {
        if let Some(mid) = message["message_id"].as_i64() {
            let mut p = json!({"chat_id": ctx.chat_id, "message_id": mid, "text": text});
            if let Some(m) = &markup {
                p["reply_markup"] = m.clone();
            }
            match self.tg1("editMessageText", p).await {
                Ok(_) => return Ok(()),
                Err(e) => log(format!("edit compact picker failed: {e}")),
            }
        }
        self.send_markup(ctx, &ctx.chat_id, text, markup).await?;
        Ok(())
    }

    async fn handle_compact_callback(self: &Arc<Self>, ctx: &Ctx, callback_id: &Value, message: &Value, data: &str) -> Result<()> {
        let parts: Vec<&str> = data.splitn(3, ':').collect();
        let action = parts.get(1).copied().unwrap_or("");
        let arg = parts.get(2).copied().unwrap_or("");
        self.answer_callback(callback_id, None).await;
        match (action, arg.is_empty()) {
            ("p", _) => {
                let (t, m) = self.compact_picker(ctx, arg.parse().unwrap_or(0)).await?;
                self.edit_or_send(ctx, message, &t, Some(m)).await
            }
            ("a", false) => {
                let (t, m) = self.compact_confirm_view(arg).await;
                self.edit_or_send(ctx, message, &t, Some(m)).await
            }
            ("y", false) => {
                self.edit_or_send(ctx, message, &format!("🗜 Compacting {}…", take(arg, 8)), Some(json!({"inline_keyboard": []}))).await?;
                self.compact_current_session(ctx, Some(arg.to_string())).await
            }
            _ => self.edit_or_send(ctx, message, "Compaction cancelled.", Some(json!({"inline_keyboard": []}))).await,
        }
    }

    /// /undo: Kurisu takes back the current session's last turn (files, CLI
    /// conversation where possible, its history), then this deletes that turn's
    /// Telegram messages: the prompt and the bot's replies, where the Bot API
    /// allows (sent less than 48 hours ago; in a group only the bot's own).
    async fn undo_current_session(self: &Arc<Self>, ctx: &Ctx) -> Result<()> {
        let Some(sid) = self.current_session(ctx) else {
            self.say(ctx, "No session selected.").await?;
            return Ok(());
        };
        if self.is_active(&sid) {
            self.say(ctx, "Undo refused: the session is busy. Wait for the reply or /stop, then /undo.").await?;
            return Ok(());
        }
        let r = match self.k(json!({"op": "undo", "name": sid})).await {
            Ok(r) => r,
            Err(e) => {
                self.say(ctx, &format!("Undo failed, nothing was changed: {e}")).await?;
                return Ok(());
            }
        };
        let mut text = r["text"].as_str().unwrap_or("Undid the last turn.").to_string();
        let updates: Vec<i64> = r["telegram_updates"].as_array().into_iter().flatten().filter_map(Value::as_i64).collect();
        text.push_str("\nTelegram: ");
        if updates.is_empty() {
            text.push_str("no Telegram messages belong to that turn (it did not come from this chat), so none were deleted.");
        } else {
            let chat = vstr(&ctx.chat_id);
            let msgs = self.server.with_store(|st| st.tg_update_messages(&chat, &updates)).await.unwrap_or_default();
            let (mut deleted, mut old, mut failed) = (0, 0, vec![]);
            let now = now_i();
            for (mid, at, _from_user) in &msgs {
                if now - at >= 48 * 3600 - 60 {
                    old += 1;
                    continue;
                }
                match self.tg("deleteMessage", Some(json!({"chat_id": ctx.chat_id, "message_id": mid})), 20, Some(0)).await {
                    Ok(_) => deleted += 1,
                    Err(e) => failed.push(format!("{e}")),
                }
            }
            let forgotten = self.server.with_store(|st| st.tg_forget_updates(&chat, &updates)).await.unwrap_or(0);
            text.push_str(&format!("deleted {deleted} of {} message(s) (your prompt and the replies).", msgs.len()));
            if old > 0 {
                text.push_str(&format!(" {old} left in place: older than 48 hours, which Telegram does not allow bots to delete."));
            }
            if !failed.is_empty() {
                text.push_str(&format!(" {} could not be deleted ({}).", failed.len(), take(&failed[0], 120)));
            }
            text.push_str(&format!(" Removed {forgotten} row(s) from the Telegram history sessions can read."));
            text.push_str(" Files Kurisu sent as attachments and edited cards are not tracked and stay.");
        }
        self.say(ctx, &text).await?;
        Ok(())
    }

    async fn compact_current_session(self: &Arc<Self>, ctx: &Ctx, sid: Option<String>) -> Result<()> {
        let Some(sid) = sid.or_else(|| self.current_session(ctx)) else {
            self.say(ctx, "No session selected.").await?;
            return Ok(());
        };
        if self.compactions.lock().unwrap().contains(&sid) {
            self.say(ctx, &format!("🗜 Compaction is already in progress for {}. I'll report the result here.", take(&sid, 8))).await?;
            return Ok(());
        }
        let snapshot = match self.session_snapshot(&sid).await {
            Ok(s) => s,
            Err(e) => {
                self.say(ctx, &format!("Compact failed for {}: {e}", take(&sid, 8))).await?;
                return Ok(());
            }
        };
        let status = snapshot["status"].as_str().unwrap_or("");
        if busy(status) || self.is_active(&sid) {
            self.say(ctx, &format!("⏳ {} is busy. Compaction can only start while the session is idle; wait for the current response or use /stop, then retry.", take(&sid, 8))).await?;
            return Ok(());
        }
        // Start the watcher (it runs Kurisu's blocking compact).
        let started = {
            let mut w = self.compactions.lock().unwrap();
            w.insert(sid.clone())
        };
        if started {
            let (b, c, s) = (self.clone(), ctx.clone(), sid.clone());
            tokio::spawn(async move { b.kurisu_watch_compaction(&c, &s).await });
        }
        self.say(ctx, &format!("🗜 Compaction requested for {}.\nKurisu will summarize older conversation context. The session may show 'running' until it finishes; I'll send the result here.\nContext before: {}",
                               take(&sid, 8), format_context_usage(&snapshot))).await?;
        Ok(())
    }

    async fn kurisu_watch_compaction(&self, ctx: &Ctx, sid: &str) {
        let chat = self.reply(ctx);
        let r = self.k(json!({"op": "compact", "name": sid})).await;
        let result = match r {
            Ok(v) => v,
            Err(e) => {
                let _ = self.send_text(ctx, &chat, &format!("❌ Compaction failed for {}: {}\nYou can retry with /compact.", take(sid, 8), take(&e.to_string(), 500))).await;
                self.compactions.lock().unwrap().remove(sid);
                return;
            }
        };
        let snapshot = self.session_snapshot(sid).await.ok();
        let text = compaction_complete_text(sid, &result, snapshot.as_ref());
        let _ = self.send_text(ctx, &chat, &text).await;
        self.compactions.lock().unwrap().remove(sid);
    }

    // ------------------------------------------------------------ /stop

    async fn stop_current_session(&self, ctx: &Ctx) -> Result<()> {
        let Some(sid) = self.current_session(ctx) else {
            self.say(ctx, "Nothing is running.").await?;
            return Ok(());
        };
        let status = match self.session_snapshot(&sid).await {
            Ok(s) => s["status"].as_str().unwrap_or("").to_string(),
            Err(e) => {
                self.say(ctx, &format!("Stop failed: {e}")).await?;
                return Ok(());
            }
        };
        if !busy(&status) && !self.is_active(&sid) {
            self.say(ctx, &format!("Nothing is running in {}.", take(&sid, 8))).await?;
            return Ok(());
        }
        if self.is_active(&sid) {
            self.act(|a| a.stop.insert(sid.clone()));
        }
        let mut idle = false;
        for _ in 0..2 {
            if let Err(e) = self.k(json!({"op": "stop", "name": sid})).await {
                if self.is_active(&sid) {
                    self.end_relay(&sid);
                }
                self.say(ctx, &format!("Stop failed for {}: {e}", take(&sid, 8))).await?;
                return Ok(());
            }
            let deadline = Instant::now() + Duration::from_millis(2500);
            while Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(350)).await;
                match self.status_of(&sid).await {
                    None => break,
                    Some(s) if s == "idle" => {
                        idle = true;
                        break;
                    }
                    _ => {}
                }
            }
            if idle {
                break;
            }
        }
        if idle {
            self.say(ctx, &format!("🛑 Stopped the current run in {}. The session is still active.", take(&sid, 8))).await?;
        } else {
            self.say(ctx, &format!("🛑 Stop requested for {}. The agent is still winding down; try /stop again.", take(&sid, 8))).await?;
        }
        Ok(())
    }

    // ------------------------------------------------------------ /model

    /// Store a /model card (token -> card) with the bridge's TTL and limit.
    fn model_card(&self, mut card: Value) -> String {
        let tok = token(16);
        let now = now_f();
        card["created_at"] = json!(now as i64);
        self.with_state(|st| {
            let cards = st.entry("model_cards").or_insert(json!({}));
            if !cards.is_object() {
                *cards = json!({});
            }
            let m = cards.as_object_mut().unwrap();
            m.retain(|_, c| c["created_at"].as_f64().is_some_and(|t| now - t <= MODEL_CARD_TTL_S));
            m.insert(tok.clone(), card);
            while m.len() > MODEL_CARD_LIMIT {
                let first = m.keys().next().cloned().unwrap();
                m.remove(&first);
            }
        });
        self.save_state();
        tok
    }

    fn session_harness(snapshot: &Value) -> String {
        snapshot["kurisu_harness"].as_str().filter(|h| !h.is_empty()).unwrap_or("claude").to_string()
    }

    /// Harnesses /model offers in step 1: the installed CLIs (the session's own always).
    async fn available_harnesses(&self, snapshot: &Value) -> Vec<String> {
        let current = Self::session_harness(snapshot);
        let mut found: Vec<String> = match self.k(json!({"op": "clis"})).await {
            Ok(v) => v["clis"].as_array().into_iter().flatten()
                .filter(|c| c["installed"] == json!(true) && (c["name"] == "claude" || c["name"] == "codex" || c["name"] == "opencode" || c["name"] == "agy" || c["name"] == "fx" || c["name"] == "pi"))
                .filter_map(|c| c["name"].as_str().map(String::from)).collect(),
            Err(e) => {
                log(format!("cli list failed: {e}"));
                vec![]
            }
        };
        if (current == "claude" || current == "codex" || current == "opencode" || current == "agy" || current == "fx" || current == "pi") && !found.contains(&current) {
            found.insert(0, current);
        }
        // One button per harness (bridge 982f783).
        let mut seen: Vec<String> = vec![];
        for h in found {
            if !seen.contains(&h) {
                seen.push(h);
            }
        }
        seen
    }

    /// Step 1 (harness) of /model, or step 2 straight away when only one harness is available.
    async fn model_picker(&self, ctx: &Ctx) -> Result<(String, Option<Value>)> {
        let sid = self.ensure_session(ctx).await?;
        let snapshot = self.session_snapshot(&sid).await?;
        let harnesses = self.available_harnesses(&snapshot).await;
        let current = Self::session_harness(&snapshot);
        if harnesses.len() <= 1 {
            return self.model_overview(&snapshot, None, 0, None).await;
        }
        let title = squash(snapshot["title"].as_str().filter(|t| !t.is_empty()).unwrap_or("(untitled)"));
        let model = snapshot["model_override"].as_str().filter(|m| !m.is_empty()).or(snapshot["llm_model"].as_str().filter(|m| !m.is_empty())).unwrap_or("agent default");
        let tok = self.model_card(json!({"session_id": sid, "current_harness": current, "harnesses": harnesses, "step": "harness"}));
        let label = |h: &str| match h { "claude" => "Claude Code", "codex" => "Codex", "opencode" => "OpenCode", "agy" => "Antigravity", "fx" => "fx", "pi" => "pi", other => other }.to_string();
        let lines = [format!("Session: {title} ({})", take(&sid, 8)), format!("Current: {current} · {model}"), String::new(),
                     "Step 1 of 3 — pick a harness:".into(), String::new(), "You can also send /model <model-id> (same harness).".into()];
        let buttons: Vec<Value> = harnesses.iter().enumerate().map(|(i, h)| json!([{
            "text": format!("{}{} ({h})", if *h == current { "✓ " } else { "" }, label(h)), "callback_data": format!("mh:{tok}:{i}")}])).collect();
        Ok((lines.join("\n"), Some(json!({"inline_keyboard": buttons}))))
    }

    /// Step 2: a harness's models (the session's own catalogue for its current
    /// harness), one page at a time, optionally filtered by a search query.
    async fn model_overview(&self, snapshot: &Value, harness: Option<String>, page: i64, filter: Option<String>) -> Result<(String, Option<Value>)> {
        let sid = snapshot["id"].as_str().unwrap_or("").to_string();
        let current_harness = Self::session_harness(snapshot);
        let harness = harness.unwrap_or_else(|| current_harness.clone());
        let switching = harness != current_harness;
        let mut for_harness = snapshot.clone();
        for_harness["harness"] = json!(if harness == "claude" { "claude-native" } else { &harness });
        for_harness["kurisu_harness"] = json!(harness);
        let (mut options, err) = self.model_options(&for_harness).await?;
        let model_now = snapshot["model_override"].as_str().filter(|m| !m.is_empty()).or(snapshot["llm_model"].as_str().filter(|m| !m.is_empty())).unwrap_or("agent default").to_string();
        let current = if switching { String::new() } else { model_now.clone() };
        let title = squash(snapshot["title"].as_str().filter(|t| !t.is_empty()).unwrap_or("(untitled)"));
        if switching && harness == "codex" {
            // Codex's catalogue is embedded in Kurisu; surface the configured default's real id.
            let real = self.configured_codex_model().await;
            if let Some(default) = options.iter_mut().find(|o| o["id"] == "default") {
                default["label"] = json!(format!("Codex default{}", real.map(|r| format!(" ({r})")).unwrap_or_default()));
            }
        }
        let total = options.len();
        let filter = filter.filter(|q| !q.trim().is_empty());
        let filtered = match &filter {
            Some(q) => filter_models(&options, q),
            None => options,
        };
        let tok = self.model_card(json!({"session_id": sid,
                                         "models": filtered.iter().map(|o| o["id"].clone()).collect::<Vec<_>>(),
                                         "harness": harness, "current_harness": current_harness, "filter": filter}));
        let view = ModelPage { tok: &tok, sid: &sid, title: &title, harness: &harness, current_harness: &current_harness,
                               current_model: &current, switching, filtered_of: filter.as_ref().map(|_| total) };
        let (mut text, markup) = model_page_view(&view, &filtered, filter.as_deref(), page);
        if total == 0 {
            if let Some(e) = err {
                text.push_str(&format!("\nReason: {e}"));
            }
        }
        let has_buttons = markup["inline_keyboard"].as_array().is_some_and(|a| !a.is_empty());
        Ok((text, has_buttons.then_some(markup)))
    }

    async fn edit_model_card(&self, ctx: &Ctx, message: &Value, text: &str, markup: Option<Value>) -> Result<()> {
        if let Some(mid) = message["message_id"].as_i64() {
            match self.tg1("editMessageText", json!({"chat_id": ctx.chat_id, "message_id": mid, "text": text, "disable_web_page_preview": true,
                                                       "reply_markup": markup.clone().unwrap_or(json!({"inline_keyboard": []}))})).await {
                Ok(_) => return Ok(()),
                Err(e) => log(format!("edit model card failed: {e}")),
            }
        }
        self.send_markup(ctx, &ctx.chat_id, text, markup).await?;
        Ok(())
    }

    /// mh:<token>:<index> (step 1 pick) and mx:<token>:y|n (harness switch confirm).
    async fn handle_model_harness_callback(&self, ctx: &Ctx, cid: &Value, message: &Value, data: &str) -> Result<()> {
        let parts: Vec<&str> = data.splitn(3, ':').collect();
        let (kind, tok, arg) = (parts[0], parts.get(1).copied().unwrap_or(""), parts.get(2).copied().unwrap_or(""));
        let Some(card) = self.with_state(|st| st.get("model_cards").and_then(|c| c.get(tok)).cloned()) else {
            self.answer_callback(cid, Some("Model picker expired; run /model again")).await;
            return Ok(());
        };
        let remove = |b: &Self| b.with_state(|st| st.get_mut("model_cards").and_then(|c| c.as_object_mut()).map(|c| c.remove(tok)));
        let sid = card["session_id"].as_str().unwrap_or("").to_string();
        if kind == "mh" {
            let Some(harness) = arg.parse::<usize>().ok().and_then(|i| card["harnesses"].get(i)).and_then(|h| h.as_str()).map(String::from) else {
                self.answer_callback(cid, Some("Model picker expired; run /model again")).await;
                return Ok(());
            };
            let snapshot = self.session_snapshot(&sid).await?;
            remove(self);
            let (text, markup) = self.model_overview(&snapshot, Some(harness.clone()), 0, None).await?;
            self.answer_callback(cid, Some(&harness)).await;
            return self.edit_model_card(ctx, message, &text, markup).await;
        }
        // mx: confirm a harness switch
        remove(self);
        self.save_state();
        if arg != "y" {
            self.answer_callback(cid, Some("Cancelled")).await;
            return self.edit_model_card(ctx, message, "Harness switch cancelled; the session is unchanged.", None).await;
        }
        let snapshot = self.session_snapshot(&sid).await?;
        if busy(snapshot["status"].as_str().unwrap_or("")) || self.is_active(&sid) {
            self.alert(cid, "A run is active; use /stop or wait for it to finish").await;
            return Ok(());
        }
        let harness = card["harness"].as_str().unwrap_or("").to_string();
        let model = card["model"].as_str().filter(|m| !m.is_empty()).map(String::from);
        let mut fields = json!({"harness": harness});
        if let Some(m) = &model {
            fields["model"] = json!(m);
        }
        let r = match self.k(json!({"op": "update", "name": sid, "fields": fields})).await {
            Ok(r) if r["new"]["id"].is_string() => r,
            Ok(_) => {
                self.alert(cid, "Harness switch failed: Kurisu returned no replacement session").await;
                return Ok(());
            }
            Err(e) => {
                log(format!("harness switch failed: {e}"));
                self.alert(cid, &format!("Harness switch failed: {e}")).await;
                return Ok(());
            }
        };
        // The server already pointed every chat that used the old session at the new one.
        let new_id = r["new"]["id"].as_str().unwrap_or("").to_string();
        // Effort: the pick, or ("default") the old setting when the new harness supports it.
        let effort = if harness=="opencode" { card["effort"].as_str().map(String::from) } else {
            card["effort"].as_str().or(card["old_effort"].as_str()).filter(|e|crate::store::effort_levels(&harness).contains(e)).map(String::from)
        };
        if let Some(e) = &effort {
            if let Err(err) = self.k(json!({"op": "update", "name": new_id, "fields": {"effort": e}})).await {
                log(format!("effort after harness switch failed: {err}"));
            }
        }
        self.answer_callback(cid, Some(&format!("Switched to {harness}"))).await;
        let title = squash(snapshot["title"].as_str().filter(|t| !t.is_empty()).unwrap_or("(untitled)"));
        let note = r["note"].as_str().filter(|n| !n.is_empty()).map(|n| format!("\n{n}")).unwrap_or_default();
        self.edit_model_card(ctx, message, &format!("✅ Switched {title} to {harness}{}{}.\nNew session: {new_id}{note}",
                                                      model.map(|m| format!(" ({m})")).unwrap_or_default(),
                                                      effort.map(|e| format!(", effort {e}")).unwrap_or_default()), None).await
    }

    async fn alert(&self, cid: &Value, text: &str) {
        if let Err(e) = self.tg1("answerCallbackQuery", json!({"callback_query_id": cid, "text": take(text, 180), "show_alert": true})).await {
            log(format!("callback alert failed: {e}"));
        }
    }

    async fn switch_session_model(&self, sid: &str, model_id: &str) -> Result<Value> {
        let model_id = model_id.trim();
        if model_id.is_empty() {
            bail!("model id cannot be empty");
        }
        if disabled_model(model_id) {
            bail!("Kimi models are disabled");
        }
        let snapshot = self.session_snapshot(sid).await?;
        if busy(snapshot["status"].as_str().unwrap_or("")) || self.is_active(sid) {
            bail!("a run is active; use /stop or wait for it to finish");
        }
        self.kurisu_switch_model(&snapshot, model_id).await
    }

    async fn handle_model_command(&self, ctx: &Ctx, arg: &str) -> Result<()> {
        let target = arg.trim();
        if !target.is_empty() {
            let sid = self.ensure_session(ctx).await?;
            match self.switch_session_model(&sid, target).await {
                Ok(snap) => self.say(ctx, &model_update_text(&snap, target)).await?,
                Err(e) => self.say(ctx, &format!("Model switch failed for {}: {e}", take(&sid, 8))).await?,
            };
            return Ok(());
        }
        match self.model_picker(ctx).await {
            Ok((t, m)) => self.send_markup(ctx, &ctx.chat_id, &t, m).await?,
            Err(e) => self.say(ctx, &format!("Model list failed: {e}")).await?,
        };
        Ok(())
    }

    async fn handle_model_callback(&self, ctx: &Ctx, callback_id: &Value, message: &Value, data: &str) -> Result<()> {
        let parts: Vec<&str> = data.splitn(3, ':').collect();
        let tok = parts.get(1).copied().unwrap_or("");
        let index: i64 = parts.get(2).and_then(|i| i.parse().ok()).unwrap_or(-1);
        let card = self.with_state(|st| st.get("model_cards").and_then(|c| c.get(tok)).cloned());
        let picked = card.as_ref().and_then(|c| {
            if index < 0 {
                return None;
            }
            let model = if data.starts_with("me:") { c["model"].as_str()? } else { c["models"].get(index as usize)?.as_str()? };
            Some((model.to_string(), c["session_id"].as_str()?.to_string()))
        });
        let Some((model_id, sid)) = picked else {
            self.answer_callback(callback_id, Some("Model picker expired; run /model again")).await;
            return Ok(());
        };
        let card = card.unwrap_or_default();
        // Step 3: effort, unless this is already the effort pick (index came via me:).
        if !data.starts_with("me:") {
            let h = card["harness"].as_str().filter(|h| !h.is_empty()).unwrap_or(card["current_harness"].as_str().unwrap_or("claude")).to_string();
            let snapshot = self.session_snapshot(&sid).await?;
            self.with_state(|st| st.get_mut("model_cards").and_then(|c| c.as_object_mut()).map(|c| c.remove(tok)));
            let choices: Vec<String> = if h=="opencode" {
                let r=self.k(json!({"op":"models","harness":"opencode","model":model_id})).await?;
                let mut values=vec!["default".to_string()];
                if let Some(opts)=r["config_options"].as_array().and_then(|a|a.iter().find(|c|c["id"]=="effort")).and_then(|c|c["options"].as_array()) {
                    for o in opts { if let Some(v)=o["value"].as_str().filter(|v|*v!="default") { values.push(v.to_string()); } }
                }
                values
            } else { effort_choices(&h).iter().map(|s|s.to_string()).collect() };
            let token=self.model_card(json!({"session_id":sid,"harness":card["harness"],"current_harness":card["current_harness"],"model":model_id,"effort_step":true,"efforts":choices}));
            let refs: Vec<&str>=choices.iter().map(String::as_str).collect();
            let (text,markup)=effort_step_options(token,&h,&model_id,snapshot["reasoning_effort"].as_str(),&refs);
            self.answer_callback(callback_id, Some("Pick an effort")).await;
            return self.edit_model_card(ctx, message, &text, Some(markup)).await;
        }
        let effort = picked_effort(&card, data);
        // A model on another harness: confirm the harness switch first.
        let (h, cur) = (card["harness"].as_str().unwrap_or(""), card["current_harness"].as_str().unwrap_or(""));
        if !h.is_empty() && !cur.is_empty() && h != cur {
            let snapshot = self.session_snapshot(&sid).await?;
            self.with_state(|st| st.get_mut("model_cards").and_then(|c| c.as_object_mut()).map(|c| c.remove(tok)));
            let confirm = self.model_card(json!({"session_id": sid, "harness": h, "current_harness": cur, "model": model_id, "effort": effort,
                                                 "old_effort": snapshot["reasoning_effort"]}));
            self.answer_callback(callback_id, Some("Confirm the switch")).await;
            let title = squash(snapshot["title"].as_str().filter(|t| !t.is_empty()).unwrap_or("(untitled)"));
            let text = format!("🔁 Switch {title} from {cur} to {h}{}{}?\nIt restarts as a new session that reads the old history.",
                               if model_id.is_empty() { String::new() } else { format!(" ({model_id})") },
                               effort.as_deref().map(|e| format!(", effort {e}")).unwrap_or_default());
            return self.edit_model_card(ctx, message, &text, Some(json!({"inline_keyboard": [[
                {"text": "🔁 Switch", "callback_data": format!("mx:{confirm}:y")}, {"text": "✖ Cancel", "callback_data": format!("mx:{confirm}:n")}]]}))).await;
        }
        let snap = match self.switch_session_model(&sid, &model_id).await {
            Ok(s) => match &effort {
                Some(e) => match self.k(json!({"op": "update", "name": sid, "fields": {"effort": e}})).await {
                    Ok(_) => self.session_snapshot(&sid).await.unwrap_or(s),
                    Err(e) => {
                        self.answer_callback(callback_id, Some(&take(&format!("Effort change failed: {e}"), 180))).await;
                        return Ok(());
                    }
                },
                None => s,
            },
            Err(e) => {
                self.answer_callback(callback_id, Some(&take(&format!("Model switch failed: {e}"), 180))).await;
                return Ok(());
            }
        };
        self.with_state(|st| st.get_mut("model_cards").and_then(|c| c.as_object_mut()).map(|c| c.remove(tok)));
        self.save_state();
        self.answer_callback(callback_id, Some("Model switched")).await;
        let mut text = model_update_text(&snap, &model_id);
        text.push_str(&format!("\nEffort: {}", snap["reasoning_effort"].as_str().unwrap_or("default")));
        if let Some(mid) = message["message_id"].as_i64() {
            match self.tg1("editMessageText", json!({"chat_id": ctx.chat_id, "message_id": mid, "text": text, "disable_web_page_preview": true,
                                                       "reply_markup": {"inline_keyboard": []}})).await {
                Ok(_) => return Ok(()),
                Err(e) => log(format!("edit model picker failed: {e}")),
            }
        }
        self.say(ctx, &text).await?;
        Ok(())
    }

    /// mp:<tok>:<page> (page), mf:<tok> (show all), ms:<tok> (ask for a keyword)
    /// and msc (cancel the search prompt).
    async fn handle_model_page_callback(&self, ctx: &Ctx, cid: &Value, message: &Value, data: &str) -> Result<()> {
        if data == "msc" {
            self.clear_model_search(ctx);
            self.answer_callback(cid, Some("Cancelled")).await;
            return self.edit_model_card(ctx, message, "Model search cancelled. Send /model to pick a model.", None).await;
        }
        let parts: Vec<&str> = data.splitn(3, ':').collect();
        let (kind, tok, arg) = (parts[0], parts.get(1).copied().unwrap_or(""), parts.get(2).copied().unwrap_or(""));
        let Some(card) = self.with_state(|st| st.get("model_cards").and_then(|c| c.get(tok)).cloned()) else {
            self.answer_callback(cid, Some("Model picker expired; run /model again")).await;
            return Ok(());
        };
        let sid = card["session_id"].as_str().unwrap_or("").to_string();
        let harness = card["harness"].as_str().filter(|h| !h.is_empty()).map(String::from);
        self.with_state(|st| st.get_mut("model_cards").and_then(|c| c.as_object_mut()).map(|c| c.remove(tok)));
        self.save_state();
        if kind == "ms" {
            let label = harness.as_deref().or(card["current_harness"].as_str()).unwrap_or("the");
            self.start_model_search(ctx, &sid, harness.as_deref());
            self.answer_callback(cid, Some("Send a keyword")).await;
            let text = format!("🔎 Search {label} models\nSend a keyword to filter by model id or display name.\nYour next message is the search; /model cancels.");
            return self.edit_model_card(ctx, message, &text, Some(json!({"inline_keyboard": [[{"text": "✖ Cancel", "callback_data": "msc"}]]}))).await;
        }
        let filter = if kind == "mf" { None } else { card["filter"].as_str().filter(|f| !f.is_empty()).map(String::from) };
        let page = if kind == "mf" { 0 } else { arg.parse().unwrap_or(0) };
        let snapshot = self.session_snapshot(&sid).await?;
        let (text, markup) = self.model_overview(&snapshot, harness, page, filter).await?;
        self.answer_callback(cid, Some("Updated")).await;
        self.edit_model_card(ctx, message, &text, markup).await
    }

    fn start_model_search(&self, ctx: &Ctx, sid: &str, harness: Option<&str>) {
        self.with_state(|st| {
            let searches = st.entry("model_searches").or_insert(json!({}));
            if !searches.is_object() {
                *searches = json!({});
            }
            searches[&ctx.scope] = json!({"session_id": sid, "harness": harness, "created_at": now_i()});
        });
        self.save_state();
    }

    fn clear_model_search(&self, ctx: &Ctx) {
        let removed = self.with_state(|st| st.get_mut("model_searches").and_then(|m| m.as_object_mut()).and_then(|m| m.remove(&ctx.scope)));
        if removed.is_some() {
            self.save_state();
        }
    }

    /// The next plain text message after tapping Search: filter the catalogue
    /// and show its first page. True when the message was consumed (never sent
    /// to the agent).
    async fn answer_model_search(&self, ctx: &Ctx, text: &str) -> Result<bool> {
        let entry = self.with_state(|st| {
            let searches = st.get_mut("model_searches").and_then(|m| m.as_object_mut())?;
            searches.remove(&ctx.scope)
        });
        let Some(entry) = entry else { return Ok(false) };
        self.save_state();
        if now_i() - entry["created_at"].as_i64().unwrap_or(0) > MODEL_SEARCH_TTL_S {
            return Ok(false);
        }
        let Some(sid) = entry["session_id"].as_str().filter(|s| !s.is_empty()) else { return Ok(false) };
        let harness = entry["harness"].as_str().filter(|h| !h.is_empty()).map(String::from);
        let snapshot = match self.session_snapshot(sid).await {
            Ok(s) => s,
            Err(e) => {
                self.say(ctx, &format!("Model search failed: {e}")).await?;
                return Ok(true);
            }
        };
        let query = text.trim();
        let filter = (!query.is_empty()).then(|| query.to_string());
        match self.model_overview(&snapshot, harness, 0, filter).await {
            Ok((t, m)) => { self.send_markup(ctx, &ctx.chat_id, &t, m).await?; },
            Err(e) => { self.say(ctx, &format!("Model search failed: {e}")).await?; },
        }
        Ok(true)
    }

    // ------------------------------------------------------------ /mcp

    async fn mcp_overview(&self, ctx: &Ctx, notice: Option<&str>, interactive: bool) -> Result<(String, Option<Value>)> {
        let session = match self.current_session(ctx) {
            Some(sid) => match self.session_snapshot(&sid).await {
                Ok(s) => s,
                Err(e) => {
                    log(format!("mcp snapshot failed: {e}"));
                    json!({"id": sid})
                }
            },
            None => json!({}),
        };
        self.kurisu_mcp_overview(&session, notice, interactive).await
    }

    async fn handle_mcp_callback(&self, ctx: &Ctx, callback_id: &Value, message: &Value, data: &str) -> Result<()> {
        if !self.is_owner_dm(&ctx.chat_id) {
            self.answer_callback(callback_id, Some("MCP changes are only allowed from the owner's DM")).await;
            return Ok(());
        }
        let mut notice = None;
        if data == "mcp:r" {
            self.answer_callback(callback_id, Some("Updated")).await;
        } else {
            let parts: Vec<&str> = data.splitn(3, ':').collect();
            let tok = parts.get(1).copied().unwrap_or("").to_string();
            let action = parts.get(2).and_then(|i| i.parse::<i64>().ok()).filter(|i| *i >= 0).and_then(|i| {
                self.with_state(|st| st.get("mcp_cards").and_then(|c| c.get(&tok)).and_then(|c| c["actions"].get(i as usize)).cloned())
            });
            let Some(action) = action else {
                self.answer_callback(callback_id, Some("MCP card expired; run /mcp again")).await;
                return Ok(());
            };
            match self.kurisu_mcp_apply(&action).await {
                Ok(n) => notice = Some(n),
                Err(e) => {
                    log(format!("mcp action failed: {e}"));
                    self.answer_callback(callback_id, Some(&format!("MCP change failed: {e}"))).await;
                    return Ok(());
                }
            }
            self.with_state(|st| st.get_mut("mcp_cards").and_then(|c| c.as_object_mut()).map(|c| c.remove(&tok)));
            log(format!("mcp action {} kurisu {}", vstr(&action["op"]), action["name"].as_str().unwrap_or("lb")));
            let first = notice.as_deref().unwrap_or("").split('\n').next().unwrap_or("").to_string();
            self.answer_callback(callback_id, Some(&first)).await;
        }
        let (text, markup) = self.mcp_overview(ctx, notice.as_deref(), true).await?;
        if let Some(mid) = message["message_id"].as_i64() {
            let mut p = json!({"chat_id": ctx.chat_id, "message_id": mid, "text": text, "disable_web_page_preview": true});
            if let Some(m) = &markup {
                p["reply_markup"] = m.clone();
            }
            match self.tg1("editMessageText", p).await {
                Ok(_) => return Ok(()),
                Err(e) => log(format!("edit mcp card failed: {e}")),
            }
        }
        self.send_markup(ctx, &ctx.chat_id, &text, markup).await?;
        Ok(())
    }

    /// `/mcp_server` buttons: msv:on / msv:off start and stop the in-process
    /// listener; msv:url sends the full URL in its own message. Owner DM only.
    async fn handle_mcp_server_callback(&self, ctx: &Ctx, callback_id: &Value, message: &Value, data: &str) -> Result<()> {
        if !self.is_owner_dm(&ctx.chat_id) {
            self.answer_callback(callback_id, Some("MCP server controls are only allowed from the owner's DM")).await;
            return Ok(());
        }
        let notice = match data {
            "msv:on" => match self.k(json!({"op": "mcp_server_enable"})).await {
                Ok(v) => {
                    let addr = v["listen"].as_str().unwrap_or("").to_string();
                    self.answer_callback(callback_id, Some("Enabled")).await;
                    format!("✅ Enabled: listening on {addr}.")
                }
                Err(e) => {
                    // Nothing was started and the stored switch stayed/became off;
                    // the status line below carries the reason.
                    let why = e.to_string();
                    self.answer_callback(callback_id, Some("Enable failed")).await;
                    format!("⚠️ Enable failed: {why}")
                }
            },
            "msv:off" => {
                let r = self.k(json!({"op": "mcp_server_disable"})).await?;
                let was = r["was_on"] == json!(true);
                self.answer_callback(callback_id, Some(if was { "Stopped" } else { "Already stopped" })).await;
                if was {
                    "⏹ Disabled: the listener is stopped and will not come back after a restart.".to_string()
                } else {
                    "The outward MCP server was already off.".to_string()
                }
            }
            "msv:url" => {
                let v = self.k(json!({"op": "mcp_server_url"})).await?;
                self.answer_callback(callback_id, Some("URL sent")).await;
                let url = v["url"].as_str().unwrap_or("");
                self.say(ctx, &format!(
                    "🔗 Outward MCP server URL — keep it secret (anyone with it can list sessions and send as you)\n{url}\n\ntoken file: {}",
                    v["token_path"].as_str().unwrap_or(""))).await?;
                return Ok(());
            }
            d if d.starts_with("msv:rv:") => {
                let id = &d["msv:rv:".len()..];
                let r = self.k(json!({"op": "mcp_oauth_revoke", "client_id": id})).await?;
                self.answer_callback(callback_id, Some("Signed out")).await;
                format!("🚫 Signed out: {} token(s) removed; that client must sign in again.", r["removed"].as_u64().unwrap_or(0))
            }
            _ => {
                self.answer_callback(callback_id, Some("MCP server card expired; send /mcp_server again")).await;
                return Ok(());
            }
        };
        let (text, markup) = self.mcp_server_overview(Some(&notice)).await?;
        if let Some(mid) = message["message_id"].as_i64() {
            let mut p = json!({"chat_id": ctx.chat_id, "message_id": mid, "text": text, "disable_web_page_preview": true});
            if let Some(m) = &markup {
                p["reply_markup"] = m.clone();
            }
            match self.tg1("editMessageText", p).await {
                Ok(_) => return Ok(()),
                Err(e) => log(format!("edit mcp server card failed: {e}")),
            }
        }
        self.send_markup(ctx, &ctx.chat_id, &text, markup).await?;
        Ok(())
    }

    // ------------------------------------------------------------ /catch_up and /send_now

    async fn handle_catch_up(&self, ctx: &Ctx, override_session: Option<String>) -> Result<()> {
        let queued = self.work_pending();
        self.say(ctx, if queued { "⏳ Catch-up queued behind the current work." } else { "⏳ Catching you up..." }).await?;
        self.enqueue(Job { ctx: ctx.clone(), label: "catch-up overview".into(), kind: JobKind::CatchUp, override_session });
        Ok(())
    }

    async fn handle_send_now(self: &Arc<Self>, ctx: &Ctx) -> Result<()> {
        let sid = self.current_session(ctx);
        let Some(sid) = sid.filter(|s| self.is_active(s)) else {
            self.say(ctx, "No active run in the current session; queued messages were left unchanged.").await?;
            return Ok(());
        };
        let released = self.request_send_now(&sid);
        if released {
            // Let the waiting (oldest) message go first so the order is kept.
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline && self.act(|a| a.waiting.contains(&sid)) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        let (chat, scope) = (vstr(&ctx.chat_id), ctx.scope.clone());
        let jobs = self.drain_queued(|j| matches!(j.kind, JobKind::Text { .. } | JobKind::Attachment { .. }) && vstr(&j.ctx.chat_id) == chat && j.ctx.scope == scope);
        if jobs.is_empty() {
            self.say(ctx, if released { "🚀 Sent your waiting message to the active run." } else { "No queued messages to send." }).await?;
            return Ok(());
        }
        let mut sent = 0;
        for mut job in jobs.clone() {
            job.override_session = Some(sid.clone());
            match self.run_job_once(&job, true).await {
                Ok(()) => {
                    sent += 1;
                    log(format!("sent queued job now: {}", job.label));
                }
                Err(e) => {
                    log(format!("send-now failed ({}): {e}", job.label));
                    self.clear_pending(job.ctx.update_id);
                    self.on_job_error(&job, &e).await;
                }
            }
        }
        if sent == jobs.len() {
            self.say(ctx, &format!("🚀 Sent {sent} queued message{} to the active run.", if sent == 1 { "" } else { "s" })).await?;
        } else {
            self.say(ctx, &format!("🚀 Sent {sent} of {} queued messages to the active run; errors were reported above.", jobs.len())).await?;
        }
        Ok(())
    }

    // ------------------------------------------------------------ text and commands

    pub(crate) async fn handle_text(self: &Arc<Self>, ctx: &Ctx, text: &str, override_session: Option<String>, persist: bool,
                                    ack: bool, send_immediately: bool) -> Result<()> {
        let text = text.trim();
        if text.is_empty() {
            return Ok(());
        }
        // The picker's "Type a path" step: the next message is the directory,
        // unless it is a real slash command (so /stop still works).
        if !is_known_command(text) && self.take_terminal_path_wait(ctx) {
            return self.open_terminal_dir(ctx, text).await;
        }
        if text.starts_with('/') {
            self.clear_model_search(ctx);
            let (command, arg) = text.split_once(' ').unwrap_or((text, ""));
            let command = command.split('@').next().unwrap_or("").to_lowercase();
            match command.as_str() {
                "/start" | "/help" => {
                    self.say(ctx, HELP).await?;
                }
                "/list" => match self.session_overview(ctx, 0).await {
                    Ok((t, m)) => {
                        self.send_markup(ctx, &ctx.chat_id, &t, m).await?;
                    }
                    Err(e) => {
                        self.say(ctx, &format!("List failed: {e}")).await?;
                    }
                },
                "/id" => {
                    self.say(ctx, &self.current_session(ctx).unwrap_or_else(|| "No session selected.".into())).await?;
                }
                "/catch_up" | "/catch-up" => self.handle_catch_up(ctx, override_session).await?,
                "/stop" => self.stop_current_session(ctx).await?,
                "/undo" => self.undo_current_session(ctx).await?,
                "/model" => self.handle_model_command(ctx, arg).await?,
                "/mode" => self.handle_mode_command(ctx, arg).await?,
                "/mcp" => match self.mcp_overview(ctx, None, self.is_owner_dm(&ctx.chat_id)).await {
                    Ok((t, m)) => {
                        self.send_markup(ctx, &ctx.chat_id, &t, m).await?;
                    }
                    Err(e) => {
                        self.say(ctx, &format!("MCP list failed: {e}")).await?;
                    }
                },
                "/mcp_server" => {
                    if !self.is_owner_dm(&ctx.chat_id) {
                        self.say(ctx, "/mcp_server works only in the owner's private chat.").await?;
                    } else {
                        match self.mcp_server_overview(None).await {
                            Ok((t, m)) => {
                                self.send_markup(ctx, &ctx.chat_id, &t, m).await?;
                            }
                            Err(e) => {
                                self.say(ctx, &format!("MCP server status failed: {e}")).await?;
                            }
                        }
                    }
                },
                "/version" => {
                    let info = self.k(json!({"op": "version"})).await?;
                    self.say(ctx, &crate::version::format(&info)).await?;
                }
                "/usage" => self.handle_usage_command(ctx).await?,
                "/schedule" | "/wakes" => {
                    let (t, m) = self.schedule_card().await;
                    self.send_markup(ctx, &ctx.chat_id, &t, m).await?;
                }
                "/browser" => self.remote_command(ctx, "browser", arg).await?,
                "/web" => self.remote_command(ctx, "web", arg).await?,
                "/terminal" => self.remote_command(ctx, "terminal", arg).await?,
                "/send-now" | "/send_now" => self.handle_send_now(ctx).await?,
                "/compact" => {
                    if ["this", "current", "here", "."].contains(&arg.trim().to_lowercase().as_str()) {
                        self.compact_current_session(ctx, None).await?;
                    } else {
                        match self.compact_picker(ctx, 0).await {
                            Ok((t, m)) => {
                                self.send_markup(ctx, &ctx.chat_id, &t, Some(m)).await?;
                            }
                            Err(e) => {
                                self.say(ctx, &format!("Compact picker failed: {e}")).await?;
                            }
                        }
                    }
                }
                "/status" => {
                    let sid = self.ensure_session(ctx).await?;
                    match self.session_snapshot(&sid).await {
                        Ok(snap) => {
                            let harness = harness_label(&snap, None);
                            let (model_id, short) = self.resolved_model(&snap, harness.as_deref()).await;
                            let mut model = model_id.clone().unwrap_or(short);
                            let raw = snap["model_override"].as_str().filter(|m| !m.is_empty()).or(snap["llm_model"].as_str().filter(|m| !m.is_empty())).unwrap_or("default");
                            if model_id.is_some() && raw.to_lowercase() == "default" {
                                model.push_str(" (codex default)");
                            }
                            let mut clis = self.cli_status_line().await;
                            if let Ok(b) = self.k(json!({"op": "browser_status"})).await {
                                if let Some(l) = b["line"].as_str() {
                                    let mut l = l.to_string();
                                    if let Some(r) = l.get_mut(0..1) {
                                        r.make_ascii_uppercase();
                                    }
                                    clis.push_str(&format!("\n{l}"));
                                }
                            }
                            self.say(ctx, &format!("status: {}\ntitle: {}\nharness: {}\nmodel: {model}\neffort: {}\npermissions: {} (/mode)\ncontext: {}\nsession: {sid}\n{clis}",
                                                   vstr(&snap["status"]), snap["title"].as_str().filter(|t| !t.is_empty()).unwrap_or("(untitled)"),
                                                   harness.unwrap_or_else(|| "unknown".into()),
                                                   snap["reasoning_effort"].as_str().filter(|e| !e.is_empty()).unwrap_or("default"),
                                                   snap["permission_mode"].as_str().unwrap_or("default"),
                                                   format_context_usage(&snap))).await?;
                        }
                        Err(e) => {
                            self.say(ctx, &format!("Status failed: {e}")).await?;
                        }
                    }
                }
                "/new" | "/reset" => {
                    let sid = self.create_session(ctx).await?;
                    self.select_session(ctx, &sid);
                    if persist {
                        self.save_state();
                    }
                    self.say(ctx, &format!("Started a new session: {sid}")).await?;
                }
                "/use" => {
                    let mut target = arg.trim().to_string();
                    if target.is_empty() {
                        self.say(ctx, "Usage: /use <session-id-or-prefix>").await?;
                        return Ok(());
                    }
                    let ids: Vec<String> = match self.fetch_sessions().await {
                        Ok(s) => s.iter().filter_map(|x| x["id"].as_str().map(String::from)).collect(),
                        Err(e) => {
                            self.say(ctx, &format!("Cannot list sessions: {e}")).await?;
                            return Ok(());
                        }
                    };
                    if !ids.contains(&target) {
                        let matches: Vec<&String> = ids.iter().filter(|i| i.starts_with(&target)).collect();
                        match matches.len() {
                            1 => target = matches[0].clone(),
                            0 => {
                                self.say(ctx, &format!("No session matches {target}")).await?;
                                return Ok(());
                            }
                            _ => {
                                self.say(ctx, &format!("Ambiguous prefix. Matches: {}", matches.iter().take(8).map(|m| take(m, 8)).collect::<Vec<_>>().join(", "))).await?;
                                return Ok(());
                            }
                        }
                    }
                    let snap = match self.session_snapshot(&target).await {
                        Ok(s) => s,
                        Err(e) => {
                            self.say(ctx, &format!("Cannot use {target}: {e}")).await?;
                            return Ok(());
                        }
                    };
                    self.select_session(ctx, &target);
                    if persist {
                        self.save_state();
                    }
                    self.say(ctx, &format!("Now using {target} ({})", vstr(&snap["status"]))).await?;
                }
                _ => {
                    self.say(ctx, "Unknown command. Try /help.").await?;
                }
            }
            return Ok(());
        }
        if ack && send_immediately {
            self.say(ctx, "⏳ On it.").await?;
        }
        let announce = if ack && !send_immediately { Announce::Yes } else { Announce::No };
        self.dispatch_content(ctx, vec![json!({"type": "input_text", "text": text})], override_session, persist, send_immediately, announce).await
    }

    // ------------------------------------------------------------ callbacks

    async fn handle_callback(self: &Arc<Self>, callback: &Value) -> Result<()> {
        let cid = &callback["id"];
        let data = callback["data"].as_str().unwrap_or("");
        let message = &callback["message"];
        let ctx = self.ctx(message["chat"]["id"].clone(), message["message_thread_id"].as_i64(), None);
        if !self.allowed_sender(&ctx.chat_id, &callback["from"]) {
            self.answer_callback(cid, Some("Not allowed")).await;
            return Ok(());
        }
        let guard = |label: &'static str| move |e: anyhow::Error| (label, e);
        let res: std::result::Result<(), (&str, anyhow::Error)> = if data.starts_with("approval:") {
            self.handle_approval_callback(&ctx, cid, data).await.map_err(guard("Approval failed"))
        } else if let Some(rest) = data.strip_prefix("mode:") {
            let (sid, mode) = rest.rsplit_once(':').unwrap_or(("", ""));
            self.answer_callback(cid, Some(mode)).await;
            self.set_mode(&ctx, sid, mode, Some(message)).await.map_err(guard("Mode change failed"))
        } else if data.starts_with("en:") {
            self.handle_enable_callback(&ctx, cid, data).await.map_err(guard("Approval failed"))
        } else if data.starts_with("pair:") {
            self.handle_pair_callback(&ctx, cid, data).await.map_err(guard("Approval failed"))
        } else if data.starts_with("mh:") || data.starts_with("mx:") {
            self.handle_model_harness_callback(&ctx, cid, message, data).await.map_err(guard("Model switch failed"))
        } else if data.starts_with("mp:") || data.starts_with("mf:") || data.starts_with("ms:") || data == "msc" {
            self.handle_model_page_callback(&ctx, cid, message, data).await.map_err(guard("Model list failed"))
        } else if data.starts_with("model:") || data.starts_with("me:") {
            self.handle_model_callback(&ctx, cid, message, data).await.map_err(guard("Model switch failed"))
        } else if data.starts_with("q:") {
            self.handle_question_callback(&ctx, cid, data).await.map_err(guard("Answer failed"))
        } else if data.starts_with("td:") {
            self.handle_terminal_callback(&ctx, cid, message, data).await.map_err(guard("Terminal failed"))
        } else if data.starts_with("mcp:") {
            self.handle_mcp_callback(&ctx, cid, message, data).await.map_err(guard("MCP action failed"))
        } else if data.starts_with("moa:") {
            self.handle_oauth_callback(&ctx, cid, data).await.map_err(guard("Sign-in approval failed"))
        } else if data.starts_with("msv:") {
            self.handle_mcp_server_callback(&ctx, cid, message, data).await.map_err(guard("MCP server action failed"))
        } else if data.starts_with("cmp:") {
            if let Err(e) = self.handle_compact_callback(&ctx, cid, message, data).await {
                log(format!("compact callback failed: {e}"));
                self.say(&ctx, &format!("Compact failed: {e}")).await?;
            }
            return Ok(());
        } else {
            let mut page = 0;
            if let Some(target) = data.strip_prefix("use:") {
                match self.session_snapshot(target).await {
                    Ok(snap) => {
                        self.select_session(&ctx, target);
                        self.save_state();
                        self.answer_callback(cid, Some(&format!("Selected {} ({})", take(target, 8), vstr(&snap["status"])))).await;
                    }
                    Err(e) => {
                        self.answer_callback(cid, Some(&format!("Cannot use {}: {e}", take(target, 8)))).await;
                        return Ok(());
                    }
                }
            } else if data == "sched" || data.starts_with("wc:") {
                if let Some(id) = data.strip_prefix("wc:").and_then(|i| i.parse::<i64>().ok()) {
                    match self.k(json!({"op": "wake_cancel", "id": id})).await {
                        Ok(_) => self.answer_callback(cid, Some(&format!("Cancelled wake #{id}"))).await,
                        Err(e) => self.answer_callback(cid, Some(&format!("Cannot cancel #{id}: {e}"))).await,
                    }
                } else {
                    self.answer_callback(cid, Some("Updated")).await;
                }
                let (text, markup) = self.schedule_card().await;
                if let Some(mid) = message["message_id"].as_i64() {
                    if let Err(e) = self.tg1("editMessageText", json!({"chat_id": ctx.chat_id, "message_id": mid, "text": text,
                            "disable_web_page_preview": true, "reply_markup": markup.unwrap_or(json!({"inline_keyboard": []}))})).await {
                        log(format!("edit schedule failed: {e}"));
                    }
                }
                return Ok(());
            } else if data == "list" || data.starts_with("list:") {
                page = data.strip_prefix("list:").and_then(|p| p.parse().ok()).unwrap_or(0);
                self.answer_callback(cid, Some("Updated")).await;
            } else {
                self.answer_callback(cid, Some("Unknown action")).await;
                return Ok(());
            }
            let (text, markup) = self.session_overview(&ctx, page).await?;
            if let Some(mid) = message["message_id"].as_i64() {
                let mut p = json!({"chat_id": ctx.chat_id, "message_id": mid, "text": text});
                if let Some(m) = &markup {
                    p["reply_markup"] = m.clone();
                }
                match self.tg1("editMessageText", p).await {
                    Ok(_) => return Ok(()),
                    Err(e) => log(format!("edit list failed: {e}")),
                }
            }
            self.send_markup(&ctx, &ctx.chat_id, &text, markup).await?;
            return Ok(());
        };
        if let Err((label, e)) = res {
            log(format!("{} callback failed: {e}", label.split(' ').next().unwrap_or("").to_lowercase()));
            self.answer_callback(cid, Some(label)).await;
        }
        Ok(())
    }

    // ------------------------------------------------------------ the update loop

    fn enqueue_update_job(&self, update_id: i64, entry: &Value, ack: bool) {
        let ctx = self.ctx(entry["chat_id"].clone(), entry["thread_id"].as_i64(), Some(update_id));
        let kind = if entry["kind"] == "attachment" {
            JobKind::Attachment { attachment: entry["attachment"].clone(), caption: entry["caption"].as_str().unwrap_or("").to_string() }
        } else {
            JobKind::Text { text: entry["text"].as_str().unwrap_or("").to_string(), ack }
        };
        let label = format!("{} update {update_id}", if entry["kind"] == "attachment" { "attachment" } else { "message" });
        self.enqueue(Job { ctx, label, kind, override_session: None });
    }

    fn replay_pending_updates(&self) {
        let pending: Vec<(i64, Value)> = self.with_state(|st| st.get("pending_updates").and_then(|p| p.as_object()).map(|m| {
            m.iter().filter_map(|(k, v)| k.parse::<i64>().ok().map(|k| (k, v.clone()))).collect()
        }).unwrap_or_default());
        let mut pending = pending;
        pending.sort_by_key(|(k, _)| *k);
        for (k, entry) in pending {
            log(format!("replaying unposted update {k}"));
            self.enqueue_update_job(k, &entry, false);
        }
    }

    async fn initial_offset(&self) -> i64 {
        if let Some(o) = self.with_state(|st| st.get("offset").and_then(Value::as_i64)) {
            return o;
        }
        let r = self.tg("getUpdates", Some(json!({"offset": -1, "timeout": 0, "allowed_updates": ALLOWED_UPDATES})), 60, Some(120)).await;
        match r {
            Ok(Value::Array(u)) if !u.is_empty() => u.iter().filter_map(|x| x["update_id"].as_i64()).max().unwrap_or(-1) + 1,
            _ => 0,
        }
    }

    fn set_offset(&self, offset: i64) {
        self.with_state(|st| st.insert("offset".into(), json!(offset)));
        self.save_state();
    }

    pub(crate) async fn run_forever(self: Arc<Self>) {
        let mut offset = self.initial_offset().await;
        self.set_offset(offset);
        log(format!("bridge started; session={} chat={}", self.current_session(&self.dm()).unwrap_or_else(|| "None".into()), self.cfg.chat_id));
        match self.sync_bot_commands().await {
            Ok(()) => log("Telegram command menu synced"),
            Err(e) => log(format!("Telegram command menu sync failed: {e}")),
        }
        let broadcaster = {
            let b = self.clone();
            async move { b.broadcast_loop().await }
        };
        self.replay_pending_updates();
        let poller = async {
            let mut failures: u32 = 0;
            loop {
                let mut current: Option<i64> = None;
                let r: Result<()> = async {
                    let updates = self.tg("getUpdates", Some(json!({"offset": offset, "timeout": 50, "allowed_updates": ALLOWED_UPDATES})), 60, Some(0)).await?;
                    failures = 0;
                    if let Some(o) = self.take_outage() {
                        let dm = self.dm();
                        self.send_notice(&dm, &self.chat_id(), &outage_notice_text(o)).await;
                    }
                    for update in updates.as_array().cloned().unwrap_or_default() {
                        let update_id = update["update_id"].as_i64().unwrap_or(0);
                        current = Some(update_id);
                        offset = update_id + 1;
                        self.set_offset(offset);
                        self.handle_update(update_id, &update).await?;
                        self.set_offset(offset);
                    }
                    Ok(())
                }.await;
                if let Err(e) = r {
                    log(format!("poll failed: {e}"));
                    if let Some(c) = current.filter(|c| is_network_error(&e) && !self.is_pending(*c)) {
                        // Never skip an update we could not finish handling: fetch it again.
                        offset = c;
                        self.set_offset(c);
                        log(format!("rewound offset to unhandled update {c}"));
                    }
                    failures += 1;
                    tokio::time::sleep(Duration::from_secs((5 * 2u64.pow(failures.saturating_sub(1).min(3))).min(30))).await;
                }
            }
        };
        let handovers = {
            let b = self.clone();
            async move { b.handover_loop().await }
        };
        tokio::join!(broadcaster, poller, handovers);
    }

    async fn handle_update(self: &Arc<Self>, update_id: i64, update: &Value) -> Result<()> {
        if let Some(cb) = update.get("callback_query").filter(|c| !c.is_null()) {
            if let Err(e) = self.handle_callback(cb).await {
                if is_network_error(&e) {
                    return Err(e);
                }
                log(format!("callback handling failed: {e}"));
            }
            return Ok(());
        }
        if let Some(m) = update.get("my_chat_member").filter(|c| !c.is_null()) {
            let chat = &m["chat"];
            log(format!("membership update chat={} type={} title={} status={}", vstr(&chat["id"]), vstr(&chat["type"]), vstr(&chat["title"]), vstr(&m["new_chat_member"]["status"])));
            self.with_state(|st| {
                let chats = st.entry("chats").or_insert(json!({}));
                chats[vstr(&chat["id"])] = json!({"type": chat["type"], "title": chat["title"], "status": m["new_chat_member"]["status"]});
            });
            self.save_state();
            return Ok(());
        }
        let is_channel_post = update.get("channel_post").is_some_and(|c| !c.is_null());
        let mut message = update.get("message").filter(|m| !m.is_null()).or(update.get("channel_post")).cloned().unwrap_or(json!({}));
        let chat_id = message["chat"]["id"].clone();
        let ctx = self.ctx(chat_id.clone(), message["message_thread_id"].as_i64(), Some(update_id));
        let origin_chat = message["forward_origin"]["chat"].clone();
        let origin_chat = if origin_chat.is_object() { origin_chat } else { message["forward_from_chat"].clone() };
        if !origin_chat["id"].is_null() {
            log(format!("forwarded from chat={} type={} title={}", vstr(&origin_chat["id"]), vstr(&origin_chat["type"]), vstr(&origin_chat["title"])));
            self.with_state(|st| {
                let chats = st.entry("chats").or_insert(json!({}));
                chats[vstr(&origin_chat["id"])] = json!({"type": origin_chat["type"], "title": origin_chat["title"]});
            });
            self.save_state();
        }
        let mut text = message["text"].as_str().map(String::from);
        let attachment = attachment_from_message(&message);
        if is_channel_post {
            if message["chat"]["type"] != "channel" {
                return Ok(());
            }
            let explicit = text.as_deref().unwrap_or("").trim().starts_with('/');
            if !explicit && !self.bot_mentioned(&message).await {
                log(format!("ignoring channel post {update_id} chat={} (bot not mentioned)", vstr(&chat_id)));
                return Ok(());
            }
            let clean_text = self.strip_bot_mention(text.as_deref()).await;
            let clean_caption = self.strip_bot_mention(message["caption"].as_str()).await;
            if clean_text != text || clean_caption.as_deref() != message["caption"].as_str() {
                message["text"] = json!(clean_text);
                message["caption"] = json!(clean_caption);
                text = clean_text;
            }
        } else if !self.allowed_sender(&chat_id, &message["from"]) {
            return Ok(());
        }
        // Kept before any work: a message that arrived while a turn was running,
        // or whose reply never went out, is still in `telegram_history` afterwards.
        if self.is_owner_dm(&chat_id) {
            let at = message["date"].as_i64().filter(|d| *d > 0).unwrap_or_else(now_i);
            let body = text.as_deref().or(message["caption"].as_str()).unwrap_or("");
            super::history::inbound(&self.server, &self.cfg.chat_id, update_id, message["message_id"].as_i64(),
                                    body, attachment.as_ref(), at).await;
        }
        if let Some(att) = attachment {
            log(format!("incoming attachment update {update_id} chat={} name={}", vstr(&chat_id), vstr(&att["name"])));
            let entry = json!({"kind": "attachment", "chat_id": chat_id, "thread_id": message["message_thread_id"], "attachment": att,
                               "caption": message["caption"].as_str().unwrap_or("")});
            let ctx = self.ctx(chat_id.clone(), message["message_thread_id"].as_i64(), Some(update_id));
            if is_audio_attachment(att["name"].as_str().unwrap_or(""), att["mime"].as_str().unwrap_or("")) && self.text_question_pending(&ctx) {
                // A voice note while a question waits is its spoken answer: transcribe it now
                // (the asking turn is blocked on it, so the queue would not get to it).
                let (b, message) = (self.clone(), message.clone());
                tokio::spawn(async move {
                    if !b.answer_question_with_voice(&ctx, &message, &att).await {
                        b.record_pending(update_id, entry.clone());
                        b.enqueue_update_job(update_id, &entry, false);
                    }
                });
                return Ok(());
            }
            self.record_pending(update_id, entry.clone());
            if self.work_pending() {
                self.send_notice(&ctx, &chat_id, "⏳ Attachment queued. Tap /send_now to send it now.").await;
            }
            self.enqueue_update_job(update_id, &entry, false);
            return Ok(());
        }
        let Some(text) = text.filter(|t| !t.trim().is_empty()) else {
            if vstr(&chat_id) == self.cfg.chat_id || message["chat"]["type"] == "private" {
                self.send_text(&ctx, &chat_id, "Send text, a photo, or a file.").await?;
            }
            return Ok(());
        };
        log(format!("incoming update {update_id} chat={} chars={}", vstr(&chat_id), clen(&text)));
        if text.trim_start().starts_with('/') {
            if let Err(e) = self.handle_text(&ctx, &text, None, true, true, false).await {
                if is_network_error(&e) {
                    return Err(e);
                }
                log(format!("message handling failed: {e}"));
                let _ = self.send_text(&ctx, &chat_id, &format!("Bridge error: {e}")).await;
            } else if self.is_owner_dm(&chat_id) {
                // The command got its reply right here (no turn is started).
                super::history::replied(&self.server, &self.cfg.chat_id, update_id, now_i()).await;
            }
        } else if self.answer_question_with_text(&ctx, &message, &text).await {
            log(format!("update {update_id} answered a pending question"));
        } else if self.answer_model_search(&ctx, &text).await? {
            log(format!("update {update_id} answered a model search"));
        } else {
            let text = quoted_message_text(&message, &text);
            let pending = self.work_pending();
            let entry = json!({"kind": "text", "chat_id": chat_id, "thread_id": message["message_thread_id"], "text": text});
            // Kept on disk until posted to its session, so a failed reply or a restart cannot lose it.
            self.record_pending(update_id, entry.clone());
            if pending {
                self.send_notice(&ctx, &chat_id, "⏳ Queued — finishing the current task first. Tap /send_now to send it now.").await;
            }
            // Otherwise the worker acknowledges once it knows whether the session is free or busy.
            self.enqueue_update_job(update_id, &entry, !pending);
        }
        Ok(())
    }
}

const ALLOWED_UPDATES: &str = r#"["message", "channel_post", "my_chat_member", "callback_query"]"#;

// ------------------------------------------------------------------ helpers

/// The first N.N[.N] in a version string.
pub fn short_version(text: &str) -> String {
    let b = text.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i].is_ascii_digit() {
            let start = i;
            let mut dots = 0;
            let mut j = i;
            while j < b.len() && (b[j].is_ascii_digit() || (b[j] == b'.' && dots < 2 && j + 1 < b.len() && b[j + 1].is_ascii_digit())) {
                if b[j] == b'.' {
                    dots += 1;
                }
                j += 1;
            }
            if dots >= 1 {
                return text[start..j].to_string();
            }
            i = j;
        } else {
            i += 1;
        }
    }
    String::new()
}

pub fn page_bounds(total: usize, page: i64) -> (i64, usize) {
    let pages = total.div_ceil(LIST_PAGE_SIZE).max(1) as i64;
    let page = page.clamp(0, pages - 1);
    (page, page as usize * LIST_PAGE_SIZE)
}

/// One /model page: the session it belongs to, the selected harness and the
/// card token its buttons point at.
pub struct ModelPage<'a> {
    pub tok: &'a str,
    pub sid: &'a str,
    pub title: &'a str,
    pub harness: &'a str,
    pub current_harness: &'a str,
    pub current_model: &'a str,
    pub switching: bool,
    /// Models before filtering, when a search query is active.
    pub filtered_of: Option<usize>,
}

/// The visible page and its first index, clamped to the last page.
pub fn model_page_bounds(total: usize, page: i64) -> (i64, usize) {
    let pages = total.div_ceil(MODEL_PAGE_SIZE).max(1) as i64;
    let page = page.clamp(0, pages - 1);
    (page, page as usize * MODEL_PAGE_SIZE)
}

/// Models whose id or display label contains `query`, case-insensitively.
pub fn filter_models(options: &[Value], query: &str) -> Vec<Value> {
    let q = query.trim().to_lowercase();
    if q.is_empty() {
        return options.to_vec();
    }
    options.iter().filter(|o| {
        let id = o["id"].as_str().unwrap_or("").to_lowercase();
        let label = o["label"].as_str().unwrap_or("").to_lowercase();
        id.contains(&q) || label.contains(&q)
    }).cloned().collect()
}

/// Text and keyboard for one /model page. The last button is always Search,
/// and a filtered page also offers a way back to the full list.
pub fn model_page_view(page: &ModelPage, options: &[Value], filter: Option<&str>, index: i64) -> (String, Value) {
    let total = options.len();
    let (page_no, start) = model_page_bounds(total, index);
    let end = (start + MODEL_PAGE_SIZE).min(total);
    let mut lines = if page.switching {
        vec![format!("Step 2 of 3 — {} models (session is on {} now)", page.harness, page.current_harness)]
    } else {
        vec![format!("Current model: {}", page.current_model)]
    };
    lines.push(format!("Session: {} ({})", page.title, take(page.sid, 8)));
    if let Some(q) = filter.filter(|q| !q.trim().is_empty()) {
        lines.push(format!("🔎 Filter: \"{q}\" · {total} of {} models", page.filtered_of.unwrap_or(total)));
    }
    if total == 0 {
        lines.push(if filter.is_some_and(|q| !q.trim().is_empty()) {
            "No models match that search.".into()
        } else {
            "No model choices are available from the host.".into()
        });
    } else {
        lines.push(format!("Page {}/{} · models {}-{end} of {total}",
                           page_no + 1, total.div_ceil(MODEL_PAGE_SIZE).max(1), start + 1));
        lines.extend(["".into(), "Tap a model to switch this session:".into()]);
    }
    let mut buttons: Vec<Value> = vec![];
    for (i, o) in options.iter().enumerate().skip(start).take(MODEL_PAGE_SIZE) {
        let id = o["id"].as_str().unwrap_or("");
        let selected = !page.current_model.is_empty() && (id == page.current_model || id.rsplit('/').next() == Some(page.current_model));
        let label = format!("{}{}", if selected { "✓ " } else { "" }, o["label"].as_str().unwrap_or(id));
        buttons.push(json!([{"text": take(&label, 64), "callback_data": format!("model:{}:{i}", page.tok)}]));
    }
    let mut nav = vec![];
    if page_no > 0 {
        nav.push(json!({"text": "⬅️ Previous", "callback_data": format!("mp:{}:{}", page.tok, page_no - 1)}));
    }
    if end < total {
        nav.push(json!({"text": "Next ➜", "callback_data": format!("mp:{}:{}", page.tok, page_no + 1)}));
    }
    if !nav.is_empty() {
        buttons.push(json!(nav));
    }
    if filter.is_some_and(|q| !q.trim().is_empty()) {
        buttons.push(json!([{"text": "✖ Show all models", "callback_data": format!("mf:{}", page.tok)}]));
    }
    if total > 0 || filter.is_some() {
        buttons.push(json!([{"text": "🔎 Search", "callback_data": format!("ms:{}", page.tok)}]));
    }
    if page.switching {
        lines.extend(["".into(), "/model <model-id> changes the model on the current harness only.".into()]);
    } else {
        lines.extend(["".into(), "You can also send /model <model-id>.".into(), "Send /model default to restore the agent default.".into()]);
    }
    (lines.join("\n"), json!({"inline_keyboard": buttons}))
}

/// A folder for a phone-width line: ~ for home, and only the last parts when long.
pub fn short_path(path: &str, home: Option<&str>) -> String {
    let mut p = path.trim_end_matches('/').to_string();
    if let Some(h) = home.map(|h| h.trim_end_matches('/')).filter(|h| !h.is_empty()) {
        if p == h {
            p = "~".into();
        } else if let Some(rest) = p.strip_prefix(&format!("{h}/")) {
            p = format!("~/{rest}");
        }
    }
    if clen(&p) <= 40 {
        return p;
    }
    let parts: Vec<&str> = p.split('/').filter(|x| !x.is_empty()).collect();
    let mut tail: Vec<&str> = vec![];
    for part in parts.iter().rev() {
        let len: usize = tail.iter().map(|t| clen(t) + 1).sum::<usize>() + clen(part);
        if !tail.is_empty() && len > 36 {
            break;
        }
        tail.insert(0, part);
    }
    format!("…/{}", tail.join("/"))
}

/// Directories bare /terminal offers: one per session workspace (its own worker
/// folder, named like /list), then the instance home and the workspaces root.
pub fn terminal_dir_entries(home: &std::path::Path, sessions: &[Value]) -> Vec<(String, String)> {
    let home_s = home.to_string_lossy().trim_end_matches('/').to_string();
    let home_env = std::env::var("HOME").ok();
    let mut seen = std::collections::HashSet::new();
    let mut out: Vec<(String, String)> = vec![];
    for s in sessions {
        let Some(dir) = s["workspace"].as_str().filter(|d| !d.is_empty()) else { continue };
        if !seen.insert(dir.to_string()) {
            continue;
        }
        let name = squash(s["title"].as_str().filter(|t| !t.is_empty()).unwrap_or("(untitled)"));
        out.push((format!("{name} · {}", short_path(dir, home_env.as_deref())), dir.to_string()));
    }
    for (label, dir) in [("🏠 Kurisu home", home_s.clone()),
                         ("📁 Workspaces", format!("{home_s}/workspaces"))] {
        if seen.insert(dir.clone()) {
            out.push((format!("{label} · {}", short_path(&dir, home_env.as_deref())), dir));
        }
    }
    out
}

/// The picker's text: numbered directories, no links.
pub fn terminal_picker_text(entries: &[(String, String)]) -> String {
    let mut lines = vec!["🖥 Terminal — which directory?".to_string(), String::new()];
    for (i, (label, _)) in entries.iter().enumerate() {
        lines.push(format!("{}. {label}", i + 1));
    }
    lines.extend([String::new(), "Pick a folder below, or send /terminal <path>.".into()]);
    lines.join("\n")
}

/// A directory typed after /terminal: `~` expands to $HOME and it must be an
/// existing absolute directory.
pub fn terminal_dir(raw: &str) -> Result<std::path::PathBuf> {
    let raw = raw.trim();
    if raw.is_empty() {
        bail!("send a directory path");
    }
    let expanded = if raw == "~" || raw.starts_with("~/") {
        let home = std::env::var("HOME").unwrap_or_default();
        if home.is_empty() {
            bail!("HOME is not set; send an absolute path");
        }
        format!("{home}{}", &raw[1..])
    } else {
        raw.to_string()
    };
    let path = std::path::PathBuf::from(&expanded);
    if !path.is_absolute() {
        bail!("{expanded} is not an absolute path");
    }
    if !path.is_dir() {
        bail!("{expanded} is not a directory");
    }
    Ok(path)
}

/// The slash commands handle_text dispatches. While /terminal waits for a typed
/// path, a real command still runs instead of being taken as a directory.
pub fn is_known_command(text: &str) -> bool {
    let first = text.split_whitespace().next().unwrap_or("");
    let cmd = first.split('@').next().unwrap_or("").to_lowercase();
    matches!(cmd.as_str(), "/start" | "/help" | "/list" | "/id" | "/catch_up" | "/catch-up" | "/stop"
        | "/model" | "/mode" | "/mcp" | "/mcp_server" | "/version" | "/usage" | "/schedule" | "/wakes" | "/browser"
        | "/web" | "/terminal" | "/send-now" | "/send_now" | "/compact" | "/status" | "/new" | "/reset" | "/use" | "/undo")
}

/// Relative time to a future (or overdue) unix time: "in 2h 05m", "in 3d 4h", "overdue 5m".
pub fn relative_to(at: i64, now: i64) -> String {
    let d = at - now;
    let s = d.abs();
    let (days, h, m) = (s / 86400, s % 86400 / 3600, s % 3600 / 60);
    let span = if days > 0 { format!("{days}d {h}h") } else if h > 0 { format!("{h}h {m:02}m") } else if m > 0 { format!("{m}m") } else { format!("{s}s") };
    if d >= 0 { format!("in {span}") } else { format!("overdue {span}") }
}

/// Text and Cancel buttons for /schedule from the server's wake rows (pending only, soonest first).
pub fn schedule_text(wakes: &[Value], now: i64) -> (String, Option<Value>) {
    let mut pending: Vec<&Value> = wakes.iter().filter(|w| w["status"].as_str().unwrap_or("pending") == "pending").collect();
    pending.sort_by_key(|w| w["at"].as_i64().unwrap_or(i64::MAX));
    if pending.is_empty() {
        return ("⏰ No pending wakes.".into(), Some(json!({"inline_keyboard": [[{"text": "🔄 Refresh", "callback_data": "sched"}]]})));
    }
    let mut lines = vec![format!("⏰ Pending wakes ({})", pending.len()), String::new()];
    let mut buttons = vec![];
    for w in pending {
        let id = w["id"].as_i64().unwrap_or(0);
        let target = w["session_name"].as_str().filter(|n| !n.is_empty()).or(w["session"].as_str()).unwrap_or("?");
        let at = w["at"].as_i64().unwrap_or(0);
        let local = w["at_local"].as_str().map(String::from).unwrap_or_else(|| crate::wakes::show(at));
        lines.push(format!("#{id} → {target} · {local} ({})", relative_to(at, now)));
        if let Some(r) = w["repeat"].as_str().filter(|r| !r.is_empty()) {
            lines.push(format!("   🔁 {r}"));
        }
        let mut msg = squash(w["message"].as_str().unwrap_or(""));
        if clen(&msg) > 80 {
            msg = format!("{}…", take(&msg, 79));
        }
        lines.push(format!("   {msg}"));
        buttons.push(json!([{"text": format!("✖ Cancel #{id} ({})", take(target, 20)), "callback_data": format!("wc:{id}")}]));
    }
    buttons.push(json!([{"text": "🔄 Refresh", "callback_data": "sched"}]));
    (lines.join("\n"), Some(json!({"inline_keyboard": buttons})))
}

pub fn duration_ago(ts: f64) -> String {
    let s = (now_f() - ts).max(0.0) as i64;
    let (d, r) = (s / 86400, s % 86400);
    let (h, r) = (r / 3600, r % 3600);
    let (m, sec) = (r / 60, r % 60);
    format!("{d}d {h:02}h {m:02}m {sec:02}s ago")
}

/// Phone-width model name: claude-opus-5-5 -> opus-5.5.
pub fn short_model(model: &str) -> String {
    let name = model.trim();
    if name.is_empty() {
        return "default".into();
    }
    let mut name = name.rsplit('/').next().unwrap_or(name).to_string();
    if let Some(n) = name.strip_prefix("claude-") {
        name = n.to_string();
    }
    // strip -YYYYMMDD
    if name.len() > 9 {
        let (head, tail) = name.split_at(name.len() - 9);
        if tail.starts_with('-') && tail[1..].chars().all(|c| c.is_ascii_digit()) {
            name = head.to_string();
        }
    }
    // -(\d+)-(\d+)$ -> -\1.\2
    let parts: Vec<&str> = name.rsplitn(3, '-').collect();
    if parts.len() == 3 && !parts[0].is_empty() && !parts[1].is_empty() && parts[0].chars().all(|c| c.is_ascii_digit()) && parts[1].chars().all(|c| c.is_ascii_digit()) {
        return format!("{}-{}.{}", parts[2], parts[1], parts[0]);
    }
    name
}

pub fn harness_label(snap: &Value, session: Option<&Value>) -> Option<String> {
    let raw = snap["kurisu_harness"].as_str().filter(|h| !h.is_empty())
        .or(snap["harness"].as_str().filter(|h| !h.is_empty()))
        .or(session.and_then(|s| s["harness"].as_str()).filter(|h| !h.is_empty()))?;
    Some(if raw == "pi-native" { "pi".into() } else { raw.to_string() })
}

fn price_for(model: &str) -> Option<(f64, f64)> {
    let n = model.to_lowercase();
    [("opus", (5.0, 25.0)), ("sonnet", (3.0, 15.0)), ("haiku", (1.0, 5.0))].into_iter().find(|(f, _)| n.contains(f)).map(|(_, p)| p)
}

/// API-equivalent spend, or None.
pub fn estimated_cost(snap: &Value) -> Option<f64> {
    if let Some(t) = snap["total_cost_usd"].as_f64().filter(|t| *t > 0.0) {
        return Some(t);
    }
    let usage = snap["usage_by_model"].as_object()?;
    let (mut cost, mut priced) = (0.0, false);
    for (model, u) in usage {
        let Some((inp, out)) = price_for(model) else { continue };
        let n = |k: &str| u[k].as_f64().unwrap_or(0.0);
        let tokens = n("input_tokens") + n("output_tokens") + n("cache_creation_input_tokens") + n("cache_read_input_tokens");
        if tokens == 0.0 {
            continue;
        }
        priced = true;
        cost += (n("input_tokens") * inp + n("output_tokens") * out + n("cache_creation_input_tokens") * inp * 1.25 + n("cache_read_input_tokens") * inp * 0.1) / 1_000_000.0;
    }
    priced.then_some(cost)
}

pub fn short_tokens(count: f64) -> String {
    let c = count as i64;
    if c >= 1_000_000 {
        let t = format!("{:.1}", c as f64 / 1_000_000.0);
        let t = t.trim_end_matches('0').trim_end_matches('.');
        return format!("{t}M");
    }
    if c >= 1000 {
        return format!("{}K", (c as f64 / 1000.0).round_ties_even() as i64);
    }
    c.to_string()
}

pub fn context_label(snap: &Value) -> Option<String> {
    let used = snap["last_total_tokens"].as_f64();
    let window = snap["context_window"].as_f64().filter(|w| *w > 0.0);
    if snap["context_pending"] == json!(true) && used.is_none() {
        return Some(match window {
            Some(w) => format!("pending/{}", short_tokens(w)),
            None => "pending".into(),
        });
    }
    let used = used.filter(|u| *u > 0.0)?;
    Some(match window {
        Some(w) => format!("{}/{}", short_tokens(used), short_tokens(w)),
        None => short_tokens(used),
    })
}

pub fn short_list(names: &[String], limit: usize) -> String {
    if names.len() <= limit {
        return names.join(", ");
    }
    format!("{} +{}", names[..limit].join(", "), names.len() - limit)
}

pub fn format_context_usage(snap: &Value) -> String {
    let used = snap["last_total_tokens"].as_i64().filter(|u| *u >= 0);
    let window = snap["context_window"].as_i64().filter(|w| *w > 0);
    if snap["context_pending"] == json!(true) && used.is_none() {
        return match window {
            Some(w) => format!("pending until the next turn (window: {} tokens)", fmt_int(w)),
            None => "pending until the next turn".into(),
        };
    }
    let Some(used) = used else {
        return match window {
            Some(w) => format!("not reported (window: {} tokens)", fmt_int(w)),
            None => "not reported yet".into(),
        };
    };
    match window {
        Some(w) => format!("{} / {} tokens ({:.1}% used)", fmt_int(used), fmt_int(w), used as f64 * 100.0 / w as f64),
        None => format!("{} tokens used (window unknown)", fmt_int(used)),
    }
}

/// Effort choices on step 3 of /model: "default" (keep the current setting), then the
/// harness's levels. Index 0 is default; i>0 is effort_levels(harness)[i-1].
pub fn effort_choices(harness: &str) -> Vec<&'static str> {
    let h = harness;
    std::iter::once("default").chain(crate::store::effort_levels(h).iter().copied()).collect()
}

/// Text and keyboard for step 3 (effort) of /model.
#[cfg(test)]
fn effort_step(tok: String, harness: &str, model: &str, current: Option<&str>) -> (String, Value) {
    effort_step_options(tok,harness,model,current,&effort_choices(harness))
}
fn effort_step_options(tok:String,harness:&str,model:&str,current:Option<&str>,choices:&[&str])->(String,Value) {
    let cur = current.filter(|c| !c.is_empty()).unwrap_or("default");
    let text = format!("Step 3 of 3 — effort for {}\nCurrent effort: {cur}\nApplied from the next turn; \"default\" keeps the current setting.",
                       if model.is_empty() { format!("{harness} default model") } else { model.to_string() });
    let text=if harness=="opencode" {text.replace("keeps the current setting", "uses the model default")} else {text};
    let row: Vec<Value> = choices.iter().enumerate()
        .map(|(i, e)| json!({"text": format!("{}{e}", if *e == cur && i > 0 { "✓ " } else { "" }), "callback_data": format!("me:{tok}:{i}")})).collect();
    (text, json!({"inline_keyboard": row.chunks(3).collect::<Vec<_>>()}))
}

/// The effort picked on step 3 (None = default / keep).
fn picked_effort(card: &Value, data: &str) -> Option<String> {
    let h = card["harness"].as_str().filter(|h| !h.is_empty()).unwrap_or(card["current_harness"].as_str().unwrap_or("claude"));
    let i: usize = data.strip_prefix("me:")?.rsplit(':').next()?.parse().ok()?;
    if h=="opencode" { return card["efforts"].get(i).and_then(Value::as_str).map(String::from); }
    effort_choices(h).get(i).filter(|e| **e != "default").map(|e| e.to_string())
}

pub fn model_update_text(snap: &Value, requested: &str) -> String {
    let reset = ["default", "off", "reset"].contains(&requested.trim().to_lowercase().as_str());
    let selection = if reset { "agent default".to_string() } else { snap["model_override"].as_str().filter(|m| !m.is_empty()).unwrap_or(requested).to_string() };
    let title = squash(snap["title"].as_str().filter(|t| !t.is_empty()).unwrap_or("(untitled)"));
    let sid = snap["id"].as_str().unwrap_or("?");
    format!("✅ Model updated for {title} ({}).\nModel: {selection}\nUsed for subsequent turns.", take(sid, 8))
}

fn countdown(seconds: i64) -> String {
    let s = seconds.max(0);
    let (d, r) = (s / 86400, s % 86400);
    let (h, r) = (r / 3600, r % 3600);
    let m = r / 60;
    if d > 0 {
        format!("{d}d {h}h")
    } else if h > 0 {
        format!("{h}h {m:02}m")
    } else {
        format!("{m}m")
    }
}

fn usage_age(seconds: i64) -> String {
    let s = seconds.max(0);
    if s < 90 {
        format!("{s}s")
    } else if s < 5400 {
        format!("{} min", (s as f64 / 60.0).round_ties_even() as i64)
    } else {
        countdown(s)
    }
}

/// claude-monitor's local estimate (cost, tokens, burn rate), if it is installed.
pub async fn local_usage_estimate() -> Option<Value> {
    let exe = crate::stt::which_bin("claude-monitor")
        .or_else(|| std::env::var("HOME").ok().map(|h| std::path::PathBuf::from(h).join(".local/bin/claude-monitor")))?;
    let out = tokio::time::timeout(Duration::from_secs(6),
        tokio::process::Command::new(exe).args(["--once", "--output", "json", "--plan", "pro"]).kill_on_drop(true).output()).await;
    let out = match out {
        Ok(Ok(o)) => o,
        other => {
            log(format!("claude-monitor estimate skipped: {}", if other.is_err() { "timeout".to_string() } else { "not runnable".to_string() }));
            return None;
        }
    };
    let v: Value = serde_json::from_slice(&out.stdout).ok()?;
    let local = v.get("local").filter(|l| l.is_object())?;
    let (cost, tokens, burn) = (local["cost_usd"].clone(), local["tokens"]["total_tokens"].clone(), local["burn_rate_cost_per_hour"].clone());
    if ![&cost, &tokens, &burn].iter().any(|x| x.is_number()) {
        return None;
    }
    Some(json!({"cost_usd": cost, "total_tokens": tokens, "burn_cost_per_hour": burn}))
}

/// Plan limits from the server's `usage` op, shaped for /usage (percent, reset, staleness).
pub fn shape_official(r: &Value, now: i64) -> Value {
    let mut out = json!({"source": "kurisu_statusline", "confidence": "unavailable", "captured_at": null, "age_seconds": null,
                         "stale": true, "five_hour": null, "seven_day": null, "sessions_seen": 0});
    out["sessions_seen"] = json!(r["sessions"].as_object().map(|m| m.len()).unwrap_or(0));
    let limits = &r["rate_limits"];
    if !limits.is_object() {
        return out;
    }
    out["confidence"] = json!("official");
    if let Some(c) = limits["captured_at"].as_f64() {
        out["captured_at"] = json!(c as i64);
        out["age_seconds"] = json!(now - c as i64);
    }
    for key in ["five_hour", "seven_day"] {
        let w = &limits[key];
        let (Some(pct), Some(rst)) = (w["used_percentage"].as_f64(), w["resets_at"].as_f64()) else { continue };
        let rst = rst as i64;
        out[key] = json!({"used_percentage": w["used_percentage"], "pct": pct, "resets_at": rst, "window_over": rst <= now,
                          "resets_in_seconds": (rst - now).max(0)});
    }
    let age = out["age_seconds"].as_i64();
    let over = ["five_hour", "seven_day"].iter().any(|k| out[*k]["window_over"] == json!(true));
    out["stale"] = json!(age.is_none_or(|a| a > USAGE_STALE_AFTER_S) || over);
    out
}

/// One line: "Claude: 5h 34% (resets 23:00, in 1h 02m) · 7d 12% (resets Mon 10 Nov 09:00, in 2d 11h)".
pub fn usage_summary(official: &Value, now: i64) -> String {
    let mut parts = vec![];
    for (key, label, f) in [("five_hour", "5h", "%H:%M"), ("seven_day", "7d", "%a %d %b %H:%M")] {
        let w = &official[key];
        if !w.is_object() {
            continue;
        }
        let pct = fmt_g(w["used_percentage"].as_f64().unwrap_or(0.0));
        if w["window_over"] == json!(true) {
            parts.push(format!("{label} {pct}% (window already reset)"));
        } else {
            let rst = w["resets_at"].as_i64().unwrap_or(0);
            parts.push(format!("{label} {pct}% (resets {}, in {})", local_fmt(rst, f), countdown(rst - now)));
        }
    }
    if parts.is_empty() {
        return "Claude: no plan limits reported yet (subscription login and a session turn required)".into();
    }
    let stale = if official["stale"] == json!(true) { " ⚠️ stale" } else { "" };
    format!("Claude: {}{stale}", parts.join(" · "))
}

/// The /usage report: what Telegram shows (`text`), a one-line `summary`, and the data.
pub fn usage_report(raw: &Value, estimate: Option<Value>, claude_installed: bool, codex: Option<Value>, agy: Option<Value>, now: i64) -> Value {
    let mut harnesses = json!({});
    let mut summaries = Vec::new();
    let mut sections = Vec::new();
    let estimate = estimate.filter(|_| claude_installed);
    if claude_installed {
        let official = shape_official(raw, now);
        summaries.push(usage_summary(&official, now));
        sections.push(format_usage(&official, estimate.as_ref(), now));
        harnesses["claude"] = official;
    }
    if let Some(codex) = codex {
        let lines = codex_usage_lines(&codex, now);
        summaries.push(format!("Codex: {}", lines.join(" · ")));
        sections.push(format!("📊 Codex usage\n{}", lines.join("\n")));
        harnesses["codex"] = codex;
    }
    if let Some(agy) = agy {
        let lines = agy_usage_lines(&agy, now);
        if !lines.is_empty() {
            summaries.push(format!("agy: {}", agy_usage_summary(&agy)));
            sections.push(format!("📊 agy usage\n{}", lines.join("\n")));
            harnesses["agy"] = agy;
        }
    }
    if summaries.is_empty() {
        summaries.push("No supported CLI installed (claude, codex or agy).".into());
        sections = summaries.clone();
    }
    json!({"summary": summaries.join("; "), "text": sections.join("\n\n"),
           "harnesses": harnesses, "local_estimate": estimate})
}

/// One compact line per bucket, in the same shape as Claude/Codex:
/// "agy · Gemini Models weekly: 26% used · resets Mon 16 Nov 03:47 (in 6d 21h)".
fn agy_usage_lines(usage: &Value, now: i64) -> Vec<String> {
    let mut lines = Vec::new();
    for b in usage["buckets"].as_array().into_iter().flatten() {
        let Some(pct) = b["used_percentage"].as_f64() else { continue };
        let name = agy_bucket_name(b["bucket"].as_str().unwrap_or(""), b["label"].as_str().unwrap_or(""));
        let reset = match b["resets_at"].as_i64() {
            Some(r) if r <= now => " · window already reset".to_string(),
            Some(r) => format!(" · resets {} (in {})", local_fmt(r, "%a %d %b %H:%M"), countdown(r - now)),
            None => String::new(),
        };
        lines.push(format!("agy · {name}: {}% used{reset}", fmt_g(pct)));
    }
    lines
}

fn agy_usage_summary(usage: &Value) -> String {
    usage["buckets"].as_array().into_iter().flatten().filter_map(|b| {
        let pct = b["used_percentage"].as_f64()?;
        Some(format!("{} {}% used", agy_bucket_name(b["bucket"].as_str().unwrap_or(""),
            b["label"].as_str().unwrap_or("")), fmt_g(pct)))
    }).collect::<Vec<_>>().join(" · ")
}

/// "Gemini Models" + "Weekly Limit Remaining" -> "Gemini Models weekly";
/// "Claude and GPT models" + "Weekly Limit Remaining" -> "Claude and GPT models weekly".
fn agy_bucket_name(bucket: &str, label: &str) -> String {
    let mut rest = label.trim();
    while let Some(head) = rest.strip_suffix(" Remaining").or_else(|| rest.strip_suffix(" Limit")) {
        rest = head.trim();
    }
    let rest = rest.strip_prefix(bucket).map(str::trim_start).unwrap_or(rest);
    if rest.is_empty() { bucket.to_string() } else { format!("{bucket} {}", rest.to_lowercase()) }
}

/// Installed, but the official CLI cannot provide quotas. Never invent zero usage.
pub fn codex_usage_unavailable() -> Value {
    json!({"source": "codex_app_server", "confidence": "unavailable",
           "note": "Limits unavailable; check codex login and account support, or try again."})
}

/// Parse only quota fields from the installed CLI's generated app-server schema.
/// Preserve multiple limit buckets and nullable durations/resets without assuming 5h/7d.
pub fn shape_codex_usage(response: &Value, now: i64) -> Value {
    let mut buckets = serde_json::Map::new();
    let mut add = |id: &str, snapshot: &Value| {
        let mut windows = serde_json::Map::new();
        for key in ["primary", "secondary"] {
            let w = &snapshot[key];
            let Some(pct) = w["usedPercent"].as_f64().filter(|p| p.is_finite() && *p >= 0.0 && *p <= 100.0) else { continue };
            let reset = w["resetsAt"].as_i64().filter(|r| *r > 0);
            let duration = w["windowDurationMins"].as_i64().filter(|d| *d > 0);
            windows.insert(key.into(), json!({"used_percentage": pct, "window_minutes": duration,
                "resets_at": reset, "window_over": reset.map(|r| r <= now),
                "resets_in_seconds": reset.map(|r| (r - now).max(0))}));
        }
        if !windows.is_empty() {
            buckets.insert(id.into(), json!({"windows": windows}));
        }
    };
    if let Some(all) = response["rateLimitsByLimitId"].as_object().filter(|m| !m.is_empty()) {
        for (id, snapshot) in all { add(id, snapshot); }
    } else {
        let snapshot = &response["rateLimits"];
        add(snapshot["limitId"].as_str().unwrap_or("codex"), snapshot);
    }
    if buckets.is_empty() { return codex_usage_unavailable(); }
    json!({"source": "codex_app_server", "confidence": "official", "captured_at": now, "buckets": buckets})
}

fn codex_usage_lines(usage: &Value, now: i64) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(buckets) = usage["buckets"].as_object() {
        for (id, bucket) in buckets {
            for key in ["primary", "secondary"] {
                let w = &bucket["windows"][key];
                let Some(pct) = w["used_percentage"].as_f64() else { continue };
                let label = match w["window_minutes"].as_i64() {
                    Some(300) => "5-hour".into(),
                    Some(10080) => "7-day".into(),
                    Some(mins) => format!("{mins}-minute"),
                    None => key.into(),
                };
                let reset = match w["resets_at"].as_i64() {
                    Some(r) if r <= now => "window already reset; figure is old".into(),
                    Some(r) => format!("resets {} (in {})", local_fmt(r, "%a %d %b %H:%M"), countdown(r - now)),
                    None => "reset not reported".into(),
                };
                lines.push(format!("{id} {label}: {}% used · {reset}", fmt_g(pct)));
            }
        }
    }
    if lines.is_empty() { lines.push(usage["note"].as_str().unwrap_or("Limits unavailable.").into()); }
    lines
}

pub fn format_usage(official: &Value, estimate: Option<&Value>, now: i64) -> String {
    let mut lines = vec!["📊 Claude usage".to_string()];
    let five = official["five_hour"].is_object();
    let seven = official["seven_day"].is_object();
    if official["confidence"] == "unavailable" || (!five && !seven) {
        lines.push("No Claude limits reported; check subscription login and run a session turn.".into());
    } else {
        for (key, label, f) in [("five_hour", "5-hour", "%H:%M"), ("seven_day", "7-day", "%a %d %b %H:%M")] {
            let w = &official[key];
            if !w.is_object() {
                lines.push(format!("{label}: not reported"));
                continue;
            }
            let pct = fmt_g(w["used_percentage"].as_f64().unwrap_or(0.0));
            if w["window_over"] == json!(true) {
                lines.push(format!("{label}: {pct}% used (window already reset; figure is old)"));
                continue;
            }
            let rst = w["resets_at"].as_i64().unwrap_or(0);
            lines.push(format!("{label}: {pct}% used · resets {} (in {})", local_fmt(rst, f), countdown(rst - now)));
        }
        if let Some(age) = official["age_seconds"].as_i64() {
            if official["stale"] == json!(true) {
                lines.push(format!("⚠️ Stale — last known {} ago.", usage_age(age)));
            } else {
                lines.push(format!("Captured {} ago.", usage_age(age)));
            }
        }
    }
    if let Some(e) = estimate {
        lines.push(String::new());
        lines.push("Local estimate (this machine's Claude Code logs)".into());
        if let Some(c) = e["cost_usd"].as_f64() {
            lines.push(format!("API-price equivalent: ${} (estimate)", fmt_money(c)));
        }
        if let Some(t) = e["total_tokens"].as_f64() {
            lines.push(format!("Tokens: {} (estimate)", fmt_int(t as i64)));
        }
        if let Some(b) = e["burn_cost_per_hour"].as_f64() {
            lines.push(format!("Burn rate: ${}/h (estimate)", fmt_money(b)));
        }
    }
    lines.join("\n")
}

fn compaction_complete_text(sid: &str, result: &Value, snapshot: Option<&Value>) -> String {
    let mut metrics = vec![];
    if let Some(n) = result["pre_tokens"].as_i64() {
        metrics.push(format!("before: {} tokens", fmt_int(n)));
    }
    if let Some(n) = result["post_tokens_estimate"].as_i64() {
        metrics.push(format!("after: ~{} tokens (estimate)", fmt_int(n)));
    }
    let summary = result["summary"].as_str().map(str::trim).filter(|s| !s.is_empty())
        .map(|s| format!("\n\n📝 Compaction summary\n{s}"))
        .unwrap_or_else(|| "\n\nConversation context compacted. No readable summary was reported.".into());
    format!("✅ Compaction complete for {}.\nContext now: {}{}{summary}", take(sid, 8),
                       snapshot.map(format_context_usage).unwrap_or_else(|| "unknown".into()),
                       if metrics.is_empty() { String::new() } else { format!("\nCompaction tokens — {}", metrics.join(", ")) })
}

pub fn mime_for_path(path: &str) -> String {
    let ext = std::path::Path::new(path).extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();
    match ext.as_str() {
        "gif" => "image/gif", "jpeg" | "jpg" => "image/jpeg", "png" => "image/png", "webp" => "image/webp",
        "pdf" => "application/pdf", "txt" => "text/plain", "md" => "text/markdown", "json" => "application/json",
        "csv" => "text/csv", "html" | "htm" => "text/html", "zip" => "application/zip",
        "mp3" => "audio/mpeg", "ogg" | "oga" | "opus" => "audio/ogg", "wav" => "audio/x-wav", "m4a" => "audio/mp4",
        "flac" => "audio/flac", "mp4" => "video/mp4", "webm" => "video/webm",
        _ => "application/octet-stream",
    }.to_string()
}

pub fn attachment_from_message(message: &Value) -> Option<Value> {
    if let Some(photos) = message["photo"].as_array().filter(|p| !p.is_empty()) {
        let best = photos.iter().max_by_key(|p| (p["file_size"].as_i64().unwrap_or(0), p["width"].as_i64().unwrap_or(0) * p["height"].as_i64().unwrap_or(0)))?;
        if let Some(fid) = best["file_id"].as_str() {
            return Some(json!({"file_id": fid, "name": "photo.jpg", "mime": "image/jpeg"}));
        }
    }
    for (key, fallback) in [("document", "file"), ("audio", "audio.mp3"), ("voice", "voice.ogg"), ("video", "video.mp4"),
                            ("video_note", "video-note.mp4"), ("animation", "animation.mp4")] {
        let media = &message[key];
        let Some(fid) = media["file_id"].as_str().filter(|f| !f.is_empty()) else { continue };
        let name = media["file_name"].as_str().filter(|n| !n.is_empty()).unwrap_or(fallback).to_string();
        let mime = media["mime_type"].as_str().map(str::to_lowercase).filter(|m| !m.is_empty()).unwrap_or_else(|| mime_for_path(&name));
        return Some(json!({"file_id": fid, "name": name, "mime": mime}));
    }
    None
}

pub fn is_audio_attachment(name: &str, mime: &str) -> bool {
    let ext = std::path::Path::new(name).extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();
    mime.starts_with("audio/") || ["ogg", "oga", "opus", "mp3", "m4a", "wav", "flac", "aac", "webm"].contains(&ext.as_str())
}

/// A file name Kurisu's speech-to-text accepts (an audio extension).
pub(crate) fn audio_name(name: &str, mime: &str) -> String {
    if crate::stt::is_audio(name) {
        return name.to_string();
    }
    let ext = match mime {
        "audio/mpeg" => "mp3", "audio/mp4" | "audio/x-m4a" => "m4a", "audio/wav" | "audio/x-wav" => "wav",
        "audio/flac" => "flac", "audio/aac" => "aac", "audio/webm" => "webm", _ => "ogg",
    };
    format!("{name}.{ext}")
}

pub fn voice_transcript_text(name: &str, path: &str, transcript: &str, caption: &str) -> String {
    let mut text = format!("The user sent a voice/audio message (\"{name}\"). Local speech-to-text transcript:\n\n{}\n\n(Audio file: {path} — transcription may contain mistakes.)",
                           if transcript.is_empty() { "(no speech detected)" } else { transcript });
    if !caption.is_empty() {
        text.push_str(&format!("\n\nUser note: {caption}"));
    }
    text
}

/// A top-level `key = "value"` from a TOML file (before any [table]).
fn toml_top_level_string(text: &str, key: &str) -> Option<String> {
    for line in text.lines() {
        let l = line.trim();
        if l.starts_with('[') {
            return None;
        }
        if let Some((k, v)) = l.split_once('=') {
            if k.trim() == key {
                let v = v.trim();
                let v = v.split(" #").next().unwrap_or(v).trim();
                return Some(v.trim_matches('"').trim_matches('\'').to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handover_status_only_counts_an_unclaimed_ask() {
        assert_eq!(Bridge::handover_key(&json!({"tab": 1, "handover": {"active": false, "claimed": false, "message": ""}})), None);
        assert_eq!(Bridge::handover_key(&json!({"tab": 1, "handover": {"active": true, "claimed": true, "message": "log in"}})), None);
        assert_eq!(Bridge::handover_key(&json!({"tab": 2, "handover": {"active": true, "claimed": false, "message": "log in"}})).as_deref(), Some("2|log in"));
    }

    /// One notice per ask, and a fresh link before the first one expires.
    #[test]
    fn handovers_announce_once_then_again_after_the_link_expires() {
        let t0 = Instant::now();
        let keys = vec!["36|log in".to_string(), "37|captcha".to_string()];
        assert_eq!(Bridge::handovers_due(&keys, &HashMap::new(), t0), keys);
        let mut announced = HashMap::new();
        announced.insert("36|log in".to_string(), t0);
        announced.insert("37|captcha".to_string(), t0);
        assert!(Bridge::handovers_due(&keys, &announced, t0 + Duration::from_secs(10)).is_empty());
        assert_eq!(Bridge::handovers_due(&keys, &announced, t0 + Duration::from_secs(241)), keys);
        // A different ask on the same tab is new.
        assert_eq!(Bridge::handovers_due(&["36|2FA".to_string()], &announced, t0), vec!["36|2FA".to_string()]);
    }

    /// "Click Done in the browser" is only actionable with the link that opens it.
    #[test]
    fn handover_notice_carries_the_browser_link() {
        let t = Bridge::handover_text("", "http://phone:8793/_ku/r/tok");
        assert!(t.contains("http://phone:8793/_ku/r/tok"));
        assert!(t.contains("press Done"));
        assert!(t.contains("Send /browser"));
        assert!(t.contains("a login, a captcha or 2FA"));
        let t = Bridge::handover_text("please log in", "http://phone:8793/_ku/r/tok");
        assert!(t.starts_with("🖐 The live browser needs you: please log in"));
    }

    #[test]
    fn compact_completion_handles_metadata_only_and_claude_summary() {
        let text = compaction_complete_text("session", &json!({"summary": "", "pre_tokens": null}), None);
        assert!(text.contains("✅ Compaction complete"));
        assert!(text.contains("No readable summary was reported"));
        assert!(!text.contains("📝 Compaction summary") && !text.contains("tokens"));
        let text = compaction_complete_text("session", &json!({"summary": "Kept decisions", "pre_tokens": 1000, "post_tokens_estimate": 100}), None);
        assert!(text.contains("📝 Compaction summary\nKept decisions"));
        assert!(text.contains("before: 1,000 tokens") && text.contains("after: ~100 tokens"));
    }

    #[test]
    fn usage_report_matches_telegram_and_summarizes() {
        let now = 1_000_000;
        let raw = json!({"sessions": {"a": {}, "b": {}}, "rate_limits": {"captured_at": now - 60,
            "five_hour": {"used_percentage": 34.0, "resets_at": now + 3720}, "seven_day": {"used_percentage": 12.5, "resets_at": now + 2 * 86400}}});
        let r = usage_report(&raw, Some(json!({"cost_usd": 1.5, "total_tokens": 1000, "burn_cost_per_hour": 0.5})), true, None, None, now);
        let official = shape_official(&raw, now);
        assert_eq!(r["text"], format_usage(&official, r["local_estimate"].as_object().map(|_| &r["local_estimate"]), now));
        assert_eq!(r["harnesses"]["claude"]["five_hour"]["pct"], 34.0);
        assert_eq!(r["harnesses"]["claude"]["sessions_seen"], 2);
        let s = r["summary"].as_str().unwrap();
        assert!(s.starts_with("Claude: 5h 34% (resets ") && s.contains("in 1h") && s.contains("7d 12.5%") && !s.contains('\n'), "{s}");
        let empty = usage_report(&json!({"sessions": {}, "rate_limits": null}), None, true, None, None, now);
        assert!(empty["summary"].as_str().unwrap().contains("no plan limits reported yet"));
        assert!(empty["text"].as_str().unwrap().contains("No Claude limits reported; check subscription login and run a session turn."));
    }

    #[test]
    fn usage_installed_matrix_and_unavailable() {
        let now = 1000;
        // Saved Claude limits and monitor estimates must not resurrect an absent CLI.
        let raw = json!({"rate_limits": {"captured_at": now, "five_hour": {"used_percentage": 22, "resets_at": 2000}}});
        for claude in [false, true] {
            for codex in [false, true] {
                let r = usage_report(&raw, Some(json!({"cost_usd": 4})), claude,
                    codex.then(codex_usage_unavailable), None, now);
                let harnesses = r["harnesses"].as_object().unwrap();
                assert_eq!(harnesses.contains_key("claude"), claude);
                assert_eq!(harnesses.contains_key("codex"), codex);
                for field in ["summary", "text"] {
                    let text = r[field].as_str().unwrap();
                    assert_eq!(text.contains("Claude"), claude);
                    assert_eq!(text.contains("Codex"), codex);
                    if codex { assert!(text.contains("Limits unavailable")); }
                }
                assert_eq!(r["local_estimate"].is_object(), claude);
                if !claude && !codex { assert!(r["text"].as_str().unwrap().contains("No supported CLI installed")); }
            }
        }
    }

    #[test]
    fn agy_quota_lines_render_compact_and_fail_soft() {
        let now = 1_700_000_000;
        let usage = json!({"source": "agy_quota", "confidence": "official", "buckets": [
            {"bucket": "Gemini Models", "label": "Weekly Limit Remaining", "used_percentage": 26.0, "resets_at": now + 6 * 86400 + 21 * 3600},
            {"bucket": "Claude and GPT models", "label": "Weekly Limit Remaining", "used_percentage": 0.0, "resets_at": null}
        ]});
        let r = usage_report(&json!({}), None, false, None, Some(usage), now);
        let text = r["text"].as_str().unwrap();
        assert!(text.contains("agy · Gemini Models weekly: 26% used · resets "), "{text}");
        assert!(text.contains("(in 6d 21h)"), "{text}");
        assert!(text.contains("agy · Claude and GPT models weekly: 0% used"), "{text}");
        assert!(!text.contains("remaining"), "{text}");
        assert_eq!(r["summary"].as_str().unwrap(), "agy: Gemini Models weekly 26% used · Claude and GPT models weekly 0% used");
        assert_eq!(r["harnesses"]["agy"]["confidence"], "official");
        // No usable buckets: the section disappears, /usage still renders.
        let r = usage_report(&json!({}), None, false, None, Some(json!({"buckets": []})), now);
        assert!(!r["text"].as_str().unwrap().contains("agy ·"));
        assert!(r["harnesses"].get("agy").is_none());
        assert!(r["text"].as_str().unwrap().contains("No supported CLI installed"));
    }

    #[test]
    fn codex_quotas_parse_nullable_and_multiple_windows() {
        let now = 1000;
        let response = json!({"accountId": "private", "rateLimits": {"primary": {"usedPercent": 99}},
            "rateLimitsByLimitId": {
                "codex": {"primary": {"usedPercent": 4, "windowDurationMins": 300, "resetsAt": 4600},
                          "secondary": {"usedPercent": 1, "windowDurationMins": 10080, "resetsAt": 900}},
                "review": {"primary": {"usedPercent": 0, "windowDurationMins": null, "resetsAt": null},
                           "secondary": {"usedPercent": 12.5, "windowDurationMins": 60, "resetsAt": 8200}}
            }});
        let shaped = shape_codex_usage(&response, now);
        assert_eq!(shaped["confidence"], "official");
        assert_eq!(shaped["buckets"]["codex"]["windows"]["primary"]["used_percentage"], 4.0);
        assert_eq!(shaped["buckets"]["codex"]["windows"]["secondary"]["window_over"], true);
        assert!(shaped["buckets"]["review"]["windows"]["primary"]["resets_at"].is_null());
        assert!(!shaped.to_string().contains("private"));
        let r = usage_report(&json!({}), None, false, Some(shaped), None, now);
        let text = r["text"].as_str().unwrap();
        for expected in ["5-hour: 4%", "7-day: 1%", "window already reset", "review primary: 0%", "reset not reported", "60-minute: 12.5%"] {
            assert!(text.contains(expected), "{text}");
        }
        assert!(!r["summary"].as_str().unwrap().contains('\n'));
    }

    #[test]
    fn codex_quotas_reject_malformed_without_fabricated_zero() {
        for response in [json!(null), json!({}), json!({"error": {"message": "private"}}),
            json!({"rateLimits": {"primary": {"usedPercent": "4"}}}),
            json!({"rateLimits": {"primary": {"usedPercent": -1}}}),
            json!({"rateLimits": {"primary": {"usedPercent": 101}}})] {
            let shaped = shape_codex_usage(&response, 1000);
            assert_eq!(shaped["confidence"], "unavailable");
            assert!(!codex_usage_lines(&shaped, 1000).join(" ").contains("0%"));
        }
        let legacy = shape_codex_usage(&json!({"rateLimits": {"primary": {"usedPercent": 0}}}), 1000);
        assert_eq!(legacy["buckets"]["codex"]["windows"]["primary"]["used_percentage"], 0.0);
    }

    #[test]
    fn short_path_uses_tilde_and_tail() {
        assert_eq!(short_path("/home/user/proj", Some("/home/user")), "~/proj");
        assert_eq!(short_path("/home/user/", Some("/home/user")), "~");
        assert_eq!(short_path("/srv/x", Some("/home/user")), "/srv/x");
        assert_eq!(short_path("/home/user-extra/x", Some("/home/user")), "/home/user-extra/x");
        let long = "/srv/projects/example/instances/kurisu-home/workspaces/developer";
        let s = short_path(long, None);
        assert!(s.starts_with("…/") && s.ends_with("workspaces/developer") && clen(&s) <= 40, "{s}");
        assert_eq!(short_path("/a/this-is-one-very-long-folder-name-without-any-slashes-at-all", None).matches('/').count(), 1);
    }

    #[test]
    fn terminal_picker_lists_session_workdirs_then_instance_roots() {
        let home = std::path::Path::new("/srv/kurisu");
        let sessions = vec![
            json!({"title": "lead", "workspace": "/srv/kurisu/workspaces/lead"}),
            json!({"title": "dev", "workspace": "/srv/kurisu/workspaces/dev"}),
            json!({"title": "dev again", "workspace": "/srv/kurisu/workspaces/dev"}),
            json!({"title": "no dir", "workspace": ""}),
        ];
        let e = terminal_dir_entries(home, &sessions);
        assert_eq!(e[0].1, "/srv/kurisu/workspaces/lead");
        assert!(e[0].0.contains("lead") && e[0].0.contains("workspaces/lead"), "{:?}", e[0]);
        assert_eq!(e[1].1, "/srv/kurisu/workspaces/dev", "deduped per directory");
        assert_eq!(e.iter().filter(|(_, p)| p == "/srv/kurisu/workspaces/dev").count(), 1);
        let paths: Vec<&str> = e.iter().map(|(_, p)| p.as_str()).collect();
        assert_eq!(&paths[2..], &["/srv/kurisu", "/srv/kurisu/workspaces"]);
        let text = terminal_picker_text(&e);
        assert!(text.contains("which directory") && text.contains("1. lead"), "{text}");
        assert!(!text.contains("http"), "the picker must not carry a link: {text}");
    }

    #[test]
    fn terminal_dir_expands_home_and_requires_a_real_directory() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(terminal_dir(dir.path().to_str().unwrap()).unwrap(), dir.path());
        let err = terminal_dir("relative/dir").unwrap_err().to_string();
        assert!(err.contains("absolute"), "{err}");
        let err = terminal_dir("/definitely/not/a/kurisu/terminal/dir").unwrap_err().to_string();
        assert!(err.contains("not a directory"), "{err}");
        if let Ok(home) = std::env::var("HOME") {
            if !home.is_empty() {
                assert_eq!(terminal_dir("~").unwrap(), std::path::PathBuf::from(home));
            }
        }
    }

    #[test]
    fn schedule_lists_pending_wakes_soonest_first() {
        let now = 1_000_000;
        let wakes = vec![
            serde_json::json!({"id": 7, "session_name": "lead", "at": now + 7500, "at_local": "2026-11-08 06:00 EST", "repeat": "0 6 * * *",
                               "message": "daily standup ".repeat(10), "status": "pending"}),
            serde_json::json!({"id": 3, "session_name": "w1", "at": now + 300, "message": "check CI", "status": "pending"}),
            serde_json::json!({"id": 1, "session_name": "w1", "at": now - 10, "message": "old", "status": "fired"}),
        ];
        let (text, kb) = schedule_text(&wakes, now);
        let i3 = text.find("#3 → w1").unwrap();
        let i7 = text.find("#7 → lead · 2026-11-08 06:00 EST (in 2h 05m)").unwrap();
        assert!(i3 < i7, "{text}");
        assert!(text.contains("(in 5m)") && text.contains("🔁 0 6 * * *") && text.contains("…") && !text.contains("#1 "), "{text}");
        let kb = kb.unwrap();
        assert_eq!(kb["inline_keyboard"][0][0]["callback_data"], "wc:3");
        assert_eq!(kb["inline_keyboard"][2][0]["callback_data"], "sched");
        assert!(schedule_text(&[], now).0.contains("No pending wakes"));
        assert_eq!(relative_to(now - 120, now), "overdue 2m");
        assert_eq!(relative_to(now + 3 * 86400 + 4 * 3600, now), "in 3d 4h");
    }

    #[test]
    fn model_page_maths_clamps_first_last_and_overflow() {
        assert_eq!(MODEL_PAGE_SIZE, 8);
        assert_eq!(model_page_bounds(0, 0), (0, 0));
        assert_eq!(model_page_bounds(0, 9), (0, 0));
        assert_eq!(model_page_bounds(8, 0), (0, 0));
        assert_eq!(model_page_bounds(8, 4), (0, 0));
        assert_eq!(model_page_bounds(9, 0), (0, 0));
        assert_eq!(model_page_bounds(9, 1), (1, 8));
        assert_eq!(model_page_bounds(9, 99), (1, 8));
        assert_eq!(model_page_bounds(25, 2), (2, 16));
        assert_eq!(model_page_bounds(25, 99), (3, 24));
        assert_eq!(model_page_bounds(25, -7), (0, 0));
    }

    #[test]
    fn model_search_filters_ids_and_labels_case_insensitively() {
        let options = vec![
            json!({"id": "claude-haiku-5-5", "label": "Haiku 5.5"}),
            json!({"id": "claude-opus-5-5", "label": "Opus 5.5"}),
            json!({"id": "ollama/deepseek:cloud", "label": "DeepSeek"}),
            json!({"id": "gpt-6.1-sol", "label": "GPT-6.1-Sol"}),
        ];
        let ids = |v: Vec<Value>| v.into_iter().map(|o| o["id"].as_str().unwrap().to_string()).collect::<Vec<_>>();
        assert_eq!(ids(filter_models(&options, "HAIKU")), ["claude-haiku-5-5"]);
        assert_eq!(ids(filter_models(&options, "deepseek")), ["ollama/deepseek:cloud"]);
        assert_eq!(ids(filter_models(&options, "sol")), ["gpt-6.1-sol"]);
        assert_eq!(filter_models(&options, "  ").len(), 4);
        assert!(filter_models(&options, "not-a-model").is_empty());
    }

    #[test]
    fn model_page_view_shows_indicator_search_last_and_clear_filter() {
        let options: Vec<Value> = (0..10).map(|i| json!({"id": format!("m{i}"), "label": format!("Model {i}")})).collect();
        let page = ModelPage { tok: "T", sid: "abcdefgh1234", title: "Demo", harness: "codex", current_harness: "codex",
                               current_model: "m2", switching: false, filtered_of: None };
        let (text, kb) = model_page_view(&page, &options, None, 0);
        assert!(text.contains("Page 1/2") && text.contains("models 1-8 of 10"), "{text}");
        assert_eq!(kb["inline_keyboard"][2][0]["text"], "✓ Model 2");
        let rows = kb["inline_keyboard"].as_array().unwrap();
        assert_eq!(rows.last().unwrap()[0]["callback_data"], "ms:T");
        assert!(rows.iter().any(|r| r[0]["callback_data"] == "mp:T:1"));
        let (text, kb) = model_page_view(&page, &options, None, 1);
        assert!(text.contains("Page 2/2") && text.contains("models 9-10 of 10"), "{text}");
        assert!(kb["inline_keyboard"].as_array().unwrap().iter().any(|r| r[0]["callback_data"] == "mp:T:0"));
        let filtered = filter_models(&options, "9");
        let p2 = ModelPage { filtered_of: Some(10), ..page };
        let (text, kb) = model_page_view(&p2, &filtered, Some("9"), 0);
        assert!(text.contains("Filter: \"9\" · 1 of 10 models") && text.contains("Page 1/1"), "{text}");
        let rows = kb["inline_keyboard"].as_array().unwrap();
        assert_eq!(rows[0][0]["callback_data"], "model:T:0");
        assert!(rows.iter().any(|r| r[0]["callback_data"] == "mf:T"));
        assert_eq!(rows.last().unwrap()[0]["callback_data"], "ms:T");
    }

    #[test]
    fn model_picker_effort_step() {
        assert_eq!(effort_choices("claude"), ["default", "low", "medium", "high", "xhigh", "max"]);
        assert_eq!(effort_choices("codex"), ["default", "low", "medium", "high", "xhigh"]);
        let (text, kb) = effort_step("T".into(), "codex", "gpt-x", Some("high"));
        assert!(text.contains("Step 3 of 3") && text.contains("Current effort: high"), "{text}");
        let btns: Vec<Value> = kb["inline_keyboard"].as_array().unwrap().iter().flat_map(|r| r.as_array().unwrap().clone()).collect();
        assert_eq!(btns.len(), 5);
        assert_eq!(btns[0]["callback_data"], "me:T:0");
        assert_eq!(btns[3]["text"], "✓ high");
        assert!(btns.iter().all(|b| b["text"] != "max"));
        let card = serde_json::json!({"harness": "", "current_harness": "claude"});
        assert_eq!(picked_effort(&card, "me:T:0"), None);
        assert_eq!(picked_effort(&card, "me:T:5").as_deref(), Some("max"));
        assert_eq!(picked_effort(&serde_json::json!({"harness": "codex"}), "me:T:5"), None);
        assert_eq!(picked_effort(&card, "model:T:1"), None);
    }

    use super::*;

    /// A tiny Telegram Bot API stand-in: records every call as (path, payload)
    /// and answers every call with an ok result.
    async fn mock_telegram() -> (String, std::sync::Arc<std::sync::Mutex<Vec<(String, Value)>>>, tokio::task::JoinHandle<()>) {
        mock_telegram_failures(0).await
    }

    async fn mock_telegram_failures(failures: usize) -> (String, std::sync::Arc<std::sync::Mutex<Vec<(String, Value)>>>, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let remaining = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(failures));
        // Message ids start at 1 and increase per accepted send, so an edit
        // can be mapped back to the bubble it completed.
        let next_mid = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(1));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = format!("http://{}", listener.local_addr().unwrap());
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = sent.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { break };
                let seen = seen.clone();
                let remaining = remaining.clone();
                let next_mid = next_mid.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    loop {
                        let n = match sock.read(&mut tmp).await { Ok(0) | Err(_) => return, Ok(n) => n };
                        buf.extend_from_slice(&tmp[..n]);
                        let Some(head_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else { continue };
                        let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                        let len = head.lines()
                            .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse::<usize>().unwrap_or(0)))
                            .unwrap_or(0);
                        while buf.len() < head_end + 4 + len {
                            match sock.read(&mut tmp).await { Ok(0) | Err(_) => return, Ok(n) => buf.extend_from_slice(&tmp[..n]) };
                        }
                        let body = &buf[head_end + 4..head_end + 4 + len];
                        let path = head.lines().next().unwrap_or("").to_string();
                        let payload: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
                        let (reply, _) = if path.contains("sendMessage") {
                            let mid = next_mid.fetch_add(1, Ordering::SeqCst);
                            seen.lock().unwrap().push((path, payload));
                            if remaining.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok() {
                                (r#"{"ok":false,"description":"test rejection"}"#.to_string(), false)
                            } else {
                                (format!(r#"{{"ok":true,"result":{{"message_id":{mid}}}}}"#), true)
                            }
                        } else {
                            seen.lock().unwrap().push((path, payload));
                            (r#"{"ok":true,"result":true}"#.to_string(), false)
                        };
                        let resp = format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}", reply.len());
                        let _ = sock.write_all(resp.as_bytes()).await;
                        return;
                    }
                });
            }
        });
        (api, sent, task)
    }

    fn sent_texts(sent: &std::sync::Arc<std::sync::Mutex<Vec<(String, Value)>>>) -> Vec<String> {
        sent.lock().unwrap().iter()
            .filter(|(p, _)| p.contains("sendMessage") || p.contains("editMessageText"))
            .filter_map(|(_, v)| v["text"].as_str().map(String::from)).collect()
    }

    /// What the chat actually shows: each sendMessage opens a bubble and an
    /// editMessageText replaces the bubble with the same message id (the mock
    /// numbers sends 1, 2, 3…), so an appended tail is not counted twice.
    fn visible_texts(sent: &std::sync::Arc<std::sync::Mutex<Vec<(String, Value)>>>) -> Vec<String> {
        let calls = sent.lock().unwrap().clone();
        let mut bubbles: Vec<String> = Vec::new();
        for (path, v) in calls {
            let Some(text) = v["text"].as_str().map(String::from) else { continue };
            if path.contains("sendMessage") {
                bubbles.push(text);
            } else if path.contains("editMessageText") {
                if let Some(mid) = v["message_id"].as_i64().filter(|mid| *mid >= 1) {
                    if let Some(b) = bubbles.get_mut(mid as usize - 1) {
                        *b = text;
                    }
                }
            }
        }
        bubbles
    }

    fn last_message(sent: &std::sync::Arc<std::sync::Mutex<Vec<(String, Value)>>>) -> Value {
        sent.lock().unwrap().iter().rev()
            .find(|(p, _)| p.contains("sendMessage") || p.contains("editMessageText"))
            .map(|(_, v)| v.clone()).unwrap_or(Value::Null)
    }

    fn test_bridge(server: std::sync::Arc<crate::server::Server>, home: std::path::PathBuf, api: String) -> Bridge {
        use std::sync::atomic::AtomicUsize;
        use std::sync::Mutex;
        Bridge {
            server, cfg: crate::telegram::Config {
                token: "TEST".into(), chat_id: "1".into(), owner_user_id: None,
                new_session_cwd: None, enabled: true,
            }, home, api, http: reqwest::Client::new(),
            state: Mutex::new(json!({})), relayed: Mutex::default(), last_bubble: Mutex::default(), net: Mutex::default(), act: Mutex::default(),
            jobs: Mutex::default(), queued: AtomicUsize::new(0), job_notify: tokio::sync::Notify::new(),
            compactions: Mutex::default(), question_lock: tokio::sync::Mutex::new(()),
            mcp_cache: Mutex::default(), codex_model: Mutex::new((0.0, None)), bot_info: Mutex::new(json!({})),
            skills_cache: Mutex::default(), stt_probe: Mutex::new((0.0, None)),
        }
    }

    fn callback(data: &str) -> Value {
        json!({"id": "cb", "data": data, "message": {"message_id": 11, "chat": {"id": 1}}, "from": {"id": 1}})
    }

    fn button_callback(message: &Value, prefix: &str) -> Option<String> {
        message["reply_markup"]["inline_keyboard"].as_array()?.iter()
            .flat_map(|r| r.as_array().into_iter().flatten())
            .filter_map(|b| b["callback_data"].as_str())
            .find(|d| d.starts_with(prefix)).map(String::from)
    }

    #[tokio::test]
    async fn ask_user_question_card_taps_toggles_text_and_dismiss() {
        let dir = tempfile::tempdir().unwrap();
        let server = std::sync::Arc::new(crate::server::Server::new(crate::paths::Paths { home: dir.path().join("home") }).unwrap());
        let cwd = dir.path().join("work");
        std::fs::create_dir_all(&cwd).unwrap();
        let sid = server.test_store().lock().await
            .create("asker", "codex", cwd.to_str().unwrap(), "lead", "gpt-6-luna", "", None).unwrap().id;
        let (api, sent, _http) = mock_telegram().await;
        let bridge = std::sync::Arc::new(test_bridge(server.clone(), dir.path().join("home"), api));
        let session = json!({"id": sid, "title": "asker", "pending_elicitations_count": 1});
        let ask = |extra: Value| {
            let (server, sid) = (server.clone(), sid.clone());
            let mut req = json!({"op": "ask_user", "caller": sid, "question": "Which fruit?", "header": "Fruit", "choices": ["Mango", "Kiwi", "Papaya"]});
            for (k, v) in extra.as_object().unwrap() { req[k] = v.clone(); }
            tokio::spawn(async move { server.test_call(req).await.unwrap() })
        };
        let card = |bridge: std::sync::Arc<Bridge>, session: Value, sent: std::sync::Arc<std::sync::Mutex<Vec<(String, Value)>>>| async move {
            for _ in 0..200 {
                bridge.approvals_once(&[session.clone()]).await;
                let m = last_message(&sent);
                if m["reply_markup"]["inline_keyboard"].as_array().is_some_and(|k| !k.is_empty()) && m["text"].as_str().is_some_and(|t| t.contains("Which fruit?")) { return m; }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            panic!("no question card");
        };
        let labels = |m: &Value| -> Vec<String> { m["reply_markup"]["inline_keyboard"].as_array().unwrap().iter()
            .flat_map(|r| r.as_array().unwrap().iter().map(|b| b["text"].as_str().unwrap().to_string())).collect() };
        // Single choice: one button per choice plus Dismiss; a tap answers and the card shows it.
        let t = ask(json!({}));
        let m = card(bridge.clone(), session.clone(), sent.clone()).await;
        assert_eq!(labels(&m), ["Mango", "Kiwi", "Papaya", "✖ Dismiss"]);
        assert!(m["text"].as_str().unwrap().contains("…or type your own answer"), "{m}");
        bridge.handle_callback(&callback(&button_callback(&m, "q:").unwrap().replace(":o:0", ":o:1"))).await.unwrap();
        assert_eq!(t.await.unwrap(), json!({"status": "answered", "answer": "Kiwi"}));
        let fin = last_message(&sent);
        assert!(fin["text"].as_str().unwrap().contains("• Which fruit? → Kiwi"), "{fin}");
        assert_eq!(fin["reply_markup"]["inline_keyboard"], json!([]));
        // Multi-select: toggles, then Submit.
        let t = ask(json!({"multi_select": true}));
        let m = card(bridge.clone(), session.clone(), sent.clone()).await;
        assert_eq!(labels(&m), ["⬜ Mango", "⬜ Kiwi", "⬜ Papaya", "✅ Submit", "✖ Dismiss"]);
        let base = button_callback(&m, "q:").unwrap(); // q:<tok>:0:o:0
        let at = |suffix: &str| base.replace(":o:0", suffix);
        bridge.handle_callback(&callback(&at(":o:0"))).await.unwrap();
        bridge.handle_callback(&callback(&at(":o:2"))).await.unwrap();
        assert_eq!(labels(&last_message(&sent))[..3], ["☑️ Mango", "⬜ Kiwi", "☑️ Papaya"]);
        bridge.handle_callback(&callback(&at(":s"))).await.unwrap();
        assert_eq!(t.await.unwrap()["answer"], "Mango, Papaya");
        // A typed reply answers it.
        let t = ask(json!({}));
        card(bridge.clone(), session.clone(), sent.clone()).await;
        assert!(bridge.answer_question_with_text(&bridge.dm(), &json!({}), "a ripe papaya").await);
        assert_eq!(t.await.unwrap()["answer"], "a ripe papaya");
        // Dismiss returns `dismissed`.
        let t = ask(json!({}));
        let m = card(bridge.clone(), session.clone(), sent.clone()).await;
        bridge.handle_callback(&callback(&button_callback(&m, "q:").unwrap().replace(":o:0", ":x"))).await.unwrap();
        assert_eq!(t.await.unwrap()["status"], "dismissed");
    }

    #[tokio::test]
    async fn model_pages_search_and_effort_flow() {
        let dir = tempfile::tempdir().unwrap();
        let paths = crate::paths::Paths { home: dir.path().join("home") };
        let server = std::sync::Arc::new(crate::server::Server::new(paths).unwrap());
        let cwd = dir.path().join("work");
        std::fs::create_dir_all(&cwd).unwrap();
        let sid = server.test_store().lock().await
            .create("pager", "claude", cwd.to_str().unwrap(), "worker", "claude-haiku-5-5", "", None)
            .unwrap().id;
        let (api, sent, http) = mock_telegram().await;
        let bridge = std::sync::Arc::new(test_bridge(server.clone(), dir.path().join("home"), api));
        let ctx = bridge.dm();
        bridge.select_session(&ctx, &sid);

        // /model opens step 1 when several harnesses are installed; pick the
        // session's own harness to reach the model page deterministically.
        bridge.clone().handle_text(&ctx, "/model", None, true, false, false).await.unwrap();
        let mut first = last_message(&sent);
        if first["text"].as_str().unwrap().contains("Step 1 of 3") {
            let mh = first["reply_markup"]["inline_keyboard"].as_array().unwrap().iter()
                .flat_map(|r| r.as_array().into_iter().flatten())
                .find(|b| b["text"].as_str().is_some_and(|t| t.contains("(claude)")))
                .and_then(|b| b["callback_data"].as_str()).unwrap().to_string();
            bridge.clone().handle_callback(&callback(&mh)).await.unwrap();
            first = last_message(&sent);
        }
        let text = first["text"].as_str().unwrap();
        assert!(text.contains("Page 1/2") && text.contains("models 1-8 of 11"), "{text}");
        let rows = first["reply_markup"]["inline_keyboard"].as_array().unwrap();
        assert_eq!(rows.len(), 10, "8 models + nav + search: {rows:?}");
        assert_eq!(rows.last().unwrap()[0]["callback_data"], button_callback(&first, "ms:").unwrap());

        // Next -> second page, Previous is offered and Search stays last.
        let next = button_callback(&first, "mp:").unwrap();
        bridge.clone().handle_callback(&callback(&next)).await.unwrap();
        let second = last_message(&sent);
        let text = second["text"].as_str().unwrap();
        assert!(text.contains("Page 2/2") && text.contains("models 9-11 of 11"), "{text}");
        assert!(button_callback(&second, "mp:").unwrap().ends_with(":0"));
        assert_eq!(second["reply_markup"]["inline_keyboard"].as_array().unwrap().last().unwrap()[0]["callback_data"],
                   button_callback(&second, "ms:").unwrap());

        // Search asks for a keyword; the next plain text filters instead of reaching the agent.
        let search = button_callback(&second, "ms:").unwrap();
        bridge.clone().handle_callback(&callback(&search)).await.unwrap();
        let prompt = last_message(&sent);
        assert!(prompt["text"].as_str().unwrap().contains("Send a keyword"), "{prompt}");
        bridge.clone().handle_update(500, &json!({"update_id": 500,
            "message": {"message_id": 12, "chat": {"id": 1}, "from": {"id": 1}, "text": "HAIKU"}})).await.unwrap();
        let filtered = last_message(&sent);
        let ftext = filtered["text"].as_str().unwrap();
        assert!(ftext.contains("Filter: \"HAIKU\" · 2 of 11 models") && ftext.contains("Page 1/1"), "{ftext}");
        assert!(button_callback(&filtered, "mf:").is_some(), "a filtered page needs a way back: {filtered}");
        assert_eq!(filtered["reply_markup"]["inline_keyboard"].as_array().unwrap().last().unwrap()[0]["callback_data"],
                   button_callback(&filtered, "ms:").unwrap());

        // Picking a filtered model still reaches the effort step for that model.
        let pick = button_callback(&filtered, "model:").unwrap();
        bridge.clone().handle_callback(&callback(&pick)).await.unwrap();
        let effort = last_message(&sent);
        let etext = effort["text"].as_str().unwrap();
        assert!(etext.contains("Step 3 of 3 — effort for claude-haiku-5-5"), "{etext}");
        assert!(button_callback(&effort, "me:").is_some(), "{effort}");

        http.abort();
    }

    #[tokio::test]
    async fn failed_delivery_retries_once_then_sends_fallback() {
        for failures in [1, 2] {
            let dir = tempfile::tempdir().unwrap();
            let home = dir.path().join("home");
            let server = std::sync::Arc::new(crate::server::Server::new(crate::paths::Paths { home: home.clone() }).unwrap());
            let (api, sent, http) = mock_telegram_failures(failures).await;
            let bridge = test_bridge(server, home, api);
            let result = bridge.send_text(&bridge.dm(), &json!(1), "undeliverable").await;
            let texts = sent_texts(&sent);
            assert_eq!(&texts[..2], ["undeliverable", "undeliverable"]);
            if failures == 2 {
                assert!(result.is_err());
                assert_eq!(texts.len(), 3);
                assert!(texts[2].contains("could not deliver reply, 13 chars, error Telegram sendMessage rejected: test rejection"));
            } else {
                assert!(result.is_ok());
                assert_eq!(texts.len(), 2);
            }
            http.abort();
        }
    }

    #[test]
    fn stream_holds_oauth_url_hash_and_fence_across_1024_byte_deltas() {
        let url = format!("https://accounts.google.com/o/oauth2/auth?client_id=example&scope={}",
            "https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform+".repeat(24));
        assert!(url.len() > 1000);
        let hash = format!("`{}`", "62966c07e464e042".repeat(12));
        let fence = format!("```text\n{}\n```", "atomic fenced code\n".repeat(20));
        let reply = format!("{}\n\nInstall complete: {url}\nSHA256 {hash}\n{fence}\nDone.", "Progress words. ".repeat(160));
        let mut stream = TurnStream::default();
        let mut chunks = Vec::new();
        for delta in reply.as_bytes().chunks(1024) {
            stream.push(std::str::from_utf8(delta).unwrap(), None);
            if stream.ready(Duration::from_secs(2)) {
                if let Some(chunk) = stream.flush_partial() {
                    chunks.extend(super::super::tg::split_text(&chunk, MAX_TG_CHARS));
                }
            }
        }
        if let Some(chunk) = stream.reconcile(&reply) {
            chunks.extend(super::super::tg::split_text(&chunk, MAX_TG_CHARS));
        }
        for (i, chunk) in chunks.iter().enumerate() {
            println!("chunk {i}: {} chars: {chunk}", clen(chunk));
        }
        for token in [&url, &hash, &fence] {
            assert!(chunks.iter().any(|c| c.contains(token)), "token split: {token}");
        }
        assert!(chunks.iter().all(|c| clen(c) <= MAX_TG_CHARS));
        assert_eq!(stream.reconcile(&reply), None);
    }

    #[tokio::test]
    async fn delta_stream_sends_progressively_without_replaying_the_finished_row() {
        let dir = tempfile::tempdir().unwrap();
        let paths = crate::paths::Paths { home: dir.path().join("home") };
        let server = std::sync::Arc::new(crate::server::Server::new(paths).unwrap());
        let cwd = dir.path().join("work");
        std::fs::create_dir_all(&cwd).unwrap();
        let sid = server.test_store().lock().await
            .create("streamer", "claude", cwd.to_str().unwrap(), "worker", "claude-haiku-5-5", "", None)
            .unwrap().id;
        let (api, sent, http) = mock_telegram().await;
        let bridge = std::sync::Arc::new(test_bridge(server.clone(), dir.path().join("home"), api));
        let ctx = bridge.dm();
        let baseline = bridge.latest_item_id(&sid).await.unwrap();
        let (tx, rx) = broadcast::channel(64);
        let relay = tokio::spawn({
            let (bridge, sid) = (bridge.clone(), sid.clone());
            let watch = EventWatch::new(sid.clone(), rx);
            async move { bridge.relay_turn_inner(&ctx, &sid, baseline, Some(watch)).await }
        });
        // Deltas arrive in order; reasoning must not join the prose stream. A
        // tool call between the two prose chunks is the separate-message
        // boundary that must keep them from merging.
        tx.send(json!({"type": "delta", "session": sid, "kind": "reasoning", "text": "hidden"})).unwrap();
        tx.send(json!({"type": "delta", "session": sid, "kind": "text", "text": "Good idea."})).unwrap();
        tx.send(json!({"type": "tool", "session": sid, "tool": "shell", "status": "running"})).unwrap();
        tx.send(json!({"type": "delta", "session": sid, "kind": "text", "text": " Let's do it."})).unwrap();
        tokio::time::sleep(Duration::from_millis(1600)).await;
        // The turn finishes: the history row is the exact concatenation.
        let row = server.test_store().lock().await.append(&sid, "agent", "Good idea. Let's do it.").unwrap();
        server.test_store().lock().await.set_status(&sid, "idle").unwrap();
        tokio::time::timeout(Duration::from_secs(8), relay).await
            .expect("relay did not end after the finished row").unwrap().unwrap();
        let texts = visible_texts(&sent);
        assert!(texts.len() >= 2, "expected progressive sends, got {texts:?}");
        // split_text trims each chunk, so compare without whitespace; the point
        // is that the finished row added no text of its own.
        let no_space = |s: &str| s.chars().filter(|c| !c.is_whitespace()).collect::<String>();
        assert_eq!(no_space(&texts.concat()), no_space("Good idea. Let's do it."),
                   "streamed text was replayed: {texts:?}");
        assert!(!texts.iter().any(|t| t.trim() == "Good idea. Let's do it."),
                "the finished row was sent again: {texts:?}");
        assert!(bridge.was_relayed(&row.to_string()), "finished row was not marked relayed");
        http.abort();
    }

    /// Drive one relayed turn through the real relay: `head` streams, a pause
    /// longer than the 2s batching interval forces a timed flush (which sends
    /// `head` and keeps its last token pending), then `tail` arrives and the
    /// row finishes. Returns the visible bubbles and the raw HTTP calls.
    async fn timed_flush_turn(head: &str, tail: &str) -> (Vec<String>, Vec<(String, Value)>) {
        let dir = tempfile::tempdir().unwrap();
        let paths = crate::paths::Paths { home: dir.path().join("home") };
        let server = std::sync::Arc::new(crate::server::Server::new(paths).unwrap());
        let cwd = dir.path().join("work");
        std::fs::create_dir_all(&cwd).unwrap();
        let sid = server.test_store().lock().await
            .create("streamer", "claude", cwd.to_str().unwrap(), "worker", "claude-haiku-5-5", "", None)
            .unwrap().id;
        let (api, sent, http) = mock_telegram().await;
        let bridge = std::sync::Arc::new(test_bridge(server.clone(), dir.path().join("home"), api));
        let ctx = bridge.dm();
        let baseline = bridge.latest_item_id(&sid).await.unwrap();
        let (tx, rx) = broadcast::channel(64);
        let relay = tokio::spawn({
            let (bridge, sid) = (bridge.clone(), sid.clone());
            let watch = EventWatch::new(sid.clone(), rx);
            async move { bridge.relay_turn_inner(&ctx, &sid, baseline, Some(watch)).await }
        });
        let full = format!("{head}{tail}");
        tx.send(json!({"type": "delta", "session": sid, "kind": "text", "text": head})).unwrap();
        // The pause is the trigger, not the length: any mid-reply gap this
        // long flushes the bubbles even for a message of 190 characters.
        tokio::time::sleep(Duration::from_millis(2400)).await;
        tx.send(json!({"type": "delta", "session": sid, "kind": "text", "text": tail})).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        server.test_store().lock().await.append(&sid, "agent", &full).unwrap();
        server.test_store().lock().await.set_status(&sid, "idle").unwrap();
        tokio::time::timeout(Duration::from_secs(8), relay).await
            .expect("relay did not end after the finished row").unwrap().unwrap();
        let bubbles = visible_texts(&sent);
        let calls = sent.lock().unwrap().clone();
        http.abort();
        (bubbles, calls)
    }

    /// The reported bug: a timed flush holds the unfinished word back, and the
    /// finished row then delivers that word as its own bubble ("... asking it
    /// costs" then "nothing."). The short tail must be appended to the bubble
    /// it belongs to, not sent as a second message.
    #[tokio::test]
    async fn a_timed_flush_appends_the_held_back_word_to_the_same_bubble() {
        let head = "Here is the plan, and asking it costs";
        let tail = " nothing.";
        let (bubbles, calls) = timed_flush_turn(head, tail).await;
        assert_eq!(bubbles.len(), 1, "the held-back word became its own message: {bubbles:?}");
        assert_eq!(bubbles[0], format!("{head}{tail}"), "the tail must complete the same bubble");
        assert!(calls.iter().any(|(p, _)| p.contains("editMessageText")),
                "the tail was not appended with an edit: {calls:?}");
    }

    /// The second report: a ~190 char reply, far under the Telegram limit,
    /// still split on a mid-sentence pause. Length is irrelevant; one turn of
    /// one pause must reach the user as one bubble.
    #[tokio::test]
    async fn a_short_message_is_not_split_by_a_mid_reply_pause() {
        let head = "That is fair, and worth saying plainly: the reply is short, so the Telegram limit cannot be what splits it, yet a two second pause before my next tool call still holds one word back, until";
        let tail = " chat.";
        let full = format!("{head}{tail}");
        assert!(full.chars().count() < 200, "this case must stay well under the char limit ({} chars)", full.chars().count());
        let (bubbles, _calls) = timed_flush_turn(head, tail).await;
        assert_eq!(bubbles.len(), 1, "a short reply was split on a pause: {bubbles:?}");
        assert_eq!(bubbles[0], full, "the tail must complete the same bubble");
    }

    /// Drive the real relay from a script of broadcast events, one per
    /// `(delay_ms, event)`, and let the item poll see the finished `rows` at
    /// `rows_at_ms` -- which may fall before a later event, as the live race
    /// did when a finished row overtook a lagging delta.
    /// Returns the bubbles the chat ends up showing and every HTTP call.
    async fn scripted_turn(events: Vec<(u64, Value)>, rows: Vec<&str>, rows_at_ms: u64)
        -> (Vec<String>, Vec<(String, Value)>) {
        let dir = tempfile::tempdir().unwrap();
        let paths = crate::paths::Paths { home: dir.path().join("home") };
        let server = std::sync::Arc::new(crate::server::Server::new(paths).unwrap());
        let cwd = dir.path().join("work");
        std::fs::create_dir_all(&cwd).unwrap();
        let sid = server.test_store().lock().await
            .create("streamer", "claude", cwd.to_str().unwrap(), "worker", "claude-haiku-5-5", "", None)
            .unwrap().id;
        let (api, sent, http) = mock_telegram().await;
        let bridge = std::sync::Arc::new(test_bridge(server.clone(), dir.path().join("home"), api));
        let ctx = bridge.dm();
        let baseline = bridge.latest_item_id(&sid).await.unwrap();
        let (tx, rx) = broadcast::channel(64);
        let relay = tokio::spawn({
            let (bridge, sid) = (bridge.clone(), sid.clone());
            let watch = EventWatch::new(sid.clone(), rx);
            async move { bridge.relay_turn_inner(&ctx, &sid, baseline, Some(watch)).await }
        });
        let rows: Vec<String> = rows.into_iter().map(String::from).collect();
        let sid_events = sid.clone();
        let script = tokio::spawn({
            async move {
                let mut at = 0;
                for (delay, ev) in events {
                    tokio::time::sleep(Duration::from_millis(delay - at)).await;
                    at = delay;
                    let mut ev = ev;
                    ev["session"] = json!(sid_events.clone());
                    let _ = tx.send(ev);
                }
            }
        });
        let row_script = tokio::spawn({
            let (server, sid) = (server.clone(), sid.clone());
            async move {
                tokio::time::sleep(Duration::from_millis(rows_at_ms)).await;
                for row in rows {
                    server.test_store().lock().await.append(&sid, "agent", &row).unwrap();
                }
                server.test_store().lock().await.set_status(&sid, "idle").unwrap();
            }
        });
        let _ = tokio::join!(script, row_script);
        tokio::time::timeout(Duration::from_secs(10), relay).await
            .expect("relay did not end after the finished rows").unwrap().unwrap();
        let bubbles = visible_texts(&sent);
        let calls = sent.lock().unwrap().clone();
        http.abort();
        (bubbles, calls)
    }

    /// The live bug the reconcile patch missed: what the user sees is sent by
    /// the streamed flush path. 148 chars stream, a 2s pause, then the seven
    /// chars " in." and a tool boundary. The word must complete the bubble it
    /// came from, never land as a message of its own.
    #[tokio::test]
    async fn a_streamed_tail_after_a_pause_completes_the_same_bubble() {
        let head = "The reply arrives as the model writes it, so this sentence reaches the user in pieces and asks whether the person on the other end is still allowed ";
        assert_eq!(head.chars().count(), 148, "mirrors the live log's 148-char flush");
        let tail = " in.";
        let events = vec![
            (0u64, json!({"type": "delta", "session": "SID", "kind": "text", "text": head, "message_id": "m1"})),
            (2400, json!({"type": "delta", "session": "SID", "kind": "text", "text": tail, "message_id": "m1"})),
            (2600, json!({"type": "tool", "session": "SID"})),
        ];
        let full = format!("{head}{tail}");
        let (bubbles, calls) = scripted_turn(events, vec![&full], 3200).await;
        assert_eq!(bubbles.len(), 1, "the streamed tail became its own message: {bubbles:?}");
        assert_eq!(bubbles[0], "The reply arrives as the model writes it, so this sentence reaches the user in pieces and asks whether the person on the other end is still allowed in.");
        assert!(calls.iter().any(|(p, _)| p.contains("editMessageText")),
                "the tail was not appended with an edit: {calls:?}");
    }

    /// The other side of the same rule: a tool boundary ends the assistant
    /// message, so a short *new* message after it is a new bubble, never a tail
    /// appended to the previous one.
    #[tokio::test]
    async fn a_short_new_message_after_a_tool_is_its_own_bubble() {
        let events = vec![
            (0u64, json!({"type": "delta", "session": "SID", "kind": "text", "text": "Let me check the logs.", "message_id": "m1"})),
            (300, json!({"type": "tool", "session": "SID"})),
            (600, json!({"type": "delta", "session": "SID", "kind": "text", "text": "Found it.", "message_id": "m2"})),
        ];
        let (bubbles, calls) = scripted_turn(events, vec!["Let me check the logs.", "Found it."], 1600).await;
        assert_eq!(bubbles, vec!["Let me check the logs.".to_string(), "Found it.".to_string()],
                   "two assistant messages were merged: {bubbles:?} calls={calls:?}");
        let sends = calls.iter().filter(|(p, _)| p.contains("sendMessage")).count();
        assert_eq!(sends, 2, "the second message was not opened as its own bubble: {calls:?}");
    }

    #[tokio::test]
    async fn terminal_picker_picks_a_workspace_before_it_links() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        std::fs::create_dir_all(home.join("workspaces")).unwrap();
        let paths = crate::paths::Paths { home: home.clone() };
        let server = std::sync::Arc::new(crate::server::Server::new(paths).unwrap());
        let work = home.join("workspaces").join("dev");
        let other = home.join("workspaces").join("other");
        std::fs::create_dir_all(&work).unwrap();
        std::fs::create_dir_all(&other).unwrap();
        {
            let store = server.test_store();
            let st = store.lock().await;
            st.create("dev", "claude", work.to_str().unwrap(), "worker", "claude-haiku-5-5", "", None).unwrap();
            st.create("other", "claude", other.to_str().unwrap(), "worker", "claude-haiku-5-5", "", None).unwrap();
        }
        // Enabled on a loopback ephemeral port: no live port and no phone in tests.
        std::fs::write(home.join("terminal.json"), serde_json::to_vec(&json!({"enabled": true, "listen": "127.0.0.1:0"})).unwrap()).unwrap();
        let (api, sent, http) = mock_telegram().await;
        let bridge = std::sync::Arc::new(test_bridge(server.clone(), home.clone(), api));
        let ctx = bridge.dm();

        bridge.handle_text(&ctx, "/terminal", None, false, false, true).await.unwrap();
        let picker = sent_texts(&sent);
        assert!(picker.iter().any(|t| t.contains("Terminal — which directory")), "{picker:?}");
        assert!(picker.iter().any(|t| t.contains("workspaces/dev")), "{picker:?}");
        assert!(picker.iter().all(|t| !t.contains("http")), "the picker leaked a link: {picker:?}");

        let callback = |tok: &str, action: &str| json!({"id": "cb", "data": format!("td:{tok}:{action}"),
            "message": {"message_id": 7, "chat": {"id": 1}}, "from": {"id": 1}});
        let pick = |bridge: &Bridge| bridge.with_state(|st| {
            let (tok, card) = st["terminal_cards"].as_object().unwrap().iter().next().unwrap();
            let i = card["dirs"].as_array().unwrap().iter()
                .position(|d| d["path"] == json!(work.to_str().unwrap())).unwrap();
            (tok.clone(), i)
        });

        // An aged card is refused without printing anything.
        let (tok, index) = pick(&bridge);
        bridge.with_state(|st| st["terminal_cards"][&tok]["created_at"] = json!(now_f() - TERMINAL_CARD_TTL_S - 1.0));
        let before = sent_texts(&sent).len();
        bridge.handle_callback(&callback(&tok, &index.to_string())).await.unwrap();
        assert_eq!(sent_texts(&sent).len(), before, "an expired picker still printed a message");

        // A fresh pick links only after the choice, in that session's folder.
        bridge.handle_text(&ctx, "/terminal", None, false, false, true).await.unwrap();
        let (tok, index) = pick(&bridge);
        bridge.handle_callback(&callback(&tok, &index.to_string())).await.unwrap();
        let link = sent_texts(&sent).last().cloned().unwrap_or_default();
        assert!(link.contains("One-time terminal link") || link.contains("Terminal unavailable"), "{link}");
        assert!(link.contains("workspaces/dev"), "the link did not name the chosen directory: {link}");
        assert!(!link.contains("workspaces/other"), "{link}");
        assert!(bridge.with_state(|st| st["terminal_cards"].as_object().map(|c| c.is_empty()).unwrap_or(true)),
                "the picker token is still usable after a pick");
        http.abort();
    }

    #[tokio::test]
    async fn terminal_custom_path_skips_the_picker() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        std::fs::create_dir_all(home.join("workspaces")).unwrap();
        let paths = crate::paths::Paths { home: home.clone() };
        let server = std::sync::Arc::new(crate::server::Server::new(paths).unwrap());
        let custom = home.join("custom");
        std::fs::create_dir_all(&custom).unwrap();
        std::fs::write(home.join("terminal.json"), serde_json::to_vec(&json!({"enabled": true, "listen": "127.0.0.1:0"})).unwrap()).unwrap();
        let (api, sent, http) = mock_telegram().await;
        let bridge = std::sync::Arc::new(test_bridge(server.clone(), home.clone(), api));
        let ctx = bridge.dm();

        // /terminal <path> goes straight to a link, no picker.
        bridge.handle_text(&ctx, &format!("/terminal {}", custom.display()), None, false, false, true).await.unwrap();
        let texts = sent_texts(&sent);
        assert!(!texts.iter().any(|t| t.contains("which directory")), "{texts:?}");
        assert!(texts.last().unwrap().contains("/custom"), "{texts:?}");

        // The picker's "Type a path" step takes the next plain message.
        bridge.handle_text(&ctx, "/terminal", None, false, false, true).await.unwrap();
        let tok = bridge.with_state(|st| st["terminal_cards"].as_object().unwrap().keys().next().unwrap().clone());
        bridge.handle_callback(&json!({"id": "cb", "data": format!("td:{tok}:c"),
            "message": {"message_id": 9, "chat": {"id": 1}}, "from": {"id": 1}})).await.unwrap();
        assert!(bridge.with_state(|st| st["terminal_path_wait"].get("1").is_some()), "no path prompt was armed");
        bridge.handle_text(&ctx, custom.to_str().unwrap(), None, false, false, true).await.unwrap();
        let last = sent_texts(&sent).last().cloned().unwrap_or_default();
        assert!(last.contains("/custom"), "the typed path was not used: {last}");

        // A missing directory is refused and mints no link.
        let before = sent_texts(&sent).len();
        bridge.handle_text(&ctx, "/terminal /definitely/not/a/kurisu/dir", None, false, false, true).await.unwrap();
        let msgs = sent_texts(&sent);
        assert_eq!(msgs.len(), before + 1, "{msgs:?}");
        assert!(msgs.last().unwrap().contains("not a directory"), "{:?}", msgs.last());
        http.abort();
    }

    #[test]
    fn finish_row_after_streaming_is_not_replayed() {
        let mut stream = TurnStream::default();
        stream.push("Good idea.", None);
        assert_eq!(stream.flush().as_deref(), Some("Good idea."));
        stream.push(" Let's do it.", None);
        assert_eq!(stream.reconcile("Good idea. Let's do it.").as_deref(), Some(" Let's do it."));
        assert_eq!(stream.reconcile("Good idea. Let's do it."), None);
    }

    #[test]
    fn dead_watch_falls_back_to_only_the_missing_tail() {
        let mut stream = TurnStream::default();
        stream.push("Good idea.", None);
        assert_eq!(stream.flush().as_deref(), Some("Good idea."));
        // The watch died after the first sentence: the row carries text the
        // stream never delivered, and only that tail is sent.
        assert_eq!(stream.reconcile("Good idea. Let's finish.").as_deref(), Some(" Let's finish."));
        assert_eq!(stream.reconcile("Good idea. Let's finish."), None);
    }

    #[test]
    fn tool_call_separates_prose_into_distinct_messages() {
        use tokio::sync::broadcast;
        let (tx, rx) = broadcast::channel(16);
        let mut watch = EventWatch::new("s", rx);
        let mut stream = TurnStream::default();
        tx.send(json!({"type": "delta", "session": "s", "kind": "text", "text": "name it."})).unwrap();
        tx.send(json!({"type": "tool", "session": "s", "tool": "shell", "status": "running"})).unwrap();
        tx.send(json!({"type": "delta", "session": "s", "kind": "text", "text": "That ran 65s"})).unwrap();
        assert!(watch.drain(&mut stream), "a tool call must flush the preceding prose");
        assert_eq!(stream.flush().as_deref(), Some("name it."));
        assert!(!watch.drain(&mut stream));
        assert_eq!(stream.flush().as_deref(), Some("That ran 65s"));
        // The final row is their concatenation, and it has all been sent.
        assert_eq!(stream.reconcile("name it.That ran 65s"), None);
    }

    #[test]
    fn message_id_change_keeps_two_assistant_messages_apart() {
        use tokio::sync::broadcast;
        let (tx, rx) = broadcast::channel(16);
        let mut watch = EventWatch::new("s", rx);
        let mut stream = TurnStream::default();
        tx.send(json!({"type": "delta", "session": "s", "kind": "text", "message_id": "m1", "text": "First step."})).unwrap();
        tx.send(json!({"type": "delta", "session": "s", "kind": "text", "message_id": "m2", "text": "Second step."})).unwrap();
        assert!(watch.drain(&mut stream));
        assert_eq!(stream.flush().as_deref(), Some("First step."));
        assert!(!watch.drain(&mut stream));
        assert_eq!(stream.flush().as_deref(), Some("Second step."));
    }

    #[test]
    fn stream_batches_on_boundaries_timer_and_size_but_never_sends_whitespace() {
        let mut stream = TurnStream::default();
        stream.push("   ", None);
        assert!(!stream.ready(Duration::from_secs(10)));
        assert_eq!(stream.flush(), None);
        stream.push("A sentence.", None);
        assert!(stream.ready(Duration::from_millis(1)));
        stream.push(&"x".repeat(800), None);
        assert!(stream.ready(Duration::from_millis(1)));
        let mut stream = TurnStream::default();
        stream.push("a word without punctuation", None);
        assert!(!stream.ready(Duration::from_millis(1)));
        assert!(stream.ready(Duration::from_secs(2)));
    }

    #[test]
    fn progress_line_keeps_a_max_length_activity_under_telegram_cap() {
        let activity = format!("running Shell: {}", "x".repeat(crate::activity::MAX_TITLE_CHARS));
        let line = progress_line(&activity, 123);
        assert!(clen(&line) < 4096, "line is {} chars", clen(&line));
        assert!(line.starts_with("⏳ Still working — running Shell: "));
        assert!(line.ends_with(" (123s)"));
        assert!(line.contains(&"x".repeat(1000)), "the command must arrive intact");
    }

    #[test]
    fn progress_line_clamps_an_overlong_activity_instead_of_dropping_it() {
        let line = progress_line(&"y".repeat(10_000), 5);
        assert!(clen(&line) <= MAX_TG_CHARS);
        assert!(line.ends_with("… (5s)"));
    }

    #[test]
    fn formatting_matches_the_bridge() {
        assert_eq!(short_model("claude-opus-5-5"), "opus-5.5");
        assert_eq!(short_model("claude-haiku-4-5-20251001"), "haiku-4.5");
        assert_eq!(short_model("ollama/deepseek-v4.1-flash:cloud"), "deepseek-v4.1-flash:cloud");
        assert_eq!(short_model(""), "default");
        assert_eq!(short_tokens(230_000.0), "230K");
        assert_eq!(short_tokens(1_000_000.0), "1M");
        assert_eq!(short_tokens(1_500_000.0), "1.5M");
        assert_eq!(short_tokens(500.0), "500");
        assert_eq!(fmt_int(1234567), "1,234,567");
        assert_eq!(fmt_money(4.2), "4.20");
        assert_eq!(fmt_g(38.0), "38");
        assert_eq!(fmt_g(38.25), "38.2");
        assert_eq!(countdown(3 * 3600 + 5 * 60), "3h 05m");
        assert_eq!(countdown(2 * 86400 + 3600), "2d 1h");
        assert_eq!(usage_age(30), "30s");
        assert_eq!(usage_age(600), "10 min");
        assert_eq!(context_label(&json!({"last_total_tokens": 35468, "context_window": 200000})).unwrap(), "35K/200K");
        assert_eq!(context_label(&json!({"context_pending": true, "context_window": 200000})).unwrap(), "pending/200K");
        assert_eq!(format_context_usage(&json!({"last_total_tokens": 35468, "context_window": 200000})), "35,468 / 200,000 tokens (17.7% used)");
        assert_eq!(format_context_usage(&json!({})), "not reported yet");
        assert_eq!(page_bounds(12, 9), (2, 10));
        assert_eq!(short_version("2.1.319 (Claude Code)"), "2.1.319");
        assert_eq!(short_version("codex-cli 0.166.0"), "0.166.0");
        assert_eq!(short_version("v12"), "");
        assert_eq!(page_bounds(0, 3), (0, 0));
        assert_eq!(short_list(&["a".into(), "b".into(), "c".into(), "d".into()], 3), "a, b, c +1");
        assert_eq!(estimated_cost(&json!({"usage_by_model": {"claude-haiku-5-5": {"input_tokens": 1_000_000, "output_tokens": 0}}})), Some(1.0));
        assert_eq!(toml_top_level_string("model = \"deepseek:cloud\"\n[tui]\nmodel = \"x\"", "model").unwrap(), "deepseek:cloud");
        let usage = json!({"confidence": "official", "five_hour": {"used_percentage": 38, "resets_at": 1000 + 3600, "window_over": false},
                           "seven_day": null, "age_seconds": 30, "stale": false});
        let t = format_usage(&usage, None, 1000);
        assert!(t.contains("5-hour: 38% used · resets ") && t.contains("(in 1h 00m)") && t.contains("7-day: not reported") && t.ends_with("Captured 30s ago."), "{t}");
    }

    #[test]
    fn attachments_and_voice() {
        let m = json!({"photo": [{"file_id": "s", "file_size": 10}, {"file_id": "b", "file_size": 99}]});
        assert_eq!(attachment_from_message(&m).unwrap()["file_id"], "b");
        let v = json!({"voice": {"file_id": "v", "mime_type": "audio/ogg"}});
        assert_eq!(attachment_from_message(&v).unwrap(), json!({"file_id": "v", "name": "voice.ogg", "mime": "audio/ogg"}));
        assert!(is_audio_attachment("voice.ogg", "") && is_audio_attachment("x", "audio/mpeg") && !is_audio_attachment("a.txt", "text/plain"));
        assert_eq!(audio_name("rec", "audio/mpeg"), "rec.mp3");
        assert!(voice_transcript_text("voice.ogg", "/p", "", "note").contains("(no speech detected)"));
    }

    /// The `/model` picker reads `op models`, so a model that exists only in the
    /// OpenCode CLI fixture (hardcoded nowhere in Kurisu) must reach its options.
    #[tokio::test]
    async fn model_picker_lists_opencode_models_discovered_from_the_cli() {
        let _env = crate::server::OPENCODE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("fixture-models.txt"), "fixture/picker-only-model\n").unwrap();
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../scripts/fixtures/opencode-models.py");
        assert!(fixture.is_file(), "fixture missing at {}", fixture.display());
        std::env::set_var("KURISU_OPENCODE_BIN", &fixture);
        std::env::set_var("KURISU_FIXTURE_HOME", dir.path());
        std::env::set_var("KURISU_OPENCODE_CONFIG", dir.path().join("no-opencode.json"));
        std::env::set_var("KURISU_OPENCODE_MODELS_TTL_SECS", "0");
        let server = Arc::new(crate::server::Server::new(crate::paths::Paths { home: dir.path().join("home") }).unwrap());
        let bridge = Arc::new(test_bridge(server, dir.path().join("home"), "http://127.0.0.1:1".into()));
        let (options, note) = bridge.model_options(&json!({"kurisu_harness": "opencode"})).await.unwrap();
        assert!(options.iter().any(|o| o["id"] == "fixture/picker-only-model"), "{options:?}");
        assert_eq!(note, None);
    }

    /// Feed updates through the channel boundary the way the poller does, then
    /// ask the agent-facing tool what it sees.
    #[tokio::test]
    async fn telegram_history_records_updates_and_pages_searches_and_stays_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let paths = crate::paths::Paths { home: dir.path().join("home") };
        let server = std::sync::Arc::new(crate::server::Server::new(paths.clone()).unwrap());
        // The tool needs to know who the owner is (the live server reads this at startup).
        crate::telegram::save_config(&paths.home, &crate::telegram::Config {
            token: "TEST".into(), chat_id: "1".into(), owner_user_id: None, new_session_cwd: None, enabled: false,
        }).unwrap();
        let (api, sent, http) = mock_telegram().await;
        let bridge = std::sync::Arc::new(test_bridge(server.clone(), paths.home.clone(), api));
        let ask = |v: Value| { let s = server.clone(); async move { s.test_call(v).await } };

        let text_update = |id: i64, mid: i64, text: &str| json!({"update_id": id, "message": {
            "message_id": mid, "date": 1_700_000_000 + id, "chat": {"id": 1, "type": "private"},
            "from": {"id": 7, "is_bot": false}, "text": text}});
        bridge.handle_update(11, &text_update(11, 101, "deploy the site")).await.unwrap();
        // A file with a caption.
        bridge.handle_update(12, &json!({"update_id": 12, "message": {
            "message_id": 102, "date": 1_700_000_012, "chat": {"id": 1, "type": "private"},
            "from": {"id": 7, "is_bot": false}, "caption": "here it is",
            "document": {"file_id": "f1", "file_name": "report.pdf", "mime_type": "application/pdf"}}})).await.unwrap();
        // A message that arrives while a turn is running: queued, and its notice is a reply
        // in the chat but not an agent answer.
        bridge.queued.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        bridge.handle_update(13, &text_update(13, 103, "and then?")).await.unwrap();
        bridge.queued.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        let sent_so_far = sent_texts(&sent);
        let saw_queued_notice = sent_so_far.iter().any(|t| t.contains("Queued"));
        assert!(saw_queued_notice, "{sent_so_far:?}");
        // The relayed answer for update 13.
        let ctx = bridge.ctx(json!(1), None, Some(13));
        bridge.relay_text(&ctx, &json!(1), "s1", "Deploying now.").await.unwrap();
        // A group message (the owner's sender id does not allow it) is never recorded.
        bridge.handle_update(14, &json!({"update_id": 14, "message": {
            "message_id": 104, "date": 1_700_000_014, "chat": {"id": -100, "type": "supergroup"},
            "from": {"id": 7, "is_bot": false}, "text": "secret group talk"}})).await.unwrap();

        let page = ask(json!({"op": "telegram_history"})).await.unwrap();
        let msgs = page["messages"].as_array().unwrap().clone();
        let rows: Vec<(String, String, bool)> = msgs.iter()
            .map(|m| (m["direction"].as_str().unwrap().to_string(), m["text"].as_str().unwrap().to_string(),
                      m["answered"].as_bool().unwrap())).collect();
        assert_eq!(rows.len(), 5, "{rows:?}");
        assert_eq!(rows[0], ("user".into(), "deploy the site".into(), false));
        assert_eq!(rows[1], ("user".into(), "here it is".into(), false));
        assert_eq!(msgs[1]["attachment"]["name"], "report.pdf");
        assert_eq!(msgs[1]["attachment"]["kind"], "file");
        // The queue notice is a chat message, not an answer.
        assert_eq!(rows[2].0, "agent");
        assert!(rows[2].1.contains("Attachment queued"), "{:?}", rows[2].1);
        assert_eq!(rows[3], ("user".into(), "and then?".into(), true));
        // The queued notice and the answer are one agent message for that update.
        assert_eq!(rows[4].0, "agent");
        assert!(rows[4].1.contains("Deploying now.") && rows[4].1.contains("Queued"), "{:?}", rows[4].1);
        assert!(!rows.iter().any(|r| r.1.contains("group talk")), "a group message leaked into the tool: {rows:?}");
        // A replayed update (offset rewound) is not stored twice: only the queue
        // notice for it is added, the user message is not duplicated.
        bridge.handle_update(11, &text_update(11, 101, "deploy the site")).await.unwrap();
        let again = ask(json!({"op": "telegram_history"})).await.unwrap();
        assert_eq!(again["messages"].as_array().unwrap().iter().filter(|m| m["direction"] == "user").count(), 3);
        assert_eq!(again["total"], 6);

        // Paging: newest page first, then back with before=<first_seq>.
        let p1 = ask(json!({"op": "telegram_history", "limit": 2})).await.unwrap();
        assert_eq!(p1["messages"].as_array().unwrap().len(), 2);
        assert_eq!(p1["has_more"], true);
        assert_eq!((p1["first_seq"].as_i64(), p1["last_seq"].as_i64()),
                   (again["messages"][4]["seq"].as_i64(), again["messages"][5]["seq"].as_i64()));
        let p2 = ask(json!({"op": "telegram_history", "limit": 2, "before": p1["first_seq"]})).await.unwrap();
        assert_eq!(p2["messages"].as_array().unwrap().len(), 2);
        assert_eq!(p2["has_more"], true);
        let p3 = ask(json!({"op": "telegram_history", "limit": 2, "before": p2["first_seq"].clone()})).await.unwrap();
        assert_eq!(p3["has_more"], false);
        assert_eq!(p3["messages"][0]["text"], "deploy the site");

        // Search matches text and attachment names.
        let found = ask(json!({"op": "telegram_history", "search": "REPORT"})).await.unwrap();
        assert_eq!(found["total"], 1);
        assert_eq!(found["messages"][0]["message_id"], 102);
        // Unanswered: the two messages nobody replied to.
        let open = ask(json!({"op": "telegram_history", "unanswered": true})).await.unwrap();
        assert_eq!(open["total"], 2);
        assert_eq!(open["messages"][1]["text"], "here it is");

        // Owner-only: the owner's own chat ids work, anything else is refused.
        for chat in ["1", "dm", "me"] {
            assert!(ask(json!({"op": "telegram_history", "chat": chat})).await.is_ok(), "{chat}");
        }
        for chat in ["2", "-100123", "someone"] {
            let e = ask(json!({"op": "telegram_history", "chat": chat})).await.unwrap_err().to_string();
            assert!(e.contains("only reads the owner's DM"), "{chat}: {e}");
        }
        http.abort();
    }

    async fn test_session(server: &std::sync::Arc<crate::server::Server>, name: &str,
                          dir: &std::path::Path) -> String {
        let cwd = dir.join("work");
        std::fs::create_dir_all(&cwd).unwrap();
        server.test_store().lock().await
            .create(name, "claude", cwd.to_str().unwrap(), "worker", "claude-haiku-5-5", "", None)
            .unwrap().id
    }

    /// The turn relay and the broadcast watcher are the two paths that can
    /// both see the same finished row. Whichever runs first, the item is sent
    /// once: the loser must not repeat it.
    #[tokio::test]
    async fn relay_and_broadcast_send_a_final_item_once_in_either_order() {
        for relay_first in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let home = dir.path().join("home");
            let server = std::sync::Arc::new(crate::server::Server::new(crate::paths::Paths { home: home.clone() }).unwrap());
            let sid = test_session(&server, "dup", dir.path()).await;
            let (api, sent, http) = mock_telegram().await;
            let bridge = std::sync::Arc::new(test_bridge(server.clone(), home, api));
            let ctx = bridge.dm();
            let text = "First paragraph of the final answer.\n\nSecond paragraph.";
            let row = server.test_store().lock().await.append(&sid, "agent", text).unwrap();
            server.test_store().lock().await.set_status(&sid, "idle").unwrap();
            bridge.with_state(|st| {
                st.insert("broadcast_cursors".into(), json!({ sid.clone(): "0" }));
                st.insert("broadcast_run_started_at".into(), json!(0));
            });
            let session = json!({"id": sid, "title": "dup", "status": "idle", "created_at": 0});
            if relay_first {
                bridge.relay_turn_inner(&ctx, &sid, None, None).await.unwrap();
                assert_eq!(sent_texts(&sent).len(), 1, "relay path alone sent {sent:?}");
                bridge.broadcast_once(&[session]).await.unwrap();
            } else {
                bridge.broadcast_once(&[session]).await.unwrap();
                assert_eq!(sent_texts(&sent).len(), 1, "broadcast path alone sent {sent:?}");
                bridge.relay_turn_inner(&ctx, &sid, None, None).await.unwrap();
            }
            let texts = sent_texts(&sent);
            assert_eq!(texts.len(), 1, "relay_first={relay_first} sent the same item twice: {texts:?}");
            assert!(bridge.was_relayed(&row.to_string()), "the item was never recorded");
            http.abort();
        }
    }

    /// The durable half: the id is on disk before the send, so a crash or a
    /// restart between the send and any later write cannot relay it again.
    #[tokio::test]
    async fn a_relay_claim_is_on_disk_before_the_send_and_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let server = std::sync::Arc::new(crate::server::Server::new(crate::paths::Paths { home: home.clone() }).unwrap());
        let (api, sent, http) = mock_telegram().await;
        let bridge = test_bridge(server.clone(), home.clone(), api.clone());
        assert!(bridge.claim_relayed("4242"));
        // No send has happened, but the record is already flushed to disk.
        let disk = std::fs::read_to_string(bridge.relayed_file()).unwrap();
        assert!(disk.contains("4242"), "claim was not persisted before the send: {disk}");

        let restarted = test_bridge(server, home, api);
        restarted.load_relayed();
        assert!(restarted.was_relayed("4242"));
        assert!(!restarted.claim_relayed("4242"), "a restarted bridge claimed the same item again");
        assert!(sent_texts(&sent).is_empty(), "the claim path must not send by itself");
        http.abort();
    }

    /// A send whose response was lost is ambiguous: Telegram may already have
    /// the message. Retrying it is a literal identical duplicate, so the
    /// transport must give up instead of re-posting the same text.
    #[tokio::test]
    async fn an_ambiguous_send_failure_is_not_retried() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = format!("http://{}", listener.local_addr().unwrap());
        let conns = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = conns.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { break };
                seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Headers promise a body, then the connection dies: the
                // request was delivered, the reply was not readable.
                let _ = sock.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 60\r\nconnection: close\r\n\r\n").await;
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                drop(sock);
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let server = std::sync::Arc::new(crate::server::Server::new(crate::paths::Paths { home: home.clone() }).unwrap());
        let bridge = test_bridge(server, home, api);
        let e = bridge.tg("sendMessage", Some(json!({"chat_id": 1, "text": "same bubble"})), 5, Some(30)).await.unwrap_err();
        assert!(is_network_error(&e), "{e}");
        assert!(!failed_before_delivery(&e), "an unreadable response must be treated as maybe-delivered: {e}");
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(conns.load(std::sync::atomic::Ordering::SeqCst), 1,
                   "the ambiguous send was retried and the bubble arrived twice");
        task.abort();
    }

    /// The second belt: two distinct items carrying the same text, moments
    /// apart in one session, still produce one bubble - but the same text in
    /// another session, or a genuinely different text, is not suppressed.
    #[tokio::test]
    async fn identical_text_is_one_bubble_per_session_but_not_across_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let server = std::sync::Arc::new(crate::server::Server::new(crate::paths::Paths { home }).unwrap());
        let (api, sent, http) = mock_telegram().await;
        let bridge = test_bridge(server, dir.path().join("home"), api);
        let ctx = bridge.dm();
        bridge.relay_text(&ctx, &json!(1), "s1", "Same answer.").await.unwrap();
        bridge.relay_text(&ctx, &json!(1), "s1", "Same answer.").await.unwrap();
        bridge.relay_text(&ctx, &json!(1), "s2", "Same answer.").await.unwrap();
        bridge.relay_text(&ctx, &json!(1), "s1", "Different answer.").await.unwrap();
        assert_eq!(sent_texts(&sent),
                   ["Same answer.", "Same answer.", "Different answer."].map(String::from));
        http.abort();
    }

    /// Two rows with identical text in one session's history must not both be
    /// broadcast: id-level claim plus the text belt keep one bubble.
    #[tokio::test]
    async fn two_distinct_items_with_the_same_text_broadcast_once() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let server = std::sync::Arc::new(crate::server::Server::new(crate::paths::Paths { home: home.clone() }).unwrap());
        let sid = test_session(&server, "dup", dir.path()).await;
        let s = server.test_store();
        let st = s.lock().await;
        st.append(&sid, "user", "question one").unwrap();
        st.append(&sid, "agent", "Same text.").unwrap();
        st.append(&sid, "user", "question two").unwrap();
        st.append(&sid, "agent", "Same text.").unwrap();
        st.set_status(&sid, "idle").unwrap();
        drop(st);
        let (api, sent, http) = mock_telegram().await;
        let bridge = std::sync::Arc::new(test_bridge(server, home, api));
        bridge.with_state(|st| {
            st.insert("broadcast_cursors".into(), json!({ sid.clone(): "0" }));
            st.insert("broadcast_run_started_at".into(), json!(0));
        });
        bridge.broadcast_once(&[json!({"id": sid, "title": "dup", "status": "idle", "created_at": 0})]).await.unwrap();
        let texts = sent_texts(&sent);
        assert_eq!(texts.len(), 1, "two identical items produced {texts:?}");
        assert!(texts[0].ends_with("Same text."), "{texts:?}");
        http.abort();
    }

    /// Two turns finishing in one session at the same time: each row is
    /// claimed by one relay only, so neither is sent twice.
    #[tokio::test]
    async fn two_concurrent_turns_in_one_session_each_send_once() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let server = std::sync::Arc::new(crate::server::Server::new(crate::paths::Paths { home: home.clone() }).unwrap());
        let sid = test_session(&server, "dup", dir.path()).await;
        {
            let s = server.test_store();
            let st = s.lock().await;
            st.append(&sid, "agent", "Answer one.").unwrap();
            st.append(&sid, "agent", "Answer two.").unwrap();
            st.set_status(&sid, "idle").unwrap();
        }
        let (api, sent, http) = mock_telegram().await;
        let bridge = std::sync::Arc::new(test_bridge(server, home, api));
        let ctx = bridge.dm();
        let mut tasks = vec![];
        for _ in 0..2 {
            let (bridge, sid, ctx) = (bridge.clone(), sid.clone(), ctx.clone());
            tasks.push(tokio::spawn(async move { bridge.relay_turn_inner(&ctx, &sid, None, None).await }));
        }
        for t in tasks {
            tokio::time::timeout(Duration::from_secs(10), t).await
                .expect("a concurrent relay did not finish").unwrap().unwrap();
        }
        let mut texts = sent_texts(&sent);
        texts.sort();
        assert_eq!(texts, ["Answer one.", "Answer two."].map(String::from),
                   "a concurrent turn was relayed twice: {texts:?}");
        http.abort();
    }

    /// A streamed prefix followed by the finished row must send only the tail,
    /// including when the row trim drops trailing whitespace from the stream.
    #[test]
    fn reconcile_sends_only_the_tail_after_a_streamed_prefix() {
        let mut s = TurnStream::default();
        s.push("The answer is ", None);
        assert_eq!(s.flush().as_deref(), Some("The answer is "));
        assert_eq!(s.reconcile("The answer is forty-two.").map(|t| t.trim().to_string()),
                   Some("forty-two.".to_string()));
        // The same finished row again adds nothing.
        assert_eq!(s.reconcile("The answer is forty-two."), None);

        let mut s = TurnStream::default();
        s.push("First line.\n\n", None);
        assert!(s.flush().is_some());
        assert_eq!(s.reconcile("First line.\n\nSecond line.").map(|t| t.trim().to_string()),
                   Some("Second line.".to_string()));

        // The ordinary case: the row is exactly what the stream already sent.
        let mut s = TurnStream::default();
        s.push("Complete reply.", None);
        assert!(s.flush().is_some());
        assert_eq!(s.reconcile("Complete reply."), None);

        // A delta that loses the race with the finished row is not streamed
        // again; only a different assistant message is still live.
        let mut s = TurnStream::default();
        s.push("Complete reply.", Some("m1"));
        assert!(s.flush().is_some());
        assert_eq!(s.reconcile("Complete reply."), None);
        s.push(" reply.", Some("m1"));
        assert!(!s.has_pending_text(), "a settled message kept accepting its lagging deltas");
        s.push("Second message.", Some("m2"));
        assert!(s.has_pending_text(), "a new assistant message was dropped with the settled one");
    }

    /// Live 2026-11-09 17:40 (message 19600), reproduced from the log: the
    /// deltas streamed 1246 chars, "relayed assistant item 19600 (1273 chars)"
    /// followed, and then one lagging delta -- " merged or deployed." -- was
    /// drained and streamed as 11 chars *after* the finished row. Its timed
    /// flush appended " merged or" to the bubble that already held the whole
    /// sentence, so the reply ended "...deployed. merged or".
    #[tokio::test]
    async fn final_reply_never_reappends_stale_fragment() {
        let mut head = String::from(r#"Good call. Let me make the month produce evidence instead of a feeling.Locked in, with a mechanism rather than a resolution:

**Recorded** in `AGENTS.md` (mirrored to the gist) and `TODO.md`, plus a **wake on 2026-12-09 09:00** that fires the decision for me — so it can't quietly evaporate in a month of chat scrollback.

**What the month has to produce** or the answer stays "private":
- `TASKS.md` — one line per real task: date, what shipped, who did it, and *"would Codex/Claude Code alone have matched this?"*
- count of **gate catches** (currently 1: `needs-auth` on #103)
- count of tasks where **harness switching actually mattered** — not just was possible
- anything you abandoned Kurisu for, and why

**My bar, written down so it's falsifiable:** ≥10 logged tasks, ≥3 gate catches, and at least one task where someone other than you got value. Below that, it's a private tool and there's nothing wrong with that.

One rule I added for the month: **stop arguing about uniqueness.** It's settled — distinctive shape, not distinctive capability. The only question left is whether it makes your actual work better, and that's an evidence question, not a debate.

Also: `kurisu-dev` is still finishing the binary-level acceptance on #105;"#);
        head.push(' ');
        assert_eq!(head.chars().count(), 1246, "mirrors the live log's pre-row stream");
        let tail = "nothing merged or deployed.";
        let full = format!("{head}{tail}");
        // The delta that lost the race with the item poll: a strict suffix of
        // the row that had already been relayed.
        let late = " merged or deployed.";
        let events = vec![
            (0u64, json!({"type": "delta", "session": "SID", "kind": "text", "text": head, "message_id": "m1"})),
            (2600, json!({"type": "delta", "session": "SID", "kind": "text", "text": late, "message_id": "m1"})),
        ];
        let (bubbles, calls) = scripted_turn(events, vec![&full], 1600).await;
        assert_eq!(bubbles.len(), 1, "reply was fragmented: {bubbles:?}");
        assert_eq!(bubbles[0], full,
                   "a lagging delta was re-appended after the finished row: {bubbles:?} calls={calls:?}");
    }
}
