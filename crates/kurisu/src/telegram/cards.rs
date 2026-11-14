//! Broadcasts of final replies, approval cards and question cards (Python bridge).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use serde_json::{json, Value};

use super::backend::item_text;
use super::{clen, log, now_i, squash, take, token, vstr, Bridge, Ctx};
use super::bridge::audio_name;

const BROADCAST_POLL_S: u64 = 3;
const QUESTION_SECRET_MASK: &str = "••••";

fn title60(s: &Value) -> String {
    let t = squash(s["title"].as_str().filter(|t| !t.is_empty()).unwrap_or("(untitled)"));
    if clen(&t) > 60 { format!("{}…", take(&t, 59)) } else { t }
}

pub fn broadcast_prefix(session: &Value) -> String {
    format!("📡 {} · {}", title60(session), take(session["id"].as_str().unwrap_or("?"), 8))
}

/// The last assistant message of each turn (a turn ends at the next user message).
pub fn broadcast_final_messages(items: &[Value]) -> Vec<(Value, String)> {
    let mut turns = vec![];
    let mut current: Option<(Value, String)> = None;
    for item in items {
        if item["type"] == "message" && item["role"] == "user" {
            if let Some(c) = current.take() {
                turns.push(c);
            }
            continue;
        }
        if item["type"] != "message" || item["role"] != "assistant" {
            continue;
        }
        let text = item_text(item);
        if !text.is_empty() {
            current = Some((item.clone(), text));
        }
    }
    if let Some(c) = current {
        turns.push(c);
    }
    turns
}

pub fn approval_prompt_text(session: &Value, event: &Value) -> (String, String) {
    let params = &event["params"];
    let message = squash(params["message"].as_str().filter(|m| !m.is_empty()).unwrap_or("Approval required"));
    let preview = squash(params["content_preview"].as_str().unwrap_or(""));
    let policy = params["policy_name"].as_str().unwrap_or("");
    let sid = params["target_session_id"].as_str().filter(|s| !s.is_empty()).or(session["id"].as_str()).unwrap_or("?").to_string();
    let mut lines = vec!["🔐 Approval required".to_string(), format!("📡 {} · {}", title60(session), take(&sid, 8)), String::new(), message];
    if !policy.is_empty() {
        lines.extend([String::new(), format!("Policy: {policy}")]);
    }
    if !preview.is_empty() {
        lines.extend([String::new(), take(&preview, 1200)]);
    }
    let expiry = expiry_line(event_expires_at(event));
    if !expiry.is_empty() {
        lines.extend([String::new(), expiry]);
    }
    (take(&lines.join("\n"), 3600), sid)
}

/// When the request expires (Kurisu's expires_at), if the backend says.
fn event_expires_at(event: &Value) -> Option<f64> {
    event["expires_at"].as_f64().filter(|v| *v > 0.0).or(event["params"]["expires_at"].as_f64().filter(|v| *v > 0.0))
}

fn countdown_text(seconds: f64) -> String {
    let s = seconds.max(0.0) as i64;
    if s < 90 {
        format!("{s}s")
    } else if s < 5400 {
        format!("{} min", (s as f64 / 60.0).round_ties_even() as i64)
    } else {
        format!("{}h {:02}m", s / 3600, s % 3600 / 60)
    }
}

fn expiry_line(at: Option<f64>) -> String {
    let Some(at) = at else { return String::new() };
    let hm = chrono::DateTime::from_timestamp(at as i64, 0).map(|d| d.with_timezone(&chrono::Local).format("%H:%M").to_string()).unwrap_or_default();
    format!("⌛ Expires at {hm} (in {})", countdown_text(at - super::now_f()))
}

// What happened to a request that is no longer pending (shown on its card).
const GONE_EXPIRED: &str = "⌛ Expired — the session moved on.";
const GONE_ELSEWHERE: &str = "✔️ Answered in another client.";
const GONE_UNKNOWN: &str = "⌛ No longer pending — it expired or was answered elsewhere.";

fn schema_questions(schema: &Value) -> Option<Vec<Value>> {
    let props = schema["properties"].as_object()?;
    let required: HashSet<String> = schema["required"].as_array().into_iter().flatten().filter_map(|v| v.as_str().map(String::from)).collect();
    let mut qs = vec![];
    let mut has_options = false;
    for (name, prop) in props {
        if !prop.is_object() {
            return None;
        }
        let text = prop["description"].as_str().filter(|s| !s.is_empty()).or(prop["title"].as_str().filter(|s| !s.is_empty())).unwrap_or(name);
        let mut q = json!({"key": name, "question": text, "header": if prop["description"].as_str().is_some_and(|d| !d.is_empty()) { prop["title"].clone() } else { Value::Null },
                           "options": [], "multi": false, "other": false, "secret": false, "optional": !required.contains(name)});
        let kind = prop["type"].as_str().unwrap_or("");
        if kind == "string" && prop["enum"].is_array() {
            let names = prop["enumNames"].as_array().cloned().unwrap_or_default();
            q["options"] = json!(prop["enum"].as_array().unwrap().iter().enumerate().map(|(i, v)| json!({"label": names.get(i).map(vstr).unwrap_or_else(|| vstr(v)), "value": v})).collect::<Vec<_>>());
        } else if kind == "string" && prop["oneOf"].is_array() {
            q["options"] = json!(prop["oneOf"].as_array().unwrap().iter().filter(|o| o.get("const").is_some())
                .map(|o| json!({"label": o["title"].as_str().filter(|t| !t.is_empty()).map(String::from).unwrap_or_else(|| vstr(&o["const"])), "value": o["const"]})).collect::<Vec<_>>());
        } else if kind == "boolean" {
            q["options"] = json!([{"label": "Yes", "value": true}, {"label": "No", "value": false}]);
        } else if kind == "array" && prop["items"]["enum"].is_array() {
            q["options"] = json!(prop["items"]["enum"].as_array().unwrap().iter().map(|v| json!({"label": vstr(v), "value": v})).collect::<Vec<_>>());
            q["multi"] = json!(true);
        } else if kind == "string" {
            q["other"] = json!(true);
        } else {
            return None;
        }
        has_options |= q["options"].as_array().is_some_and(|o| !o.is_empty());
        qs.push(q);
    }
    has_options.then_some(qs)
}

/// Normalized questions for a question elicitation, or None for a plain approval.
pub fn question_spec(event: &Value) -> Option<Vec<Value>> {
    let params = &event["params"];
    if let Some(raw_qs) = params["ask_user_question"]["questions"].as_array() {
        let mut qs = vec![];
        for raw in raw_qs {
            let Some(q) = raw["question"].as_str().filter(|q| !q.is_empty()) else { continue };
            let mut options = vec![];
            for opt in raw["options"].as_array().into_iter().flatten() {
                let label = opt["label"].as_str().or(opt.as_str()).unwrap_or("");
                if label.is_empty() {
                    continue;
                }
                let mut desc = opt["description"].as_str().map(str::trim).unwrap_or("").to_string();
                if desc.to_lowercase() == label.trim().to_lowercase() {
                    desc.clear(); // models often repeat the label as its description
                }
                options.push(json!({"label": label, "description": desc}));
            }
            let other = raw["isOther"] != json!(false) || options.is_empty();
            qs.push(json!({
                "key": raw["id"].as_str().filter(|i| !i.is_empty()).unwrap_or(q),
                "question": q, "header": raw["header"].as_str(),
                "options": options, "multi": raw["multiSelect"] == json!(true), "other": other,
                "secret": raw["isSecret"] == json!(true), "optional": false,
            }));
        }
        return if qs.is_empty() { None } else { Some(qs) };
    }
    if params["mode"] == "form" {
        return schema_questions(&params["requestedSchema"]);
    }
    None
}

fn question_header(card: &Value) -> String {
    format!("❓ Question from {} · {}", title60(card), take(card["session_id"].as_str().unwrap_or(""), 8))
}

fn opt_value(o: &Value) -> Value {
    o.get("value").cloned().unwrap_or_else(|| o["label"].clone())
}

fn answer_display(q: &Value, value: &Value) -> String {
    if q["secret"] == json!(true) {
        return QUESTION_SECRET_MASK.into();
    }
    let opts = q["options"].as_array().cloned().unwrap_or_default();
    let label_of = |v: &Value| opts.iter().find(|o| opt_value(o) == *v).and_then(|o| o["label"].as_str().map(String::from)).unwrap_or_else(|| if v.is_null() { "None".into() } else { vstr(v) });
    let values: Vec<Value> = match value {
        Value::Array(a) => a.clone(),
        other => vec![other.clone()],
    };
    let s = values.iter().map(label_of).collect::<Vec<_>>().join(", ");
    if s.is_empty() { "(none)".into() } else { s }
}

/// Text + inline keyboard for the card's current question.
pub fn question_card_view(card: &Value) -> (String, Value) {
    let qs = card["questions"].as_array().cloned().unwrap_or_default();
    let index = card["index"].as_u64().unwrap_or(0) as usize;
    let q = &qs[index.min(qs.len().saturating_sub(1))];
    let mut lines = vec![question_header(card)];
    if let Some(m) = card["message"].as_str().filter(|m| !m.is_empty()) {
        lines.push(take(m, 300));
    }
    let expiry = expiry_line(card["expires_at"].as_f64().filter(|v| *v > 0.0));
    if !expiry.is_empty() {
        lines.push(expiry);
    }
    for done in &qs[..index.min(qs.len())] {
        let key = done["key"].as_str().unwrap_or("");
        lines.push(format!("✔ {} → {}", take(done["question"].as_str().unwrap_or(""), 80), answer_display(done, &card["answers"][key])));
    }
    lines.push(String::new());
    let mut label = q["header"].as_str().unwrap_or("").to_string();
    if qs.len() > 1 {
        label = format!("{label} ({}/{})", index + 1, qs.len()).trim().to_string();
    }
    if !label.is_empty() {
        lines.push(label);
    }
    lines.push(take(q["question"].as_str().unwrap_or(""), 1500));
    let opts = q["options"].as_array().cloned().unwrap_or_default();
    let described: Vec<&Value> = opts.iter().filter(|o| o["description"].as_str().is_some_and(|d| !d.is_empty())).collect();
    if !described.is_empty() {
        lines.push(String::new());
        for o in described {
            lines.push(take(&format!("• {} — {}", vstr(&o["label"]), vstr(&o["description"])), 300));
        }
    }
    lines.push(String::new());
    let multi = q["multi"] == json!(true);
    if multi {
        lines.push("Pick one or more, then tap ✅ Submit.".into());
    }
    if q["other"] == json!(true) {
        lines.push(format!("✍️ {}", if opts.is_empty() { "Type your answer as your next message." } else { "…or type your own answer as your next message." }));
        if q["secret"] == json!(true) {
            lines.push("⚠️ This answer is marked secret; Telegram keeps it in your chat history.".into());
        }
    }
    let tok = card["token"].as_str().unwrap_or("");
    let selected: HashSet<u64> = card["selected"].as_array().into_iter().flatten().filter_map(Value::as_u64).collect();
    let mut buttons = vec![];
    for (i, o) in opts.iter().enumerate() {
        let mut text = vstr(&o["label"]);
        let desc = o["description"].as_str().unwrap_or("");
        if !desc.is_empty() && clen(&text) + clen(desc) <= 45 {
            text = format!("{text} — {desc}");
        }
        if multi {
            text = format!("{}{text}", if selected.contains(&(i as u64)) { "☑️ " } else { "⬜ " });
        }
        buttons.push(json!([{"text": take(&text, 64), "callback_data": format!("q:{tok}:{index}:o:{i}")}]));
    }
    let mut tail = vec![];
    if multi {
        tail.push(json!({"text": "✅ Submit", "callback_data": format!("q:{tok}:{index}:s")}));
    } else if q["optional"] == json!(true) {
        tail.push(json!({"text": "⏭ Skip", "callback_data": format!("q:{tok}:{index}:s")}));
    }
    tail.push(json!({"text": "✖ Dismiss", "callback_data": format!("q:{tok}:{index}:x")}));
    buttons.push(json!(tail));
    (take(&lines.join("\n"), 3900), json!({"inline_keyboard": buttons}))
}

fn question_final_text(card: &Value, outcome: &str) -> String {
    let mut lines = vec![question_header(card)];
    for q in card["questions"].as_array().into_iter().flatten() {
        let key = q["key"].as_str().unwrap_or("");
        let text = take(q["question"].as_str().unwrap_or(""), 200);
        match card["answers"].get(key) {
            Some(a) => lines.push(format!("• {text} → {}", answer_display(q, a))),
            None => lines.push(format!("• {text}")),
        }
    }
    lines.extend([String::new(), outcome.to_string()]);
    take(&lines.join("\n"), 3900)
}

impl Bridge {
    /// Card text for a request that is no longer pending.
    async fn gone_outcome(&self, eid: &str) -> &'static str {
        let rows = match self.k(json!({"op": "approvals", "all": true})).await {
            Ok(r) => r,
            Err(e) => {
                log(format!("approval lookup failed: {e}"));
                return GONE_UNKNOWN;
            }
        };
        let Some(rec) = rows.as_array().into_iter().flatten().find(|a| a["id"] == eid) else { return GONE_UNKNOWN };
        match rec["status"].as_str().unwrap_or("") {
            "expired" | "cancelled" => GONE_EXPIRED,
            st @ ("approved" | "denied") => {
                if rec["decided_by"].as_str().unwrap_or("").starts_with("rule") {
                    return if st == "approved" { "✔️ Allowed by a rule." } else { "✔️ Denied by a rule." };
                }
                if st == "approved" || rec["kind"] == "question" { GONE_ELSEWHERE } else { "✖ Denied in another client." }
            }
            _ => GONE_UNKNOWN,
        }
    }

    /// Whether the request still waits for an answer; None if that can't be checked.
    async fn elicitation_pending(&self, sid: &str, eid: &str) -> Option<bool> {
        match self.session_snapshot(sid).await {
            Ok(s) => Some(s["pending_elicitations"].as_array().into_iter().flatten().any(|e| e["elicitation_id"] == eid)),
            Err(e) => {
                log(format!("pending check failed for {eid}: {e}"));
                None
            }
        }
    }

    async fn close_approval_card(&self, key: &str, outcome: &str) {
        let Some(card) = self.cards_remove("approval_cards", key) else { return };
        self.save_state();
        let text = format!("{}\n\n{outcome}", card["text"].as_str().filter(|t| !t.is_empty()).unwrap_or("Approval required"));
        if let Err(e) = self.tg1("editMessageText", json!({"chat_id": card["chat_id"], "message_id": card["message_id"], "text": text,
                                                         "disable_web_page_preview": true, "reply_markup": {"inline_keyboard": []}})).await {
            log(format!("approval card close failed: {e}"));
        }
    }

    async fn alert_callback(&self, cid: &Value, text: &str) {
        if let Err(e) = self.tg1("answerCallbackQuery", json!({"callback_query_id": cid, "text": take(text, 180), "show_alert": true})).await {
            log(format!("callback alert failed: {e}"));
        }
    }

    /// Edit a remote-access card into its outcome (buttons: none, or a Close button).
    pub(crate) async fn close_pair_card(&self, attempt: &str, outcome: &str, buttons: Option<Value>) {
        let card = if buttons.is_some() { self.cards_get("pair_cards", attempt) } else { self.cards_remove("pair_cards", attempt) };
        let Some(card) = card else { return };
        self.save_state();
        let text = format!("{}\n\n{outcome}", card["text"].as_str().unwrap_or("Access request"));
        if let Err(e) = self.tg1("editMessageText", json!({"chat_id": card["chat_id"], "message_id": card["message_id"], "text": text,
                                                         "disable_web_page_preview": true, "reply_markup": buttons.unwrap_or(json!({"inline_keyboard": []}))})).await {
            log(format!("access card edit failed: {e}"));
        }
    }

    /// "Enable the remote terminal?" / "Set up Live Browser?" card (owner approves).
    pub(crate) async fn post_enable_card(&self, kind: &str, place: &str, lb_installed: bool) -> Result<()> {
        let expires = now_i() + 120;
        let text = if kind == "browser" {
            format!("🌐 Set up the Live Browser?\nKurisu will {}check for Chrome, give every session a shared browser, and serve the live view on {place}. \
Each link still needs your approval here.\n{}", if lb_installed { "" } else { "download Live Browser (lb, about 28 MB), " }, expiry_line(Some(expires as f64)))
        } else if kind == "web" {
            format!("💬 Enable the web chat?\nIt will listen on {place}: your sessions, their history and a box to reply. It still needs your approval here for every link.\n{}", expiry_line(Some(expires as f64)))
        } else {
            format!("🖥 Enable the remote terminal?\nIt will listen on {place}. Each link still needs your approval here.\n{}", expiry_line(Some(expires as f64)))
        };
        let tok = token(12);
        let markup = json!({"inline_keyboard": [[{"text": "✅ Approve", "callback_data": format!("en:{tok}:a")}, {"text": "❌ Deny", "callback_data": format!("en:{tok}:d")}]]});
        let dm = self.dm();
        let mid = self.send_markup(&dm, &self.chat_id(), &text, Some(markup)).await?.ok_or_else(|| anyhow!("Telegram returned no message id"))?;
        self.cards_put("enable_cards", &tok, json!({"kind": kind, "chat_id": self.chat_id(), "message_id": mid, "text": text, "expires_at": expires}));
        self.save_state();
        Ok(())
    }

    async fn close_enable_card(&self, tok: &str, outcome: &str) {
        let Some(card) = self.cards_remove("enable_cards", tok) else { return };
        self.save_state();
        let text = format!("{}\n\n{outcome}", card["text"].as_str().unwrap_or(""));
        if let Err(e) = self.tg1("editMessageText", json!({"chat_id": card["chat_id"], "message_id": card["message_id"], "text": text,
                                                         "disable_web_page_preview": true, "reply_markup": {"inline_keyboard": []}})).await {
            log(format!("enable card edit failed: {e}"));
        }
    }

    /// en:<token>:a|d
    pub(crate) async fn handle_enable_callback(&self, ctx: &Ctx, cid: &Value, data: &str) -> Result<()> {
        if !self.is_owner_dm(&ctx.chat_id) {
            self.answer_callback(cid, Some("Only the owner's DM can approve this")).await;
            return Ok(());
        }
        let parts: Vec<&str> = data.splitn(3, ':').collect();
        let (tok, action) = (parts.get(1).copied().unwrap_or(""), parts.get(2).copied().unwrap_or(""));
        let Some(card) = self.cards_get("enable_cards", tok) else {
            self.alert_callback(cid, "⌛ This request already expired.").await;
            return Ok(());
        };
        let kind = card["kind"].as_str().unwrap_or("terminal").to_string();
        if card["expires_at"].as_i64().unwrap_or(0) < now_i() {
            self.close_enable_card(tok, "⌛ Expired — nothing was changed.").await;
            self.alert_callback(cid, "⌛ This request already expired.").await;
            return Ok(());
        }
        if action != "a" {
            let _ = self.k(json!({"op": "remote_declined", "kind": kind})).await;
            self.answer_callback(cid, Some("Denied")).await;
            self.close_enable_card(tok, "❌ Not enabled.").await;
            return Ok(());
        }
        self.answer_callback(cid, Some(if kind == "browser" { "Setting up…" } else { "Enabling…" })).await;
        let _ = self.tg1("editMessageText", json!({"chat_id": card["chat_id"], "message_id": card["message_id"],
                                                    "text": format!("{}\n\n⏳ {}", card["text"].as_str().unwrap_or(""), if kind == "browser" { "Setting up…" } else { "Enabling…" }),
                                                    "reply_markup": {"inline_keyboard": []}})).await;
        match self.k(json!({"op": "remote_enable", "kind": kind})).await {
            Ok(v) => {
                self.close_enable_card(tok, &format!("✅ Enabled — {}", v["summary"].as_str().unwrap_or("on"))).await;
                self.send_remote_link(ctx, &kind).await?;
            }
            Err(e) => {
                // Only here may the text point at the computer (enabling failed there).
                self.close_enable_card(tok, &format!("⚠️ Could not enable: {}", e.to_string().trim_start_matches("HTTP 400: "))).await;
            }
        }
        Ok(())
    }

    /// pair:<attempt>:a|d|x (approve, deny, close an approved session).
    pub(crate) async fn handle_pair_callback(&self, ctx: &Ctx, cid: &Value, data: &str) -> Result<()> {
        if !self.is_owner_dm(&ctx.chat_id) {
            self.answer_callback(cid, Some("Only the owner's DM can approve access")).await;
            return Ok(());
        }
        let parts: Vec<&str> = data.splitn(3, ':').collect();
        let (attempt, action) = (parts.get(1).copied().unwrap_or(""), parts.get(2).copied().unwrap_or(""));
        let kind = self.cards_get("pair_cards", attempt).and_then(|c| c["kind"].as_str().map(String::from)).unwrap_or_else(|| "terminal".into());
        let what = match kind.as_str() { "browser" => "browser", "web" => "web chat", _ => "terminal" };
        match action {
            "a" | "d" => match self.k(json!({"op": "remote_decide", "attempt": attempt, "approve": action == "a"})).await {
                Err(_) => {
                    self.close_pair_card(attempt, "⌛ Expired — the link is dead.", None).await;
                    self.alert_callback(cid, "⌛ This request already expired.").await;
                }
                Ok(_) if action == "a" => {
                    self.answer_callback(cid, Some("Approved")).await;
                    self.close_pair_card(attempt, &format!("✅ Approved — the {what} is open."),
                        Some(json!({"inline_keyboard": [[{"text": format!("⏹ Close {what}"), "callback_data": format!("pair:{attempt}:x")}]]}))).await;
                }
                Ok(_) => {
                    self.answer_callback(cid, Some("Denied")).await;
                    self.close_pair_card(attempt, "❌ Denied — the link is dead.", None).await;
                }
            },
            "x" => {
                let closed = self.k(json!({"op": "remote_close_attempt", "attempt": attempt})).await.map(|v| v["closed"] == json!(true)).unwrap_or(false);
                self.answer_callback(cid, Some(if closed { "Closed" } else { "Already closed" })).await;
                self.close_pair_card(attempt, &format!("⏹ Closed from Telegram."), None).await;
            }
            _ => self.answer_callback(cid, Some("Unknown action")).await,
        }
        Ok(())
    }

    /// Outward MCP sign-in card: moa:<id>:a / moa:<id>:d. Owner DM only.
    pub(crate) async fn handle_oauth_callback(&self, ctx: &Ctx, cid: &Value, data: &str) -> Result<()> {
        if !self.is_owner_dm(&ctx.chat_id) {
            self.answer_callback(cid, Some("Only the owner's DM can approve a sign-in")).await;
            return Ok(());
        }
        let parts: Vec<&str> = data.splitn(3, ':').collect();
        let (id, action) = (parts.get(1).copied().unwrap_or(""), parts.get(2).copied().unwrap_or(""));
        if action != "a" && action != "d" {
            self.answer_callback(cid, Some("Unknown action")).await;
            return Ok(());
        }
        let outcome = match self.k(json!({"op": "mcp_oauth_decide", "id": id, "approve": action == "a"})).await {
            Err(e) => {
                self.alert_callback(cid, &format!("⌛ {e}")).await;
                format!("⌛ {e}")
            }
            Ok(_) if action == "a" => {
                self.answer_callback(cid, Some("Approved")).await;
                "✅ Approved — the browser hands the sign-in back to the client.".to_string()
            }
            Ok(_) => {
                self.answer_callback(cid, Some("Denied")).await;
                "❌ Denied.".to_string()
            }
        };
        if let Some(card) = self.cards_remove("oauth_cards", id) {
            self.save_state();
            let text = format!("{}\n\n{outcome}", card["text"].as_str().unwrap_or("MCP sign-in request"));
            if let Err(e) = self.tg1("editMessageText", json!({"chat_id": card["chat_id"], "message_id": card["message_id"], "text": text,
                                                             "disable_web_page_preview": true, "reply_markup": {"inline_keyboard": []}})).await {
                log(format!("sign-in card edit failed: {e}"));
            }
        }
        Ok(())
    }

    fn cards_get(&self, kind: &str, key: &str) -> Option<Value> {
        self.with_state(|st| st.get(kind).and_then(|c| c.get(key)).cloned())
    }

    fn cards_put(&self, kind: &str, key: &str, card: Value) {
        self.with_state(|st| {
            let c = st.entry(kind).or_insert(json!({}));
            if !c.is_object() {
                *c = json!({});
            }
            c[key] = card;
        });
    }

    fn cards_remove(&self, kind: &str, key: &str) -> Option<Value> {
        self.with_state(|st| st.get_mut(kind).and_then(|c| c.as_object_mut()).and_then(|c| c.remove(key)))
    }

    fn cards_keys(&self, kind: &str) -> Vec<(String, Value)> {
        self.with_state(|st| st.get(kind).and_then(|c| c.as_object()).map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()).unwrap_or_default())
    }

    async fn close_question_card(&self, key: &str, outcome: &str) {
        let Some(card) = self.cards_remove("question_cards", key) else { return };
        self.save_state();
        if let Err(e) = self.tg1("editMessageText", json!({"chat_id": card["chat_id"], "message_id": card["message_id"], "text": question_final_text(&card, outcome),
                                                         "disable_web_page_preview": true, "reply_markup": {"inline_keyboard": []}})).await {
            log(format!("question card edit failed: {e}"));
        }
    }

    async fn refresh_question_card(&self, card: &Value) {
        let (text, markup) = question_card_view(card);
        if let Err(e) = self.tg1("editMessageText", json!({"chat_id": card["chat_id"], "message_id": card["message_id"], "text": text,
                                                         "disable_web_page_preview": true, "reply_markup": markup})).await {
            if !e.to_string().contains("not modified") {
                log(format!("question card refresh failed: {e}"));
            }
        }
    }

    async fn post_question_card(&self, session: &Value, event: &Value, questions: Vec<Value>) -> Result<Value> {
        let params = &event["params"];
        let sid = params["target_session_id"].as_str().filter(|s| !s.is_empty()).or(session["id"].as_str()).unwrap_or("").to_string();
        let mut message = squash(params["message"].as_str().unwrap_or(""));
        if !params["ask_user_question"].is_null() {
            message.clear(); // "X wants to call AskUserQuestion" adds nothing
        }
        let mut card = json!({"token": token(12), "session_id": sid, "elicitation_id": event["elicitation_id"], "title": session["title"],
                              "message": message, "questions": questions, "index": 0, "answers": {}, "selected": [],
                              "chat_id": self.chat_id(), "created_at": now_i(), "expires_at": event_expires_at(event)});
        let (text, markup) = question_card_view(&card);
        let dm = self.dm();
        let mid = self.send_markup(&dm, &self.chat_id(), &text, Some(markup)).await?.ok_or_else(|| anyhow!("Telegram returned no message id"))?;
        card["message_id"] = json!(mid);
        Ok(card)
    }

    async fn finish_question(&self, key: &str, card: &Value) -> &'static str {
        if let Err(e) = self.resolve_elicitation(card["elicitation_id"].as_str().unwrap_or(""), "accept", Some(card["answers"].clone())).await {
            log(format!("question resolve failed for {}: {e}", vstr(&card["elicitation_id"])));
            self.close_question_card(key, &format!("⚠️ Could not send the answer ({}). It may have been answered elsewhere.", take(&e.to_string(), 120))).await;
            return "Answer failed";
        }
        log(format!("question answered {} for {}", vstr(&card["elicitation_id"]), take(card["session_id"].as_str().unwrap_or(""), 8)));
        self.close_question_card(key, "✅ Answered from Telegram").await;
        "Answered"
    }

    /// Store an answer for the current question; advance or resolve. Returns toast text.
    async fn record_answer(&self, key: &str, mut card: Value, value: Option<Value>) -> &'static str {
        let index = card["index"].as_u64().unwrap_or(0) as usize;
        let qkey = card["questions"][index]["key"].as_str().unwrap_or("").to_string();
        if let Some(v) = value {
            card["answers"][&qkey] = v;
        }
        card["selected"] = json!([]);
        card["index"] = json!(index + 1);
        if index + 1 >= card["questions"].as_array().map(|q| q.len()).unwrap_or(0) {
            self.cards_put("question_cards", key, card.clone());
            return self.finish_question(key, &card).await;
        }
        self.cards_put("question_cards", key, card.clone());
        self.save_state();
        self.refresh_question_card(&card).await;
        "Next question"
    }

    pub(crate) async fn handle_question_callback(&self, ctx: &Ctx, cid: &Value, data: &str) -> Result<()> {
        if !self.is_owner_dm(&ctx.chat_id) {
            self.answer_callback(cid, Some("Questions can only be answered from the owner's DM")).await;
            return Ok(());
        }
        let parts: Vec<&str> = data.split(':').collect();
        let _g = self.question_lock.lock().await;
        let found = if parts.len() >= 4 { self.cards_keys("question_cards").into_iter().find(|(_, c)| c["token"] == parts[1]) } else { None };
        let valid = found.as_ref().and_then(|(k, c)| {
            let index = c["index"].as_u64()?;
            if parts[2].parse::<u64>().ok()? != index {
                return None;
            }
            let q = c["questions"].get(index as usize)?.clone();
            let action = parts[3];
            let option = if action == "o" {
                let o: usize = parts.get(4)?.parse().ok()?;
                if o >= q["options"].as_array()?.len() {
                    return None;
                }
                Some(o)
            } else {
                None
            };
            Some((k.clone(), c.clone(), q, action.to_string(), option))
        });
        if let Some((key, card)) = &found {
            let sid = card["session_id"].as_str().unwrap_or("").to_string();
            let eid = card["elicitation_id"].as_str().unwrap_or("").to_string();
            if self.elicitation_pending(&sid, &eid).await == Some(false) {
                let outcome = self.gone_outcome(&eid).await;
                self.close_question_card(key, outcome).await;
                self.alert_callback(cid, if outcome != GONE_ELSEWHERE { "⌛ This question already expired." } else { "This question was already answered elsewhere." }).await;
                return Ok(());
            }
        }
        let Some((key, mut card, q, action, option)) = valid else {
            self.answer_callback(cid, Some("This question is no longer pending")).await;
            return Ok(());
        };
        let multi = q["multi"] == json!(true);
        match action.as_str() {
            "x" => {
                if let Err(e) = self.resolve_elicitation(card["elicitation_id"].as_str().unwrap_or(""), "decline", None).await {
                    log(format!("question dismiss failed for {}: {e}", vstr(&card["elicitation_id"])));
                }
                self.close_question_card(&key, "✖ Dismissed").await;
                self.answer_callback(cid, Some("Dismissed")).await;
            }
            "o" if multi => {
                let o = option.unwrap() as u64;
                let mut sel: Vec<u64> = card["selected"].as_array().into_iter().flatten().filter_map(Value::as_u64).collect();
                let now_on = if let Some(p) = sel.iter().position(|x| *x == o) { sel.remove(p); false } else { sel.push(o); true };
                sel.sort();
                card["selected"] = json!(sel);
                self.cards_put("question_cards", &key, card.clone());
                self.save_state();
                self.refresh_question_card(&card).await;
                self.answer_callback(cid, Some(if now_on { "Selected" } else { "Unselected" })).await;
            }
            "o" => {
                let v = opt_value(&q["options"][option.unwrap()]);
                let toast = self.record_answer(&key, card, Some(v)).await;
                self.answer_callback(cid, Some(toast)).await;
            }
            "s" => {
                if multi {
                    let values: Vec<Value> = card["selected"].as_array().into_iter().flatten().filter_map(Value::as_u64).map(|i| opt_value(&q["options"][i as usize])).collect();
                    if values.is_empty() && q["optional"] != json!(true) {
                        self.answer_callback(cid, Some("Pick at least one option, or type your own answer")).await;
                        return Ok(());
                    }
                    let toast = self.record_answer(&key, card, if values.is_empty() { None } else { Some(json!(values)) }).await;
                    self.answer_callback(cid, Some(toast)).await;
                } else {
                    let toast = self.record_answer(&key, card, None).await;
                    self.answer_callback(cid, Some(toast)).await;
                }
            }
            _ => self.answer_callback(cid, Some("Unknown action")).await,
        }
        Ok(())
    }

    /// An owner-DM question card is waiting for a typed (or spoken) answer.
    pub(crate) fn text_question_pending(&self, ctx: &Ctx) -> bool {
        self.is_owner_dm(&ctx.chat_id) && self.cards_keys("question_cards").iter().any(|(_, c)| {
            let i = c["index"].as_u64().unwrap_or(0) as usize;
            vstr(&c["chat_id"]) == vstr(&ctx.chat_id) && c["questions"][i]["other"] == json!(true)
        })
    }

    /// A voice note as the answer to a pending question: its transcript, if speech-to-text
    /// is set up and heard something. True if consumed.
    pub(crate) async fn answer_question_with_voice(&self, ctx: &Ctx, message: &Value, att: &Value) -> bool {
        let Some(stt) = self.stt_config().await else { return false };
        let heard = async {
            let (data, file_path) = self.download_file(att["file_id"].as_str().unwrap_or("")).await?;
            let name = att["name"].as_str().filter(|n| !n.is_empty()).unwrap_or(&file_path).to_string();
            let path = self.save_upload(&audio_name(&name, att["mime"].as_str().unwrap_or("")), &data)?;
            crate::stt::transcribe(&stt, &path).await
        }.await;
        match heard {
            Ok(t) if !t.trim().is_empty() => {
                let _ = self.say(ctx, &format!("🎙 Heard: {}", t.trim())).await;
                self.answer_question_with_text(ctx, message, t.trim()).await
            }
            Ok(_) => false,
            Err(e) => {
                log(format!("voice answer: stt failed: {e:#}"));
                false
            }
        }
    }

    /// A plain DM message as the free-text answer to a pending question. True if consumed.
    pub(crate) async fn answer_question_with_text(&self, ctx: &Ctx, message: &Value, text: &str) -> bool {
        if !self.is_owner_dm(&ctx.chat_id) {
            return false;
        }
        let result = {
            let _g = self.question_lock.lock().await;
            let candidates: Vec<(String, Value)> = self.cards_keys("question_cards").into_iter().filter(|(_, c)| {
                let i = c["index"].as_u64().unwrap_or(0) as usize;
                vstr(&c["chat_id"]) == vstr(&ctx.chat_id) && c["questions"][i]["other"] == json!(true)
            }).collect();
            if candidates.is_empty() {
                return false;
            }
            let replied = message["reply_to_message"]["message_id"].as_i64();
            let chosen = candidates.iter().find(|(_, c)| replied.is_some() && c["message_id"].as_i64() == replied).cloned()
                .unwrap_or_else(|| candidates.iter().max_by_key(|(_, c)| c["created_at"].as_i64().unwrap_or(0)).cloned().unwrap());
            let (key, card) = chosen;
            let i = card["index"].as_u64().unwrap_or(0) as usize;
            let q = card["questions"][i].clone();
            let mut value = json!(text.trim());
            if q["multi"] == json!(true) {
                let mut vals: Vec<Value> = card["selected"].as_array().into_iter().flatten().filter_map(Value::as_u64).map(|x| opt_value(&q["options"][x as usize])).collect();
                vals.push(value);
                value = json!(vals);
            }
            self.record_answer(&key, card, Some(value)).await
        };
        let ack = match result {
            "Next question" => Some("✍️ Answer recorded; next question is on the card."),
            "Answered" => None,
            _ => Some("⚠️ Your answer could not be delivered; the question may have closed."),
        };
        if let Some(a) = ack {
            if let Err(e) = self.send_text(ctx, &ctx.chat_id, a).await {
                log(format!("question text ack failed: {e}"));
            }
        }
        true
    }

    pub(crate) async fn handle_approval_callback(&self, ctx: &Ctx, cid: &Value, data: &str) -> Result<()> {
        let parts: Vec<&str> = data.splitn(3, ':').collect();
        let tok = parts.get(1).copied().unwrap_or("");
        let action = match parts.get(2).copied() {
            Some("a") => Some("accept"),
            Some("d") => Some("decline"),
            Some("y") => Some("accept_session"),
            _ => None,
        };
        let found = self.cards_keys("approval_cards").into_iter().find(|(_, c)| c["token"] == tok);
        let (Some(action), Some((key, card))) = (action, found) else {
            self.answer_callback(cid, Some("Approval expired or unknown")).await;
            return Ok(());
        };
        let (sid, eid) = (card["session_id"].as_str().unwrap_or("").to_string(), card["elicitation_id"].as_str().unwrap_or("").to_string());
        if self.elicitation_pending(&sid, &eid).await == Some(false) {
            let outcome = self.gone_outcome(&eid).await;
            self.close_approval_card(&key, outcome).await;
            self.alert_callback(cid, if outcome != GONE_ELSEWHERE { "⌛ This request already expired." } else { "This request was already answered elsewhere." }).await;
            return Ok(());
        }
        if let Err(e) = self.resolve_elicitation(&eid, action, None).await {
            log(format!("approval resolve failed for {eid}: {e}"));
            if self.elicitation_pending(&sid, &eid).await == Some(false) {
                let outcome = self.gone_outcome(&eid).await;
                self.close_approval_card(&key, outcome).await;
                self.alert_callback(cid, "⌛ This request already expired.").await;
                return Ok(());
            }
            self.answer_callback(cid, Some(&take(&format!("Approval failed: {e}"), 180))).await;
            return Ok(());
        }
        self.cards_remove("approval_cards", &key);
        self.save_state();
        let (toast, label) = match action {
            "accept" => ("Approved", "✅ Approved — ▶️ the session continues."),
            "accept_session" => ("Always allowed for this session", "✅ Always allowed for this session — ▶️ the session continues."),
            _ => ("Rejected", "❌ Rejected — the session was told."),
        };
        self.answer_callback(cid, Some(toast)).await;
        let text = card["text"].as_str().filter(|t| !t.is_empty()).unwrap_or("Approval required");
        let chat = if card["chat_id"].is_null() { ctx.chat_id.clone() } else { card["chat_id"].clone() };
        if let Err(e) = self.tg1("editMessageText", json!({"chat_id": chat, "message_id": card["message_id"], "text": format!("{text}\n\n{label}"),
                                                         "disable_web_page_preview": true, "reply_markup": {"inline_keyboard": []}})).await {
            log(format!("approval message edit failed: {e}"));
            self.send_text(ctx, &ctx.chat_id, &format!("{} {} · {}", label.split(" — ").next().unwrap_or(label), take(card["session_id"].as_str().unwrap_or(""), 8), vstr(&card["elicitation_id"]))).await?;
        }
        Ok(())
    }

    pub(crate) async fn approvals_once(&self, sessions: &[Value]) -> bool {
        let mut changed = false;
        let mut pending_keys = HashSet::new();
        let mut complete = true;
        let dm = self.dm();
        for session in sessions {
            let Some(sid) = session["id"].as_str() else { continue };
            if session["pending_elicitations_count"].as_u64().unwrap_or(0) == 0 {
                continue;
            }
            let snapshot = match self.session_snapshot(sid).await {
                Ok(s) => s,
                Err(e) => {
                    log(format!("approval snapshot failed for {}: {e}", take(sid, 8)));
                    complete = false;
                    continue;
                }
            };
            for event in snapshot["pending_elicitations"].as_array().cloned().unwrap_or_default() {
                let Some(eid) = event["elicitation_id"].as_str().filter(|e| !e.is_empty()).map(String::from) else { continue };
                let (text, resolve_sid) = approval_prompt_text(session, &event);
                let key = format!("{resolve_sid}:{eid}");
                pending_keys.insert(key.clone());
                if let Some(qs) = question_spec(&event) {
                    let _g = self.question_lock.lock().await;
                    if self.cards_get("question_cards", &key).is_some() {
                        continue;
                    }
                    match self.post_question_card(session, &event, qs).await {
                        Ok(card) => self.cards_put("question_cards", &key, card),
                        Err(e) => {
                            log(format!("question notification failed for {eid}: {e}"));
                            continue;
                        }
                    }
                    changed = true;
                    log(format!("question notification {eid} from {}", take(&resolve_sid, 8)));
                    continue;
                }
                if self.cards_get("approval_cards", &key).is_some() {
                    continue;
                }
                let tok = token(16);
                let markup = json!({"inline_keyboard": [
                    [{"text": "✅ Approve", "callback_data": format!("approval:{tok}:a")},
                     {"text": "❌ Reject", "callback_data": format!("approval:{tok}:d")}],
                    [{"text": "♾️ Always allow (this session)", "callback_data": format!("approval:{tok}:y")}]
                ]});
                let mid = match self.send_markup(&dm, &self.chat_id(), &text, Some(markup)).await {
                    Ok(Some(m)) => m,
                    Ok(None) => {
                        log(format!("approval notification returned no message id for {eid}"));
                        continue;
                    }
                    Err(e) => {
                        log(format!("approval notification failed for {eid}: {e}"));
                        continue;
                    }
                };
                self.cards_put("approval_cards", &key, json!({"token": tok, "session_id": resolve_sid, "elicitation_id": eid, "chat_id": self.chat_id(),
                                                             "message_id": mid, "text": text, "created_at": now_i()}));
                changed = true;
                log(format!("approval notification {eid} from {}", take(&resolve_sid, 8)));
            }
        }
        if complete {
            // A request answered in another client, auto-denied or expired is no longer
            // pending: close its card at once so it never looks tappable.
            let _g = self.question_lock.lock().await;
            for (key, card) in self.cards_keys("question_cards") {
                if !pending_keys.contains(&key) {
                    log(format!("question {key} no longer pending"));
                    let outcome = self.gone_outcome(card["elicitation_id"].as_str().unwrap_or("")).await;
                    self.close_question_card(&key, outcome).await;
                    changed = true;
                }
            }
            for (tok, card) in self.cards_keys("enable_cards") {
                if card["expires_at"].as_i64().unwrap_or(0) < now_i() {
                    self.close_enable_card(&tok, "⌛ Expired — nothing was changed.").await;
                    changed = true;
                }
            }
            for (key, card) in self.cards_keys("approval_cards") {
                if !pending_keys.contains(&key) {
                    log(format!("approval {key} no longer pending"));
                    let outcome = self.gone_outcome(card["elicitation_id"].as_str().unwrap_or("")).await;
                    self.close_approval_card(&key, outcome).await;
                    changed = true;
                }
            }
        }
        changed
    }

    pub(crate) async fn broadcast_once(&self, sessions: &[Value]) -> Result<bool> {
        let run_started = self.with_state(|st| st.get("broadcast_run_started_at").and_then(Value::as_i64)).unwrap_or_else(now_i);
        let parent_ids: HashSet<String> = sessions.iter().filter_map(|s| s["parent_session_id"].as_str().map(String::from)).collect();
        let mut changed = false;
        let dm = self.dm();
        for session in sessions {
            let Some(sid) = session["id"].as_str() else { continue };
            let cursor = self.with_state(|st| st.get("broadcast_cursors").and_then(|c| c.get(sid)).map(vstr));
            let Some(mut cursor) = cursor else {
                let since = run_started.max(session["created_at"].as_i64().unwrap_or(0));
                self.with_state(|st| {
                    st.entry("broadcast_cursors").or_insert(json!({}))[sid] = json!("__empty__");
                    st.entry("broadcast_since").or_insert(json!({}))[sid] = json!(since);
                });
                changed = true;
                continue;
            };
            if self.is_active(sid) {
                continue;
            }
            let mut status = session["status"].as_str().unwrap_or("").to_string();
            if matches!(status.as_str(), "running" | "starting" | "waiting") && parent_ids.contains(sid) {
                match self.session_snapshot(sid).await {
                    Ok(s) => status = s["status"].as_str().unwrap_or("").to_string(),
                    Err(e) => log(format!("broadcast snapshot failed for {}: {e}", take(sid, 8))),
                }
            }
            if matches!(status.as_str(), "running" | "starting" | "waiting") {
                continue;
            }
            let (items, baseline, started_empty) = if cursor == "__empty__" {
                let raw = self.list_items(sid, None, 100, true).await?;
                let mut raw = raw["data"].as_array().cloned().unwrap_or_default();
                let baseline = raw.first().map(|i| vstr(&i["id"]));
                raw.reverse();
                let since = self.with_state(|st| st.get("broadcast_since").and_then(|c| c.get(sid)).and_then(Value::as_f64)).unwrap_or(0.0);
                (raw.into_iter().filter(|i| i["created_at"].as_f64().unwrap_or(0.0) >= since).collect::<Vec<_>>(), baseline, true)
            } else {
                let mut items = vec![];
                for _ in 0..5 {
                    let page = self.list_items(sid, Some(&cursor), 100, false).await?;
                    let pi = page["data"].as_array().cloned().unwrap_or_default();
                    if let Some(l) = pi.last() {
                        cursor = vstr(&l["id"]);
                    }
                    let more = page["has_more"] == json!(true) && !pi.is_empty();
                    items.extend(pi);
                    if !more {
                        break;
                    }
                }
                (items, None, false)
            };
            if items.is_empty() {
                if let (true, Some(b)) = (started_empty, baseline) {
                    self.with_state(|st| st.entry("broadcast_cursors").or_insert(json!({}))[sid] = json!(b));
                    changed = true;
                }
                continue;
            }
            for (item, text) in broadcast_final_messages(&items) {
                let id = vstr(&item["id"]);
                // Claim before sending, exactly as the turn relay does: of the
                // two paths, only the one that wins this claim sends.
                if !self.claim_relayed(&id) {
                    continue;
                }
                if self.repeated_bubble(sid, &text) {
                    log(format!("suppressed a repeated broadcast bubble for {} (item {id})", take(sid, 8)));
                    changed = true;
                    continue;
                }
                let body = format!("{}\n\n{text}", broadcast_prefix(session));
                if let Err(e) = self.send_text(&dm, &self.chat_id(), &body).await {
                    if super::tg::failed_before_delivery(&e)
                        && super::tg::split_text(&body, super::tg::MAX_TG_CHARS).len() <= 1 {
                        self.release_relayed(&id);
                    }
                    return Err(e);
                }
                self.note_bubble(sid, &text);
                changed = true;
                log(format!("broadcast final item {id} from {}", take(sid, 8)));
            }
            let next = vstr(&items[items.len() - 1]["id"]);
            if !next.is_empty() {
                self.with_state(|st| st.entry("broadcast_cursors").or_insert(json!({}))[sid] = json!(next));
                if started_empty {
                    changed = true;
                }
            }
        }
        Ok(changed)
    }

    pub(crate) async fn broadcast_loop(self: Arc<Self>) {
        self.with_state(|st| st.insert("broadcast_run_started_at".into(), json!(now_i())));
        log("broadcast watcher started");
        loop {
            let r: Result<()> = async {
                let sessions = self.fetch_sessions().await?;
                let mut changed = self.broadcast_once(&sessions).await?;
                changed = self.approvals_once(&sessions).await || changed;
                if changed {
                    self.save_state();
                }
                Ok(())
            }.await;
            if let Err(e) = r {
                log(format!("broadcast watcher failed: {e}"));
            }
            tokio::time::sleep(Duration::from_secs(BROADCAST_POLL_S)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finals_cards_and_questions() {
        let items = vec![json!({"type": "message", "role": "assistant", "id": "1", "content": [{"type": "output_text", "text": "a"}]}),
                         json!({"type": "message", "role": "assistant", "id": "2", "content": [{"type": "output_text", "text": "b"}]}),
                         json!({"type": "message", "role": "user", "id": "3", "content": [{"type": "input_text", "text": "q"}]}),
                         json!({"type": "message", "role": "assistant", "id": "4", "content": [{"type": "output_text", "text": "c"}]})];
        let f = broadcast_final_messages(&items);
        assert_eq!(f.iter().map(|(i, t)| (vstr(&i["id"]), t.clone())).collect::<Vec<_>>(), [("2".into(), "b".to_string()), ("4".into(), "c".into())]);
        let (t, sid) = approval_prompt_text(&json!({"id": "abcdef123456", "title": "lead"}), &json!({"params": {"message": "Bash: ls", "content_preview": "ls -la"}}));
        assert_eq!(t, "🔐 Approval required\n📡 lead · abcdef12\n\nBash: ls\n\nls -la");
        assert_eq!(sid, "abcdef123456");
        let ev = json!({"params": {"ask_user_question": {"questions": [{"question": "Which color?", "header": "Color",
                         "options": [{"label": "Red", "description": "red"}, {"label": "Blue", "description": "cool"}], "multiSelect": false}]}}});
        let qs = question_spec(&ev).unwrap();
        assert_eq!(qs[0]["options"][0]["description"], "");
        assert_eq!(qs[0]["other"], true);
        let card = json!({"token": "t1", "session_id": "abcdef123456", "title": "lead", "message": "", "questions": qs, "index": 0, "answers": {}, "selected": []});
        let (text, markup) = question_card_view(&card);
        assert!(text.starts_with("❓ Question from lead · abcdef12\n\nColor\nWhich color?\n\n• Blue — cool\n\n✍️ …or type your own answer"), "{text}");
        assert_eq!(markup["inline_keyboard"][1][0]["text"], "Blue — cool");
        assert_eq!(markup["inline_keyboard"][2][0]["callback_data"], "q:t1:0:x");
        let mut done = card.clone();
        done["answers"] = json!({"Which color?": "Blue"});
        assert!(question_final_text(&done, "✅ Answered from Telegram").contains("• Which color? → Blue"));
    }
}
