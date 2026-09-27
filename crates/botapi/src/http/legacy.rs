//! Parameters gone from the docs that telegram-bot-api still honours, rewritten into
//! their current form so older bots and libraries keep working instead of silently
//! losing them:
//! - `reply_to_message_id` + `allow_sending_without_reply` → `reply_parameters`
//!   (only when `reply_parameters` is absent, as in `get_reply_parameters`)
//! - `disable_web_page_preview` → `link_preview_options` (only when that is absent)

use serde_json::{Map, Value, json};
use simula_botapi_types::MethodInfo;

pub fn rewrite(info: &MethodInfo, params: &mut Map<String, Value>) {
    let takes = |name: &str| info.params.iter().any(|p| p.name == name);

    if takes("reply_parameters") && !params.contains_key("reply_parameters") {
        let message_id = params
            .remove("reply_to_message_id")
            .map_or(0, |v| integer(&v));
        let allow = params
            .remove("allow_sending_without_reply")
            .is_some_and(|v| truthy(&v));
        if message_id > 0 {
            params.insert(
                "reply_parameters".into(),
                json!({ "message_id": message_id, "allow_sending_without_reply": allow }),
            );
        }
    }

    if takes("link_preview_options") && !is_set(params.get("link_preview_options")) {
        let disable = params
            .remove("disable_web_page_preview")
            .is_some_and(|v| truthy(&v));
        if disable {
            params.insert(
                "link_preview_options".into(),
                json!({ "is_disabled": true }),
            );
        }
    }
}

fn is_set(value: Option<&Value>) -> bool {
    !matches!(value, None | Some(Value::Null)) && value != Some(&Value::String(String::new()))
}

/// telegram-bot-api's `to_bool`: "true", "yes" or "1", case-insensitive.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_i64() == Some(1),
        Value::String(s) => matches!(s.trim().to_ascii_lowercase().as_str(), "true" | "yes" | "1"),
        _ => false,
    }
}

/// `td::to_integer<int32>`: a leading number, anything else is 0.
fn integer(value: &Value) -> i64 {
    match value {
        Value::Number(n) => n.as_i64().unwrap_or(0),
        Value::String(s) => {
            let s = s.trim();
            let digits = s.strip_prefix('-').unwrap_or(s);
            let end = digits
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(digits.len());
            let n: i64 = digits[..end].parse().unwrap_or(0);
            if s.starts_with('-') { -n } else { n }
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use simula_botapi_types::registry;

    use super::*;

    fn rewritten(method: &str, params: Value) -> Value {
        let info = registry::METHODS.iter().find(|m| m.name == method).unwrap();
        let mut params = params.as_object().unwrap().clone();
        rewrite(info, &mut params);
        Value::Object(params)
    }

    #[test]
    fn old_reply_and_preview_parameters_still_work() {
        assert_eq!(
            rewritten(
                "sendMessage",
                json!({ "reply_to_message_id": "5", "allow_sending_without_reply": "yes",
                        "disable_web_page_preview": true })
            ),
            json!({
                "reply_parameters": { "message_id": 5, "allow_sending_without_reply": true },
                "link_preview_options": { "is_disabled": true }
            })
        );
        // The current form wins, as in telegram-bot-api.
        let current = json!({ "reply_parameters": { "message_id": 2 }, "reply_to_message_id": 5 });
        assert_eq!(rewritten("sendMessage", current.clone()), current);
    }
}
