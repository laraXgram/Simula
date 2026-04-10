use serde_json::Value;
use std::cell::RefCell;

use crate::types::ApiError;

thread_local! {
    static REQUEST_ACTOR_USER_ID: RefCell<Option<i64>> = RefCell::new(None);
}

pub fn value_to_chat_key(v: &Value) -> Result<String, ApiError> {
    match v {
        Value::String(s) if !s.trim().is_empty() => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        _ => Err(ApiError::bad_request("chat_id is empty or invalid")),
    }
}

pub fn current_request_actor_user_id() -> Option<i64> {
    REQUEST_ACTOR_USER_ID.with(|slot| *slot.borrow())
}

pub fn with_request_actor_user_id<T>(actor_user_id: Option<i64>, action: impl FnOnce() -> T) -> T {
    REQUEST_ACTOR_USER_ID.with(|slot| {
        let previous = slot.replace(actor_user_id);
        let result = action();
        slot.replace(previous);
        result
    })
}

pub fn value_to_optional_bool_loose(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(v) => Some(*v),
        Value::Number(n) => {
            if n.as_i64() == Some(1) {
                Some(true)
            } else if n.as_i64() == Some(0) {
                Some(false)
            } else {
                None
            }
        }
        Value::String(raw) => {
            let normalized = raw.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "1" | "true" | "yes" | "on" => Some(true),
                "0" | "false" | "no" | "off" | "" => Some(false),
                _ => None,
            }
        }
        _ => None,
    }
}
