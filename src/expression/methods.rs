use crate::bson;
use crate::bson::Value;

pub(super) fn string_impl(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => bson::to_json(other),
    }
}
