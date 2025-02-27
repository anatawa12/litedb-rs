use crate::bson::Value;
use base64::prelude::*;
use std::fmt::Write;

pub fn to_json(value: &Value) -> String {
    fn inner(value: &Value, buf: &mut String) {
        match value {
            Value::Null => buf.push_str("null"),

            Value::Array(v) => {
                buf.push('[');
                let mut first = true;
                for x in v.as_slice() {
                    if !first {
                        buf.push(',');
                    }
                    inner(x, buf);
                    first = false;
                }
                buf.push(']')
            }

            Value::Document(v) => {
                buf.push('{');
                let mut first = true;
                for (k, v) in v.iter() {
                    if !first {
                        buf.push(',');
                    }
                    buf.push('"');
                    buf.push_str(k);
                    buf.push('"');
                    inner(v, buf);
                    first = false;
                }
                buf.push('}')
            }

            &Value::Boolean(v) => buf.push_str(if v { "true" } else { "false" }),

            Value::String(v) => {
                buf.push('"');
                buf.push_str(v); // TODO: escape sequence
                buf.push('"');
            }

            Value::Int32(v) => write!(buf, "{}", v).unwrap(),

            Value::Double(v) => {
                if v.is_finite() {
                    write!(buf, "{}", v).unwrap();
                } else {
                    buf.push_str("null");
                }
            }

            Value::Binary(v) => write!(
                buf,
                r##"{{"$binary":"{}"}}"##,
                BASE64_STANDARD.encode(v.bytes())
            )
            .unwrap(),
            Value::ObjectId(v) => {
                write!(buf, r##"{{"$oid":"{:?}"}}"##, hex::encode(*v.as_bytes())).unwrap()
            }
            Value::Guid(v) => {
                write!(buf, r##"{{"$guid":"{:?}"}}"##, hex::encode(v.as_bytes())).unwrap()
            }
            Value::DateTime(v) => write!(buf, r##"{{"$date":"{:?}Z"}}"##, v).unwrap(),
            Value::Int64(v) => write!(buf, r##"{{"$numberLong":"{}"}}"##, v).unwrap(),
            Value::Decimal(v) => write!(buf, r##"{{"$numberLong":"{}"}}"##, v).unwrap(),

            Value::MinValue => buf.push_str(r#"{"$minValue":"1"}"#),
            Value::MaxValue => buf.push_str(r#"{"$maxValue":"1"}"#),
        }
    }

    let mut builder = String::new();
    inner(value, &mut builder);
    builder
}
