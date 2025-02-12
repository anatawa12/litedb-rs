use crate::bson::Value;
use crate::bson::utils::ToHex;
use std::fmt::Display;
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

            Value::Binary(v) => write!(buf, r##"{{"$binary":"{}"}}"##, Base64(v.bytes())).unwrap(),
            Value::ObjectId(v) => {
                write!(buf, r##"{{"$oid":"{:?}"}}"##, ToHex(*v.as_bytes())).unwrap()
            }
            Value::Guid(v) => write!(buf, r##"{{"$guid":"{:?}"}}"##, ToHex(v.to_bytes())).unwrap(),
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

struct Base64<'a>(&'a [u8]);

impl Display for Base64<'_> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        struct FourChars([u8; 4]);

        impl Display for FourChars {
            fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
                std::str::from_utf8(&self.0).unwrap().fmt(fmt)
            }
        }

        fn process_chunk(chunk: &[u8]) -> FourChars {
            static TABLE: [u8; 64] =
                *b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

            let [b0, b1, b2] = match *chunk {
                [b0] => [b0, 0, 0],
                [b0, b1] => [b0, b1, 0],
                [b0, b1, b2] => [b0, b1, b2],
                _ => unreachable!(),
            };

            let c0 = (b0 & 0xFC) >> 2;
            let c1 = ((b0 & 0x03) << 4) | ((b1 & 0xF0) >> 4);
            let c2 = ((b1 & 0x0F) << 2) | ((b2 & 0xC0) >> 2);
            let c3 = b2 & 0x3F;

            let c0 = TABLE[c0 as usize];
            let c1 = TABLE[c1 as usize];
            let mut c2 = TABLE[c2 as usize];
            let mut c3 = TABLE[c3 as usize];

            match chunk.len() {
                1 => (c2, c3) = (b'=', b'='),
                2 => c3 = b'=',
                3 => (),
                _ => unreachable!(),
            }

            FourChars([c0, c1, c2, c3])
        }

        for chunk in self.0.chunks(3).map(process_chunk) {
            chunk.fmt(fmt)?;
        }

        Ok(())
    }
}
