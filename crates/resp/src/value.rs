use bytes::{BufMut, Bytes, BytesMut};

/// The RESP protocol version negotiated with a client via `HELLO`.
/// Defaults to RESP2 until a client opts into RESP3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protocol {
    #[default]
    Resp2,
    Resp3,
}

/// A reply value that can be encoded to either RESP2 or RESP3 wire format.
///
/// RESP3-only variants (`Boolean`, `Double`, `Map`, `Set`, `Null`, ...) are
/// transparently downgraded to their RESP2 equivalents when the connection
/// hasn't negotiated RESP3 via `HELLO 3`.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    NullBulk,
    Array(Vec<Value>),
    NullArray,
    Boolean(bool),
    Double(f64),
    BigNumber(String),
    Map(Vec<(Value, Value)>),
    Set(Vec<Value>),
    Push(Vec<Value>),
    Null,
}

impl Value {
    pub fn ok() -> Self {
        Value::Simple("OK".into())
    }

    pub fn bulk(data: impl Into<Bytes>) -> Self {
        Value::Bulk(data.into())
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Value::Error(msg.into())
    }

    pub fn encode(&self, buf: &mut BytesMut, proto: Protocol) {
        match self {
            Value::Simple(s) => {
                buf.put_u8(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Value::Error(s) => {
                buf.put_u8(b'-');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Value::Integer(i) => {
                buf.put_u8(b':');
                buf.extend_from_slice(i.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Value::Bulk(data) => {
                buf.put_u8(b'$');
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            Value::NullBulk => match proto {
                Protocol::Resp2 => buf.extend_from_slice(b"$-1\r\n"),
                Protocol::Resp3 => buf.extend_from_slice(b"_\r\n"),
            },
            Value::NullArray => match proto {
                Protocol::Resp2 => buf.extend_from_slice(b"*-1\r\n"),
                Protocol::Resp3 => buf.extend_from_slice(b"_\r\n"),
            },
            Value::Null => match proto {
                Protocol::Resp2 => buf.extend_from_slice(b"$-1\r\n"),
                Protocol::Resp3 => buf.extend_from_slice(b"_\r\n"),
            },
            Value::Array(items) => {
                buf.put_u8(b'*');
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode(buf, proto);
                }
            }
            Value::Boolean(b) => match proto {
                Protocol::Resp3 => {
                    buf.extend_from_slice(if *b { b"#t\r\n" } else { b"#f\r\n" });
                }
                Protocol::Resp2 => {
                    Value::Integer(if *b { 1 } else { 0 }).encode(buf, proto);
                }
            },
            Value::Double(d) => match proto {
                Protocol::Resp3 => {
                    buf.put_u8(b',');
                    buf.extend_from_slice(format_double(*d).as_bytes());
                    buf.extend_from_slice(b"\r\n");
                }
                Protocol::Resp2 => {
                    Value::Bulk(Bytes::from(format_double(*d))).encode(buf, proto);
                }
            },
            Value::BigNumber(s) => match proto {
                Protocol::Resp3 => {
                    buf.put_u8(b'(');
                    buf.extend_from_slice(s.as_bytes());
                    buf.extend_from_slice(b"\r\n");
                }
                Protocol::Resp2 => {
                    Value::Bulk(Bytes::from(s.clone())).encode(buf, proto);
                }
            },
            Value::Map(pairs) => match proto {
                Protocol::Resp3 => {
                    buf.put_u8(b'%');
                    buf.extend_from_slice(pairs.len().to_string().as_bytes());
                    buf.extend_from_slice(b"\r\n");
                    for (k, v) in pairs {
                        k.encode(buf, proto);
                        v.encode(buf, proto);
                    }
                }
                Protocol::Resp2 => {
                    let mut flat = Vec::with_capacity(pairs.len() * 2);
                    for (k, v) in pairs {
                        flat.push(k.clone());
                        flat.push(v.clone());
                    }
                    Value::Array(flat).encode(buf, proto);
                }
            },
            Value::Set(items) => match proto {
                Protocol::Resp3 => {
                    buf.put_u8(b'~');
                    buf.extend_from_slice(items.len().to_string().as_bytes());
                    buf.extend_from_slice(b"\r\n");
                    for item in items {
                        item.encode(buf, proto);
                    }
                }
                Protocol::Resp2 => {
                    Value::Array(items.clone()).encode(buf, proto);
                }
            },
            Value::Push(items) => match proto {
                Protocol::Resp3 => {
                    buf.put_u8(b'>');
                    buf.extend_from_slice(items.len().to_string().as_bytes());
                    buf.extend_from_slice(b"\r\n");
                    for item in items {
                        item.encode(buf, proto);
                    }
                }
                Protocol::Resp2 => {
                    Value::Array(items.clone()).encode(buf, proto);
                }
            },
        }
    }
}

fn format_double(d: f64) -> String {
    if d.is_infinite() {
        if d > 0.0 {
            "inf".to_string()
        } else {
            "-inf".to_string()
        }
    } else if d.is_nan() {
        "nan".to_string()
    } else {
        let mut s = format!("{d}");
        if !s.contains('.') && !s.contains('e') {
            s.push_str(".0");
        }
        s
    }
}
