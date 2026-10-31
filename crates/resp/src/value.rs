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

/// Appends `n`'s decimal digits straight into `buf` with no intermediate
/// heap allocation. `n.to_string()` would allocate a disposable `String`
/// just to copy its bytes out and drop it again — on the hottest path in
/// this crate, since every integer reply and every bulk/array/map/set
/// length goes through this.
fn put_u64(buf: &mut BytesMut, mut n: u64) {
    let mut tmp = [0u8; 20]; // u64::MAX is 20 digits
    let mut i = tmp.len();
    loop {
        i -= 1;
        tmp[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    buf.extend_from_slice(&tmp[i..]);
}

fn put_i64(buf: &mut BytesMut, n: i64) {
    if n < 0 {
        buf.put_u8(b'-');
    }
    put_u64(buf, n.unsigned_abs());
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
                put_i64(buf, *i);
                buf.extend_from_slice(b"\r\n");
            }
            Value::Bulk(data) => {
                buf.put_u8(b'$');
                put_u64(buf, data.len() as u64);
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
                put_u64(buf, items.len() as u64);
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
                    put_u64(buf, pairs.len() as u64);
                    buf.extend_from_slice(b"\r\n");
                    for (k, v) in pairs {
                        k.encode(buf, proto);
                        v.encode(buf, proto);
                    }
                }
                // Flattened `[k1, v1, k2, v2, ...]` array, same wire shape as
                // `Value::Array` — written directly instead of cloning every
                // key/value into a throwaway `Vec<Value>` first.
                Protocol::Resp2 => {
                    buf.put_u8(b'*');
                    put_u64(buf, (pairs.len() * 2) as u64);
                    buf.extend_from_slice(b"\r\n");
                    for (k, v) in pairs {
                        k.encode(buf, proto);
                        v.encode(buf, proto);
                    }
                }
            },
            Value::Set(items) => match proto {
                Protocol::Resp3 => {
                    buf.put_u8(b'~');
                    put_u64(buf, items.len() as u64);
                    buf.extend_from_slice(b"\r\n");
                    for item in items {
                        item.encode(buf, proto);
                    }
                }
                Protocol::Resp2 => {
                    buf.put_u8(b'*');
                    put_u64(buf, items.len() as u64);
                    buf.extend_from_slice(b"\r\n");
                    for item in items {
                        item.encode(buf, proto);
                    }
                }
            },
            Value::Push(items) => match proto {
                Protocol::Resp3 => {
                    buf.put_u8(b'>');
                    put_u64(buf, items.len() as u64);
                    buf.extend_from_slice(b"\r\n");
                    for item in items {
                        item.encode(buf, proto);
                    }
                }
                Protocol::Resp2 => {
                    buf.put_u8(b'*');
                    put_u64(buf, items.len() as u64);
                    buf.extend_from_slice(b"\r\n");
                    for item in items {
                        item.encode(buf, proto);
                    }
                }
            },
        }
    }

    /// RFX wire encoding: same tag characters as RESP, but lengths are
    /// fixed-width big-endian integers and there are no CRLF markers.
    /// `docs/RFX.md` is the spec clients implement against.
    pub fn encode_rfx(&self, buf: &mut BytesMut) {
        match self {
            Value::Simple(s) => {
                buf.put_u8(b'+');
                buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            Value::Error(s) => {
                buf.put_u8(b'-');
                buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            Value::Integer(i) => {
                buf.put_u8(b':');
                buf.extend_from_slice(&i.to_be_bytes());
            }
            Value::Bulk(data) => {
                buf.put_u8(b'$');
                buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
                buf.extend_from_slice(data);
            }
            Value::NullBulk | Value::NullArray | Value::Null => buf.put_u8(b'_'),
            Value::Array(items) | Value::Push(items) | Value::Set(items) => {
                buf.put_u8(match self {
                    Value::Push(_) => b'>',
                    Value::Set(_) => b'~',
                    _ => b'*',
                });
                buf.extend_from_slice(&(items.len() as u32).to_be_bytes());
                for item in items {
                    item.encode_rfx(buf);
                }
            }
            Value::Boolean(b) => {
                buf.put_u8(b'#');
                buf.put_u8(*b as u8);
            }
            Value::Double(d) => {
                buf.put_u8(b',');
                buf.extend_from_slice(&d.to_be_bytes());
            }
            Value::BigNumber(s) => {
                buf.put_u8(b'(');
                buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            Value::Map(pairs) => {
                buf.put_u8(b'%');
                buf.extend_from_slice(&(pairs.len() as u32).to_be_bytes());
                for (k, v) in pairs {
                    k.encode_rfx(buf);
                    v.encode_rfx(buf);
                }
            }
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
