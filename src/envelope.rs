// SPDX-License-Identifier: LicenseRef-Reticulum
//! RRC envelopes: CBOR maps with small integer keys.
//!
//! The header fields are decoded into Rust types. The body and every key the
//! bouncer does not know (rBrowser2's `K_FMT = 64`, anything newer) are kept
//! as the CBOR bytes they arrived as and written back unchanged, so relaying
//! an envelope never alters what the bouncer does not understand.

use std::fmt;

use ciborium::Value;

pub const RRC_VERSION: u64 = 1;

pub const K_V: u64 = 0;
pub const K_T: u64 = 1;
pub const K_ID: u64 = 2;
pub const K_TS: u64 = 3;
pub const K_SRC: u64 = 4;
pub const K_ROOM: u64 = 5;
pub const K_BODY: u64 = 6;
pub const K_NICK: u64 = 7;
pub const K_DST: u64 = 8;
/// rBrowser2's colour spans.
pub const K_FMT: u64 = 64;

/// Nesting deeper than this is refused rather than followed.
const MAX_DEPTH: usize = 64;

/// The message type (`K_T`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageType {
    Hello,
    Welcome,
    Join,
    Joined,
    Part,
    Parted,
    Msg,
    Notice,
    Action,
    Ping,
    Pong,
    Error,
    ResourceEnvelope,
    Unknown(u64),
}

impl MessageType {
    pub fn from_u64(t: u64) -> Self {
        match t {
            1 => Self::Hello,
            2 => Self::Welcome,
            10 => Self::Join,
            11 => Self::Joined,
            12 => Self::Part,
            13 => Self::Parted,
            20 => Self::Msg,
            21 => Self::Notice,
            22 => Self::Action,
            30 => Self::Ping,
            31 => Self::Pong,
            40 => Self::Error,
            50 => Self::ResourceEnvelope,
            other => Self::Unknown(other),
        }
    }

    pub fn as_u64(self) -> u64 {
        match self {
            Self::Hello => 1,
            Self::Welcome => 2,
            Self::Join => 10,
            Self::Joined => 11,
            Self::Part => 12,
            Self::Parted => 13,
            Self::Msg => 20,
            Self::Notice => 21,
            Self::Action => 22,
            Self::Ping => 30,
            Self::Pong => 31,
            Self::Error => 40,
            Self::ResourceEnvelope => 50,
            Self::Unknown(other) => other,
        }
    }
}

/// Why bytes are not an envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError(pub String);

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bad envelope: {}", self.0)
    }
}

impl std::error::Error for DecodeError {}

fn err<T>(text: impl Into<String>) -> Result<T, DecodeError> {
    Err(DecodeError(text.into()))
}

/// One CBOR data item, as encoded.
#[derive(Clone, PartialEq, Eq)]
pub struct Raw(Vec<u8>);

impl Raw {
    /// Check that `bytes` is exactly one well-formed CBOR item.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        if item_end(bytes, 0, 0)? != bytes.len() {
            return err("trailing bytes after the item");
        }
        ciborium::from_reader::<Value, _>(bytes).map_err(|e| DecodeError(e.to_string()))?;
        Ok(Self(bytes.to_vec()))
    }

    pub fn from_value(value: &Value) -> Self {
        let mut out = Vec::new();
        // Writing into a Vec only fails on values ciborium cannot represent,
        // which a `Value` never is.
        ciborium::into_writer(value, &mut out).unwrap_or_default();
        Self(out)
    }

    pub fn text(text: &str) -> Self {
        Self::from_value(&Value::Text(text.to_string()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn value(&self) -> Value {
        ciborium::from_reader(self.0.as_slice()).unwrap_or(Value::Null)
    }
}

impl fmt::Debug for Raw {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Raw({:?})", self.value())
    }
}

/// An RRC envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub t: MessageType,
    pub id: Vec<u8>,
    /// Milliseconds since the Unix epoch, as the sender stamped it.
    pub ts: u64,
    pub src: Vec<u8>,
    pub room: Option<String>,
    pub body: Option<Raw>,
    pub nick: Option<String>,
    pub dst: Option<Vec<u8>>,
    /// Keys this codec does not know, in the order they came.
    pub extra: Vec<(u64, Raw)>,
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

impl Envelope {
    /// A new envelope with a random 8-byte id, stamped now.
    pub fn new(t: MessageType, src: &[u8]) -> Self {
        Self {
            t,
            id: rand::random::<[u8; 8]>().to_vec(),
            ts: now_ms(),
            src: src.to_vec(),
            room: None,
            body: None,
            nick: None,
            dst: None,
            extra: Vec::new(),
        }
    }

    pub fn with_room(mut self, room: &str) -> Self {
        self.room = Some(room.to_string());
        self
    }

    pub fn with_body(mut self, body: Raw) -> Self {
        self.body = Some(body);
        self
    }

    pub fn with_nick(mut self, nick: Option<&str>) -> Self {
        self.nick = nick.map(String::from);
        self
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let (major, arg, mut pos) = head(bytes, 0)?;
        if major != 5 {
            return err("not a CBOR map");
        }
        let count = match arg {
            Arg::Indefinite => None,
            Arg::Value(n) => Some(n),
        };
        let mut seen: Vec<u64> = Vec::new();
        let (mut v, mut t, mut id, mut ts, mut src) = (None, None, None, None, None);
        let mut env = Self {
            t: MessageType::Unknown(0),
            id: Vec::new(),
            ts: 0,
            src: Vec::new(),
            room: None,
            body: None,
            nick: None,
            dst: None,
            extra: Vec::new(),
        };
        let mut read = 0u64;
        loop {
            match count {
                Some(n) if read == n => break,
                None if bytes.get(pos) == Some(&0xff) => {
                    pos += 1;
                    break;
                }
                _ => {}
            }
            let (key_major, key, after_key) = head(bytes, pos)?;
            let key = match (key_major, key) {
                (0, Arg::Value(k)) => k,
                (1, _) => return err("negative key"),
                _ => return err("keys must be unsigned integers"),
            };
            if seen.contains(&key) {
                return err(format!("key {key} appears twice"));
            }
            seen.push(key);
            let end = item_end(bytes, after_key, 0)?;
            let item = &bytes[after_key..end];
            pos = end;
            read += 1;
            match key {
                K_V => v = Some(uint(item, "v")?),
                K_T => t = Some(uint(item, "t")?),
                K_ID => id = Some(byte_string(item, "id")?),
                K_TS => ts = Some(uint(item, "ts")?),
                K_SRC => src = Some(byte_string(item, "src")?),
                K_ROOM => env.room = Some(text(item, "room")?),
                K_BODY => env.body = Some(Raw::from_bytes(item)?),
                K_NICK => env.nick = Some(text(item, "nick")?),
                K_DST => env.dst = Some(byte_string(item, "dst")?),
                other => env.extra.push((other, Raw::from_bytes(item)?)),
            }
        }
        if pos != bytes.len() {
            return err("trailing bytes after the map");
        }
        match v {
            None => return err("no version"),
            Some(RRC_VERSION) => {}
            Some(other) => return err(format!("version {other}")),
        }
        env.t = MessageType::from_u64(t.ok_or_else(|| DecodeError("no type".into()))?);
        env.id = id.ok_or_else(|| DecodeError("no id".into()))?;
        env.ts = ts.ok_or_else(|| DecodeError("no timestamp".into()))?;
        env.src = src.ok_or_else(|| DecodeError("no source".into()))?;
        Ok(env)
    }

    /// The envelope as CBOR: known keys in the order rrcd writes them
    /// (v, t, id, ts, src, dst, room, body, nick), then the others in the
    /// order they came.
    pub fn encode(&self) -> Vec<u8> {
        let fields = 5
            + usize::from(self.dst.is_some())
            + usize::from(self.room.is_some())
            + usize::from(self.body.is_some())
            + usize::from(self.nick.is_some())
            + self.extra.len();
        let mut out = Vec::with_capacity(64);
        write_head(&mut out, 5, fields as u64);
        write_head(&mut out, 0, K_V);
        write_head(&mut out, 0, RRC_VERSION);
        write_head(&mut out, 0, K_T);
        write_head(&mut out, 0, self.t.as_u64());
        write_head(&mut out, 0, K_ID);
        write_bytes(&mut out, 2, &self.id);
        write_head(&mut out, 0, K_TS);
        write_head(&mut out, 0, self.ts);
        write_head(&mut out, 0, K_SRC);
        write_bytes(&mut out, 2, &self.src);
        if let Some(dst) = &self.dst {
            write_head(&mut out, 0, K_DST);
            write_bytes(&mut out, 2, dst);
        }
        if let Some(room) = &self.room {
            write_head(&mut out, 0, K_ROOM);
            write_bytes(&mut out, 3, room.as_bytes());
        }
        if let Some(body) = &self.body {
            write_head(&mut out, 0, K_BODY);
            out.extend_from_slice(body.as_bytes());
        }
        if let Some(nick) = &self.nick {
            write_head(&mut out, 0, K_NICK);
            write_bytes(&mut out, 3, nick.as_bytes());
        }
        for (key, value) in &self.extra {
            write_head(&mut out, 0, *key);
            out.extend_from_slice(value.as_bytes());
        }
        out
    }

    /// The body as text (MSG, NOTICE, ACTION, ERROR).
    pub fn text_body(&self) -> Option<String> {
        match self.body.as_ref()?.value() {
            Value::Text(text) => Some(text),
            _ => None,
        }
    }

    /// The member identity hashes of a JOINED or PARTED body.
    pub fn members(&self) -> Option<Vec<Vec<u8>>> {
        match self.body.as_ref()?.value() {
            Value::Array(items) => items
                .into_iter()
                .map(|item| match item {
                    Value::Bytes(b) => Some(b),
                    _ => None,
                })
                .collect(),
            _ => None,
        }
    }

    pub fn extra(&self, key: u64) -> Option<&Raw> {
        self.extra.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
    }
}

enum Arg {
    Value(u64),
    Indefinite,
}

/// The major type, argument and end of the head at `pos`.
fn head(bytes: &[u8], pos: usize) -> Result<(u8, Arg, usize), DecodeError> {
    let Some(&initial) = bytes.get(pos) else {
        return err("truncated");
    };
    let major = initial >> 5;
    let info = initial & 0x1f;
    let size = match info {
        0..=23 => return Ok((major, Arg::Value(u64::from(info)), pos + 1)),
        24 => 1,
        25 => 2,
        26 => 4,
        27 => 8,
        31 if matches!(major, 2..=5 | 7) => return Ok((major, Arg::Indefinite, pos + 1)),
        _ => return err(format!("reserved CBOR head {initial:#04x}")),
    };
    let Some(arg) = bytes.get(pos + 1..pos + 1 + size) else {
        return err("truncated");
    };
    let value = arg.iter().fold(0u64, |acc, b| (acc << 8) | u64::from(*b));
    Ok((major, Arg::Value(value), pos + 1 + size))
}

/// Where the CBOR item starting at `pos` ends.
fn item_end(bytes: &[u8], pos: usize, depth: usize) -> Result<usize, DecodeError> {
    if depth > MAX_DEPTH {
        return err("nested too deep");
    }
    let (major, arg, pos) = head(bytes, pos)?;
    let skip = |pos: usize, len: u64| -> Result<usize, DecodeError> {
        usize::try_from(len)
            .ok()
            .and_then(|len| pos.checked_add(len))
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| DecodeError("truncated".into()))
    };
    match (major, arg) {
        (0 | 1, Arg::Value(_)) => Ok(pos),
        (2 | 3, Arg::Value(len)) => skip(pos, len),
        (2 | 3, Arg::Indefinite) => {
            let mut pos = pos;
            loop {
                if bytes.get(pos) == Some(&0xff) {
                    return Ok(pos + 1);
                }
                match head(bytes, pos)? {
                    (chunk_major, Arg::Value(len), after) if chunk_major == major => pos = skip(after, len)?,
                    _ => return err("bad chunk in an indefinite-length string"),
                }
            }
        }
        (4 | 5, Arg::Value(n)) => {
            let items = if major == 5 { n.checked_mul(2) } else { Some(n) };
            let items = items.ok_or_else(|| DecodeError("too long".into()))?;
            // Every item takes at least one byte.
            if items > (bytes.len() - pos) as u64 {
                return err("truncated");
            }
            let mut pos = pos;
            for _ in 0..items {
                pos = item_end(bytes, pos, depth + 1)?;
            }
            Ok(pos)
        }
        (4 | 5, Arg::Indefinite) => {
            let mut pos = pos;
            let mut items = 0usize;
            loop {
                if bytes.get(pos) == Some(&0xff) {
                    if major == 5 && items % 2 == 1 {
                        return err("map without a value for its last key");
                    }
                    return Ok(pos + 1);
                }
                pos = item_end(bytes, pos, depth + 1)?;
                items += 1;
            }
        }
        (6, Arg::Value(_)) => item_end(bytes, pos, depth + 1),
        (7, Arg::Value(_)) => Ok(pos),
        (7, Arg::Indefinite) => err("unexpected break"),
        _ => err("malformed CBOR"),
    }
}

fn parse(item: &[u8], field: &str) -> Result<Value, DecodeError> {
    ciborium::from_reader(item).map_err(|e| DecodeError(format!("{field}: {e}")))
}

fn uint(item: &[u8], field: &str) -> Result<u64, DecodeError> {
    match parse(item, field)? {
        Value::Integer(i) => u64::try_from(i).or_else(|_| err(format!("{field} is negative"))),
        _ => err(format!("{field} is not an integer")),
    }
}

fn byte_string(item: &[u8], field: &str) -> Result<Vec<u8>, DecodeError> {
    match parse(item, field)? {
        Value::Bytes(b) => Ok(b),
        _ => err(format!("{field} is not a byte string")),
    }
}

fn text(item: &[u8], field: &str) -> Result<String, DecodeError> {
    match parse(item, field)? {
        Value::Text(t) => Ok(t),
        _ => err(format!("{field} is not a text string")),
    }
}

fn write_head(out: &mut Vec<u8>, major: u8, value: u64) {
    let major = major << 5;
    match value {
        0..=23 => out.push(major | value as u8),
        24..=0xff => out.extend_from_slice(&[major | 24, value as u8]),
        0x100..=0xffff => {
            out.push(major | 25);
            out.extend_from_slice(&(value as u16).to_be_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(major | 26);
            out.extend_from_slice(&(value as u32).to_be_bytes());
        }
        _ => {
            out.push(major | 27);
            out.extend_from_slice(&value.to_be_bytes());
        }
    }
}

fn write_bytes(out: &mut Vec<u8>, major: u8, data: &[u8]) {
    write_head(out, major, data.len() as u64);
    out.extend_from_slice(data);
}

// Bodies.

fn int(key: u64) -> Value {
    Value::Integer(key.into())
}

fn lookup(map: &[(Value, Value)], key: u64) -> Option<&Value> {
    map.iter()
        .find(|(k, _)| matches!(k, Value::Integer(i) if u64::try_from(*i) == Ok(key)))
        .map(|(_, v)| v)
}

fn as_map(value: &Value) -> &[(Value, Value)] {
    match value {
        Value::Map(map) => map,
        _ => &[],
    }
}

fn as_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::Text(t) => Some(t.clone()),
        _ => None,
    }
}

fn as_uint(value: Option<&Value>) -> Option<u64> {
    match value? {
        Value::Integer(i) => u64::try_from(*i).ok(),
        _ => None,
    }
}

/// Capabilities (HELLO and WELCOME `caps`). Values are advisory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Caps {
    pub resource_envelope: bool,
    pub action: bool,
    pub direct_notice: bool,
}

impl Caps {
    pub fn from_value(value: Option<&Value>) -> Self {
        let map = value.map_or(&[][..], as_map);
        let on = |key| matches!(lookup(map, key), Some(Value::Bool(true)));
        Self {
            resource_envelope: on(0),
            action: on(1),
            direct_notice: on(2),
        }
    }

    pub fn to_value(self) -> Value {
        Value::Map(vec![
            (int(0), Value::Bool(self.resource_envelope)),
            (int(1), Value::Bool(self.action)),
            (int(2), Value::Bool(self.direct_notice)),
        ])
    }
}

/// A hub's limits (WELCOME `limits`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_nick_bytes: u64,
    pub max_room_name_bytes: u64,
    pub max_msg_body_bytes: u64,
    pub max_rooms_per_session: u64,
    pub rate_limit_msgs_per_minute: u64,
}

impl Default for Limits {
    /// rrcd's defaults (`rrcd/config.py`).
    fn default() -> Self {
        Self {
            max_nick_bytes: 32,
            max_room_name_bytes: 64,
            max_msg_body_bytes: 350,
            max_rooms_per_session: 32,
            rate_limit_msgs_per_minute: 240,
        }
    }
}

impl Limits {
    /// Missing or unreadable limits take rrcd's defaults.
    pub fn from_value(value: Option<&Value>) -> Self {
        let map = value.map_or(&[][..], as_map);
        let d = Self::default();
        let get = |key, default| as_uint(lookup(map, key)).unwrap_or(default);
        Self {
            max_nick_bytes: get(0, d.max_nick_bytes),
            max_room_name_bytes: get(1, d.max_room_name_bytes),
            max_msg_body_bytes: get(2, d.max_msg_body_bytes),
            max_rooms_per_session: get(3, d.max_rooms_per_session),
            rate_limit_msgs_per_minute: get(4, d.rate_limit_msgs_per_minute),
        }
    }

    pub fn to_value(self) -> Value {
        Value::Map(vec![
            (int(0), int(self.max_nick_bytes)),
            (int(1), int(self.max_room_name_bytes)),
            (int(2), int(self.max_msg_body_bytes)),
            (int(3), int(self.max_rooms_per_session)),
            (int(4), int(self.rate_limit_msgs_per_minute)),
        ])
    }
}

/// A HELLO body.
#[derive(Debug, Clone, PartialEq)]
pub struct Hello {
    pub name: Option<String>,
    /// A number or a string, depending on the client.
    pub ver: Option<Value>,
    pub caps: Caps,
    /// The nick in the body (key 64), from clients older than `K_NICK`.
    pub legacy_nick: Option<String>,
}

impl Hello {
    pub fn from_body(body: &Raw) -> Self {
        let value = body.value();
        let map = as_map(&value);
        Self {
            name: as_text(lookup(map, 0)),
            ver: lookup(map, 1).cloned(),
            caps: Caps::from_value(lookup(map, 2)),
            legacy_nick: as_text(lookup(map, 64)),
        }
    }

    pub fn to_body(&self) -> Raw {
        let mut map = Vec::new();
        if let Some(name) = &self.name {
            map.push((int(0), Value::Text(name.clone())));
        }
        if let Some(ver) = &self.ver {
            map.push((int(1), ver.clone()));
        }
        map.push((int(2), self.caps.to_value()));
        if let Some(nick) = &self.legacy_nick {
            map.push((int(64), Value::Text(nick.clone())));
        }
        Raw::from_value(&Value::Map(map))
    }
}

/// A WELCOME body.
#[derive(Debug, Clone, PartialEq)]
pub struct Welcome {
    pub hub: Option<String>,
    /// rrcd sends its package version as a string.
    pub ver: Option<Value>,
    pub caps: Caps,
    pub limits: Limits,
}

impl Welcome {
    pub fn from_body(body: &Raw) -> Self {
        let value = body.value();
        let map = as_map(&value);
        Self {
            hub: as_text(lookup(map, 0)),
            ver: lookup(map, 1).cloned(),
            caps: Caps::from_value(lookup(map, 2)),
            limits: Limits::from_value(lookup(map, 3)),
        }
    }

    pub fn to_body(&self) -> Raw {
        let mut map = Vec::new();
        if let Some(hub) = &self.hub {
            map.push((int(0), Value::Text(hub.clone())));
        }
        if let Some(ver) = &self.ver {
            map.push((int(1), ver.clone()));
        }
        map.push((int(2), self.caps.to_value()));
        map.push((int(3), self.limits.to_value()));
        Raw::from_value(&Value::Map(map))
    }
}

/// A RESOURCE_ENVELOPE body: announces the Resource that follows on the
/// same link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceEnvelope {
    pub id: Vec<u8>,
    /// `notice`, `motd` or `blob`.
    pub kind: String,
    pub size: u64,
    pub sha256: Option<Vec<u8>>,
    pub encoding: Option<String>,
}

impl ResourceEnvelope {
    /// `None` when the id, kind or size is missing, as rrcd refuses those.
    pub fn from_body(body: &Raw) -> Option<Self> {
        let value = body.value();
        let map = as_map(&value);
        let id = match lookup(map, 0)? {
            Value::Bytes(b) => b.clone(),
            _ => return None,
        };
        let kind = as_text(lookup(map, 1)).filter(|k| !k.is_empty())?;
        let size = as_uint(lookup(map, 2))?;
        let sha256 = match lookup(map, 3) {
            Some(Value::Bytes(b)) => Some(b.clone()),
            _ => None,
        };
        Some(Self {
            id,
            kind,
            size,
            sha256,
            encoding: as_text(lookup(map, 4)),
        })
    }

    pub fn to_body(&self) -> Raw {
        let mut map = vec![
            (int(0), Value::Bytes(self.id.clone())),
            (int(1), Value::Text(self.kind.clone())),
            (int(2), int(self.size)),
        ];
        if let Some(sha) = &self.sha256 {
            map.push((int(3), Value::Bytes(sha.clone())));
        }
        if let Some(encoding) = &self.encoding {
            map.push((int(4), Value::Text(encoding.clone())));
        }
        Raw::from_value(&Value::Map(map))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const USER: [u8; 16] = [
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
    ];
    const HUB: [u8; 16] = [
        0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00,
    ];
    const OTHER: [u8; 16] = [
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
    ];
    const TS: u64 = 1_763_000_000_000;

    /// Envelopes made by rrcd's `make_envelope` and encoded by its codec
    /// (`tests/fixtures/make_envelopes.py`).
    const FIXTURES: &[(&str, &[u8])] = &[
        ("hello", include_bytes!("../tests/fixtures/envelopes/hello.cbor")),
        (
            "hello_legacy_nick",
            include_bytes!("../tests/fixtures/envelopes/hello_legacy_nick.cbor"),
        ),
        ("welcome", include_bytes!("../tests/fixtures/envelopes/welcome.cbor")),
        ("join", include_bytes!("../tests/fixtures/envelopes/join.cbor")),
        ("joined", include_bytes!("../tests/fixtures/envelopes/joined.cbor")),
        (
            "joined_no_body",
            include_bytes!("../tests/fixtures/envelopes/joined_no_body.cbor"),
        ),
        ("part", include_bytes!("../tests/fixtures/envelopes/part.cbor")),
        ("parted", include_bytes!("../tests/fixtures/envelopes/parted.cbor")),
        ("msg", include_bytes!("../tests/fixtures/envelopes/msg.cbor")),
        ("msg_fmt", include_bytes!("../tests/fixtures/envelopes/msg_fmt.cbor")),
        ("notice", include_bytes!("../tests/fixtures/envelopes/notice.cbor")),
        (
            "notice_direct",
            include_bytes!("../tests/fixtures/envelopes/notice_direct.cbor"),
        ),
        ("action", include_bytes!("../tests/fixtures/envelopes/action.cbor")),
        ("ping", include_bytes!("../tests/fixtures/envelopes/ping.cbor")),
        (
            "ping_short_float",
            include_bytes!("../tests/fixtures/envelopes/ping_short_float.cbor"),
        ),
        ("pong", include_bytes!("../tests/fixtures/envelopes/pong.cbor")),
        ("error", include_bytes!("../tests/fixtures/envelopes/error.cbor")),
        (
            "resource_envelope",
            include_bytes!("../tests/fixtures/envelopes/resource_envelope.cbor"),
        ),
        (
            "unknown_type",
            include_bytes!("../tests/fixtures/envelopes/unknown_type.cbor"),
        ),
    ];

    fn fixture(name: &str) -> Envelope {
        let (_, bytes) = FIXTURES.iter().find(|(n, _)| *n == name).unwrap();
        Envelope::decode(bytes).unwrap()
    }

    fn mid(n: u8) -> Vec<u8> {
        vec![n; 8]
    }

    #[test]
    fn rrcd_envelopes_round_trip_byte_for_byte() {
        for (name, bytes) in FIXTURES {
            let env = Envelope::decode(bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(env.encode(), *bytes, "{name}");
            assert_eq!(Envelope::decode(&env.encode()).unwrap(), env, "{name}");
        }
    }

    #[test]
    fn rrcd_envelopes_decode_to_what_was_made() {
        let hello = fixture("hello");
        assert_eq!(
            (hello.t, hello.id.clone(), hello.ts, hello.src.clone()),
            (MessageType::Hello, mid(1), TS + 1, USER.to_vec())
        );
        assert_eq!(hello.nick.as_deref(), Some("alice"));
        let body = Hello::from_body(hello.body.as_ref().unwrap());
        assert_eq!(body.name.as_deref(), Some("weechat-rrc"));
        assert_eq!(body.ver, Some(Value::Text("0.1.0".into())));
        assert_eq!(
            body.caps,
            Caps {
                resource_envelope: true,
                action: true,
                direct_notice: true
            }
        );
        let legacy = Hello::from_body(fixture("hello_legacy_nick").body.as_ref().unwrap());
        assert_eq!(legacy.legacy_nick.as_deref(), Some("bob"));
        assert_eq!(legacy.ver, Some(int(1)));

        let welcome = fixture("welcome");
        assert_eq!((welcome.t, welcome.src.clone()), (MessageType::Welcome, HUB.to_vec()));
        let body = Welcome::from_body(welcome.body.as_ref().unwrap());
        assert_eq!(body.hub.as_deref(), Some("test hub"));
        assert_eq!(body.ver, Some(Value::Text("0.3.2".into())));
        assert_eq!(body.limits, Limits::default());
        assert!(body.caps.resource_envelope && body.caps.action && body.caps.direct_notice);

        let join = fixture("join");
        assert_eq!((join.t, join.room.as_deref()), (MessageType::Join, Some("lobby")));
        assert!(join.body.is_none());
        let joined = fixture("joined");
        assert_eq!(joined.members(), Some(vec![USER.to_vec(), OTHER.to_vec()]));
        let joined = fixture("joined_no_body");
        assert_eq!((joined.body.is_none(), joined.nick.as_deref()), (true, Some("carol")));
        assert_eq!(fixture("part").t, MessageType::Part);
        assert_eq!(fixture("parted").members(), Some(vec![OTHER.to_vec()]));

        let msg = fixture("msg");
        assert_eq!(msg.t, MessageType::Msg);
        assert_eq!(msg.text_body().as_deref(), Some("hello, world ✓"));
        assert!(msg.extra.is_empty());
        let action = fixture("action");
        assert_eq!(
            (action.t, action.text_body().as_deref()),
            (MessageType::Action, Some("waves"))
        );
        let notice = fixture("notice");
        assert_eq!((notice.t, notice.dst.clone()), (MessageType::Notice, None));
        let direct = fixture("notice_direct");
        assert_eq!(
            (direct.dst.clone(), direct.room.clone(), direct.src.clone()),
            (Some(USER.to_vec()), None, OTHER.to_vec())
        );
        let error = fixture("error");
        assert_eq!(
            (error.t, error.text_body().as_deref()),
            (MessageType::Error, Some("not in room"))
        );

        let ping = fixture("ping");
        assert_eq!(ping.t, MessageType::Ping);
        assert_eq!(ping.body.as_ref().unwrap().value(), Value::Float(12345.678901));
        let pong = fixture("pong");
        assert_eq!((pong.t, pong.body.clone()), (MessageType::Pong, ping.body.clone()));

        let res = fixture("resource_envelope");
        assert_eq!(res.t, MessageType::ResourceEnvelope);
        let body = ResourceEnvelope::from_body(res.body.as_ref().unwrap()).unwrap();
        assert_eq!(
            body,
            ResourceEnvelope {
                id: (0..16).collect(),
                kind: "motd".into(),
                size: 1200,
                sha256: Some((0..32).collect()),
                encoding: Some("utf-8".into()),
            }
        );
    }

    #[test]
    fn unknown_keys_and_types_pass_through_untouched() {
        let (_, bytes) = FIXTURES.iter().find(|(n, _)| *n == "msg_fmt").unwrap();
        let env = Envelope::decode(bytes).unwrap();
        let fmt = env.extra(K_FMT).unwrap();
        // The value's own bytes sit at the end of the envelope, after key 64.
        assert!(bytes.ends_with(fmt.as_bytes()));
        assert_eq!(bytes[bytes.len() - fmt.as_bytes().len() - 2..][..2], [0x18, 0x40]);
        assert!(matches!(fmt.value(), Value::Array(spans) if spans.len() == 2));
        // Changing a known field leaves it alone.
        let mut relayed = env.clone();
        relayed.src = HUB.to_vec();
        let relayed = Envelope::decode(&relayed.encode()).unwrap();
        assert_eq!(relayed.extra, env.extra);
        assert_eq!(relayed.id, env.id);

        let unknown = fixture("unknown_type");
        assert_eq!(unknown.t, MessageType::Unknown(99));
        assert_eq!(unknown.extra.iter().map(|(k, _)| *k).collect::<Vec<_>>(), vec![70, 65]);
        assert_eq!(unknown.extra(65).unwrap().value(), Value::Integer((-3).into()));
    }

    #[test]
    fn a_float_body_keeps_its_width() {
        // cbor2 writes every float as 64 bits; ciborium would shrink 1.5 to
        // 16. The body is kept as it came, so a PONG echoes the PING exactly.
        let ping = fixture("ping_short_float");
        let body = ping.body.as_ref().unwrap();
        assert_eq!(body.as_bytes(), [0xfb, 0x3f, 0xf8, 0, 0, 0, 0, 0, 0]);
        assert_eq!(body.value(), Value::Float(1.5));
        assert_ne!(Raw::from_value(&Value::Float(1.5)).as_bytes(), body.as_bytes());
    }

    #[test]
    fn key_order_does_not_matter_when_reading() {
        let msg = fixture("msg");
        // Same map, keys from 8 down to 0.
        let mut shuffled = Vec::new();
        write_head(&mut shuffled, 5, 8);
        write_head(&mut shuffled, 0, K_NICK);
        write_bytes(&mut shuffled, 3, b"alice");
        write_head(&mut shuffled, 0, K_BODY);
        shuffled.extend_from_slice(msg.body.as_ref().unwrap().as_bytes());
        write_head(&mut shuffled, 0, K_ROOM);
        write_bytes(&mut shuffled, 3, b"lobby");
        write_head(&mut shuffled, 0, K_SRC);
        write_bytes(&mut shuffled, 2, &USER);
        write_head(&mut shuffled, 0, K_TS);
        write_head(&mut shuffled, 0, TS + 10);
        write_head(&mut shuffled, 0, K_ID);
        write_bytes(&mut shuffled, 2, &mid(10));
        write_head(&mut shuffled, 0, K_T);
        write_head(&mut shuffled, 0, 20);
        write_head(&mut shuffled, 0, K_V);
        write_head(&mut shuffled, 0, 1);
        assert_eq!(Envelope::decode(&shuffled).unwrap(), msg);
        // Indefinite-length map.
        let (_, bytes) = FIXTURES.iter().find(|(n, _)| *n == "msg").unwrap();
        let mut indefinite = vec![0xbf];
        indefinite.extend_from_slice(&bytes[1..]);
        indefinite.push(0xff);
        assert_eq!(Envelope::decode(&indefinite).unwrap(), msg);
    }

    #[test]
    fn made_envelopes_round_trip() {
        let env = Envelope::new(MessageType::Msg, &USER)
            .with_room("lobby")
            .with_body(Raw::text("hi"))
            .with_nick(Some("alice"));
        assert_eq!(env.id.len(), 8);
        assert!(env.ts > TS - 1_000_000_000);
        assert_eq!(Envelope::decode(&env.encode()).unwrap(), env);
        let mut big = Envelope::new(MessageType::Unknown(u64::MAX), &[0u8; 300]);
        big.ts = u64::MAX;
        big.extra.push((u64::from(u32::MAX) + 1, Raw::from_value(&Value::Null)));
        assert_eq!(Envelope::decode(&big.encode()).unwrap(), big);
        let welcome = Welcome {
            hub: Some("hub (via rrc-bouncer)".into()),
            ver: Some(int(1)),
            caps: Caps {
                resource_envelope: false,
                action: true,
                direct_notice: true,
            },
            limits: Limits {
                max_msg_body_bytes: 200,
                ..Limits::default()
            },
        };
        assert_eq!(Welcome::from_body(&welcome.to_body()), welcome);
        let hello = Hello {
            name: Some("rrc-bouncer".into()),
            ver: Some(int(1)),
            caps: Caps::default(),
            legacy_nick: None,
        };
        assert_eq!(Hello::from_body(&hello.to_body()), hello);
        let res = ResourceEnvelope {
            id: vec![1, 2],
            kind: "notice".into(),
            size: 9,
            sha256: None,
            encoding: None,
        };
        assert_eq!(ResourceEnvelope::from_body(&res.to_body()), Some(res));
    }

    type Map = Vec<(Value, Value)>;

    #[test]
    fn what_rrcd_refuses_is_refused() {
        let msg = fixture("msg");
        let encode_with = |change: &dyn Fn(&mut Map)| {
            let mut map: Map = match Raw::from_bytes(&msg.encode()).unwrap().value() {
                Value::Map(m) => m,
                _ => unreachable!(),
            };
            change(&mut map);
            Raw::from_value(&Value::Map(map)).as_bytes().to_vec()
        };
        let set = |key: u64, value: Value| {
            encode_with(&move |m: &mut Map| {
                m.retain(|(k, _)| *k != int(key));
                m.push((int(key), value.clone()));
            })
        };
        let remove = |key: u64| encode_with(&move |m: &mut Map| m.retain(|(k, _)| *k != int(key)));
        for key in [K_V, K_T, K_ID, K_TS, K_SRC] {
            assert!(Envelope::decode(&remove(key)).is_err(), "without {key}");
        }
        assert!(Envelope::decode(&set(K_V, int(2))).is_err());
        assert!(Envelope::decode(&set(K_T, Value::Text("20".into()))).is_err());
        assert!(Envelope::decode(&set(K_T, Value::Integer((-1).into()))).is_err());
        assert!(Envelope::decode(&set(K_ID, Value::Text("id".into()))).is_err());
        assert!(Envelope::decode(&set(K_TS, Value::Integer((-5).into()))).is_err());
        assert!(Envelope::decode(&set(K_TS, Value::Float(1.0))).is_err());
        assert!(Envelope::decode(&set(K_SRC, Value::Text("me".into()))).is_err());
        assert!(Envelope::decode(&set(K_ROOM, Value::Bytes(b"lobby".to_vec()))).is_err());
        assert!(Envelope::decode(&set(K_NICK, int(7))).is_err());
        assert!(Envelope::decode(&set(K_DST, Value::Text("x".into()))).is_err());
        let text_key = encode_with(&|m| m.push((Value::Text("x".into()), Value::Null)));
        assert!(Envelope::decode(&text_key).is_err());
        let negative_key = encode_with(&|m| m.push((Value::Integer((-1).into()), Value::Null)));
        assert!(Envelope::decode(&negative_key).is_err());
        let twice = encode_with(&|m| m.push((int(K_ROOM), Value::Text("other".into()))));
        assert!(Envelope::decode(&twice).is_err());

        let bytes = msg.encode();
        assert!(Envelope::decode(&bytes[..bytes.len() - 1]).is_err());
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(Envelope::decode(&trailing).is_err());
        assert!(Envelope::decode(&[]).is_err());
        assert!(Envelope::decode(&[0x80]).is_err());
        assert!(Envelope::decode(&Raw::text("hi").0).is_err());
        // A body nested past the limit, and one claiming more items than
        // there are bytes.
        let mut deep = bytes.clone();
        deep[0] += 1;
        deep.push(0x18);
        deep.push(0x50);
        deep.extend(std::iter::repeat_n(0x81, 200));
        deep.push(0x00);
        assert!(Envelope::decode(&deep).is_err());
        let mut huge = bytes;
        huge[0] += 1;
        huge.extend_from_slice(&[0x18, 0x50, 0x9b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        assert!(Envelope::decode(&huge).is_err());
        // Bad UTF-8 in an unknown key's text.
        let mut bad_text = fixture("msg").encode();
        bad_text[0] += 1;
        bad_text.extend_from_slice(&[0x18, 0x50, 0x62, 0xff, 0xfe]);
        assert!(Envelope::decode(&bad_text).is_err());
    }
}
