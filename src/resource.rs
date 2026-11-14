// SPDX-License-Identifier: LicenseRef-Reticulum
//! Resources: text too long for one link packet, sent as a
//! RESOURCE_ENVELOPE followed by a Reticulum Resource on the same link
//! (DESIGN.md § Resources).
//!
//! What both sides of the bouncer share: the bounds, checking an envelope's
//! body as rrcd checks it, matching a finished Resource to the envelope that
//! announced it, and cutting the text into NOTICEs for a client that takes
//! no Resources.

use std::time::{Duration, Instant};

use crate::envelope::{Envelope, MessageType, Raw, ResourceEnvelope};

/// The largest Resource taken or sent, in bytes: rrcd's
/// `max_resource_bytes`, and what weechat-rrc, rBrowser2 and NomadNet
/// accept.
pub const MAX_RESOURCE_BYTES: u64 = 256 * 1024;
/// Announced Resources waiting on one link, most (rrcd's
/// `max_pending_resource_expectations`).
pub const MAX_EXPECTED: usize = 8;
/// How long an announced Resource may take to arrive, from its
/// RESOURCE_ENVELOPE to its last byte.
pub const RESOURCE_TIMEOUT: Duration = Duration::from_secs(120);
/// How long an advertised Resource waits for the RESOURCE_ENVELOPE that
/// announces it, which comes on another channel and may be read later.
pub const OFFER_GRACE: Duration = Duration::from_secs(5);
/// The kinds a RESOURCE_ENVELOPE names.
pub const KIND_NOTICE: &str = "notice";
pub const KIND_MOTD: &str = "motd";
/// A Reticulum link's MDU: the most one packet to a client carries.
pub const LINK_MDU: usize = 431;

pub fn sha256(data: &[u8]) -> Vec<u8> {
    rns_crypto::sha::sha256(data).to_vec()
}

/// A RESOURCE_ENVELOPE's body, checked as rrcd checks a client's
/// (`router.py` `_handle_resource_envelope`): Err is the text of the ERROR
/// rrcd answers with.
pub fn check(env: &Envelope) -> Result<ResourceEnvelope, String> {
    let Some(body) = &env.body else {
        return Err("invalid resource envelope body".into());
    };
    let value = body.value();
    let ciborium::Value::Map(map) = &value else {
        return Err("invalid resource envelope body".into());
    };
    let get = |key: u64| {
        map.iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Integer(i) if u64::try_from(*i).ok() == Some(key)))
            .map(|(_, v)| v)
    };
    if !matches!(get(0), Some(ciborium::Value::Bytes(_))) {
        return Err("resource envelope missing id".into());
    }
    if !matches!(get(1), Some(ciborium::Value::Text(k)) if !k.is_empty()) {
        return Err("resource envelope missing kind".into());
    }
    let size = match get(2) {
        Some(ciborium::Value::Integer(n)) => u64::try_from(*n).ok().filter(|n| *n > 0),
        _ => None,
    };
    let Some(size) = size else {
        return Err("resource envelope invalid size".into());
    };
    if size > MAX_RESOURCE_BYTES {
        return Err(format!("resource too large: {size} > {MAX_RESOURCE_BYTES}"));
    }
    if get(3).is_some_and(|v| !matches!(v, ciborium::Value::Bytes(b) if b.len() == 32)) {
        return Err("resource envelope invalid sha256".into());
    }
    ResourceEnvelope::from_body(body).ok_or_else(|| "invalid resource envelope body".into())
}

/// The text a Resource carries. Only UTF-8 is known; anything else is
/// taken as UTF-8, with what does not decode replaced.
pub fn text(data: &[u8]) -> String {
    String::from_utf8_lossy(data).into_owned()
}

/// The NOTICE a RESOURCE_ENVELOPE and its text stand for: the envelope's
/// id, time, source, room, destination and nick, the text as its body.
pub fn notice_of(env: &Envelope, text: &str) -> Envelope {
    Envelope {
        t: MessageType::Notice,
        id: env.id.clone(),
        ts: env.ts,
        src: env.src.clone(),
        room: env.room.clone(),
        body: Some(Raw::text(text)),
        nick: env.nick.clone(),
        dst: env.dst.clone(),
        extra: Vec::new(),
    }
}

/// A NOTICE too long for one packet as NOTICEs that each fit one: one or
/// more per line, cut on character boundaries, blank lines left out. Each
/// has a new id; the rest is the NOTICE's. A NOTICE that fits is returned
/// as it is.
pub fn split(notice: &Envelope) -> Vec<Envelope> {
    if notice.encode().len() <= LINK_MDU {
        return vec![notice.clone()];
    }
    let text = notice.text_body().unwrap_or_default();
    let mut empty = notice.clone();
    empty.body = Some(Raw::text(""));
    // A text item's head grows from one byte to three past 23 bytes, and
    // the new ids are the same length as an 8-byte id.
    let id_growth = 8usize.saturating_sub(notice.id.len());
    let room = LINK_MDU.saturating_sub(empty.encode().len() + 2 + id_growth).max(1);
    let mut out = Vec::new();
    for line in text.lines().map(|l| l.trim_end_matches('\r')) {
        let mut rest = line;
        while !rest.trim().is_empty() {
            let mut end = rest.len().min(room);
            while !rest.is_char_boundary(end) {
                end -= 1;
            }
            if end == 0 {
                // One character longer than a packet's room: never in UTF-8.
                break;
            }
            let mut chunk = notice.clone();
            chunk.id = rand::random::<[u8; 8]>().to_vec();
            chunk.body = Some(Raw::text(&rest[..end]));
            out.push(chunk);
            rest = &rest[end..];
        }
    }
    out
}

/// A RESOURCE_ENVELOPE waiting for its Resource.
#[derive(Debug, Clone)]
pub struct Expected {
    pub envelope: Envelope,
    pub body: ResourceEnvelope,
    pub at: Instant,
}

/// What a finished Resource was, matched against the envelopes that
/// announced Resources on its link.
#[derive(Debug)]
pub enum Matched {
    /// Its envelope; the size and sha256 agree.
    Verified(Expected),
    /// An envelope announced this size with another sha256: dropped with
    /// the envelope.
    Mismatch(Expected),
    /// Nothing announced it.
    Unmatched,
}

/// The RESOURCE_ENVELOPEs of one link that wait for their Resources, in
/// the order they came.
#[derive(Debug, Default)]
pub struct Expectations {
    waiting: Vec<Expected>,
}

impl Expectations {
    pub fn len(&self) -> usize {
        self.waiting.len()
    }

    pub fn is_empty(&self) -> bool {
        self.waiting.is_empty()
    }

    /// Wait for an announced Resource: false (nothing kept) when
    /// `MAX_EXPECTED` wait already.
    pub fn add(&mut self, envelope: Envelope, body: ResourceEnvelope, now: Instant) -> bool {
        if self.waiting.len() >= MAX_EXPECTED {
            return false;
        }
        self.waiting.push(Expected {
            envelope,
            body,
            at: now,
        });
        true
    }

    /// Whether an advertised Resource of `size` bytes is announced.
    pub fn expects(&self, size: u64) -> bool {
        self.waiting.iter().any(|e| e.body.size == size)
    }

    /// The envelope a finished Resource belongs to, taken off the list:
    /// the oldest of its size whose sha256 is the data's (or that gave
    /// none).
    pub fn take(&mut self, data: &[u8]) -> Matched {
        let size = data.len() as u64;
        let hash = sha256(data);
        let same_size = |e: &Expected| e.body.size == size;
        if let Some(pos) = self
            .waiting
            .iter()
            .position(|e| same_size(e) && e.body.sha256.as_ref().is_none_or(|s| *s == hash))
        {
            return Matched::Verified(self.waiting.remove(pos));
        }
        match self.waiting.iter().position(same_size) {
            Some(pos) => Matched::Mismatch(self.waiting.remove(pos)),
            None => Matched::Unmatched,
        }
    }

    /// Forget envelopes whose Resource did not come in `RESOURCE_TIMEOUT`:
    /// how many.
    pub fn expire(&mut self, now: Instant) -> usize {
        let before = self.waiting.len();
        self.waiting
            .retain(|e| now.saturating_duration_since(e.at) < RESOURCE_TIMEOUT);
        before - self.waiting.len()
    }
}

/// The RESOURCE_ENVELOPE that announces `data` from `src`: a new id, the
/// data's size and sha256, UTF-8.
pub fn announce(src: &[u8], kind: &str, room: Option<&str>, data: &[u8]) -> Envelope {
    let body = ResourceEnvelope {
        id: rand::random::<[u8; 8]>().to_vec(),
        kind: kind.into(),
        size: data.len() as u64,
        sha256: Some(sha256(data)),
        encoding: Some("utf-8".into()),
    };
    let mut env = Envelope::new(MessageType::ResourceEnvelope, src).with_body(body.to_body());
    env.room = room.map(String::from);
    env
}

#[cfg(test)]
mod tests {
    use super::*;

    const SRC: [u8; 16] = [3; 16];

    fn body(size: u64, sha: Option<Vec<u8>>) -> ResourceEnvelope {
        ResourceEnvelope {
            id: vec![1; 8],
            kind: KIND_NOTICE.into(),
            size,
            sha256: sha,
            encoding: None,
        }
    }

    #[test]
    fn checks_the_body_as_rrcd_does() {
        let env = |body: Option<Raw>| {
            let mut env = Envelope::new(MessageType::ResourceEnvelope, &SRC);
            env.body = body;
            env
        };
        let map = |pairs: Vec<(u64, ciborium::Value)>| {
            Some(Raw::from_value(&ciborium::Value::Map(
                pairs.into_iter().map(|(k, v)| (ciborium::Value::from(k), v)).collect(),
            )))
        };
        use ciborium::Value::{Bytes, Integer, Text};
        assert_eq!(check(&env(None)).unwrap_err(), "invalid resource envelope body");
        assert_eq!(
            check(&env(Some(Raw::text("x")))).unwrap_err(),
            "invalid resource envelope body"
        );
        assert_eq!(check(&env(map(vec![]))).unwrap_err(), "resource envelope missing id");
        let id = (0, Bytes(vec![1; 8]));
        let kind = (1, Text("notice".into()));
        assert_eq!(
            check(&env(map(vec![id.clone()]))).unwrap_err(),
            "resource envelope missing kind"
        );
        assert_eq!(
            check(&env(map(vec![id.clone(), kind.clone()]))).unwrap_err(),
            "resource envelope invalid size"
        );
        assert_eq!(
            check(&env(map(vec![id.clone(), kind.clone(), (2, Integer(300_000.into()))]))).unwrap_err(),
            "resource too large: 300000 > 262144"
        );
        assert_eq!(
            check(&env(map(vec![
                id.clone(),
                kind.clone(),
                (2, Integer(600.into())),
                (3, Bytes(vec![0; 5]))
            ])))
            .unwrap_err(),
            "resource envelope invalid sha256"
        );
        let ok = check(&env(map(vec![
            id,
            kind,
            (2, Integer(600.into())),
            (3, Bytes(vec![0; 32])),
        ])))
        .unwrap();
        assert_eq!(ok.size, 600);
    }

    #[test]
    fn matches_by_size_and_hash() {
        let now = Instant::now();
        let data = b"hello there".to_vec();
        let env = Envelope::new(MessageType::ResourceEnvelope, &SRC);
        let mut e = Expectations::default();
        assert!(e.add(env.clone(), body(11, Some(sha256(b"other thing"))), now));
        assert!(e.add(env.clone(), body(11, Some(sha256(&data))), now));
        assert!(e.expects(11) && !e.expects(12));
        assert!(matches!(e.take(&data), Matched::Verified(_)));
        assert!(matches!(e.take(&data), Matched::Mismatch(_)));
        assert!(matches!(e.take(&data), Matched::Unmatched));
        for _ in 0..MAX_EXPECTED {
            assert!(e.add(env.clone(), body(5, None), now));
        }
        assert!(!e.add(env.clone(), body(5, None), now));
        assert!(matches!(e.take(b"12345"), Matched::Verified(_)));
        assert_eq!(e.expire(now + RESOURCE_TIMEOUT), MAX_EXPECTED - 1);
        assert!(e.is_empty());
    }

    #[test]
    fn splits_into_notices_that_fit_a_packet() {
        let line = "ä".repeat(400);
        let text = format!("first line\n\n{line}\r\nlast");
        let notice = Envelope::new(MessageType::Notice, &SRC)
            .with_room("lobby")
            .with_nick(Some("nick"))
            .with_body(Raw::text(&text));
        let chunks = split(&notice);
        assert!(chunks.len() >= 4);
        for chunk in &chunks {
            assert!(chunk.encode().len() <= LINK_MDU, "{}", chunk.encode().len());
            assert_eq!(chunk.room.as_deref(), Some("lobby"));
            assert_eq!(chunk.nick.as_deref(), Some("nick"));
            assert_ne!(chunk.id, notice.id);
        }
        let joined: String = chunks.iter().map(|c| c.text_body().unwrap()).collect();
        assert_eq!(joined, format!("first line{line}last"));
        let short = notice.clone().with_body(Raw::text("short"));
        assert_eq!(split(&short), vec![short]);
    }
}
