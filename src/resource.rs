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

use crate::envelope::{Envelope, MessageType, Raw, ResourceEnvelope, is_command, rrcd_space};

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
/// How long an advertised Resource, or a finished one that matches no
/// RESOURCE_ENVELOPE, waits for the envelope that announces it, which
/// comes on another channel and may be read later.
pub const OFFER_GRACE: Duration = Duration::from_secs(5);
/// Finished Resources of one link waiting for their envelope, most, and
/// their bytes, most.
pub const MAX_EARLY: usize = MAX_EXPECTED;
pub const MAX_EARLY_BYTES: usize = 2 * MAX_RESOURCE_BYTES as usize;
/// The most NOTICEs a client's long notice is sent upstream as.
pub const MAX_PARTS: usize = 16;
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

/// What a part that rrcd would take as a command starts with: U+200B ZERO
/// WIDTH SPACE. rrcd has no escape for a leading "/", and strips
/// whitespace before it looks, but U+200B is not whitespace to it, and
/// clients show it as nothing.
pub const COMMAND_GUARD: char = '\u{200b}';

/// Text cut into parts of at most `max` bytes, in order, on character
/// boundaries. A part ends at the last newline of its second half, or
/// else at its last whitespace, or else where it is full; the newline or
/// whitespace it ends at is left out, and so is a part that would be
/// only whitespace. A cut that would start the next part with what rrcd
/// takes as a command (`envelope::is_command`) is passed over for the next
/// best; a part that starts so all the same (the first, or one no cut
/// spares) starts with `COMMAND_GUARD`, which counts in its `max`. `max`
/// is at least 7 bytes: a guard and any one character. `fill`: cut where
/// each part is full, ignoring whitespace.
pub fn split_text(text: &str, max: usize, fill: bool) -> Vec<String> {
    let guard_len = COMMAND_GUARD.len_utf8();
    let max = max.max(4 + guard_len);
    let mut out = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        let guarded = is_command(rest);
        let cap = if guarded { max - guard_len } else { max };
        let push = |out: &mut Vec<String>, part: &str| {
            if part.trim_matches(rrcd_space).is_empty() {
                return;
            }
            out.push(if guarded {
                format!("{COMMAND_GUARD}{part}")
            } else {
                part.to_string()
            });
        };
        if rest.len() <= cap {
            push(&mut out, rest);
            break;
        }
        let mut full = cap;
        while !rest.is_char_boundary(full) {
            full -= 1;
        }
        let window = &rest[..full];
        // (end of this part, start of the next), best first.
        let mut cuts: Vec<(usize, usize)> = Vec::new();
        if !fill {
            cuts.extend(
                window
                    .char_indices()
                    .rev()
                    .filter(|(i, c)| *c == '\n' && *i >= cap / 2)
                    .map(|(i, _)| (i, i + 1)),
            );
            cuts.extend(
                window
                    .char_indices()
                    .rev()
                    .filter(|(i, c)| rrcd_space(*c) && *i > 0)
                    .map(|(i, c)| (i, i + c.len_utf8())),
            );
        }
        cuts.push((full, full));
        cuts.extend(window.char_indices().rev().filter(|(i, _)| *i > 0).map(|(i, _)| (i, i)));
        let (end, next) = cuts
            .iter()
            .copied()
            .find(|(_, next)| !is_command(&rest[*next..]))
            .unwrap_or((full, full));
        push(&mut out, rest[..end].strip_suffix('\r').unwrap_or(&rest[..end]));
        rest = &rest[next..];
    }
    out
}

/// The NOTICEs a client's long notice goes upstream as: each body at
/// most `max_body` bytes and each NOTICE, as the hub sends it on, within
/// one packet, in order, each with a new id; None when that takes more
/// than `MAX_PARTS`. Also the most bytes `MAX_PARTS` NOTICEs carry.
///
/// The hub sends a NOTICE on with its own source, the sender's peer hash
/// (16 bytes), in place of the client's, the room trimmed and lowercased,
/// and the nick the notice carries; ids are 8 bytes; time and the rest go
/// as they came. The room left is reckoned on that.
pub fn parts(notice: &Envelope, max_body: usize) -> (Option<Vec<Envelope>>, usize) {
    let text = notice.text_body().unwrap_or_default();
    let mut echo = notice.clone();
    echo.id = vec![0; 8];
    echo.src = vec![0; notice.src.len().max(16)];
    echo.room = notice.room.as_ref().map(|r| {
        let hub = r.trim().to_lowercase();
        if hub.len() > r.len() { hub } else { r.clone() }
    });
    echo.body = Some(Raw::text(""));
    // A text item's head grows from one byte to three past 23 bytes.
    let room = LINK_MDU.saturating_sub(echo.encode().len() + 2).max(4);
    let max = max_body.min(room).max(7);
    let most = max * MAX_PARTS;
    if text.len() > most {
        return (None, most);
    }
    let mut texts = split_text(&text, max, false);
    if texts.len() > MAX_PARTS {
        texts = split_text(&text, max, true);
    }
    if texts.len() > MAX_PARTS {
        return (None, most);
    }
    let parts = texts
        .iter()
        .map(|t| {
            let mut part = notice.clone();
            part.id = rand::random::<[u8; 8]>().to_vec();
            part.body = Some(Raw::text(t));
            part
        })
        .collect();
    (Some(parts), most)
}

/// A RESOURCE_ENVELOPE waiting for its Resource.
#[derive(Debug, Clone)]
pub struct Expected {
    pub envelope: Envelope,
    pub body: ResourceEnvelope,
    pub at: Instant,
}

/// The RESOURCE_ENVELOPEs of one link that wait for their Resources, in
/// the order they came, and the finished Resources that wait (for
/// `OFFER_GRACE`, within `MAX_EARLY` and `MAX_EARLY_BYTES`) for an
/// envelope that matches them, which may be read after them.
#[derive(Debug, Default)]
pub struct Expectations {
    waiting: Vec<Expected>,
    early: Vec<(Vec<u8>, Instant)>,
}

/// A finished Resource and the envelope that announced it.
pub type Verified = (Expected, Vec<u8>);

/// `MAX_EXPECTED` envelopes already wait for their Resources.
#[derive(Debug, PartialEq, Eq)]
pub struct TooMany;

impl Expectations {
    /// Envelopes waiting.
    pub fn len(&self) -> usize {
        self.waiting.len()
    }

    pub fn is_empty(&self) -> bool {
        self.waiting.is_empty() && self.early.is_empty()
    }

    /// Finished Resources waiting for their envelope.
    pub fn early(&self) -> usize {
        self.early.len()
    }

    /// Wait for an announced Resource: Err (nothing kept) when
    /// `MAX_EXPECTED` wait already; a finished Resource it announced, if
    /// one waits.
    pub fn add(
        &mut self,
        envelope: Envelope,
        body: ResourceEnvelope,
        now: Instant,
    ) -> Result<Option<Verified>, TooMany> {
        if self.waiting.len() >= MAX_EXPECTED {
            return Err(TooMany);
        }
        let expected = Expected {
            envelope,
            body,
            at: now,
        };
        // Only the new envelope can match a Resource that waits.
        let hit = self.early.iter().position(|(data, _)| {
            data.len() as u64 == expected.body.size && expected.body.sha256.as_ref().is_none_or(|s| *s == sha256(data))
        });
        match hit {
            Some(pos) => {
                let (data, _) = self.early.remove(pos);
                Ok(Some((expected, data)))
            }
            None => {
                self.waiting.push(expected);
                Ok(None)
            }
        }
    }

    /// Whether an advertised Resource of `size` bytes is announced.
    pub fn expects(&self, size: u64) -> bool {
        self.waiting.iter().any(|e| e.body.size == size)
    }

    /// The oldest envelope of the data's size whose sha256 is the data's
    /// (or that gave none), taken off the list.
    fn verified(&mut self, data: &[u8]) -> Option<Expected> {
        let size = data.len() as u64;
        if !self.expects(size) {
            return None;
        }
        let hash = sha256(data);
        let pos = self
            .waiting
            .iter()
            .position(|e| e.body.size == size && e.body.sha256.as_ref().is_none_or(|s| *s == hash))?;
        Some(self.waiting.remove(pos))
    }

    /// A finished Resource: Ok(Some) with the envelope that announced it;
    /// Ok(None) when kept to wait for one; Err (with why) when it cannot
    /// wait, which drops it.
    pub fn take(&mut self, data: Vec<u8>, now: Instant) -> Result<Option<Verified>, String> {
        if let Some(e) = self.verified(&data) {
            return Ok(Some((e, data)));
        }
        let bytes: usize = self.early.iter().map(|(d, _)| d.len()).sum();
        if self.early.len() >= MAX_EARLY || bytes + data.len() > MAX_EARLY_BYTES {
            return Err(format!(
                "{} resources wait for their RESOURCE_ENVELOPE already",
                self.early.len()
            ));
        }
        self.early.push((data, now));
        Ok(None)
    }

    /// Time passing. Finished Resources that waited `OFFER_GRACE` are
    /// dropped, with why: one an envelope of its size announced with
    /// another sha256 takes that envelope with it. Envelopes whose
    /// Resource has not come in `RESOURCE_TIMEOUT`, and is not on its way
    /// (`busy`, by size), are forgotten: how many.
    pub fn expire(&mut self, now: Instant, busy: impl Fn(u64) -> bool) -> (Vec<String>, usize) {
        let mut dropped = Vec::new();
        let (late, early): (Vec<_>, Vec<_>) = std::mem::take(&mut self.early)
            .into_iter()
            .partition(|(_, at)| now.saturating_duration_since(*at) >= OFFER_GRACE);
        self.early = early;
        for (data, _) in late {
            let size = data.len() as u64;
            match self.waiting.iter().position(|e| e.body.size == size) {
                Some(pos) => {
                    self.waiting.remove(pos);
                    dropped.push(format!(
                        "a resource of {size} bytes is not the sha256 its RESOURCE_ENVELOPE gave"
                    ));
                }
                None => dropped.push(format!("no RESOURCE_ENVELOPE announced a resource of {size} bytes")),
            }
        }
        let before = self.waiting.len();
        self.waiting
            .retain(|e| now.saturating_duration_since(e.at) < RESOURCE_TIMEOUT || busy(e.body.size));
        (dropped, before - self.waiting.len())
    }

    /// Forget everything: how many finished Resources that drops.
    pub fn clear(&mut self) -> usize {
        self.waiting.clear();
        std::mem::take(&mut self.early).len()
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
        assert!(matches!(
            e.add(env.clone(), body(11, Some(sha256(b"other thing"))), now),
            Ok(None)
        ));
        assert!(matches!(
            e.add(env.clone(), body(11, Some(sha256(&data))), now),
            Ok(None)
        ));
        assert!(e.expects(11) && !e.expects(12));
        // The second envelope is the one whose sha256 matches.
        let (got, back) = e.take(data.clone(), now).unwrap().unwrap();
        assert_eq!(got.body.sha256, Some(sha256(&data)));
        assert_eq!(back, data);
        // The other is not this data's: it waits as an early Resource,
        // and at OFFER_GRACE goes as a mismatch, taking the envelope.
        assert!(matches!(e.take(data.clone(), now), Ok(None)));
        assert_eq!((e.len(), e.early()), (1, 1));
        let (dropped, forgotten) = e.expire(now + OFFER_GRACE, |_| false);
        assert_eq!(dropped.len(), 1);
        assert!(dropped[0].contains("sha256"), "{dropped:?}");
        assert_eq!(forgotten, 0);
        assert!(e.is_empty());
        for _ in 0..MAX_EXPECTED {
            assert!(matches!(e.add(env.clone(), body(5, None), now), Ok(None)));
        }
        assert!(e.add(env.clone(), body(5, None), now).is_err());
        assert!(e.take(b"12345".to_vec(), now).unwrap().is_some());
        // An envelope whose Resource is on its way is not forgotten.
        let (dropped, forgotten) = e.expire(now + RESOURCE_TIMEOUT, |size| size == 5);
        assert_eq!((dropped.len(), forgotten), (0, 0));
        let (dropped, forgotten) = e.expire(now + RESOURCE_TIMEOUT, |_| false);
        assert_eq!((dropped.len(), forgotten), (0, MAX_EXPECTED - 1));
        assert!(e.is_empty());
    }

    #[test]
    fn a_resource_that_comes_before_its_envelope_waits_for_it() {
        let now = Instant::now();
        let data = b"early bird".to_vec();
        let env = Envelope::new(MessageType::ResourceEnvelope, &SRC);
        let mut e = Expectations::default();
        assert!(matches!(e.take(data.clone(), now), Ok(None)));
        assert_eq!(e.early(), 1);
        // An envelope of another size or sha256 does not take it.
        assert!(matches!(e.add(env.clone(), body(3, None), now), Ok(None)));
        assert!(matches!(
            e.add(env.clone(), body(10, Some(sha256(b"other bird"))), now),
            Ok(None)
        ));
        let (got, back) = e.add(env.clone(), body(10, Some(sha256(&data))), now).unwrap().unwrap();
        assert_eq!((got.body.size, back), (10, data.clone()));
        assert_eq!((e.len(), e.early()), (2, 0));
        // Bounded by count and bytes; what does not fit is dropped.
        let mut e = Expectations::default();
        for i in 0..MAX_EARLY {
            assert!(matches!(e.take(vec![i as u8; 4], now), Ok(None)));
        }
        assert!(e.take(vec![0; 4], now).is_err());
        let mut e = Expectations::default();
        assert!(matches!(e.take(vec![0; MAX_EARLY_BYTES - 1], now), Ok(None)));
        assert!(e.take(vec![0; 2], now).is_err());
        // One that waited OFFER_GRACE for nothing is dropped, once.
        let (dropped, _) = e.expire(now + OFFER_GRACE, |_| false);
        assert_eq!(dropped.len(), 1);
        assert!(dropped[0].contains("no RESOURCE_ENVELOPE"), "{dropped:?}");
        assert_eq!(e.expire(now + OFFER_GRACE, |_| false).0.len(), 0);
        // clear() says how many finished Resources it dropped.
        let mut e = Expectations::default();
        e.add(env.clone(), body(7, None), now).unwrap();
        e.take(vec![1; 3], now).unwrap();
        assert_eq!(e.clear(), 1);
        assert!(e.is_empty());
    }

    #[test]
    fn text_is_cut_on_whitespace_in_order() {
        let text = "alpha beta gamma delta epsilon";
        let parts = split_text(text, 12, false);
        assert_eq!(parts, vec!["alpha beta", "gamma delta", "epsilon"]);
        // A newline in the second half is the better cut.
        let parts = split_text("one two six\nthree four", 16, false);
        assert_eq!(parts, vec!["one two six", "three four"]);
        // Without whitespace, or filling, a part is full; multi-byte
        // characters are never cut.
        let text = "ä".repeat(10);
        let parts = split_text(&text, 7, false);
        assert_eq!(parts.iter().map(|p| p.len()).collect::<Vec<_>>(), [6, 6, 6, 2]);
        assert_eq!(parts.concat(), text);
        let parts = split_text("aaaa bbbb cccc", 7, true);
        assert_eq!(parts.concat(), "aaaa bbbb cccc");
        assert!(parts.iter().all(|p| p.len() == 7));
        // No part starts with what rrcd takes as a command: later parts
        // are cut elsewhere, the first is guarded.
        let parts = split_text("/me waves at /everyone /here and /there", 14, false);
        assert!(parts.iter().all(|p| !is_command(p) && p.len() <= 14), "{parts:?}");
        assert_eq!(parts[0].strip_prefix(COMMAND_GUARD).map(|p| &p[..3]), Some("/me"));
        let text: String = parts.concat().chars().filter(|c| *c != COMMAND_GUARD).collect();
        assert_eq!(
            text.replace(' ', ""),
            "/me waves at /everyone /here and /there".replace(' ', "")
        );
        // The whitespace a part is cut at is all that is lost.
        let text = "word ".repeat(100);
        let parts = split_text(text.trim_end(), 50, false);
        assert_eq!(parts.join(" "), text.trim_end());
    }

    #[test]
    fn no_part_is_a_command_to_the_hub() {
        // rrcd strips U+001C..U+001F and Unicode spaces before it looks.
        for lead in ["/", " /", "\u{1f}/", "\u{a0}\u{3000}/", "\n\t/"] {
            let text = format!("{lead}topic x {}", "y ".repeat(30));
            let parts = split_text(&text, 20, false);
            assert!(parts.iter().all(|p| !is_command(p)), "{lead:?}: {parts:?}");
            assert!(parts[0].starts_with(COMMAND_GUARD) && parts[0].len() <= 20, "{parts:?}");
            assert!(parts[1..].iter().all(|p| !p.starts_with(COMMAND_GUARD)), "{parts:?}");
        }
        // A cut that would start the next part with "/" is passed over,
        // across lines too.
        let text = "first line here\n/kick someone\n/ban them all";
        let parts = split_text(text, 18, false);
        assert!(parts.iter().all(|p| !is_command(p)), "{parts:?}");
        assert!(!parts[0].starts_with(COMMAND_GUARD));
        // Where every cut would: guarded, nothing lost.
        let text = "/".repeat(40);
        let parts = split_text(&text, 10, false);
        assert!(parts.iter().all(|p| !is_command(p) && p.len() <= 10), "{parts:?}");
        let back: String = parts.concat().chars().filter(|c| *c != COMMAND_GUARD).collect();
        assert_eq!(back, text);
        // Filling guards the same way.
        let parts = split_text(&format!("/x {}", "/y ".repeat(20)), 9, true);
        assert!(parts.iter().all(|p| !is_command(p) && p.len() <= 9), "{parts:?}");
        // A text that is no command is left as it is.
        assert_eq!(split_text("a/b", 10, false), vec!["a/b"]);
        assert_eq!(split_text("\u{200b}/x", 10, false), vec!["\u{200b}/x"]);
    }

    #[test]
    fn parts_fit_a_packet_as_the_hub_sends_them_on() {
        // A client with a short source and a mixed-case room: the hub sends
        // each part on with a 16-byte source and the room lowercased, and
        // the nick and time the part carries.
        let text = "word ".repeat(400);
        for src in [vec![1u8; 4], vec![2u8; 16], vec![3u8; 32]] {
            for room in ["Lobby", "  Lobby  ", "İİİİİİİİ"] {
                let mut notice = Envelope::new(MessageType::Notice, &src)
                    .with_room(room)
                    .with_nick(Some("a-long-nick-name"))
                    .with_body(Raw::text(text.trim_end()));
                notice.ts = u64::MAX;
                let (parts, _) = parts(&notice, 10_000);
                for p in parts.unwrap() {
                    let mut echo = p.clone();
                    echo.src = vec![9; 16];
                    echo.room = Some(room.trim().to_lowercase());
                    assert!(
                        echo.encode().len() <= LINK_MDU,
                        "{src:?} {room:?}: {}",
                        echo.encode().len()
                    );
                    assert!(p.encode().len() <= LINK_MDU);
                }
            }
        }
    }

    #[test]
    fn a_long_notice_becomes_ordered_notices_or_none() {
        let notice = |text: &str| {
            Envelope::new(MessageType::Notice, &SRC)
                .with_room("lobby")
                .with_nick(Some("nick"))
                .with_body(Raw::text(text))
        };
        let text = (0..200).map(|i| format!("w{i}")).collect::<Vec<_>>().join(" ");
        let (parts, most) = parts(&notice(&text), 350);
        let parts = parts.unwrap();
        assert!(parts.len() > 1 && parts.len() <= MAX_PARTS);
        assert!((350 * MAX_PARTS - 350..=350 * MAX_PARTS).contains(&most), "{most}");
        let mut ids = std::collections::HashSet::new();
        for p in &parts {
            assert!(p.text_body().unwrap().len() <= 350);
            // Room for a 16-byte K_SRC other than the client's.
            assert!(p.encode().len() <= LINK_MDU, "{}", p.encode().len());
            assert_eq!((p.t, p.room.as_deref()), (MessageType::Notice, Some("lobby")));
            assert!(ids.insert(p.id.clone()));
        }
        let joined: Vec<String> = parts.iter().map(|p| p.text_body().unwrap()).collect();
        assert_eq!(joined.join(" "), text);
        // A small max_msg_body_bytes makes smaller parts.
        let (small, _) = super::parts(&notice(&text), 100);
        assert!(small.unwrap().iter().all(|p| p.text_body().unwrap().len() <= 100));
        // More than MAX_PARTS NOTICEs' worth: refused, with the most.
        let long = "x".repeat(350 * MAX_PARTS + 1);
        let (none, most) = super::parts(&notice(&long), 350);
        assert!(none.is_none());
        assert!(most <= 350 * MAX_PARTS && long.len() > most);
        // Short words that do not pack into MAX_PARTS are cut full.
        let dense = "ab ".repeat(most / 3);
        let (filled, _) = super::parts(&notice(dense.trim_end()), 350);
        assert!(filled.unwrap().len() <= MAX_PARTS);
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
