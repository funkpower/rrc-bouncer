// SPDX-License-Identifier: LicenseRef-Reticulum
//! A hub's room history on disk.
//!
//! One append-only file per room, `state_dir/history/<upstream>/<room>.log`
//! (mode 0600, directories 0700): a CBOR sequence of records
//! `{"ts_ms": <receive time, ms since the epoch>, "envelope": <bytes>}`,
//! the envelope's bytes as the hub sent them. Every file operation runs on
//! one thread per hub, fed through a bounded queue the relay never waits
//! on: a record that finds the queue full is dropped and counted. Queries
//! go through the same queue, so a query sees every record sent before it.
//! See DESIGN.md § History.

use std::collections::{BTreeMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ciborium::Value;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::config::HistorySettings;
use crate::envelope::{Envelope, MessageType, now_ms};
use crate::identity::create_private_dir;

/// Jobs waiting for the history thread, most.
const QUEUE: usize = 4096;
/// How long a written record may sit in the page cache before it is synced.
const SYNC_EVERY: Duration = Duration::from_secs(5);
/// How often old records are pruned (and at start).
const PRUNE_EVERY: Duration = Duration::from_secs(3600);
/// Longest file name a room gets; a room whose encoded name is longer is
/// not recorded.
const MAX_FILE_NAME: usize = 255;
const SUFFIX: &str = ".log";
const TMP_SUFFIX: &str = ".tmp";

/// One hub's history in status.json. Every field has a default, so a
/// status file from an older bouncer still reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HistoryStatus {
    /// Bytes of history on disk.
    pub history_bytes: u64,
    /// Rooms with history on disk.
    pub history_rooms: u64,
    /// Records written since start.
    pub history_records: u64,
    /// Records not written: the queue was full, the room's file name too
    /// long, or the write failed.
    pub history_drops: u64,
}

pub type SharedHistoryStatus = Arc<Mutex<HistoryStatus>>;

/// A record read back: its receive time and the envelope's bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub ts_ms: u64,
    pub envelope: Vec<u8>,
}

/// What a query asks for: the room's newest `max` MSG, ACTION and NOTICE
/// records received at or after `since_ms`, oldest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    pub room: String,
    pub since_ms: Option<u64>,
    pub max: usize,
}

enum Job {
    Record {
        room: String,
        ts_ms: u64,
        envelope: Vec<u8>,
    },
    Query(Query, oneshot::Sender<Vec<Record>>),
    Stop(oneshot::Sender<()>),
}

/// The relay's handle on a hub's history thread. Cloning it is cheap; a
/// disabled one records nothing and answers every query with nothing.
#[derive(Clone)]
pub struct History {
    jobs: Option<SyncSender<Job>>,
    status: SharedHistoryStatus,
}

impl History {
    /// Start the history thread for the hub whose history is in `dir`.
    pub fn start(dir: PathBuf, settings: HistorySettings, status: SharedHistoryStatus) -> Result<Self, String> {
        create_private_dir(&dir)?;
        if let Some(parent) = dir.parent() {
            create_private_dir(parent)?;
        }
        let (tx, rx) = std::sync::mpsc::sync_channel(QUEUE);
        let mut store = Store {
            dir,
            settings,
            status: status.clone(),
            open: BTreeMap::new(),
            sizes: BTreeMap::new(),
            dirty: false,
        };
        std::thread::Builder::new()
            .name("history".into())
            .spawn(move || store.run(&rx))
            .map_err(|e| format!("could not start the history thread: {e}"))?;
        Ok(Self { jobs: Some(tx), status })
    }

    pub fn disabled() -> Self {
        Self {
            jobs: None,
            status: SharedHistoryStatus::default(),
        }
    }

    fn drop_one(&self) {
        if let Ok(mut s) = self.status.lock() {
            s.history_drops += 1;
        }
    }

    /// Keep `env` in `room`'s history, received now. Never waits.
    pub fn record(&self, room: &str, env: &Envelope) {
        self.record_at(room, now_ms(), env.encode());
    }

    pub fn record_at(&self, room: &str, ts_ms: u64, envelope: Vec<u8>) {
        let Some(jobs) = &self.jobs else { return };
        let job = Job::Record {
            room: room.to_string(),
            ts_ms,
            envelope,
        };
        match jobs.try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.drop_one();
                tracing::warn!("history: the queue is full: a record for {room} was dropped");
            }
            Err(TrySendError::Disconnected(_)) => self.drop_one(),
        }
    }

    /// Ask for records. The sender is dropped, unanswered, if the history
    /// is disabled or its queue is full: take that as no records.
    pub fn query(&self, query: Query) -> oneshot::Receiver<Vec<Record>> {
        let (tx, rx) = oneshot::channel();
        if let Some(jobs) = &self.jobs
            && let Err(e) = jobs.try_send(Job::Query(query, tx))
        {
            tracing::warn!("history: query not asked: {e}");
        }
        rx
    }

    /// Sync what was written and stop the thread; resolves when it has.
    pub async fn stop(&self) {
        let Some(jobs) = self.jobs.clone() else { return };
        let (tx, rx) = oneshot::channel();
        // The queue may be full: wait for room off the runtime's threads.
        let sent = tokio::task::spawn_blocking(move || jobs.send(Job::Stop(tx)).is_ok()).await;
        if sent.unwrap_or(false) {
            let _ = rx.await;
        }
    }
}

/// The file name of a room's history: bytes of `[a-z0-9._-]` as they are
/// (a `.` only after the first byte), every other byte as `%XX`, then
/// `.log`. None if that is longer than a file name may be.
pub fn file_name(room: &str) -> Option<String> {
    let mut name = String::with_capacity(room.len() + SUFFIX.len());
    for (i, b) in room.bytes().enumerate() {
        match b {
            b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' => name.push(char::from(b)),
            b'.' if i > 0 => name.push('.'),
            _ => name.push_str(&format!("%{b:02X}")),
        }
    }
    name.push_str(SUFFIX);
    (!room.is_empty() && name.len() <= MAX_FILE_NAME).then_some(name)
}

/// The room a history file name stands for (the inverse of `file_name`).
pub fn room_of(name: &str) -> Option<String> {
    let stem = name.strip_suffix(SUFFIX)?;
    let mut bytes = Vec::with_capacity(stem.len());
    let mut it = stem.bytes();
    while let Some(b) = it.next() {
        if b == b'%' {
            let hi = char::from(it.next()?).to_digit(16)?;
            let lo = char::from(it.next()?).to_digit(16)?;
            bytes.push(u8::try_from(hi * 16 + lo).ok()?);
        } else {
            bytes.push(b);
        }
    }
    let room = String::from_utf8(bytes).ok()?;
    (file_name(&room).as_deref() == Some(name)).then_some(room)
}

/// One record's bytes.
pub fn encode_record(ts_ms: u64, envelope: &[u8]) -> Vec<u8> {
    let value = Value::Map(vec![
        (Value::Text("ts_ms".into()), Value::Integer(ts_ms.into())),
        (Value::Text("envelope".into()), Value::Bytes(envelope.to_vec())),
    ]);
    let mut out = Vec::with_capacity(envelope.len() + 24);
    // Writing to a Vec cannot fail.
    let _ = ciborium::into_writer(&value, &mut out);
    out
}

fn decode_record(value: Value) -> Option<Record> {
    let Value::Map(entries) = value else { return None };
    let mut ts_ms = None;
    let mut envelope = None;
    for (k, v) in entries {
        match (k.as_text(), v) {
            (Some("ts_ms"), Value::Integer(i)) => ts_ms = u64::try_from(i).ok(),
            (Some("envelope"), Value::Bytes(b)) => envelope = Some(b),
            _ => {}
        }
    }
    Some(Record {
        ts_ms: ts_ms?,
        envelope: envelope?,
    })
}

/// The records in `data` with where each ends, and the length of the good
/// part: reading stops at the first record that does not decode (a write
/// cut short).
pub fn read_records(data: &[u8]) -> (Vec<(Record, usize)>, usize) {
    let mut records = Vec::new();
    let mut rest = data;
    let mut good = 0;
    while !rest.is_empty() {
        let Ok(value) = ciborium::from_reader::<Value, _>(&mut rest) else {
            break;
        };
        let Some(record) = decode_record(value) else {
            break;
        };
        good = data.len() - rest.len();
        records.push((record, good));
    }
    (records, good)
}

/// Whether a record is replayed: MSG, ACTION and NOTICE are; JOINED,
/// PARTED and ERROR would change the client's idea of the room.
fn replayable(envelope: &[u8]) -> bool {
    Envelope::decode(envelope)
        .is_ok_and(|e| matches!(e.t, MessageType::Msg | MessageType::Action | MessageType::Notice))
}

/// The answer to `query` from a room file's contents.
pub fn select(data: &[u8], query: &Query) -> Vec<Record> {
    let mut kept = VecDeque::new();
    if query.max == 0 {
        return Vec::new();
    }
    for (record, _) in read_records(data).0 {
        if query.since_ms.is_some_and(|since| record.ts_ms < since) || !replayable(&record.envelope) {
            continue;
        }
        if kept.len() == query.max {
            kept.pop_front();
        }
        kept.push_back(record);
    }
    kept.into()
}

/// Write `data` to `path` through a temporary file (mode 0600) and a
/// rename.
fn rewrite(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(TMP_SUFFIX);
    let tmp = PathBuf::from(tmp);
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(&tmp)?;
    file.write_all(data)?;
    file.sync_data()?;
    drop(file);
    std::fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent()
        && let Ok(dir) = File::open(dir)
    {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// A room file, scanned: its records' times and ends, and the length of
/// its good part.
struct Scan {
    path: PathBuf,
    len: usize,
    good: usize,
    records: Vec<(u64, usize)>,
}

/// The part of each file kept by retention: the index of its first kept
/// record. Records older than `cutoff_ms` go; then, while the total is over
/// `max_bytes`, the oldest records of the hub go, whichever room they are
/// in.
pub fn prune_plan(files: &[Vec<(u64, usize)>], cutoff_ms: u64, max_bytes: u64) -> Vec<usize> {
    let mut first: Vec<usize> = files
        .iter()
        .map(|records| records.iter().take_while(|(ts, _)| *ts < cutoff_ms).count())
        .collect();
    let size = |records: &[(u64, usize)], from: usize| -> u64 {
        let start = if from == 0 { 0 } else { records[from - 1].1 };
        records.last().map_or(0, |(_, end)| (end - start.min(*end)) as u64)
    };
    let mut total: u64 = files.iter().zip(&first).map(|(r, f)| size(r, *f)).sum();
    while total > max_bytes {
        // The file whose next kept record is oldest.
        let Some((i, _)) = files
            .iter()
            .enumerate()
            .filter(|(i, r)| first[*i] < r.len())
            .min_by_key(|(i, r)| r[first[*i]].0)
        else {
            break;
        };
        let records = &files[i];
        let start = if first[i] == 0 { 0 } else { records[first[i] - 1].1 };
        total -= (records[first[i]].1 - start) as u64;
        first[i] += 1;
    }
    first
}

struct Store {
    dir: PathBuf,
    settings: HistorySettings,
    status: SharedHistoryStatus,
    /// Room files open for appending.
    open: BTreeMap<String, File>,
    /// Each room file's size, by file name.
    sizes: BTreeMap<String, u64>,
    /// Written since the last sync.
    dirty: bool,
}

impl Store {
    fn run(&mut self, jobs: &Receiver<Job>) {
        self.prune();
        let mut next_prune = Instant::now() + PRUNE_EVERY;
        let mut next_sync: Option<Instant> = None;
        loop {
            let now = Instant::now();
            let due = next_sync.map_or(next_prune, |s| s.min(next_prune));
            let job = match jobs.recv_timeout(due.saturating_duration_since(now)) {
                Ok(job) => Some(job),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => break,
            };
            match job {
                Some(Job::Record { room, ts_ms, envelope }) => {
                    self.write(&room, ts_ms, &envelope);
                    if self.dirty && next_sync.is_none() {
                        next_sync = Some(Instant::now() + SYNC_EVERY);
                    }
                }
                Some(Job::Query(query, reply)) => {
                    let _ = reply.send(self.query(&query));
                }
                Some(Job::Stop(ack)) => {
                    self.sync();
                    let _ = ack.send(());
                    return;
                }
                None => {}
            }
            let now = Instant::now();
            if next_sync.is_some_and(|s| now >= s) {
                self.sync();
                next_sync = None;
            }
            if now >= next_prune {
                self.prune();
                next_prune = now + PRUNE_EVERY;
            }
        }
        self.sync();
    }

    fn show(&self, f: impl FnOnce(&mut HistoryStatus)) {
        if let Ok(mut s) = self.status.lock() {
            f(&mut s);
        }
    }

    fn show_sizes(&self) {
        let bytes = self.sizes.values().sum();
        let rooms = self.sizes.values().filter(|s| **s > 0).count() as u64;
        self.show(|s| {
            s.history_bytes = bytes;
            s.history_rooms = rooms;
        });
    }

    fn write(&mut self, room: &str, ts_ms: u64, envelope: &[u8]) {
        let Some(name) = file_name(room) else {
            tracing::warn!("history: room name too long for a file name: not recorded");
            return self.show(|s| s.history_drops += 1);
        };
        let path = self.dir.join(&name);
        if !self.open.contains_key(&name) {
            let mut options = OpenOptions::new();
            options.append(true).create(true);
            #[cfg(unix)]
            std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
            match options.open(&path) {
                Ok(file) => {
                    let len = file.metadata().map_or(0, |m| m.len());
                    self.sizes.insert(name.clone(), len);
                    self.open.insert(name.clone(), file);
                }
                Err(e) => {
                    tracing::warn!("history: {}: {e}", path.display());
                    return self.show(|s| s.history_drops += 1);
                }
            }
        }
        let record = encode_record(ts_ms, envelope);
        let len = self.sizes.get(&name).copied().unwrap_or(0);
        let Some(file) = self.open.get_mut(&name) else { return };
        match file.write_all(&record) {
            Ok(()) => {
                self.dirty = true;
                self.sizes.insert(name, len + record.len() as u64);
                self.show_sizes();
                self.show(|s| s.history_records += 1);
            }
            Err(e) => {
                tracing::warn!("history: {}: {e}", path.display());
                // Take a part written back off, so the file still reads.
                let _ = file.set_len(len);
                self.open.remove(&name);
                self.show(|s| s.history_drops += 1);
            }
        }
    }

    fn sync(&mut self) {
        if std::mem::take(&mut self.dirty) {
            for file in self.open.values() {
                let _ = file.sync_data();
            }
        }
    }

    fn query(&mut self, query: &Query) -> Vec<Record> {
        let Some(name) = file_name(&query.room) else {
            return Vec::new();
        };
        match std::fs::read(self.dir.join(name)) {
            Ok(data) => select(&data, query),
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!("history: {}: {e}", query.room);
                }
                Vec::new()
            }
        }
    }

    fn scan(path: PathBuf) -> std::io::Result<Scan> {
        let data = std::fs::read(&path)?;
        let (records, good) = read_records(&data);
        Ok(Scan {
            path,
            len: data.len(),
            good,
            records: records.into_iter().map(|(r, end)| (r.ts_ms, end)).collect(),
        })
    }

    /// Drop what retention says to, repair files a write left cut short,
    /// and remove temporary files a rewrite left behind.
    fn prune(&mut self) {
        self.sync();
        self.open.clear();
        self.sizes.clear();
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("history: {}: {e}", self.dir.display());
                return;
            }
        };
        let mut scans = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(TMP_SUFFIX) {
                let _ = std::fs::remove_file(&path);
                continue;
            }
            if room_of(&name).is_none() || !entry.file_type().is_ok_and(|t| t.is_file()) {
                continue;
            }
            match Self::scan(path) {
                Ok(scan) => scans.push(scan),
                Err(e) => tracing::warn!("history: {name}: {e}"),
            }
        }
        let cutoff = now_ms().saturating_sub(u64::try_from(self.settings.retention.as_millis()).unwrap_or(u64::MAX));
        let indexes: Vec<Vec<(u64, usize)>> = scans.iter().map(|s| s.records.clone()).collect();
        let plan = prune_plan(&indexes, cutoff, self.settings.max_bytes);
        let mut dropped = 0;
        for (scan, first) in scans.iter().zip(plan) {
            let start = if first == 0 { 0 } else { scan.records[first - 1].1 };
            let name = scan
                .path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            dropped += first;
            if start == 0 && scan.good == scan.len {
                self.sizes.insert(name, scan.len as u64);
                continue;
            }
            if scan.good < scan.len {
                tracing::warn!(
                    "history: {name}: {} bytes after the last whole record taken off",
                    scan.len - scan.good
                );
            }
            let result = if start >= scan.good {
                std::fs::remove_file(&scan.path)
            } else {
                std::fs::read(&scan.path).and_then(|data| rewrite(&scan.path, &data[start..scan.good]))
            };
            match result {
                Ok(()) if start < scan.good => {
                    self.sizes.insert(name, (scan.good - start) as u64);
                }
                Ok(()) => {}
                Err(e) => {
                    tracing::warn!("history: {name}: {e}");
                    self.sizes.insert(name, scan.len as u64);
                }
            }
        }
        if dropped > 0 {
            tracing::info!("history: pruned {dropped} records");
        }
        self.show_sizes();
    }
}

/// `ts_ms` as UTC ISO-8601 to the second, `2026-11-13T09:05:00Z`.
pub fn utc(ts_ms: u64) -> String {
    let secs = ts_ms / 1000;
    let days = i64::try_from(secs / 86_400).unwrap_or(0);
    let rem = secs % 86_400;
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(body: &str) -> Vec<u8> {
        Envelope::new(MessageType::Msg, &[9; 16])
            .with_room("lobby")
            .with_body(crate::envelope::Raw::text(body))
            .encode()
    }

    fn settings(days: u64, max_bytes: u64) -> HistorySettings {
        HistorySettings {
            retention: Duration::from_secs(days * 86_400),
            max_bytes,
            replay_window: Duration::from_secs(3600),
            replay_max: 100,
        }
    }

    #[test]
    fn room_file_names() {
        assert_eq!(file_name("lobby").as_deref(), Some("lobby.log"));
        assert_eq!(file_name("a.b_c-9").as_deref(), Some("a.b_c-9.log"));
        assert_eq!(file_name("..").as_deref(), Some("%2E..log"));
        assert_eq!(file_name(".x").as_deref(), Some("%2Ex.log"));
        assert_eq!(file_name("a/b").as_deref(), Some("a%2Fb.log"));
        assert_eq!(file_name("50%").as_deref(), Some("50%25.log"));
        assert_eq!(file_name("Ab c").as_deref(), Some("%41b%20c.log"));
        assert_eq!(file_name("é").as_deref(), Some("%C3%A9.log"));
        assert_eq!(file_name(""), None);
        assert_eq!(file_name(&"/".repeat(84)), None);
        assert!(file_name(&"/".repeat(83)).is_some());
        for room in ["lobby", "..", ".x", "a/b", "50%", "é ü", "a..b"] {
            let name = file_name(room).unwrap();
            assert!(!name.contains('/') && !name.starts_with('.'), "{name}");
            assert_eq!(room_of(&name).as_deref(), Some(room));
        }
        assert_eq!(room_of("lobby.log.tmp"), None);
        assert_eq!(room_of("%2e.log"), None);
        assert_eq!(room_of("A.log"), None);
    }

    #[test]
    fn records_read_back_and_a_cut_record_ends_the_file() {
        let two = msg("two");
        let mut data = encode_record(1, &msg("one"));
        let first = data.len();
        data.extend(encode_record(2, &two));
        let whole = data.len();
        let (records, good) = read_records(&data);
        assert_eq!(good, whole);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].1, first);
        assert_eq!(
            records[1].0,
            Record {
                ts_ms: 2,
                envelope: two
            }
        );
        let third = encode_record(3, &msg("three"));
        for cut in 1..third.len() {
            let mut cut_data = data.clone();
            cut_data.extend(&third[..cut]);
            let (records, good) = read_records(&cut_data);
            assert_eq!((records.len(), good), (2, whole), "cut at {cut}");
        }
        // Garbage that is CBOR but not a record ends it too.
        let mut bad = data.clone();
        bad.push(0x01);
        assert_eq!(read_records(&bad).1, whole);
    }

    #[test]
    fn select_keeps_the_newest_messages_since() {
        let joined = Envelope::new(MessageType::Joined, &[9; 16]).with_room("lobby").encode();
        let mut data = Vec::new();
        for ts in 1..=10u64 {
            data.extend(encode_record(ts * 1000, &msg(&ts.to_string())));
            data.extend(encode_record(ts * 1000 + 1, &joined));
        }
        let q = |since, max| Query {
            room: "lobby".into(),
            since_ms: since,
            max,
        };
        let ts = |r: Vec<Record>| r.iter().map(|r| r.ts_ms / 1000).collect::<Vec<_>>();
        assert_eq!(ts(select(&data, &q(None, 3))), vec![8, 9, 10]);
        assert_eq!(ts(select(&data, &q(Some(8500), 100))), vec![9, 10]);
        assert_eq!(ts(select(&data, &q(Some(1000), 100))).len(), 10);
        assert!(select(&data, &q(None, 0)).is_empty());
    }

    #[test]
    fn prune_drops_old_records_then_the_hubs_oldest_over_the_size() {
        // Two files: (ts, end offset).
        let a = vec![(1, 10), (4, 20), (6, 30)];
        let b = vec![(2, 10), (3, 20), (5, 30)];
        let files = vec![a, b];
        assert_eq!(prune_plan(&files, 0, 1000), vec![0, 0]);
        assert_eq!(prune_plan(&files, 3, 1000), vec![1, 1]);
        assert_eq!(prune_plan(&files, 0, 40), vec![1, 1]);
        assert_eq!(prune_plan(&files, 0, 30), vec![1, 2]);
        assert_eq!(prune_plan(&files, 0, 0), vec![3, 3]);
        assert_eq!(prune_plan(&[vec![]], 10, 0), vec![0]);
    }

    fn mode(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn the_store_prunes_repairs_and_answers() {
        let dir = tempfile::tempdir().unwrap();
        let hub = dir.path().join("history").join("ab");
        create_private_dir(&hub).unwrap();
        let now = now_ms();
        let day = 86_400_000;
        // lobby: one record 3 days old, two fresh, and a cut one.
        let mut lobby = encode_record(now - 3 * day, &msg("old"));
        lobby.extend(encode_record(now - 1000, &msg("fresh 1")));
        lobby.extend(encode_record(now - 500, &msg("fresh 2")));
        let fresh = encode_record(now - 100, &msg("cut"));
        lobby.extend(&fresh[..fresh.len() / 2]);
        std::fs::write(hub.join("lobby.log"), &lobby).unwrap();
        // old: only old records; a temp file left behind; a stranger.
        std::fs::write(hub.join("old.log"), encode_record(now - 5 * day, &msg("x"))).unwrap();
        std::fs::write(hub.join("lobby.log.tmp"), b"junk").unwrap();
        std::fs::write(hub.join("README"), b"keep").unwrap();

        let status = SharedHistoryStatus::default();
        let mut store = Store {
            dir: hub.clone(),
            settings: settings(2, 1_000_000),
            status: status.clone(),
            open: BTreeMap::new(),
            sizes: BTreeMap::new(),
            dirty: false,
        };
        store.prune();
        assert!(!hub.join("old.log").exists());
        assert!(!hub.join("lobby.log.tmp").exists());
        assert!(hub.join("README").exists());
        assert_eq!(mode(&hub.join("lobby.log")), 0o600);
        let data = std::fs::read(hub.join("lobby.log")).unwrap();
        let (records, good) = read_records(&data);
        assert_eq!((records.len(), good), (2, data.len()));
        assert_eq!(status.lock().unwrap().history_rooms, 1);
        assert_eq!(status.lock().unwrap().history_bytes, data.len() as u64);

        store.write("lobby", now, &msg("new"));
        store.write("new/room", now, &msg("other"));
        assert_eq!(mode(&hub.join("new%2Froom.log")), 0o600);
        let q = Query {
            room: "lobby".into(),
            since_ms: None,
            max: 10,
        };
        let got = store.query(&q);
        assert_eq!(got.len(), 3);
        assert_eq!(got[2].envelope, msg_body_check(&got[2].envelope, "new"));
        let s = status.lock().unwrap().clone();
        assert_eq!((s.history_records, s.history_rooms), (2, 2));

        // Over the size: the oldest go first, across rooms.
        store.settings.max_bytes = s.history_bytes - 1;
        store.prune();
        let got = store.query(&q);
        assert_eq!(got.len(), 2, "the oldest lobby record went");
        assert!(status.lock().unwrap().history_bytes < s.history_bytes);
    }

    fn msg_body_check(envelope: &[u8], body: &str) -> Vec<u8> {
        let env = Envelope::decode(envelope).unwrap();
        assert_eq!(env.text_body().as_deref(), Some(body));
        envelope.to_vec()
    }

    #[tokio::test]
    async fn the_thread_records_and_answers_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let hub = dir.path().join("history").join("cd");
        let status = SharedHistoryStatus::default();
        let history = History::start(hub.clone(), settings(30, 1_000_000), status.clone()).unwrap();
        assert_eq!(mode(&hub), 0o700);
        assert_eq!(mode(hub.parent().unwrap()), 0o700);
        for i in 0..5 {
            history.record("lobby", &Envelope::decode(&msg(&i.to_string())).unwrap());
        }
        let got = history
            .query(Query {
                room: "lobby".into(),
                since_ms: None,
                max: 3,
            })
            .await
            .unwrap();
        let bodies: Vec<String> = got
            .iter()
            .map(|r| Envelope::decode(&r.envelope).unwrap().text_body().unwrap())
            .collect();
        assert_eq!(bodies, vec!["2", "3", "4"]);
        history.stop().await;
        assert_eq!(status.lock().unwrap().history_records, 5);
        // Stopped: records are dropped, queries answer nothing.
        history.record("lobby", &Envelope::decode(&msg("late")).unwrap());
        assert_eq!(status.lock().unwrap().history_drops, 1);
        assert!(
            history
                .query(Query {
                    room: "lobby".into(),
                    since_ms: None,
                    max: 3
                })
                .await
                .unwrap_or_default()
                .is_empty()
        );
    }

    #[test]
    fn utc_times() {
        assert_eq!(utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(utc(951_782_400_000), "2000-02-29T00:00:00Z");
        assert_eq!(utc(1_794_567_845_123), "2026-11-13T11:04:05Z");
        assert_eq!(utc(4_107_542_399_000), "2100-02-28T23:59:59Z");
    }
}
