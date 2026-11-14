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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ciborium::Value;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::config::HistorySettings;
use crate::envelope::{Envelope, MessageType, now_ms};
use crate::identity::create_private_dir;
use crate::relay::MAX_HISTORY;

/// Jobs waiting for the history thread, most.
const QUEUE: usize = 4096;
/// How long a written record may sit in the page cache before it is synced;
/// also how soon a failed sync is tried again.
const SYNC_EVERY: Duration = Duration::from_secs(5);
/// How often old records are pruned (and at start).
const PRUNE_EVERY: Duration = Duration::from_secs(3600);
/// Longest file name the file system takes.
const MAX_FILE_NAME: usize = 255;
const SUFFIX: &str = ".log";
/// What a rewrite adds to a room file's name; the longest name the history
/// makes from a room file's.
const TMP_SUFFIX: &str = ".tmp";
/// Room files open at once, most; the least recently used is closed first.
const MAX_OPEN: usize = 64;
/// A room file not written for this long is closed.
const IDLE_CLOSE: Duration = Duration::from_secs(60);
/// The longest record read back. Envelopes reach the bouncer in single
/// link packets, so a real record is far shorter; a length past this is
/// damage.
const MAX_RECORD: u64 = 65_536;
/// A record stamped further than this ahead of the clock is taken, at the
/// prune, as received at the time of the prune.
const FUTURE_SLACK_MS: u64 = 86_400_000;
/// The same warning is logged at most once in this long.
const WARN_EVERY: Duration = Duration::from_secs(60);
/// How a record starts: a map of two, then the text key `ts_ms`. A scan
/// that meets damage looks for the next record from these bytes.
const MAGIC: [u8; 7] = [0xa2, 0x65, b't', b's', b'_', b'm', b's'];

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
    /// Syncs of a room file that failed (each is tried again).
    pub history_sync_errors: u64,
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

/// Records dropped because the queue was full: logged when dropping starts
/// and when it stops, not per record.
#[derive(Default)]
struct Dropping {
    on: AtomicBool,
    count: AtomicU64,
}

/// The relay's handle on a hub's history thread. Cloning it is cheap; a
/// disabled one records nothing and answers every query with nothing.
#[derive(Clone)]
pub struct History {
    jobs: Option<SyncSender<Job>>,
    status: SharedHistoryStatus,
    dropping: Arc<Dropping>,
}

impl History {
    /// Start the history thread for the hub whose history is in `dir`.
    pub fn start(dir: PathBuf, settings: HistorySettings, status: SharedHistoryStatus) -> Result<Self, String> {
        create_private_dir(&dir)?;
        if let Some(parent) = dir.parent() {
            create_private_dir(parent)?;
        }
        let (tx, rx) = std::sync::mpsc::sync_channel(QUEUE);
        let mut store = Store::new(dir, settings, status.clone());
        std::thread::Builder::new()
            .name("history".into())
            .spawn(move || store.run(&rx))
            .map_err(|e| format!("could not start the history thread: {e}"))?;
        Ok(Self {
            jobs: Some(tx),
            status,
            dropping: Arc::default(),
        })
    }

    pub fn disabled() -> Self {
        Self {
            jobs: None,
            status: SharedHistoryStatus::default(),
            dropping: Arc::default(),
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
            Ok(()) => {
                if self.dropping.on.swap(false, Ordering::Relaxed) {
                    let n = self.dropping.count.swap(0, Ordering::Relaxed);
                    tracing::warn!("history: the queue has room again; {n} records were dropped");
                }
            }
            Err(TrySendError::Full(_)) => {
                self.drop_one();
                self.dropping.count.fetch_add(1, Ordering::Relaxed);
                if !self.dropping.on.swap(true, Ordering::Relaxed) {
                    tracing::warn!("history: the queue is full: records are dropped until it has room");
                }
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

    /// Sync what was written and stop the thread, within `within`: true
    /// if the thread said it had. Never blocks a runtime thread.
    pub async fn stop(&self, within: Duration) -> bool {
        let Some(jobs) = &self.jobs else { return true };
        let deadline = tokio::time::Instant::now() + within;
        let (tx, rx) = oneshot::channel();
        let mut job = Job::Stop(tx);
        loop {
            match jobs.try_send(job) {
                Ok(()) => break,
                Err(TrySendError::Full(back)) => {
                    if tokio::time::Instant::now() >= deadline {
                        return false;
                    }
                    job = back;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(TrySendError::Disconnected(_)) => return true,
            }
        }
        tokio::time::timeout_at(deadline, rx).await.is_ok()
    }
}

/// The file name of a room's history: bytes of `[a-z0-9._-]` as they are
/// (a `.` only after the first byte), every other byte as `%XX`, then
/// `.log`. None if that, with `.tmp` added, is longer than a file name may
/// be.
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
    (!room.is_empty() && name.len() + TMP_SUFFIX.len() <= MAX_FILE_NAME).then_some(name)
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

/// Whether a record is replayed: MSG, ACTION and NOTICE are; JOINED,
/// PARTED and ERROR would change the client's idea of the room.
fn replayable(envelope: &[u8]) -> bool {
    Envelope::decode(envelope)
        .is_ok_and(|e| matches!(e.t, MessageType::Msg | MessageType::Action | MessageType::Notice))
}

/// Where a record is in its file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    ts_ms: u64,
    offset: u64,
    len: u64,
}

/// What a scan of a room file found besides its records.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Scanned {
    /// The file's length.
    len: u64,
    /// Where the last record that reads ends: past it, nothing does.
    good: u64,
    /// Bytes before `good` that are no record (damage the scan read past).
    skipped: u64,
}

/// A reader that knows where it is.
struct Counting<R> {
    inner: R,
    pos: u64,
}

impl<R: Read> Read for Counting<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl<R: Read + Seek> Counting<R> {
    fn seek_to(&mut self, pos: u64) -> std::io::Result<()> {
        self.inner.seek(SeekFrom::Start(pos))?;
        self.pos = pos;
        Ok(())
    }
}

/// The record at the reader, if one reads there. A record counts only if
/// its envelope reads too: a record cut short reads on into the next one,
/// and its envelope, cut and run on, does not read.
fn read_one<R: Read>(reader: &mut Counting<R>) -> Option<Record> {
    let value = ciborium::from_reader::<Value, _>(reader.by_ref().take(MAX_RECORD)).ok()?;
    decode_record(value).filter(|r| Envelope::decode(&r.envelope).is_ok())
}

/// Where the next record that reads starts, at or after `from`; the reader
/// is left there.
fn resync<R: Read + Seek>(reader: &mut Counting<R>, from: u64) -> std::io::Result<Option<u64>> {
    let mut at = from;
    loop {
        reader.seek_to(at)?;
        let mut base = at;
        let mut window: Vec<u8> = Vec::new();
        let mut chunk = vec![0; 16_384];
        let found = loop {
            let n = reader.read(&mut chunk)?;
            if n == 0 {
                return Ok(None);
            }
            window.extend_from_slice(&chunk[..n]);
            if let Some(i) = window.windows(MAGIC.len()).position(|w| w == MAGIC) {
                break base + i as u64;
            }
            let keep = window.len().saturating_sub(MAGIC.len() - 1);
            window.drain(..keep);
            base += keep as u64;
        };
        reader.seek_to(found)?;
        if read_one(reader).is_some() {
            reader.seek_to(found)?;
            return Ok(Some(found));
        }
        at = found + 1;
    }
}

/// Read a room file through, a record at a time, calling `each` with
/// every record that reads and where it is. Damage in the middle is read
/// past, to the next record that reads.
fn scan_file(path: &Path, mut each: impl FnMut(Entry, Vec<u8>)) -> std::io::Result<Scanned> {
    let file = File::open(path)?;
    let len = file.metadata()?.len();
    let mut reader = Counting {
        inner: BufReader::new(file),
        pos: 0,
    };
    let mut scanned = Scanned {
        len,
        ..Scanned::default()
    };
    while reader.pos < len {
        let start = reader.pos;
        if let Some(record) = read_one(&mut reader) {
            let entry = Entry {
                ts_ms: record.ts_ms,
                offset: start,
                len: reader.pos - start,
            };
            each(entry, record.envelope);
            scanned.good = reader.pos;
            continue;
        }
        match resync(&mut reader, start + 1)? {
            Some(next) => scanned.skipped += next - start,
            None => break,
        }
    }
    Ok(scanned)
}

/// The records at `entries` in the file at `path`, in that order.
fn read_entries(path: &Path, entries: &[Entry]) -> Vec<Record> {
    let Ok(file) = File::open(path) else {
        return Vec::new();
    };
    let mut records = Vec::with_capacity(entries.len());
    let mut buf = Vec::new();
    for entry in entries {
        buf.resize(usize::try_from(entry.len).unwrap_or(0), 0);
        if std::os::unix::fs::FileExt::read_exact_at(&file, &mut buf, entry.offset).is_err() {
            continue;
        }
        if let Some(record) = ciborium::from_reader::<Value, _>(&buf[..]).ok().and_then(decode_record) {
            records.push(record);
        }
    }
    records
}

/// The time at or after which records are kept: those older than
/// `cutoff_ms` go, then, while the rest are together over `max_bytes`, the
/// oldest of them, whichever file they are in. `records` holds each
/// record's time and length; their order does not matter.
pub fn prune_threshold(records: &[(u64, u64)], cutoff_ms: u64, max_bytes: u64) -> u64 {
    let bytes = |t: u64| -> u64 { records.iter().filter(|(ts, _)| *ts >= t).map(|(_, len)| len).sum() };
    if bytes(cutoff_ms) <= max_bytes {
        return cutoff_ms;
    }
    // bytes(lo) is over, bytes(hi) is not (nothing is that new).
    let mut lo = cutoff_ms;
    let mut hi = records
        .iter()
        .map(|(ts, _)| ts.saturating_add(1))
        .max()
        .unwrap_or(lo + 1);
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if bytes(mid) <= max_bytes {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    hi
}

/// A room with history: where its file ends, and an index of its newest
/// replayable records.
#[derive(Debug, Default)]
struct Room {
    /// The file's length when it is sound: where the next record goes.
    len: u64,
    /// The file is not sound and could not be put right: nothing is
    /// written to it until it is.
    broken: bool,
    /// The newest replayable records, oldest first, at most `MAX_HISTORY`
    /// (the most a query asks for).
    index: VecDeque<Entry>,
    /// The newest time of a replayable record no longer in the index.
    evicted: Option<u64>,
}

impl Room {
    fn add(&mut self, entry: Entry) {
        if self.index.len() == MAX_HISTORY
            && let Some(old) = self.index.pop_front()
        {
            self.evicted = Some(self.evicted.map_or(old.ts_ms, |e| e.max(old.ts_ms)));
        }
        self.index.push_back(entry);
    }
}

/// A record found by the prune's scan.
struct Found {
    entry: Entry,
    replayable: bool,
    /// Stamped too far ahead: kept as received now.
    future: bool,
}

/// A warning logged at most once a minute, with how many were not.
#[derive(Default)]
struct Limited {
    last: Option<Instant>,
    quiet: u64,
}

impl Limited {
    fn warn(&mut self, text: impl Display) {
        let now = Instant::now();
        if self
            .last
            .is_some_and(|last| now.saturating_duration_since(last) < WARN_EVERY)
        {
            self.quiet += 1;
            return;
        }
        if self.quiet > 0 {
            tracing::warn!("history: {text} ({} more like it not logged)", self.quiet);
        } else {
            tracing::warn!("history: {text}");
        }
        self.last = Some(now);
        self.quiet = 0;
    }
}

/// Failures the tests make happen.
#[derive(Default)]
struct Faults {
    /// Cuts (`set_len`) that fail.
    cut: u32,
    /// Syncs that fail.
    sync: u32,
}

fn fault(n: &mut u32) -> bool {
    let hit = *n > 0;
    *n = n.saturating_sub(1);
    hit
}

struct Store {
    dir: PathBuf,
    settings: HistorySettings,
    status: SharedHistoryStatus,
    /// Rooms with a file, by file name.
    rooms: BTreeMap<String, Room>,
    /// Room files open for appending, with when each was last used.
    open: BTreeMap<String, (File, Instant)>,
    /// Room files written since their last sync.
    dirty: BTreeSet<String>,
    /// Rooms whose name was too long, warned about once.
    long_names: BTreeSet<String>,
    write_warn: Limited,
    sync_warn: Limited,
    faults: Faults,
}

impl Store {
    fn new(dir: PathBuf, settings: HistorySettings, status: SharedHistoryStatus) -> Self {
        Self {
            dir,
            settings,
            status,
            rooms: BTreeMap::new(),
            open: BTreeMap::new(),
            dirty: BTreeSet::new(),
            long_names: BTreeSet::new(),
            write_warn: Limited::default(),
            sync_warn: Limited::default(),
            faults: Faults::default(),
        }
    }

    fn run(&mut self, jobs: &Receiver<Job>) {
        self.prune();
        let mut next_prune = Instant::now() + PRUNE_EVERY;
        let mut next_sync: Option<Instant> = None;
        loop {
            let now = Instant::now();
            let mut due = next_sync.map_or(next_prune, |s| s.min(next_prune));
            if !self.open.is_empty() {
                due = due.min(now + SYNC_EVERY);
            }
            let job = match jobs.recv_timeout(due.saturating_duration_since(now)) {
                Ok(job) => Some(job),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => break,
            };
            match job {
                Some(Job::Record { room, ts_ms, envelope }) => self.write(&room, ts_ms, &envelope),
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
            if !self.dirty.is_empty() && next_sync.is_none() {
                next_sync = Some(now + SYNC_EVERY);
            }
            if next_sync.is_some_and(|s| now >= s) {
                self.sync();
                next_sync = (!self.dirty.is_empty()).then(|| now + SYNC_EVERY);
            }
            self.close_idle(now);
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
        let bytes = self.rooms.values().map(|r| r.len).sum();
        let rooms = self.rooms.values().filter(|r| r.len > 0).count() as u64;
        self.show(|s| {
            s.history_bytes = bytes;
            s.history_rooms = rooms;
        });
    }

    fn dropped(&mut self, text: impl Display) {
        self.write_warn.warn(text);
        self.show(|s| s.history_drops += 1);
    }

    /// Cut the file at `path` back to `len` and sync it: a repair in
    /// place, which needs no free space.
    fn cut(&mut self, path: &Path, len: u64) -> std::io::Result<()> {
        if fault(&mut self.faults.cut) {
            return Err(std::io::Error::other("cut failed (test)"));
        }
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(len)?;
        file.sync_data()
    }

    /// Read a room's file again, build its index, and cut off a tail that
    /// is no record: false if the room is left broken.
    fn repair(&mut self, name: &str) -> bool {
        let path = self.dir.join(name);
        let mut room = Room::default();
        let scanned = match scan_file(&path, |entry, envelope| {
            if replayable(&envelope) {
                room.add(entry);
            }
        }) {
            Ok(scanned) => scanned,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Scanned::default(),
            Err(e) => {
                self.write_warn.warn(format_args!("{name}: {e}"));
                room.broken = true;
                Scanned::default()
            }
        };
        room.len = scanned.good;
        if scanned.good < scanned.len {
            tracing::warn!(
                "history: {name}: {} bytes after the last whole record taken off",
                scanned.len - scanned.good
            );
            if let Err(e) = self.cut(&path, scanned.good) {
                self.write_warn
                    .warn(format_args!("{name}: could not cut the bad tail off: {e}"));
                room.broken = true;
            }
        }
        let sound = !room.broken;
        self.rooms.insert(name.to_string(), room);
        sound
    }

    /// The room file open for appending, opened if it is not.
    fn file(&mut self, name: &str) -> std::io::Result<&mut File> {
        if !self.open.contains_key(name) {
            if self.open.len() >= MAX_OPEN
                && let Some(oldest) = self
                    .open
                    .iter()
                    .min_by_key(|(_, (_, used))| *used)
                    .map(|(n, _)| n.clone())
            {
                self.close(&oldest);
            }
            let mut options = OpenOptions::new();
            options.append(true).create(true);
            #[cfg(unix)]
            std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
            let file = options.open(self.dir.join(name))?;
            self.open.insert(name.to_string(), (file, Instant::now()));
        }
        let (file, used) = self
            .open
            .get_mut(name)
            .ok_or_else(|| std::io::Error::other("not open"))?;
        *used = Instant::now();
        Ok(file)
    }

    /// Close a room file, synced first if it was written to.
    fn close(&mut self, name: &str) {
        if self.dirty.contains(name) {
            self.sync_one(name);
        }
        self.open.remove(name);
    }

    fn close_idle(&mut self, now: Instant) {
        let idle: Vec<String> = self
            .open
            .iter()
            .filter(|(_, (_, used))| now.saturating_duration_since(*used) >= IDLE_CLOSE)
            .map(|(n, _)| n.clone())
            .collect();
        for name in idle {
            self.close(&name);
        }
    }

    fn write(&mut self, room: &str, ts_ms: u64, envelope: &[u8]) {
        let Some(name) = file_name(room) else {
            if self.long_names.len() < 1024 && self.long_names.insert(room.to_string()) {
                tracing::warn!("history: room name too long for a file name: not recorded");
            }
            return self.show(|s| s.history_drops += 1);
        };
        // A room seen for the first time, or one left broken: its file is
        // read (again) first.
        if self.rooms.get(&name).is_none_or(|r| r.broken) && !self.repair(&name) {
            return self.dropped(format_args!("{name}: broken: a record was dropped"));
        }
        let expected = self.rooms.get(&name).map_or(0, |r| r.len);
        let actual = match self.file(&name).and_then(|f| f.metadata()) {
            Ok(meta) => meta.len(),
            Err(e) => return self.dropped(format_args!("{name}: {e}")),
        };
        // Never append past bytes that are no record.
        if actual != expected {
            tracing::warn!("history: {name}: {actual} bytes where {expected} were written: read again");
            if !self.repair(&name) {
                return self.dropped(format_args!("{name}: broken: a record was dropped"));
            }
        }
        let offset = self.rooms.get(&name).map_or(0, |r| r.len);
        let record = encode_record(ts_ms, envelope);
        let written = self.file(&name).and_then(|f| f.write_all(&record));
        match written {
            Ok(()) => {
                self.dirty.insert(name.clone());
                if let Some(room) = self.rooms.get_mut(&name) {
                    room.len = offset + record.len() as u64;
                    if replayable(envelope) {
                        room.add(Entry {
                            ts_ms,
                            offset,
                            len: record.len() as u64,
                        });
                    }
                }
                self.show_sizes();
                self.show(|s| s.history_records += 1);
            }
            Err(e) => {
                // Take a part written back off, so the file still reads;
                // if that fails too, nothing more goes on it until it does.
                let path = self.dir.join(&name);
                if self.cut(&path, offset).is_err()
                    && let Some(room) = self.rooms.get_mut(&name)
                {
                    room.broken = true;
                }
                self.close(&name);
                self.dropped(format_args!("{name}: {e}"));
            }
        }
    }

    /// Sync one room file: true if it is synced (or gone).
    fn sync_one(&mut self, name: &str) -> bool {
        let result = if fault(&mut self.faults.sync) {
            Err(std::io::Error::other("sync failed (test)"))
        } else {
            match self.open.get(name) {
                Some((file, _)) => file.sync_data(),
                None => OpenOptions::new()
                    .write(true)
                    .open(self.dir.join(name))
                    .and_then(|f| f.sync_data()),
            }
        };
        match result {
            Ok(()) => {
                self.dirty.remove(name);
                true
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                self.dirty.remove(name);
                true
            }
            Err(e) => {
                self.sync_warn
                    .warn(format_args!("{name}: sync failed, tried again in 5 s: {e}"));
                self.show(|s| s.history_sync_errors += 1);
                false
            }
        }
    }

    /// Sync every room file written to since its last sync; one that fails
    /// stays to be synced.
    fn sync(&mut self) {
        let dirty: Vec<String> = self.dirty.iter().cloned().collect();
        for name in dirty {
            self.sync_one(&name);
        }
    }

    fn query(&mut self, query: &Query) -> Vec<Record> {
        if query.max == 0 {
            return Vec::new();
        }
        let Some(name) = file_name(&query.room) else {
            return Vec::new();
        };
        let Some(room) = self.rooms.get(&name) else {
            return Vec::new();
        };
        let since = query.since_ms.unwrap_or(0);
        let path = self.dir.join(&name);
        let mut picked: VecDeque<Entry> = room.index.iter().filter(|e| e.ts_ms >= since).copied().collect();
        if picked.len() < query.max && room.evicted.is_some_and(|ts| ts >= since) {
            // Records the index no longer holds may be wanted (clocks that
            // went back): read the file through.
            picked.clear();
            if let Err(e) = scan_file(&path, |entry, envelope| {
                if entry.ts_ms >= since && replayable(&envelope) {
                    if picked.len() == query.max {
                        picked.pop_front();
                    }
                    picked.push_back(entry);
                }
            }) {
                tracing::warn!("history: {name}: {e}");
            }
        }
        while picked.len() > query.max {
            picked.pop_front();
        }
        read_entries(&path, picked.make_contiguous())
    }

    /// Write the records `kept` of the file at `path` to a `.tmp` file
    /// (mode 0600), sync it and rename it over the old one; those marked
    /// `future` are stamped `now_ms`. The new file's records and length.
    fn rewrite(path: &Path, kept: &[Found], now_ms: u64) -> std::io::Result<(Vec<(Entry, bool)>, u64)> {
        let mut tmp = path.as_os_str().to_os_string();
        tmp.push(TMP_SUFFIX);
        let tmp = PathBuf::from(tmp);
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut out = BufWriter::new(options.open(&tmp)?);
        let mut src = Counting {
            inner: BufReader::new(File::open(path)?),
            pos: 0,
        };
        let mut entries = Vec::with_capacity(kept.len());
        let mut at = 0;
        let mut buf = Vec::new();
        for found in kept {
            if src.pos != found.entry.offset {
                src.seek_to(found.entry.offset)?;
            }
            buf.resize(usize::try_from(found.entry.len).unwrap_or(0), 0);
            src.read_exact(&mut buf)?;
            let mut ts_ms = found.entry.ts_ms;
            if found.future
                && let Some(record) = ciborium::from_reader::<Value, _>(&buf[..]).ok().and_then(decode_record)
            {
                buf = encode_record(now_ms, &record.envelope);
                ts_ms = now_ms;
            }
            out.write_all(&buf)?;
            let len = buf.len() as u64;
            entries.push((Entry { ts_ms, offset: at, len }, found.replayable));
            at += len;
        }
        let file = out.into_inner().map_err(|e| e.into_error())?;
        file.sync_data()?;
        drop(file);
        std::fs::rename(&tmp, path)?;
        if let Some(dir) = path.parent()
            && let Ok(dir) = File::open(dir)
        {
            let _ = dir.sync_all();
        }
        Ok((entries, at))
    }

    /// Drop what retention says to, repair damaged files, and remove
    /// temporary files a rewrite left behind; the rooms' indexes are built
    /// again.
    fn prune(&mut self) {
        self.sync();
        let open: Vec<String> = self.open.keys().cloned().collect();
        for name in open {
            self.close(&name);
        }
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("history: {}: {e}", self.dir.display());
                return;
            }
        };
        let now = now_ms();
        let mut files: Vec<(String, Scanned, Vec<Found>)> = Vec::new();
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(TMP_SUFFIX) {
                let _ = std::fs::remove_file(entry.path());
                continue;
            }
            if room_of(&name).is_none() || !entry.file_type().is_ok_and(|t| t.is_file()) {
                continue;
            }
            let mut found = Vec::new();
            let scanned = scan_file(&entry.path(), |mut entry, envelope| {
                let future = entry.ts_ms > now.saturating_add(FUTURE_SLACK_MS);
                if future {
                    entry.ts_ms = now;
                }
                found.push(Found {
                    entry,
                    replayable: replayable(&envelope),
                    future,
                });
            });
            match scanned {
                Ok(scanned) => files.push((name, scanned, found)),
                Err(e) => tracing::warn!("history: {name}: {e}"),
            }
        }
        let cutoff = now.saturating_sub(u64::try_from(self.settings.retention.as_millis()).unwrap_or(u64::MAX));
        let all: Vec<(u64, u64)> = files
            .iter()
            .flat_map(|(_, _, found)| found.iter().map(|f| (f.entry.ts_ms, f.entry.len)))
            .collect();
        let keep_from = prune_threshold(&all, cutoff, self.settings.max_bytes);
        drop(all);
        let mut dropped = 0;
        self.rooms.clear();
        for (name, scanned, found) in files {
            let path = self.dir.join(&name);
            let mut room = Room::default();
            // A bad tail is cut off in place first: that needs no space.
            if scanned.good < scanned.len {
                tracing::warn!(
                    "history: {name}: {} bytes after the last whole record taken off",
                    scanned.len - scanned.good
                );
                if let Err(e) = self.cut(&path, scanned.good) {
                    tracing::warn!("history: {name}: could not cut the bad tail off: {e}");
                    room.broken = true;
                }
            }
            if scanned.skipped > 0 {
                tracing::warn!("history: {name}: {} bytes that are no record left out", scanned.skipped);
            }
            let total = found.len();
            let kept: Vec<Found> = found.into_iter().filter(|f| f.entry.ts_ms >= keep_from).collect();
            dropped += total - kept.len();
            if kept.is_empty() {
                match std::fs::remove_file(&path) {
                    Ok(()) => continue,
                    Err(e) => tracing::warn!("history: {name}: {e}"),
                }
            }
            let changed = kept.len() < total || scanned.skipped > 0 || kept.iter().any(|f| f.future);
            let rewritten = if changed && !kept.is_empty() {
                match Self::rewrite(&path, &kept, now) {
                    Ok(done) => Some(done),
                    Err(e) => {
                        tracing::warn!("history: {name}: not pruned: {e}");
                        None
                    }
                }
            } else {
                None
            };
            match rewritten {
                Some((entries, len)) => {
                    room.broken = false;
                    room.len = len;
                    for (entry, replayable) in entries {
                        if replayable {
                            room.add(entry);
                        }
                    }
                }
                None => {
                    room.len = scanned.good;
                    for found in kept.iter().filter(|f| f.replayable) {
                        room.add(found.entry);
                    }
                }
            }
            self.rooms.insert(name, room);
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

    fn body(record: &Record) -> String {
        Envelope::decode(&record.envelope).unwrap().text_body().unwrap()
    }

    fn settings(days: u64, max_bytes: u64) -> HistorySettings {
        HistorySettings {
            retention: Duration::from_secs(days * 86_400),
            max_bytes,
            replay_window: Duration::from_secs(3600),
            replay_max: 100,
        }
    }

    fn store(dir: &Path, days: u64, max_bytes: u64) -> (Store, SharedHistoryStatus) {
        let status = SharedHistoryStatus::default();
        (
            Store::new(dir.to_path_buf(), settings(days, max_bytes), status.clone()),
            status,
        )
    }

    fn all(room: &str) -> Query {
        Query {
            room: room.into(),
            since_ms: None,
            max: MAX_HISTORY,
        }
    }

    fn hub_dir() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let hub = dir.path().join("history").join("ab");
        create_private_dir(&hub).unwrap();
        (dir, hub)
    }

    /// Every record in a file, with the scan's findings.
    fn read_all(path: &Path) -> (Vec<Record>, Scanned) {
        let mut out = Vec::new();
        let scanned = scan_file(path, |entry, envelope| {
            out.push(Record {
                ts_ms: entry.ts_ms,
                envelope,
            })
        })
        .unwrap();
        (out, scanned)
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
    fn the_longest_name_with_its_temporary_suffix_fits() {
        // 247 plain bytes: 251 with .log, 255 with .tmp.
        let longest = "a".repeat(247);
        let name = file_name(&longest).unwrap();
        assert_eq!(name.len() + TMP_SUFFIX.len(), MAX_FILE_NAME);
        assert_eq!(file_name(&"a".repeat(248)), None);
        // Encoded bytes count three each: 82 * 3 + 4 = 250, 83 * 3 + 4 = 253.
        assert!(file_name(&"/".repeat(82)).is_some());
        assert_eq!(file_name(&"/".repeat(83)), None);
        // The file system takes it, and the prune's rewrite of it too.
        let (_dir, hub) = hub_dir();
        let (mut store, status) = store(&hub, 30, 1_000_000);
        store.write(&longest, now_ms(), &msg("long"));
        std::fs::write(hub.join(format!("{name}{TMP_SUFFIX}")), b"x").unwrap();
        store.write(&"a".repeat(248), now_ms(), &msg("too long"));
        let s = status.lock().unwrap().clone();
        assert_eq!((s.history_records, s.history_drops), (1, 1));
        store.settings.max_bytes = 0;
        store.prune();
        assert!(
            !hub.join(&name).exists(),
            "pruned through a rewrite of the longest name"
        );
    }

    #[test]
    fn records_read_back_and_a_cut_record_ends_the_file() {
        let (_dir, hub) = hub_dir();
        let path = hub.join("lobby.log");
        let mut data = encode_record(1, &msg("one"));
        data.extend(encode_record(2, &msg("two")));
        let whole = data.len() as u64;
        let third = encode_record(3, &msg("three"));
        for cut in 1..third.len() {
            let mut cut_data = data.clone();
            cut_data.extend(&third[..cut]);
            std::fs::write(&path, &cut_data).unwrap();
            let (records, scanned) = read_all(&path);
            assert_eq!(
                (records.len(), scanned.good, scanned.skipped),
                (2, whole, 0),
                "cut at {cut}"
            );
        }
        // Garbage that is CBOR but not a record ends it too.
        let mut bad = data.clone();
        bad.push(0x01);
        std::fs::write(&path, &bad).unwrap();
        assert_eq!(read_all(&path).1.good, whole);
    }

    #[test]
    fn a_damaged_middle_is_read_past() {
        let (_dir, hub) = hub_dir();
        let path = hub.join("lobby.log");
        let mut data = encode_record(1, &msg("one"));
        let garbage = [0xff, 0x00, 0xa2, 0x65, b't', b's', b'_', b'm', b's', 0x13];
        data.extend(garbage);
        data.extend(encode_record(2, &msg("two")));
        data.extend(encode_record(3, &msg("three")));
        std::fs::write(&path, &data).unwrap();
        let (records, scanned) = read_all(&path);
        assert_eq!(records.iter().map(body).collect::<Vec<_>>(), ["one", "two", "three"]);
        assert_eq!(
            (scanned.good, scanned.skipped),
            (data.len() as u64, garbage.len() as u64)
        );
    }

    #[test]
    fn a_bad_first_record_keeps_the_rest_and_the_file() {
        let (_dir, hub) = hub_dir();
        let now = now_ms();
        let first = encode_record(now - 3, &msg("first"));
        let mut data = first[..first.len() - 3].to_vec();
        data.extend(encode_record(now - 2, &msg("second")));
        data.extend(encode_record(now - 1, &msg("third")));
        std::fs::write(hub.join("lobby.log"), &data).unwrap();
        // Only a bad record: cut to nothing, nothing lost.
        let only = encode_record(now, &msg("cut"));
        std::fs::write(hub.join("cut.log"), &only[..only.len() / 2]).unwrap();
        let (mut store, _) = store(&hub, 30, 1_000_000);
        store.prune();
        let got: Vec<String> = store.query(&all("lobby")).iter().map(body).collect();
        assert_eq!(got, ["second", "third"]);
        let (records, scanned) = read_all(&hub.join("lobby.log"));
        assert_eq!(
            (records.len(), scanned.skipped),
            (2, 0),
            "rewritten without the bad bytes"
        );
        assert!(!hub.join("cut.log").exists());
    }

    #[test]
    fn a_bad_tail_is_cut_and_a_failed_cut_loses_nothing_written_after() {
        let (_dir, hub) = hub_dir();
        let path = hub.join("lobby.log");
        let now = now_ms();
        let (mut store, status) = store(&hub, 30, 1_000_000);
        store.write("lobby", now, &msg("one"));
        store.write("lobby", now, &msg("two"));
        // A write cut short behind the store's back (a crash, a full disk).
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        let cut = encode_record(now, &msg("cut"));
        file.write_all(&cut[..cut.len() / 2]).unwrap();
        drop(file);
        // The repair fails: the record is dropped, not put after garbage.
        store.faults.cut = 1;
        store.write("lobby", now, &msg("lost"));
        assert!(store.rooms["lobby.log"].broken);
        assert_eq!(status.lock().unwrap().history_drops, 1);
        // The next write repairs first, then appends.
        store.write("lobby", now, &msg("three"));
        assert!(!store.rooms["lobby.log"].broken);
        let (records, scanned) = read_all(&path);
        assert_eq!(records.iter().map(body).collect::<Vec<_>>(), ["one", "two", "three"]);
        assert_eq!(
            (scanned.good, scanned.len, scanned.skipped),
            (scanned.len, scanned.len, 0)
        );
        let got: Vec<String> = store.query(&all("lobby")).iter().map(body).collect();
        assert_eq!(got, ["one", "two", "three"]);
        let s = status.lock().unwrap().clone();
        assert_eq!((s.history_records, s.history_drops), (3, 1));
        // A tail found at the prune is cut in place too.
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&cut[..3]).unwrap();
        drop(file);
        store.prune();
        assert_eq!(read_all(&path).1.good, read_all(&path).1.len);
        assert_eq!(store.query(&all("lobby")).len(), 3);
    }

    #[test]
    fn prune_judges_each_record_by_its_own_time() {
        // (ts, len), out of order, as two files' records together.
        let records = [(5, 10), (1, 10), (4, 10), (2, 10), (6, 10), (3, 10)];
        assert_eq!(prune_threshold(&records, 0, 1000), 0);
        assert_eq!(prune_threshold(&records, 3, 1000), 3);
        assert_eq!(prune_threshold(&records, 0, 30), 4);
        assert_eq!(prune_threshold(&records, 0, 35), 4);
        assert_eq!(prune_threshold(&records, 0, 40), 3);
        assert_eq!(prune_threshold(&records, 5, 30), 5);
        assert_eq!(prune_threshold(&records, 0, 0), 7);
        assert_eq!(prune_threshold(&[], 10, 0), 10);
    }

    #[test]
    fn the_store_prunes_out_of_order_records_by_days_and_size() {
        let (_dir, hub) = hub_dir();
        let now = now_ms();
        let day = 86_400_000;
        // lobby, out of order: fresh, 3 days old, fresh, a day ahead, far
        // ahead.
        let mut lobby = encode_record(now - 1000, &msg("fresh 1"));
        lobby.extend(encode_record(now - 3 * day, &msg("old")));
        lobby.extend(encode_record(now - 500, &msg("fresh 2")));
        lobby.extend(encode_record(now + 3 * day, &msg("future")));
        std::fs::write(hub.join("lobby.log"), &lobby).unwrap();
        // side: older fresh records after a new one.
        let mut side = encode_record(now - 100, &msg("side new"));
        side.extend(encode_record(now - 4000, &msg("side oldest")));
        side.extend(encode_record(now - 2000, &msg("side older")));
        std::fs::write(hub.join("side.log"), &side).unwrap();
        // old: only old records; a temp file left behind; a stranger.
        std::fs::write(hub.join("old.log"), encode_record(now - 5 * day, &msg("x"))).unwrap();
        std::fs::write(hub.join("lobby.log.tmp"), b"junk").unwrap();
        std::fs::write(hub.join("README"), b"keep").unwrap();

        let (mut store, status) = store(&hub, 2, 1_000_000);
        store.prune();
        assert!(!hub.join("old.log").exists());
        assert!(!hub.join("lobby.log.tmp").exists());
        assert!(hub.join("README").exists());
        assert_eq!(mode(&hub.join("lobby.log")), 0o600);
        let got = store.query(&all("lobby"));
        assert_eq!(
            got.iter().map(body).collect::<Vec<_>>(),
            ["fresh 1", "fresh 2", "future"]
        );
        assert!(
            got[2].ts_ms >= now && got[2].ts_ms < now + day,
            "a far-future record is stamped now"
        );
        let lobby_bytes = std::fs::metadata(hub.join("lobby.log")).unwrap().len();
        let side_bytes = std::fs::metadata(hub.join("side.log")).unwrap().len();
        let s = status.lock().unwrap().clone();
        assert_eq!((s.history_rooms, s.history_bytes), (2, lobby_bytes + side_bytes));

        // Over the size by a little: the hub's oldest record goes, which
        // is in the middle of side.
        store.settings.max_bytes = s.history_bytes - 1;
        store.prune();
        let side: Vec<String> = store.query(&all("side")).iter().map(body).collect();
        assert_eq!(side, ["side new", "side older"]);
        assert_eq!(store.query(&all("lobby")).len(), 3);
        // Then the next oldest, across rooms: side older, lobby's fresh 1.
        let s = status.lock().unwrap().clone();
        let one = encode_record(now, &msg("side older")).len() as u64;
        store.settings.max_bytes = s.history_bytes - one - 1;
        store.prune();
        assert_eq!(
            store.query(&all("side")).iter().map(body).collect::<Vec<_>>(),
            ["side new"]
        );
        assert_eq!(
            store.query(&all("lobby")).iter().map(body).collect::<Vec<_>>(),
            ["fresh 2", "future"]
        );
    }

    #[test]
    fn a_query_reads_only_what_it_returns_and_filters_by_time() {
        let (_dir, hub) = hub_dir();
        let (mut store, _) = store(&hub, 30, 100_000_000);
        let joined = Envelope::new(MessageType::Joined, &[9; 16]).with_room("lobby").encode();
        for ts in 1..=10u64 {
            store.write("lobby", ts * 1000, &msg(&ts.to_string()));
            store.write("lobby", ts * 1000 + 1, &joined);
        }
        // Out of order: an old time after the new ones.
        store.write("lobby", 500, &msg("late"));
        let q = |since, max| Query {
            room: "lobby".into(),
            since_ms: since,
            max,
        };
        let bodies = |r: Vec<Record>| r.iter().map(body).collect::<Vec<_>>();
        assert_eq!(bodies(store.query(&q(None, 3))), ["9", "10", "late"]);
        assert_eq!(bodies(store.query(&q(Some(8500), 100))), ["9", "10"]);
        assert_eq!(store.query(&q(Some(1000), 100)).len(), 10);
        assert!(store.query(&q(None, 0)).is_empty());
        assert!(store.query(&q(None, 5)).iter().all(|r| replayable(&r.envelope)));
        // Past the index: more than it holds, with the oldest newest in
        // time; the file is read through for them.
        for i in 0..MAX_HISTORY as u64 {
            store.write("lobby", 100, &msg(&format!("old {i}")));
        }
        let got = store.query(&q(Some(1000), 3));
        assert_eq!(bodies(got), ["8", "9", "10"]);
        assert_eq!(store.rooms["lobby.log"].index.len(), MAX_HISTORY);
    }

    #[test]
    fn open_files_are_capped_and_closed_when_idle() {
        let (_dir, hub) = hub_dir();
        let (mut store, status) = store(&hub, 30, 100_000_000);
        for i in 0..MAX_OPEN + 10 {
            store.write(&format!("room{i}"), now_ms(), &msg("x"));
            assert!(store.open.len() <= MAX_OPEN);
        }
        assert_eq!(store.open.len(), MAX_OPEN);
        // The least recently used went, synced on the way out.
        assert!(!store.open.contains_key("room0.log"));
        assert!(store.open.contains_key(&format!("room{}.log", MAX_OPEN + 9)));
        assert!(!store.dirty.contains("room0.log"));
        // An evicted room is opened again for its next record.
        store.write("room0", now_ms(), &msg("y"));
        assert_eq!(store.query(&all("room0")).len(), 2);
        assert_eq!(status.lock().unwrap().history_records, MAX_OPEN as u64 + 11);
        store.close_idle(Instant::now() + IDLE_CLOSE);
        assert!(store.open.is_empty());
        store.sync();
        assert!(store.dirty.is_empty());
    }

    #[test]
    fn a_failed_sync_is_counted_and_tried_again() {
        let (_dir, hub) = hub_dir();
        let (mut store, status) = store(&hub, 30, 1_000_000);
        store.write("lobby", now_ms(), &msg("one"));
        assert!(store.dirty.contains("lobby.log"));
        store.faults.sync = 2;
        store.sync();
        assert!(store.dirty.contains("lobby.log"), "still to be synced");
        assert_eq!(status.lock().unwrap().history_sync_errors, 1);
        store.sync();
        assert_eq!(status.lock().unwrap().history_sync_errors, 2);
        store.sync();
        assert!(store.dirty.is_empty());
        assert_eq!(status.lock().unwrap().history_sync_errors, 2);
        // A file closed while its sync fails is synced later all the same.
        store.write("lobby", now_ms(), &msg("two"));
        store.faults.sync = 1;
        store.close("lobby.log");
        assert!(store.dirty.contains("lobby.log") && store.open.is_empty());
        store.sync();
        assert!(store.dirty.is_empty());
    }

    fn mode(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
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
        assert_eq!(got.iter().map(body).collect::<Vec<_>>(), ["2", "3", "4"]);
        assert!(history.stop(Duration::from_secs(5)).await);
        assert_eq!(status.lock().unwrap().history_records, 5);
        assert_eq!(mode(&hub.join("lobby.log")), 0o600);
        // Stopped: records are dropped, queries answer nothing.
        history.record("lobby", &Envelope::decode(&msg("late")).unwrap());
        assert_eq!(status.lock().unwrap().history_drops, 1);
        assert!(history.query(all("lobby")).await.unwrap_or_default().is_empty());
        assert!(history.stop(Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn stop_gives_up_on_a_full_queue_in_time() {
        let (tx, _rx) = std::sync::mpsc::sync_channel(1);
        let history = History {
            jobs: Some(tx),
            status: SharedHistoryStatus::default(),
            dropping: Arc::default(),
        };
        history.record("lobby", &Envelope::decode(&msg("fills the queue")).unwrap());
        history.record("lobby", &Envelope::decode(&msg("dropped")).unwrap());
        assert!(history.dropping.on.load(Ordering::Relaxed));
        let started = Instant::now();
        assert!(!history.stop(Duration::from_millis(200)).await);
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(history.status.lock().unwrap().history_drops, 1);
    }

    #[test]
    fn utc_times() {
        assert_eq!(utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(utc(951_782_400_000), "2000-02-29T00:00:00Z");
        assert_eq!(utc(1_794_567_845_123), "2026-11-13T11:04:05Z");
        assert_eq!(utc(4_107_542_399_000), "2100-02-28T23:59:59Z");
    }
}
