// SPDX-License-Identifier: LicenseRef-Reticulum
//! The upstream session: one link to an upstream hub, as the user.
//!
//! [`Session`] is the protocol, with no I/O: it takes the envelopes the hub
//! sends and the passing of time, and says what to send. [`run`] is the
//! task that owns the link, keeps it up, and feeds the session.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ciborium::Value;
use rns_runtime::prelude::*;
use rns_transport::messages::PathRequestOptions;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc, watch};

use crate::bouncer::{hub_app_data, now};
use crate::envelope::{Caps, Envelope, Limits, MessageType, RRC_VERSION, Raw, Welcome};
use crate::rate::TokenBucket;
use crate::resource::{self, Expectations, Expected};
use crate::rooms::RoomSet;

/// The client name in the bouncer's HELLO.
pub const CLIENT_NAME: &str = "rrc-bouncer";

/// What the bouncer says it can do: it takes and sends Resources
/// (DESIGN.md § Resources), and ACTION and direct NOTICE are plain
/// envelopes the relay passes on. rrcd keeps a client's caps but does not
/// act on them.
pub const CAPS: Caps = Caps {
    resource_envelope: true,
    action: true,
    direct_notice: true,
};

/// The protocol's timings. Every one has a default for real hubs; the
/// environment variables below shorten them for tests/e2e.py.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timings {
    /// No WELCOME this long after the HELLO: the link is dropped.
    pub welcome_timeout: Duration,
    /// No JOINED or ERROR this long after a JOIN: the JOIN is sent again
    /// later.
    pub join_timeout: Duration,
    /// The wait before the first retry of a JOIN that got no answer or was
    /// rate limited; it doubles for each of the `JOIN_FAST_RETRIES`.
    pub join_retry: Duration,
    /// The wait between retries after those, for the life of the link.
    pub join_retry_slow: Duration,
    /// No PARTED this long after a PART: the room is taken as left.
    pub part_timeout: Duration,
    /// Nothing heard from the hub this long: the bouncer sends a PING.
    pub ping_idle: Duration,
    /// No PONG this long after the bouncer's PING: it PINGs once more, and
    /// no PONG this long after that drops the link.
    pub pong_timeout: Duration,
    /// The first wait before connecting again.
    pub backoff_initial: Duration,
    /// The longest wait.
    pub backoff_max: Duration,
    /// A link that was welcomed and stayed up this long resets the wait.
    pub stable: Duration,
}

impl Default for Timings {
    fn default() -> Self {
        Self {
            welcome_timeout: Duration::from_secs(30),
            join_timeout: Duration::from_secs(20),
            join_retry: Duration::from_secs(5),
            join_retry_slow: Duration::from_secs(300),
            part_timeout: Duration::from_secs(20),
            ping_idle: Duration::from_secs(120),
            pong_timeout: Duration::from_secs(60),
            backoff_initial: Duration::from_secs(5),
            backoff_max: Duration::from_secs(300),
            stable: Duration::from_secs(60),
        }
    }
}

/// Retries of a JOIN at the doubling wait before the slow one.
pub const JOIN_FAST_RETRIES: u32 = 3;
/// PINGs the bouncer sends without a PONG before it drops the link.
const PING_TRIES: u32 = 2;

/// Sets both `ping_idle` and `pong_timeout`, in seconds.
const PING_ENV: &str = "RRC_BOUNCER_PING_SECS";
/// Sets `backoff_initial`, in seconds; `backoff_max` becomes 60 times it
/// (as 300 is to 5), up to 300.
const BACKOFF_ENV: &str = "RRC_BOUNCER_BACKOFF_SECS";

fn env_secs(name: &str) -> Option<Duration> {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|&secs| secs > 0.0 && secs.is_finite())
        .map(Duration::from_secs_f64)
}

impl Timings {
    pub fn from_env() -> Self {
        let mut t = Self::default();
        if let Some(ping) = env_secs(PING_ENV) {
            t.ping_idle = ping;
            t.pong_timeout = ping;
        }
        if let Some(backoff) = env_secs(BACKOFF_ENV) {
            t.backoff_initial = backoff;
            t.backoff_max = (backoff * 60).min(t.backoff_max);
        }
        t
    }
}

/// The wait between connection attempts: `initial`, doubling up to `max`.
#[derive(Debug, Clone)]
pub struct Backoff {
    initial: Duration,
    max: Duration,
    next: Duration,
}

impl Backoff {
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max,
            next: initial,
        }
    }

    /// The wait before the next attempt.
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = (self.next * 2).min(self.max);
        delay
    }

    pub fn reset(&mut self) {
        self.next = self.initial;
    }
}

/// What the session made of an envelope from the hub.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inbound {
    /// PING, PONG, a repeated WELCOME, a late PARTED for a room already
    /// taken as left: nothing for anyone else.
    Handled,
    /// The WELCOME: the session is up.
    Welcomed,
    /// The hub's answer to the bouncer's JOIN.
    JoinConfirmed(String),
    /// rrcd refused the bouncer's JOIN (+i, +k, banned, a bad name, too
    /// many rooms): not tried again on this link.
    JoinFailed { room: String, reason: String },
    /// The hub's answer to the bouncer's PART.
    PartConfirmed(String),
    /// rrcd took the bouncer out of a room (`kicked from <room>`, or
    /// `banned from <room>` for a ban that removes members).
    Removed { room: String, reason: String },
    /// Any other ERROR. Most answer a client's envelope (the relay routes
    /// it to the client that most likely caused it); the room state does
    /// not change.
    HubError(String),
    /// Everything for the relay and history (M4): MSG, NOTICE, ACTION,
    /// other members' JOINED and PARTED, types this codec does not know.
    Relay,
}

/// Why a session ended on its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dead {
    NoWelcome,
    NoPong,
}

impl std::fmt::Display for Dead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoWelcome => write!(f, "no WELCOME from the hub"),
            Self::NoPong => write!(f, "no PONG from the hub"),
        }
    }
}

/// The rooms as the session sees them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoomsView {
    pub wanted: Vec<String>,
    pub joined: Vec<String>,
    /// Rooms rrcd refused on this link, with its text.
    pub failed: BTreeMap<String, String>,
    /// Rooms whose JOIN got no answer or was rate limited, with why; they
    /// are tried again.
    pub retrying: BTreeMap<String, String>,
    pub over_limit: Vec<String>,
}

/// rrcd's texts for an ERROR answering a JOIN that names no room
/// (`router.py` `_handle_join`; the name checks are `service.py`
/// `_norm_room`, whose texts all start "room name").
fn join_refusal_without_room(text: &str) -> bool {
    text == "JOIN requires room name" || text == "too many rooms" || text.starts_with("room name")
}

/// rrcd's texts for an ERROR naming the room that refuse a JOIN.
fn join_refusal_with_room(text: &str) -> bool {
    matches!(text, "invite-only (+i)" | "bad key (+k)" | "banned from room")
}

/// A JOIN retry: how many have been sent and when the next may go.
#[derive(Debug, Clone, Copy)]
struct Retry {
    attempts: u32,
    next_at: Instant,
}

/// One upstream session, from HELLO until the link goes.
#[derive(Debug)]
pub struct Session {
    user: [u8; 16],
    nick: Option<String>,
    timings: Timings,
    welcome: Option<Welcome>,
    hello_at: Instant,
    last_heard: Instant,
    /// The last PING the bouncer sent and how many in a row lack a PONG.
    ping_sent: Option<(Instant, u32)>,
    rooms: RoomsView,
    pending_join: Option<(String, Instant)>,
    retries: BTreeMap<String, Retry>,
    parting: Vec<(String, Instant)>,
    outbox: Vec<Envelope>,
}

impl Session {
    /// A new session on a link that was just established; the HELLO is in
    /// the outbox.
    pub fn new(user: [u8; 16], nick: Option<String>, rooms: Vec<String>, timings: Timings, now: Instant) -> Self {
        let mut session = Self {
            user,
            nick,
            timings,
            welcome: None,
            hello_at: now,
            last_heard: now,
            ping_sent: None,
            rooms: RoomsView {
                wanted: rooms,
                ..RoomsView::default()
            },
            pending_join: None,
            retries: BTreeMap::new(),
            parting: Vec::new(),
            outbox: Vec::new(),
        };
        let hello = crate::envelope::Hello {
            name: Some(CLIENT_NAME.into()),
            ver: Some(Value::Integer(RRC_VERSION.into())),
            caps: CAPS,
            legacy_nick: None,
        };
        let env = session.envelope(MessageType::Hello).with_body(hello.to_body());
        session.outbox.push(env);
        session
    }

    /// The bouncer's own envelopes carry the user's identity in K_SRC and
    /// the hub's configured nick in K_NICK.
    fn envelope(&self, t: MessageType) -> Envelope {
        Envelope::new(t, &self.user).with_nick(self.nick.as_deref())
    }

    pub fn welcome(&self) -> Option<&Welcome> {
        self.welcome.as_ref()
    }

    pub fn limits(&self) -> Limits {
        self.welcome.as_ref().map(|w| w.limits).unwrap_or_default()
    }

    pub fn rooms(&self) -> &RoomsView {
        &self.rooms
    }

    /// What to send, in order.
    pub fn take_outbox(&mut self) -> Vec<Envelope> {
        std::mem::take(&mut self.outbox)
    }

    fn is_joined(&self, room: &str) -> bool {
        self.rooms.joined.iter().any(|r| r == room)
    }

    /// Whether the bouncer is in `room` on this link and not leaving it.
    pub fn is_in(&self, room: &str) -> bool {
        self.is_joined(room) && !self.is_parting(room)
    }

    fn is_parting(&self, room: &str) -> bool {
        self.parting.iter().any(|(r, _)| r == room)
    }

    fn is_pending(&self, room: &str) -> bool {
        self.pending_join.as_ref().is_some_and(|(r, _)| r == room)
    }

    /// Whether a client envelope for `room` may go now. Until the WELCOME
    /// nothing may; after it, an envelope for a room of the room set waits
    /// until the room is joined, unless the hub will not have the bouncer
    /// in it on this link (refused, over the limit), when the hub's answer
    /// is for the client. Envelopes for other rooms, or none, go at once.
    pub fn may_send(&self, room: Option<&str>) -> bool {
        if self.welcome.is_none() {
            return false;
        }
        let Some(room) = room.and_then(crate::config::normalize_room) else {
            return true;
        };
        !self.rooms.wanted.contains(&room)
            || self.is_joined(&room)
            || self.rooms.failed.contains_key(&room)
            || self.rooms.over_limit.contains(&room)
    }

    /// Stay in `room` from now on (it is joined at the next `tick`).
    pub fn want(&mut self, room: &str) {
        if !self.rooms.wanted.iter().any(|r| r == room) {
            self.rooms.wanted.push(room.to_string());
        }
        // Asked for again: try again, now.
        self.rooms.failed.remove(room);
        self.rooms.retrying.remove(room);
        self.retries.remove(room);
    }

    /// Leave `room`.
    pub fn unwant(&mut self, room: &str, now: Instant) {
        self.rooms.wanted.retain(|r| r != room);
        self.rooms.failed.remove(room);
        self.rooms.retrying.remove(room);
        self.rooms.over_limit.retain(|r| r != room);
        self.retries.remove(room);
        if (self.is_joined(room) || self.is_pending(room)) && !self.is_parting(room) {
            self.part(room, now);
        }
    }

    fn part(&mut self, room: &str, now: Instant) {
        let env = self.envelope(MessageType::Part).with_room(room);
        self.outbox.push(env);
        self.parting.push((room.to_string(), now));
    }

    /// Whether a JOINED or PARTED is about the bouncer. rrcd sends both
    /// from its own identity (K_SRC is the hub's), to the member itself
    /// without a nick and to the others with the member's nick, which a
    /// member without a nick does not have. With
    /// `include_joined_member_list` the body lists the member's identity
    /// (the answer to a JOIN lists every member), which settles it.
    fn is_about_me(&self, env: &Envelope) -> bool {
        match env.members() {
            Some(members) => members.iter().any(|m| m[..] == self.user[..]),
            None => env.nick.is_none(),
        }
    }

    pub fn handle(&mut self, env: &Envelope, now: Instant) -> Inbound {
        self.last_heard = now;
        match env.t {
            MessageType::Ping => {
                // rrcd answers a PING with the same body; so does the bouncer.
                let mut pong = Envelope::new(MessageType::Pong, &self.user);
                pong.body = env.body.clone();
                self.outbox.push(pong);
                Inbound::Handled
            }
            MessageType::Pong => {
                self.ping_sent = None;
                Inbound::Handled
            }
            MessageType::Welcome => {
                if self.welcome.is_none() {
                    // A WELCOME without a body still welcomes; the limits
                    // are then rrcd's defaults.
                    let empty = Raw::from_value(&Value::Map(Vec::new()));
                    let welcome = Welcome::from_body(env.body.as_ref().unwrap_or(&empty));
                    self.welcome = Some(welcome);
                    Inbound::Welcomed
                } else {
                    Inbound::Handled
                }
            }
            MessageType::Joined => self.joined(env, now),
            MessageType::Parted => self.parted(env),
            MessageType::Error => self.error(env, now),
            _ => Inbound::Relay,
        }
    }

    fn joined(&mut self, env: &Envelope, now: Instant) -> Inbound {
        let Some(room) = env.room.as_deref() else {
            return Inbound::Relay;
        };
        if self.is_joined(room) {
            return Inbound::Relay;
        }
        // rrcd sends JOINED only to the room's members, so a JOINED for a
        // room the bouncer asked to join on this link and is not in yet
        // means rrcd has it there: the answer to the pending JOIN, or a
        // late answer to one that timed out.
        if !self.is_pending(room) && !self.retries.contains_key(room) {
            return Inbound::Relay;
        }
        if self.is_pending(room) {
            self.pending_join = None;
        }
        self.retries.remove(room);
        self.rooms.retrying.remove(room);
        self.rooms.joined.push(room.to_string());
        // Left again while the JOIN was out: leave it once more.
        if !self.rooms.wanted.iter().any(|r| r == room) && !self.is_parting(room) {
            self.part(room, now);
        }
        Inbound::JoinConfirmed(room.to_string())
    }

    fn parted(&mut self, env: &Envelope) -> Inbound {
        let Some(room) = env.room.as_deref() else {
            return Inbound::Relay;
        };
        if self.is_parting(room) && self.is_about_me(env) {
            self.parting.retain(|(r, _)| r != room);
            self.rooms.joined.retain(|r| r != room);
            return Inbound::PartConfirmed(room.to_string());
        }
        // rrcd tells only members about a PART, so one for a room the
        // bouncer is not in and did not try to join is the late answer to
        // a PART already taken as done.
        if !self.is_joined(room) && !self.is_pending(room) && !self.retries.contains_key(room) {
            return Inbound::Handled;
        }
        Inbound::Relay
    }

    fn error(&mut self, env: &Envelope, now: Instant) -> Inbound {
        let reason = env.text_body().unwrap_or_else(|| "(no text)".into());
        let room = env.room.as_deref();
        // A kick (`commands.py` "kicked from {r}") or a ban that removes
        // members ("banned from {r}"), both naming the room.
        if let Some(room) = room
            && self.is_joined(room)
            && (reason.starts_with("kicked from ") || reason == format!("banned from {room}"))
        {
            self.rooms.joined.retain(|r| r != room);
            self.parting.retain(|(r, _)| r != room);
            self.rooms.failed.insert(room.to_string(), reason.clone());
            return Inbound::Removed {
                room: room.to_string(),
                reason,
            };
        }
        if let Some((pending, _)) = self.pending_join.clone() {
            // rrcd names the room in a refusal about the room (+i, +k,
            // ban) and leaves it out of one about the request (too many
            // rooms, a bad name).
            let refused = match room {
                Some(room) => room == pending && join_refusal_with_room(&reason),
                None => join_refusal_without_room(&reason),
            };
            if refused {
                self.pending_join = None;
                self.retries.remove(&pending);
                self.rooms.retrying.remove(&pending);
                self.rooms.failed.insert(pending.clone(), reason.clone());
                return Inbound::JoinFailed { room: pending, reason };
            }
            // rrcd drops a packet that finds its bucket empty and says so
            // without saying which; it may have been the JOIN, which is
            // sent again later (a JOIN for a room rrcd has the bouncer in
            // is answered with JOINED again).
            if room.is_none() && reason == "rate limited" {
                self.pending_join = None;
                self.schedule_retry(&pending, "rate limited", now);
            }
        }
        Inbound::HubError(reason)
    }

    fn schedule_retry(&mut self, room: &str, reason: &str, now: Instant) {
        let attempts = self.retries.get(room).map_or(0, |r| r.attempts) + 1;
        let wait = if attempts <= JOIN_FAST_RETRIES {
            self.timings.join_retry * 2u32.pow(attempts - 1)
        } else {
            self.timings.join_retry_slow
        };
        self.retries.insert(
            room.to_string(),
            Retry {
                attempts,
                next_at: now + wait,
            },
        );
        self.rooms.retrying.insert(
            room.to_string(),
            format!("{reason}; {attempts} tr{}", if attempts == 1 { "y" } else { "ies" }),
        );
    }

    /// Time passing: join the next room, PING when idle, give up on a
    /// silent hub.
    pub fn tick(&mut self, now: Instant) -> Result<(), Dead> {
        let Some(welcome) = &self.welcome else {
            if now.saturating_duration_since(self.hello_at) >= self.timings.welcome_timeout {
                return Err(Dead::NoWelcome);
            }
            return Ok(());
        };
        let limits = welcome.limits;
        match self.ping_sent {
            Some((sent, tries)) if now.saturating_duration_since(sent) >= self.timings.pong_timeout => {
                if tries >= PING_TRIES {
                    return Err(Dead::NoPong);
                }
                self.ping(now, tries + 1);
            }
            Some(_) => {}
            None if now.saturating_duration_since(self.last_heard) >= self.timings.ping_idle => self.ping(now, 1),
            None => {}
        }
        if let Some((room, sent)) = self.pending_join.clone()
            && now.saturating_duration_since(sent) >= self.timings.join_timeout
        {
            self.pending_join = None;
            self.schedule_retry(&room, "no answer from the hub", now);
        }
        let part_timeout = self.timings.part_timeout;
        let (late, parting): (Vec<_>, Vec<_>) = std::mem::take(&mut self.parting)
            .into_iter()
            .partition(|(_, sent)| now.saturating_duration_since(*sent) >= part_timeout);
        self.parting = parting;
        for (room, _) in late {
            self.rooms.joined.retain(|r| *r != room);
        }
        self.join_next(limits, now);
        Ok(())
    }

    fn ping(&mut self, now: Instant, tries: u32) {
        let body = Raw::from_value(&Value::Float(crate::bouncer::now()));
        let env = self.envelope(MessageType::Ping).with_body(body);
        self.outbox.push(env);
        self.ping_sent = Some((now, tries));
    }

    /// One JOIN at a time, so each answer can be told apart: rrcd's errors
    /// for a refused JOIN do not always name the room.
    fn join_next(&mut self, limits: Limits, now: Instant) {
        if self.pending_join.is_some() {
            return;
        }
        // A room left (or a kick) makes room for those over the limit.
        if (self.rooms.joined.len() as u64) < limits.max_rooms_per_session {
            self.rooms.over_limit.clear();
        }
        let next = self.rooms.wanted.iter().find(|room| {
            !self.rooms.joined.contains(room)
                && !self.rooms.failed.contains_key(*room)
                && !self.rooms.over_limit.contains(room)
                && !self.parting.iter().any(|(r, _)| r == *room)
                && self.retries.get(*room).is_none_or(|r| now >= r.next_at)
        });
        let Some(room) = next.cloned() else {
            return;
        };
        if room.len() as u64 > limits.max_room_name_bytes {
            self.rooms.failed.insert(
                room,
                format!("name longer than the hub's {} bytes", limits.max_room_name_bytes),
            );
            return;
        }
        if self.rooms.joined.len() as u64 >= limits.max_rooms_per_session {
            self.rooms.over_limit.push(room);
            return;
        }
        let env = self.envelope(MessageType::Join).with_room(&room);
        self.outbox.push(env);
        self.pending_join = Some((room, now));
    }
}

/// The size of the channel an upstream session takes commands from. The
/// session never blocks on whoever sends them, and they must not block on
/// the session: send with `try_send`, and count and report a full channel
/// as a dropped send.
pub const COMMAND_QUEUE: usize = 1024;
/// How many events may wait for the relay before the session drops
/// traffic events (see [`EventSender`]).
pub const EVENT_QUEUE: usize = 4096;

/// The sending half of the channel from an upstream session to its relay.
/// It never blocks. State events (`Up`, `Down`, and the session's room
/// changes: JoinConfirmed, JoinFailed, PartConfirmed, Removed) always go,
/// so the relay's view of the session cannot fall out of step with it.
/// Traffic events (relayed envelopes, hub ERRORs, dropped sends) are
/// dropped, and counted in `dropped_events`, while `EVENT_QUEUE` events
/// wait.
#[derive(Debug, Clone)]
pub struct EventSender {
    tx: mpsc::UnboundedSender<UpstreamEvent>,
    backlog: Arc<AtomicUsize>,
}

/// The relay's half of [`EventSender`]'s channel.
#[derive(Debug)]
pub struct EventReceiver {
    rx: mpsc::UnboundedReceiver<UpstreamEvent>,
    backlog: Arc<AtomicUsize>,
}

pub fn event_channel() -> (EventSender, EventReceiver) {
    let (tx, rx) = mpsc::unbounded_channel();
    let backlog = Arc::new(AtomicUsize::new(0));
    (
        EventSender {
            tx,
            backlog: backlog.clone(),
        },
        EventReceiver { rx, backlog },
    )
}

impl EventSender {
    /// Send an event; false if it was a traffic event dropped for the
    /// backlog (or the relay is gone).
    pub fn send(&self, event: UpstreamEvent) -> bool {
        if !event.is_state() && self.backlog.load(Ordering::Relaxed) >= EVENT_QUEUE {
            return false;
        }
        self.backlog.fetch_add(1, Ordering::Relaxed);
        if self.tx.send(event).is_err() {
            self.backlog.fetch_sub(1, Ordering::Relaxed);
            return false;
        }
        true
    }
}

impl EventReceiver {
    pub async fn recv(&mut self) -> Option<UpstreamEvent> {
        let event = self.rx.recv().await?;
        self.backlog.fetch_sub(1, Ordering::Relaxed);
        Some(event)
    }

    pub fn try_recv(&mut self) -> Option<UpstreamEvent> {
        let event = self.rx.try_recv().ok()?;
        self.backlog.fetch_sub(1, Ordering::Relaxed);
        Some(event)
    }
}

/// Client envelopes on their way to the hub, and when they came.
type ClientQueue = VecDeque<(Envelope, Instant)>;

/// What the rest of the bouncer can ask of an upstream session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Send a client's envelope upstream, sharing the session's rate limit.
    /// With no link it is dropped; while the link waits for its WELCOME, or
    /// the envelope's room is being joined, it is held (up to
    /// `CLIENT_QUEUE` envelopes, each at most `CLIENT_HOLD`). A dropped
    /// envelope is counted in `dropped_sends` and reported as
    /// [`UpstreamEvent::SendDropped`].
    Send(Envelope),
    /// Add a room to the hub's room set (saved) and join it.
    Join(String),
    /// Take a room out of the room set and leave it. A room from the config
    /// file stays out until the process restarts.
    Part(String),
}

/// What an upstream session tells the rest of the bouncer.
#[derive(Debug, Clone)]
pub enum UpstreamEvent {
    /// The hub welcomed the session. `hub` is the hub's identity hash
    /// (the WELCOME's K_SRC), which the hub's own envelopes carry.
    Up { welcome: Welcome, hub: Vec<u8> },
    /// The link went.
    Down,
    /// An envelope from the hub, and what the session made of it.
    Envelope(Envelope, Inbound),
    /// A client envelope from [`Command::Send`] that was not sent, and why;
    /// its `id` is the client's.
    SendDropped { envelope: Envelope, reason: String },
    /// A RESOURCE_ENVELOPE from the hub and the Resource it announced,
    /// whose size and sha256 agree with it.
    Resource { envelope: Envelope, data: Arc<Vec<u8>> },
}

impl UpstreamEvent {
    /// Whether the event changes what the relay knows of the session's
    /// state (it is never dropped, see [`EventSender`]).
    pub fn is_state(&self) -> bool {
        match self {
            Self::Up { .. } | Self::Down => true,
            Self::Envelope(_, inbound) => matches!(
                inbound,
                Inbound::JoinConfirmed(_)
                    | Inbound::JoinFailed { .. }
                    | Inbound::PartConfirmed(_)
                    | Inbound::Removed { .. }
            ),
            Self::SendDropped { .. } | Self::Resource { .. } => false,
        }
    }
}

/// The hub's caps and limits as status.json shows them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HubCaps {
    pub resource_envelope: bool,
    pub action: bool,
    pub direct_notice: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HubLimits {
    pub max_nick_bytes: u64,
    pub max_room_name_bytes: u64,
    pub max_msg_body_bytes: u64,
    pub max_rooms_per_session: u64,
    pub rate_limit_msgs_per_minute: u64,
}

/// One upstream session in status.json. Every field has a default, so a
/// status file from an older bouncer still reads.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct UpstreamStatus {
    /// `starting`, `connecting`, `linked` (waiting for WELCOME),
    /// `connected`, `waiting` (to connect again), `stopped`.
    pub state: String,
    pub hub_name: Option<String>,
    pub hub_version: Option<String>,
    pub hub_caps: Option<HubCaps>,
    pub hub_limits: Option<HubLimits>,
    /// The room set: the config's rooms and the saved ones.
    pub rooms: Vec<String>,
    pub rooms_joined: Vec<String>,
    /// Rooms the hub refused on the current link, with its reason.
    pub rooms_failed: BTreeMap<String, String>,
    /// Rooms whose JOIN is being tried again on the current link, with why.
    pub rooms_retrying: BTreeMap<String, String>,
    /// Rooms past the hub's `max_rooms_per_session` on the current link.
    pub rooms_over_limit: Vec<String>,
    pub last_connected_at: Option<f64>,
    pub last_disconnected_at: Option<f64>,
    pub last_error: Option<String>,
    /// Sessions welcomed since start.
    pub connects: u64,
    /// Sessions welcomed after the first.
    pub reconnects: u64,
    /// Connection attempts that did not get a WELCOME.
    pub failed_attempts: u64,
    pub next_attempt_at: Option<f64>,
    /// Envelopes from the hub with no relay to take them, by type.
    pub unhandled: BTreeMap<String, u64>,
    /// Client envelopes not sent: no link, the queue full, held too long,
    /// or still queued when the link ended.
    pub dropped_sends: u64,
    /// Events the relay did not take in time (its channel was full).
    pub dropped_events: u64,
    /// Resources from the hub received whole and matching their
    /// RESOURCE_ENVELOPE.
    pub hub_resources_in: u64,
    /// Resources from the hub dropped, each once, when it ended: refused
    /// (too large, split, a request, too many at once), announced by no
    /// RESOURCE_ENVELOPE, a sha256 other than announced, failed, or still
    /// on its way when the link ended.
    pub hub_resource_drops: u64,
}

pub type SharedStatus = Arc<Mutex<UpstreamStatus>>;

fn type_name(t: MessageType) -> String {
    match t {
        MessageType::Unknown(n) => format!("t{n}"),
        other => format!("{other:?}").to_uppercase(),
    }
}

fn version_text(ver: &Option<Value>) -> Option<String> {
    match ver.as_ref()? {
        Value::Text(text) => Some(text.clone()),
        Value::Integer(n) => Some(i128::from(*n).to_string()),
        Value::Float(f) => Some(f.to_string()),
        other => Some(format!("{other:?}")),
    }
}

/// How long a whole connection attempt may take, path discovery, link
/// establishment and identification together.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(60);
/// How long to look for a path to the hub.
const PATH_TIMEOUT: Duration = Duration::from_secs(30);
/// How long to wait for the link to close after asking it to. The
/// bouncer's `SESSION_STOP` is longer, so a session stopping at shutdown
/// still writes its status.
pub const CLOSE_WAIT: Duration = Duration::from_secs(5);
/// How often the session's clock runs.
const TICK: Duration = Duration::from_millis(250);
/// Client envelopes waiting for a token or a room beyond this many are
/// dropped.
pub const CLIENT_QUEUE: usize = 256;
/// A client envelope held this long (its room not joined yet) is dropped:
/// a chat line sent much later than it was written is worse than none.
pub const CLIENT_HOLD: Duration = Duration::from_secs(60);
/// The longest local hub name, in bytes. The announce app data
/// `{"proto": "rrc", "v": 1, "hub": name}` is then at most 120 bytes,
/// well inside an announce's room for it.
pub const MAX_HUB_NAME_BYTES: usize = 96;
const VIA: &str = " (via rrc-bouncer)";

/// The local hub's name for an upstream hub named `name`, cut on a
/// character boundary to fit `MAX_HUB_NAME_BYTES`.
pub fn local_name(name: &str) -> String {
    let mut end = name.len().min(MAX_HUB_NAME_BYTES - VIA.len());
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{VIA}", &name[..end])
}

/// Everything an upstream session task needs.
pub struct Upstream {
    pub runtime: ReticulumHandle,
    /// The upstream hub's destination.
    pub address: [u8; 16],
    /// The user's identity, which the link is identified as.
    pub identity: Arc<Identity>,
    pub nick: Option<String>,
    pub rooms: RoomSet,
    /// The local hub standing for this one: it takes the upstream name.
    pub local_hub: DestinationHandle,
    pub timings: Timings,
    pub status: SharedStatus,
    /// Woken whenever `status` changes.
    pub changed: Arc<Notify>,
    /// Woken when the local hub's name changes.
    pub renamed: Arc<Notify>,
    /// The relay's channel (None: events are only counted, as
    /// `unhandled`), from [`event_channel`]; the session never waits on it.
    pub events: Option<EventSender>,
}

/// How a link ended.
struct Ended {
    welcomed_for: Option<Duration>,
    reason: String,
    shutdown: bool,
}

impl Upstream {
    fn tag(&self) -> String {
        hex::encode(self.address)
    }

    fn update(&self, f: impl FnOnce(&mut UpstreamStatus)) {
        if let Ok(mut status) = self.status.lock() {
            let before = status.clone();
            f(&mut status);
            if *status != before {
                self.changed.notify_one();
            }
        }
    }

    /// Bump a counter. Counters change with traffic, so they reach
    /// status.json with the next periodic write, not one write each.
    fn tally(&self, f: impl FnOnce(&mut UpstreamStatus)) {
        if let Ok(mut status) = self.status.lock() {
            f(&mut status);
        }
    }

    fn show_rooms(&self, view: &RoomsView) {
        self.update(|s| {
            s.rooms = view.wanted.clone();
            s.rooms_joined = view.joined.clone();
            s.rooms_failed = view.failed.clone();
            s.rooms_retrying = view.retrying.clone();
            s.rooms_over_limit = view.over_limit.clone();
        });
    }

    fn show_room_set(&self) {
        let rooms = self.rooms.rooms();
        self.update(|s| s.rooms = rooms);
    }

    /// Hand the relay an event without waiting: a traffic event finding
    /// `EVENT_QUEUE` events waiting is dropped.
    fn emit(&self, event: UpstreamEvent) {
        let Some(events) = &self.events else { return };
        if !events.send(event) {
            let mut dropped = 0;
            self.tally(|s| {
                s.dropped_events += 1;
                dropped = s.dropped_events;
            });
            // Logged at the 1st, 2nd, 4th, 8th, ... drop.
            if dropped.is_power_of_two() {
                tracing::warn!(
                    "upstream {}: the relay is not keeping up: {dropped} events dropped",
                    self.tag()
                );
            }
        }
    }

    /// A client envelope that will not be sent.
    fn drop_send(&self, envelope: Envelope, reason: &str) {
        tracing::debug!(
            "upstream {}: dropped a client {:?} room={:?}: {reason}",
            self.tag(),
            envelope.t,
            envelope.room
        );
        self.tally(|s| s.dropped_sends += 1);
        self.emit(UpstreamEvent::SendDropped {
            envelope,
            reason: reason.into(),
        });
    }

    /// Queue a client envelope for the hub.
    fn queue_client(&self, client: &mut ClientQueue, env: Envelope) {
        if client.len() >= CLIENT_QUEUE {
            tracing::warn!("upstream {}: client queue full, dropped a {:?}", self.tag(), env.t);
            self.drop_send(env, "the queue is full");
        } else {
            client.push_back((env, Instant::now()));
        }
    }

    /// A Resource from the hub, whole and matching its envelope.
    fn hub_resource(&self, e: Expected, data: Vec<u8>) {
        tracing::debug!(
            "upstream {}: received a {} resource of {} bytes room={:?}",
            self.tag(),
            e.body.kind,
            data.len(),
            e.envelope.room
        );
        self.tally(|s| s.hub_resources_in += 1);
        self.emit(UpstreamEvent::Resource {
            envelope: e.envelope,
            data: Arc::new(data),
        });
    }

    /// A Resource from the hub that will not reach the relay.
    fn drop_resource(&self, reason: &str) {
        tracing::warn!("upstream {}: dropped a resource: {reason}", self.tag());
        self.tally(|s| s.hub_resource_drops += 1);
    }

    fn count_unhandled(&self, t: MessageType) {
        self.tally(|s| *s.unhandled.entry(type_name(t)).or_default() += 1);
    }

    /// A command while there is no session: the room set still changes.
    fn command_offline(&mut self, command: Command) {
        match command {
            Command::Send(env) => self.drop_send(env, "not connected"),
            Command::Join(room) => {
                self.add_room(&room);
            }
            Command::Part(room) => {
                self.remove_room(&room);
            }
        }
    }

    fn add_room(&mut self, room: &str) {
        if let Err(e) = self.rooms.add(room) {
            tracing::warn!("upstream {}: could not save the room set: {e}", self.tag());
        }
        self.show_room_set();
    }

    fn remove_room(&mut self, room: &str) {
        let configured = crate::config::normalize_room(room).is_some_and(|r| self.rooms.is_configured(&r));
        match self.rooms.remove(room) {
            Ok(true) if configured => tracing::info!(
                "upstream {}: room {room} is in the config file; it stays out until the bouncer restarts",
                self.tag()
            ),
            Ok(_) => {}
            Err(e) => tracing::warn!("upstream {}: could not save the room set: {e}", self.tag()),
        }
        self.show_room_set();
    }

    async fn connect(&self) -> Result<LinkSession, String> {
        let key = self
            .identity
            .get_private_key()
            .ok_or("the user identity has no private key")?;
        // connect_link takes the identity by value and Identity is not
        // Clone: each attempt gets its own copy.
        let identity = Identity::from_private_key(&key[..]).map_err(|e| e.to_string())?;
        let options = LinkConnectOptions {
            path_timeout: PATH_TIMEOUT,
            client_label: "rrc-bouncer.upstream".into(),
            identify: true,
            ..LinkConnectOptions::default()
        };
        match tokio::time::timeout(
            CONNECT_TIMEOUT,
            self.runtime.connect_link(self.address, identity, options),
        )
        .await
        {
            Ok(Ok(link)) => Ok(link),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err(format!("no link after {}s", CONNECT_TIMEOUT.as_secs())),
        }
    }

    /// Run one established link until it ends.
    async fn drive(
        &mut self,
        mut link: LinkSession,
        commands: &mut mpsc::Receiver<Command>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Ended {
        let tag = self.tag();
        let started = Instant::now();
        let mut session = Session::new(
            self.identity.hash,
            self.nick.clone(),
            self.rooms.rooms(),
            self.timings,
            started,
        );
        let mut welcomed_at: Option<Instant> = None;
        // The hub's identity hash, from its WELCOME.
        let mut hub: Option<Vec<u8>> = None;
        // rrcd's bucket for a link is full when the link comes up and holds
        // its default until the WELCOME says otherwise.
        let mut bucket = TokenBucket::new(Limits::default().rate_limit_msgs_per_minute, started);
        let mut control: VecDeque<Envelope> = VecDeque::new();
        // Client envelopes and when they came.
        let mut client: ClientQueue = VecDeque::new();
        // RESOURCE_ENVELOPEs from the hub waiting for their Resource (and
        // finished Resources waiting for theirs); offers waiting for their
        // RESOURCE_ENVELOPE (the two come on different channels); the
        // sizes of the Resources being received, each until it ends.
        let mut expected = Expectations::default();
        let mut offers: Vec<(LinkSessionResourceOffer, Instant)> = Vec::new();
        let mut receiving: Vec<u64> = Vec::new();
        let (received_tx, mut received_rx) = mpsc::channel::<(u64, Result<Vec<u8>, String>)>(resource::MAX_EXPECTED);
        let mut send_at: Option<Instant> = None;
        let mut commands_open = true;
        let mut ticker = tokio::time::interval(TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        self.update(|s| s.state = "linked".into());

        let (reason, shut) = 'link: loop {
            control.extend(session.take_outbox());
            let now = Instant::now();
            // The queue is in arrival order: what was held too long is at
            // its front.
            while client
                .front()
                .is_some_and(|(_, at)| now.saturating_duration_since(*at) >= CLIENT_HOLD)
            {
                if let Some((env, _)) = client.pop_front() {
                    self.drop_send(env, "held too long: its room was not joined");
                }
            }
            // Send what the bucket allows: the bouncer's own envelopes
            // first, then the clients' that may go (in order per room).
            while send_at.is_none_or(|at| now >= at) {
                let pick = if control.is_empty() {
                    match client.iter().position(|(e, _)| session.may_send(e.room.as_deref())) {
                        Some(pos) => Some(pos),
                        None => break,
                    }
                } else {
                    None
                };
                if let Err(wait) = bucket.take(now) {
                    send_at = Some(now + wait);
                    break;
                }
                send_at = None;
                let env = match pick {
                    None => control.pop_front(),
                    Some(pos) => client.remove(pos).map(|(env, _)| env),
                };
                let Some(env) = env else { break };
                match link.handle.send_packet(env.encode()).await {
                    Ok(_) => tracing::debug!("upstream {tag}: sent {:?} room={:?}", env.t, env.room),
                    Err(LinkSessionError::PayloadTooLarge { actual, max }) => {
                        tracing::warn!(
                            "upstream {tag}: dropped a {:?} of {actual} bytes (link MDU {max})",
                            env.t
                        );
                        if pick.is_some() {
                            self.drop_send(env, &format!("{actual} bytes is more than the link carries ({max})"));
                        }
                    }
                    Err(e) => {
                        if pick.is_some() {
                            self.drop_send(env, "the link ended");
                        }
                        break 'link (format!("send failed: {e}"), false);
                    }
                }
            }
            let sleep_until = send_at.map(tokio::time::Instant::from_std);

            tokio::select! {
                event = link.events.recv() => match event {
                    Some(LinkSessionEvent::Packet { data, .. }) => {
                        let env = match Envelope::decode(&data) {
                            Ok(env) => env,
                            Err(e) => {
                                tracing::debug!("upstream {tag}: undecodable packet: {e}");
                                continue;
                            }
                        };
                        if env.t == MessageType::ResourceEnvelope {
                            session.handle(&env, Instant::now());
                            // An envelope is not counted: its Resource is,
                            // when it ends (refused, if nothing waits for it).
                            match resource::check(&env) {
                                Ok(body) => {
                                    let size = body.size;
                                    match expected.add(env, body, Instant::now()) {
                                        Err(resource::TooMany) => tracing::warn!("upstream {tag}: too many resources announced at once: one ignored"),
                                        Ok(Some((e, data))) => self.hub_resource(e, data),
                                        Ok(None) => if let Some(pos) = offers.iter().position(|(o, _)| o.data_size() as u64 == size) {
                                            let (offer, _) = offers.remove(pos);
                                            receiving.push(size);
                                            accept(offer, received_tx.clone());
                                        }
                                    }
                                }
                                Err(e) => tracing::warn!("upstream {tag}: a malformed resource envelope: {e}"),
                            }
                            continue;
                        }
                        let inbound = session.handle(&env, Instant::now());
                        match &inbound {
                            Inbound::Handled => {}
                            Inbound::Welcomed => if let Some(welcome) = session.welcome().cloned() {
                                welcomed_at = Some(Instant::now());
                                bucket.set_limit(welcome.limits.rate_limit_msgs_per_minute, Instant::now());
                                self.welcomed(&welcome).await;
                                hub = Some(env.src.clone());
                                self.emit(UpstreamEvent::Up { welcome, hub: env.src.clone() });
                            }
                            Inbound::JoinConfirmed(room) => tracing::info!("upstream {tag}: joined {room}"),
                            Inbound::JoinFailed { room, reason } => tracing::warn!("upstream {tag}: could not join {room}: {reason}"),
                            Inbound::PartConfirmed(room) => tracing::info!("upstream {tag}: left {room}"),
                            Inbound::Removed { room, reason } => tracing::warn!("upstream {tag}: out of {room}: {reason}"),
                            Inbound::HubError(reason) => tracing::warn!("upstream {tag}: hub error: {reason} (room={:?})", env.room),
                            Inbound::Relay => tracing::debug!("upstream {tag}: {:?} room={:?}", env.t, env.room),
                        }
                        if !matches!(inbound, Inbound::Handled | Inbound::Welcomed) {
                            if self.events.is_some() {
                                self.emit(UpstreamEvent::Envelope(env, inbound));
                            } else if matches!(inbound, Inbound::Relay | Inbound::HubError(_)) {
                                self.count_unhandled(env.t);
                            }
                        }
                        self.show_rooms(session.rooms());
                    }
                    Some(LinkSessionEvent::Closed { reason }) => break 'link (format!("link closed ({reason:?})"), false),
                    Some(LinkSessionEvent::Stale) => tracing::debug!("upstream {tag}: link stale"),
                    Some(LinkSessionEvent::Recovered) => tracing::debug!("upstream {tag}: link recovered"),
                    Some(_) => {}
                    None => break 'link ("link session ended".into(), false),
                },
                Some(offer) = link.resource_offers.recv() => {
                    if let Some(reason) = refuse_offer(&offer, receiving.len() + offers.len()) {
                        self.drop_resource(&format!("refused an offer of {} bytes: {reason}", offer.data_size()));
                        tokio::spawn(async move { let _ = offer.reject().await; });
                    } else if expected.expects(offer.data_size() as u64) {
                        receiving.push(offer.data_size() as u64);
                        accept(offer, received_tx.clone());
                    } else {
                        offers.push((offer, Instant::now()));
                    }
                }
                Some((size, received)) = received_rx.recv() => {
                    if let Some(pos) = receiving.iter().position(|s| *s == size) {
                        receiving.remove(pos);
                    }
                    match received {
                        // A Resource matching no envelope (yet) waits a
                        // little for one: it may be read later.
                        Ok(data) => match expected.take(data, Instant::now()) {
                            Ok(Some((e, data))) => self.hub_resource(e, data),
                            Ok(None) => {}
                            Err(why) => self.drop_resource(&why),
                        },
                        Err(e) => self.drop_resource(&format!("a resource failed: {e}")),
                    }
                }
                command = commands.recv(), if commands_open => match command {
                    Some(Command::Send(env)) => self.queue_client(&mut client, env),
                    Some(Command::Join(room)) => {
                        self.add_room(&room);
                        if let Some(room) = crate::config::normalize_room(&room) {
                            session.want(&room);
                            // The hub has the bouncer in the room already:
                            // the relay gets its answer at once.
                            if session.is_in(&room)
                                && let Some(hub) = &hub
                            {
                                let joined = Envelope::new(MessageType::Joined, hub).with_room(&room);
                                self.emit(UpstreamEvent::Envelope(joined, Inbound::JoinConfirmed(room)));
                            }
                        }
                        self.show_rooms(session.rooms());
                    }
                    Some(Command::Part(room)) => {
                        self.remove_room(&room);
                        if let Some(room) = crate::config::normalize_room(&room) {
                            session.unwant(&room, Instant::now());
                        }
                        self.show_rooms(session.rooms());
                    }
                    // Nobody can send commands any more; the session goes on.
                    None => commands_open = false,
                },
                _ = ticker.tick() => {
                    let now = Instant::now();
                    let (dropped, forgotten) = expected.expire(now, |size| receiving.contains(&size));
                    for why in dropped {
                        self.drop_resource(&why);
                    }
                    if forgotten > 0 {
                        tracing::info!("upstream {tag}: {forgotten} announced resources did not come in time");
                    }
                    let (late, waiting): (Vec<_>, Vec<_>) = offers
                        .drain(..)
                        .partition(|(_, at)| now.saturating_duration_since(*at) >= resource::OFFER_GRACE);
                    offers = waiting;
                    for (offer, _) in late {
                        self.drop_resource(&format!("no envelope announced an offer of {} bytes", offer.data_size()));
                        tokio::spawn(async move { let _ = offer.reject().await; });
                    }
                    if let Err(dead) = session.tick(Instant::now()) {
                        link.handle.close().await;
                        drain_until_closed(&mut link).await;
                        break 'link (dead.to_string(), false);
                    }
                    self.show_rooms(session.rooms());
                }
                _ = async { tokio::time::sleep_until(sleep_until.unwrap_or_else(tokio::time::Instant::now)).await }, if sleep_until.is_some() => {}
                _ = shutdown.changed() => {
                    link.handle.close().await;
                    drain_until_closed(&mut link).await;
                    break 'link ("shutting down".into(), true);
                }
            }
        };
        if reason.starts_with("send failed") {
            link.handle.close().await;
            drain_until_closed(&mut link).await;
        }
        for (env, _) in client {
            self.drop_send(env, "the link ended");
        }
        // Resources still on the way go with the link; the counts say so.
        // An envelope whose Resource never started is not a Resource.
        for _ in 0..offers.len() + receiving.len() + expected.early() {
            self.drop_resource("the link ended");
        }
        Ended {
            welcomed_for: welcomed_at.map(|t| t.elapsed()),
            reason,
            shutdown: shut,
        }
    }

    async fn welcomed(&mut self, welcome: &Welcome) {
        let tag = self.tag();
        tracing::info!(
            "upstream {tag}: welcomed by {:?} (version {:?}), limits {:?}",
            welcome.hub,
            version_text(&welcome.ver),
            welcome.limits
        );
        let mut renamed = false;
        self.update(|s| {
            if s.hub_name != welcome.hub {
                renamed = welcome.hub.is_some();
            }
            s.state = "connected".into();
            s.hub_name = welcome.hub.clone();
            s.hub_version = version_text(&welcome.ver);
            s.hub_caps = Some(HubCaps {
                resource_envelope: welcome.caps.resource_envelope,
                action: welcome.caps.action,
                direct_notice: welcome.caps.direct_notice,
            });
            let l = welcome.limits;
            s.hub_limits = Some(HubLimits {
                max_nick_bytes: l.max_nick_bytes,
                max_room_name_bytes: l.max_room_name_bytes,
                max_msg_body_bytes: l.max_msg_body_bytes,
                max_rooms_per_session: l.max_rooms_per_session,
                rate_limit_msgs_per_minute: l.rate_limit_msgs_per_minute,
            });
            s.last_connected_at = Some(now());
            if s.connects > 0 {
                s.reconnects += 1;
            }
            s.connects += 1;
            s.last_error = None;
            s.next_attempt_at = None;
        });
        if renamed && let Some(name) = &welcome.hub {
            let name = local_name(name);
            match self.local_hub.set_default_app_data(Some(hub_app_data(&name))).await {
                Ok(()) => {
                    tracing::info!("upstream {tag}: the local hub is now {name:?}");
                    self.renamed.notify_one();
                }
                Err(e) => tracing::warn!("upstream {tag}: could not rename the local hub: {e}"),
            }
        }
    }

    /// Wait out the backoff, still taking commands.
    async fn wait(
        &mut self,
        delay: Duration,
        commands: &mut mpsc::Receiver<Command>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> bool {
        self.update(|s| {
            s.state = "waiting".into();
            s.next_attempt_at = Some(now() + delay.as_secs_f64());
        });
        let deadline = tokio::time::Instant::now() + delay;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => return true,
                Some(command) = commands.recv() => self.command_offline(command),
                _ = shutdown.changed() => return false,
            }
        }
    }
}

/// Why an offer from the hub is refused before its envelope is looked for;
/// `busy` is how many Resources are being received or wait.
fn refuse_offer(offer: &LinkSessionResourceOffer, busy: usize) -> Option<String> {
    if offer.is_request() || offer.is_response() {
        Some("a request or response".into())
    } else if offer.total_segments() > 1 {
        Some(format!("split into {} segments", offer.total_segments()))
    } else if offer.data_size() as u64 > resource::MAX_RESOURCE_BYTES || offer.data_size() == 0 {
        Some(format!("not between 1 and {} bytes", resource::MAX_RESOURCE_BYTES))
    } else if busy >= resource::MAX_EXPECTED {
        Some(format!("{busy} resources are on the way already"))
    } else {
        None
    }
}

/// Take an offer and report the Resource, or why it did not come, on
/// `done`, with its size. It is waited for until it ends: rsReticulum's
/// watchdog ends a transfer that stalls, and the link's end ends them
/// all. (`concluded` takes the handle, so it cannot be cancelled while
/// it is waited for.)
fn accept(offer: LinkSessionResourceOffer, done: mpsc::Sender<(u64, Result<Vec<u8>, String>)>) {
    let size = offer.data_size() as u64;
    tokio::spawn(async move {
        let result = match offer.accept().await {
            Ok(handle) => handle.concluded().await.map(|r| r.data).map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };
        let _ = done.send((size, result)).await;
    });
}

async fn drain_until_closed(link: &mut LinkSession) {
    let _ = tokio::time::timeout(CLOSE_WAIT, async {
        while let Some(event) = link.events.recv().await {
            if matches!(event, LinkSessionEvent::Closed { .. }) {
                break;
            }
        }
    })
    .await;
}

/// The upstream session task: connect, run the link, wait, connect again,
/// until `shutdown` changes.
pub async fn run(mut up: Upstream, mut commands: mpsc::Receiver<Command>, mut shutdown: watch::Receiver<bool>) {
    let tag = up.tag();
    let mut backoff = Backoff::new(up.timings.backoff_initial, up.timings.backoff_max);
    up.show_room_set();
    loop {
        up.update(|s| {
            s.state = "connecting".into();
            s.next_attempt_at = None;
        });
        tracing::info!("upstream {tag}: connecting");
        let connected = tokio::select! {
            result = up.connect() => Some(result),
            _ = shutdown.changed() => None,
        };
        let Some(connected) = connected else { break };
        match connected {
            Ok(link) => {
                tracing::info!(
                    "upstream {tag}: link {} established",
                    hex::encode(link.handle.link_id())
                );
                let ended = up.drive(link, &mut commands, &mut shutdown).await;
                if ended.welcomed_for.is_some() {
                    up.emit(UpstreamEvent::Down);
                }
                let welcomed_for = ended.welcomed_for;
                let rooms = up.rooms.rooms();
                up.update(|s| {
                    s.state = if ended.shutdown { "stopped" } else { "disconnected" }.into();
                    s.last_disconnected_at = Some(now());
                    // The room state was the link's.
                    s.rooms = rooms;
                    s.rooms_joined.clear();
                    s.rooms_failed.clear();
                    s.rooms_retrying.clear();
                    s.rooms_over_limit.clear();
                    s.last_error = Some(ended.reason.clone());
                    if welcomed_for.is_none() {
                        s.failed_attempts += 1;
                    }
                });
                if ended.shutdown {
                    break;
                }
                tracing::warn!("upstream {tag}: disconnected: {}", ended.reason);
                if welcomed_for.is_some_and(|d| d >= up.timings.stable) {
                    backoff.reset();
                }
            }
            Err(e) => {
                tracing::warn!("upstream {tag}: could not connect: {e}");
                up.update(|s| {
                    s.failed_attempts += 1;
                    s.last_error = Some(e);
                });
                // The path may be stale (the hub moved or restarted on
                // another interface): ask again before the next attempt.
                if let Err(e) = up.runtime.request_path(up.address, PathRequestOptions::default()).await {
                    tracing::debug!("upstream {tag}: path request: {e}");
                }
            }
        }
        let delay = backoff.next_delay();
        tracing::info!("upstream {tag}: connecting again in {:.1}s", delay.as_secs_f64());
        if !up.wait(delay, &mut commands, &mut shutdown).await {
            break;
        }
    }
    up.update(|s| {
        s.state = "stopped".into();
        s.next_attempt_at = None;
        s.rooms_joined.clear();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    const USER: [u8; 16] = [7; 16];
    const HUB: [u8; 16] = [9; 16];

    fn timings() -> Timings {
        Timings::default()
    }

    fn from_hub(t: MessageType) -> Envelope {
        Envelope::new(t, &HUB)
    }

    fn welcome(max_rooms: u64) -> Envelope {
        let w = Welcome {
            hub: Some("test hub".into()),
            ver: Some(Value::Text("0.3.2".into())),
            caps: Caps {
                resource_envelope: true,
                action: true,
                direct_notice: true,
            },
            limits: Limits {
                max_rooms_per_session: max_rooms,
                max_room_name_bytes: 8,
                ..Limits::default()
            },
        };
        from_hub(MessageType::Welcome).with_body(w.to_body())
    }

    fn session(rooms: &[&str], now: Instant) -> Session {
        Session::new(
            USER,
            Some("bnc".into()),
            rooms.iter().map(|r| r.to_string()).collect(),
            timings(),
            now,
        )
    }

    fn types(out: &[Envelope]) -> Vec<(MessageType, Option<String>)> {
        out.iter().map(|e| (e.t, e.room.clone())).collect()
    }

    #[test]
    fn hello_first_and_nothing_before_the_welcome() {
        let t0 = Instant::now();
        let mut s = session(&["lobby"], t0);
        let out = s.take_outbox();
        assert_eq!(out.len(), 1);
        let hello = &out[0];
        assert_eq!(hello.t, MessageType::Hello);
        assert_eq!(hello.src, USER);
        assert_eq!(hello.nick.as_deref(), Some("bnc"));
        let body = crate::envelope::Hello::from_body(hello.body.as_ref().unwrap());
        assert_eq!(body.name.as_deref(), Some("rrc-bouncer"));
        assert_eq!(body.ver, Some(Value::Integer(1.into())));
        assert_eq!(body.caps, CAPS);
        assert!(body.caps.resource_envelope);
        // No JOIN and no PING before the WELCOME.
        s.tick(t0 + Duration::from_secs(29)).unwrap();
        assert!(s.take_outbox().is_empty());
        assert_eq!(s.tick(t0 + Duration::from_secs(30)), Err(Dead::NoWelcome));
    }

    #[test]
    fn joins_one_at_a_time_within_the_limits() {
        let t0 = Instant::now();
        let mut s = session(&["lobby", "toolongname", "side", "third"], t0);
        s.take_outbox();
        assert_eq!(s.handle(&welcome(2), t0), Inbound::Welcomed);
        assert_eq!(s.limits().max_rooms_per_session, 2);
        s.tick(t0).unwrap();
        let out = s.take_outbox();
        assert_eq!(types(&out), [(MessageType::Join, Some("lobby".into()))]);
        assert_eq!(out[0].nick.as_deref(), Some("bnc"));
        // Still waiting for the answer: nothing more.
        s.tick(t0).unwrap();
        assert!(s.take_outbox().is_empty());
        // Another member's JOINED in a room the bouncer is not in yet is
        // not the answer.
        let other = from_hub(MessageType::Joined).with_room("side").with_nick(Some("x"));
        assert_eq!(s.handle(&other, t0), Inbound::Relay);
        let answer = from_hub(MessageType::Joined).with_room("lobby");
        assert_eq!(s.handle(&answer, t0), Inbound::JoinConfirmed("lobby".into()));
        // The too-long name is skipped without a JOIN.
        s.tick(t0).unwrap();
        assert!(s.rooms().failed.contains_key("toolongname"));
        s.tick(t0).unwrap();
        assert_eq!(types(&s.take_outbox()), [(MessageType::Join, Some("side".into()))]);
        s.handle(&from_hub(MessageType::Joined).with_room("side"), t0);
        s.tick(t0).unwrap();
        assert!(s.take_outbox().is_empty());
        assert_eq!(s.rooms().joined, ["lobby", "side"]);
        assert_eq!(s.rooms().over_limit, ["third"]);
        // Now another member joining lobby is for the relay.
        let other = from_hub(MessageType::Joined).with_room("lobby").with_nick(Some("x"));
        assert_eq!(s.handle(&other, t0), Inbound::Relay);
    }

    fn error(room: Option<&str>, text: &str) -> Envelope {
        let env = from_hub(MessageType::Error).with_body(Raw::text(text));
        match room {
            Some(room) => env.with_room(room),
            None => env,
        }
    }

    /// A welcomed session with `rooms` joined and the outbox empty.
    fn joined_session(rooms: &[&str], t0: Instant) -> Session {
        let mut s = session(rooms, t0);
        s.handle(&welcome(32), t0);
        for room in rooms {
            s.tick(t0).unwrap();
            assert_eq!(
                types(&s.take_outbox()).last(),
                Some(&(MessageType::Join, Some(room.to_string())))
            );
            s.handle(&from_hub(MessageType::Joined).with_room(room), t0);
        }
        s.take_outbox();
        s
    }

    #[test]
    fn each_join_refusal_fails_the_room_for_the_link() {
        // rrcd's texts: router.py _handle_join and service.py _norm_room.
        let refusals = [
            (None, "JOIN requires room name"),
            (None, "too many rooms"),
            (None, "room name must not be empty"),
            (None, "room name too long: 9 bytes > 8 bytes"),
            (Some("a"), "invite-only (+i)"),
            (Some("a"), "bad key (+k)"),
            (Some("a"), "banned from room"),
        ];
        for (room, text) in refusals {
            let t0 = Instant::now();
            let mut s = session(&["a", "b"], t0);
            s.handle(&welcome(32), t0);
            s.tick(t0).unwrap();
            s.take_outbox();
            assert_eq!(
                s.handle(&error(room, text), t0),
                Inbound::JoinFailed {
                    room: "a".into(),
                    reason: text.into()
                },
                "{text}"
            );
            assert_eq!(s.rooms().failed.get("a").map(String::as_str), Some(text));
            // The next room is joined, and the refused one is not tried
            // again on this link, however long it lasts.
            s.tick(t0).unwrap();
            assert_eq!(types(&s.take_outbox()), [(MessageType::Join, Some("b".into()))]);
            s.handle(&from_hub(MessageType::Joined).with_room("b"), t0);
            s.tick(t0 + Duration::from_secs(3600)).unwrap();
            assert!(!types(&s.take_outbox()).iter().any(|(t, _)| *t == MessageType::Join));
            // Asking for it again tries again.
            s.want("a");
            s.tick(t0 + Duration::from_secs(3600)).unwrap();
            assert!(types(&s.take_outbox()).contains(&(MessageType::Join, Some("a".into()))));
        }
    }

    #[test]
    fn a_refusal_naming_another_room_is_not_about_the_join() {
        let t0 = Instant::now();
        let mut s = session(&["a"], t0);
        s.handle(&welcome(32), t0);
        s.tick(t0).unwrap();
        s.take_outbox();
        assert_eq!(
            s.handle(&error(Some("z"), "invite-only (+i)"), t0),
            Inbound::HubError("invite-only (+i)".into())
        );
        assert!(s.rooms().failed.is_empty());
        assert_eq!(
            s.handle(&from_hub(MessageType::Joined).with_room("a"), t0),
            Inbound::JoinConfirmed("a".into())
        );
    }

    #[test]
    fn other_errors_while_a_join_is_pending_leave_it_pending() {
        let t0 = Instant::now();
        let mut s = session(&["a"], t0);
        s.handle(&welcome(32), t0);
        s.tick(t0).unwrap();
        s.take_outbox();
        for (room, text) in [
            (None, "message too large: 400 bytes > 350 bytes"),
            (None, "destination not connected"),
            (None, "bad message: unknown type"),
            (None, "message requires room name"),
            (Some("a"), "no such room"),
            (Some("a"), "unrecognized command"),
            (Some("a"), "not authorized"),
        ] {
            assert_eq!(s.handle(&error(room, text), t0), Inbound::HubError(text.into()));
        }
        assert!(s.rooms().failed.is_empty() && s.rooms().retrying.is_empty());
        s.tick(t0).unwrap();
        assert!(s.take_outbox().is_empty(), "the JOIN is still pending");
        assert_eq!(
            s.handle(&from_hub(MessageType::Joined).with_room("a"), t0),
            Inbound::JoinConfirmed("a".into())
        );
    }

    #[test]
    fn errors_about_a_joined_room_are_not_kicks() {
        let t0 = Instant::now();
        let mut s = joined_session(&["a", "b"], t0);
        for text in [
            "room is moderated (+m)",
            "banned from room",
            "no outside messages (+n)",
            "not authorized",
            "not authorized (+t)",
            "unrecognized command",
        ] {
            assert_eq!(s.handle(&error(Some("a"), text), t0), Inbound::HubError(text.into()));
        }
        assert_eq!(s.rooms().joined, ["a", "b"]);
        assert!(s.rooms().failed.is_empty());
        // A kick, and a ban that removes members.
        assert_eq!(
            s.handle(&error(Some("a"), "kicked from a"), t0),
            Inbound::Removed {
                room: "a".into(),
                reason: "kicked from a".into()
            }
        );
        assert!(
            matches!(s.handle(&error(Some("b"), "banned from b"), t0), Inbound::Removed { room, .. } if room == "b")
        );
        assert!(s.rooms().joined.is_empty());
        // Not joined again on this link until asked for.
        s.tick(t0 + Duration::from_secs(3600)).unwrap();
        assert!(s.take_outbox().iter().all(|e| e.t != MessageType::Join));
        s.want("a");
        s.tick(t0 + Duration::from_secs(3600)).unwrap();
        assert!(types(&s.take_outbox()).contains(&(MessageType::Join, Some("a".into()))));
    }

    /// The JOINs in the outbox; the bouncer's PINGs are answered.
    fn joins(s: &mut Session, now: Instant) -> Vec<(MessageType, Option<String>)> {
        let out = s.take_outbox();
        if out.iter().any(|e| e.t == MessageType::Ping) {
            s.handle(&from_hub(MessageType::Pong), now);
        }
        types(&out)
            .into_iter()
            .filter(|(t, _)| *t == MessageType::Join)
            .collect()
    }

    #[test]
    fn an_unanswered_join_is_tried_again_with_backoff_then_slowly() {
        let t0 = Instant::now();
        let t = timings();
        let mut s = session(&["a", "b"], t0);
        s.take_outbox();
        s.handle(&welcome(32), t0);
        s.tick(t0).unwrap();
        assert_eq!(types(&s.take_outbox()), [(MessageType::Join, Some("a".into()))]);
        // No answer: "a" waits for its retry and "b" goes meanwhile.
        let mut now = t0 + t.join_timeout;
        s.tick(now).unwrap();
        assert!(s.rooms().retrying.contains_key("a"));
        assert!(s.rooms().failed.is_empty());
        assert_eq!(types(&s.take_outbox()), [(MessageType::Join, Some("b".into()))]);
        s.handle(&from_hub(MessageType::Joined).with_room("b"), now);
        // Retries after 5, 10, 20 s, then every 300 s, for as long as the
        // link lasts.
        let waits = [5u64, 10, 20, 300, 300, 300];
        for wait in waits {
            let due = now + Duration::from_secs(wait);
            s.tick(due - Duration::from_millis(1)).unwrap();
            assert!(joins(&mut s, due).is_empty(), "nothing before {wait} s");
            s.tick(due).unwrap();
            assert_eq!(
                joins(&mut s, due),
                [(MessageType::Join, Some("a".into()))],
                "after {wait} s"
            );
            now = due + t.join_timeout;
            s.tick(now).unwrap();
            assert!(joins(&mut s, now).is_empty());
        }
        assert_eq!(
            s.rooms().retrying.get("a").map(String::as_str),
            Some("no answer from the hub; 7 tries")
        );
        // An answer ends it.
        let due = now + Duration::from_secs(300);
        s.tick(due).unwrap();
        joins(&mut s, due);
        assert_eq!(
            s.handle(&from_hub(MessageType::Joined).with_room("a"), due),
            Inbound::JoinConfirmed("a".into())
        );
        assert!(s.rooms().retrying.is_empty());
        assert_eq!(s.rooms().joined, ["b", "a"]);
    }

    #[test]
    fn a_late_answer_to_a_timed_out_join_is_taken() {
        let t0 = Instant::now();
        let mut s = session(&["a", "b"], t0);
        s.handle(&welcome(32), t0);
        s.tick(t0).unwrap();
        s.take_outbox();
        let now = t0 + timings().join_timeout;
        s.tick(now).unwrap();
        assert_eq!(types(&s.take_outbox()), [(MessageType::Join, Some("b".into()))]);
        // The JOINED for "a" comes while "b" is pending.
        assert_eq!(
            s.handle(&from_hub(MessageType::Joined).with_room("a"), now),
            Inbound::JoinConfirmed("a".into())
        );
        assert!(s.rooms().retrying.is_empty());
        assert_eq!(
            s.handle(&from_hub(MessageType::Joined).with_room("b"), now),
            Inbound::JoinConfirmed("b".into())
        );
        assert_eq!(s.rooms().joined, ["a", "b"]);
        s.tick(now + Duration::from_secs(600)).unwrap();
        assert!(s.take_outbox().iter().all(|e| e.t != MessageType::Join));
    }

    #[test]
    fn a_rate_limited_join_is_tried_again() {
        let t0 = Instant::now();
        let mut s = session(&["a"], t0);
        s.handle(&welcome(32), t0);
        s.tick(t0).unwrap();
        s.take_outbox();
        // Still an error for whoever sent the lost packet.
        assert_eq!(
            s.handle(&error(None, "rate limited"), t0),
            Inbound::HubError("rate limited".into())
        );
        assert!(s.rooms().failed.is_empty());
        assert_eq!(
            s.rooms().retrying.get("a").map(String::as_str),
            Some("rate limited; 1 try")
        );
        s.tick(t0 + Duration::from_secs(4)).unwrap();
        assert!(s.take_outbox().is_empty());
        s.tick(t0 + timings().join_retry).unwrap();
        assert_eq!(types(&s.take_outbox()), [(MessageType::Join, Some("a".into()))]);
        // Without a pending JOIN, "rate limited" changes nothing.
        s.handle(&from_hub(MessageType::Joined).with_room("a"), t0);
        assert_eq!(
            s.handle(&error(None, "rate limited"), t0),
            Inbound::HubError("rate limited".into())
        );
        assert_eq!(s.rooms().joined, ["a"]);
        assert!(s.rooms().retrying.is_empty());
    }

    #[test]
    fn ping_pong_both_ways() {
        let t0 = Instant::now();
        let mut s = session(&[], t0);
        s.take_outbox();
        s.handle(&welcome(32), t0);
        // The hub's PING is answered with its body, without a nick.
        let ping = from_hub(MessageType::Ping).with_body(Raw::text("x"));
        assert_eq!(s.handle(&ping, t0), Inbound::Handled);
        let out = s.take_outbox();
        assert_eq!(out[0].t, MessageType::Pong);
        assert_eq!(out[0].body, Some(Raw::text("x")));
        assert_eq!(out[0].src, USER);
        assert_eq!(out[0].nick, None);
        // Idle: the bouncer PINGs, once.
        let idle = t0 + timings().ping_idle;
        s.tick(idle - Duration::from_millis(1)).unwrap();
        assert!(s.take_outbox().is_empty());
        s.tick(idle).unwrap();
        let out = s.take_outbox();
        assert_eq!(types(&out), [(MessageType::Ping, None)]);
        assert_eq!(out[0].nick.as_deref(), Some("bnc"));
        s.tick(idle + Duration::from_secs(1)).unwrap();
        assert!(s.take_outbox().is_empty());
        // Any traffic does not stand in for the PONG.
        s.handle(
            &from_hub(MessageType::Msg).with_room("x"),
            idle + Duration::from_secs(2),
        );
        // One lost PONG: a second PING, and the link goes only when that
        // one is not answered either.
        let second = idle + timings().pong_timeout;
        s.tick(second).unwrap();
        assert_eq!(types(&s.take_outbox()), [(MessageType::Ping, None)]);
        s.tick(second + timings().pong_timeout - Duration::from_millis(1))
            .unwrap();
        assert!(s.take_outbox().is_empty());
        assert_eq!(s.tick(second + timings().pong_timeout), Err(Dead::NoPong));
        // A PONG to the second PING keeps it up.
        let mut s = session(&[], t0);
        s.handle(&welcome(32), t0);
        s.tick(idle).unwrap();
        s.tick(second).unwrap();
        s.handle(&from_hub(MessageType::Pong), second + Duration::from_secs(1));
        s.tick(second + timings().pong_timeout).unwrap();
        // A PONG in time keeps it up.
        let mut s = session(&[], t0);
        s.handle(&welcome(32), t0);
        s.tick(idle).unwrap();
        s.handle(&from_hub(MessageType::Pong), idle + Duration::from_secs(1));
        s.tick(idle + timings().pong_timeout).unwrap();
        assert!(
            s.take_outbox()
                .iter()
                .all(|e| e.t != MessageType::Ping || e.body.is_some())
        );
    }

    #[test]
    fn part_is_confirmed_by_a_parted_about_the_bouncer() {
        let t0 = Instant::now();
        let mut s = joined_session(&["a"], t0);
        s.unwant("a", t0);
        assert_eq!(types(&s.take_outbox()), [(MessageType::Part, Some("a".into()))]);
        let other = from_hub(MessageType::Parted).with_room("a").with_nick(Some("x"));
        assert_eq!(s.handle(&other, t0), Inbound::Relay);
        assert_eq!(s.rooms().joined, ["a"]);
        // A member without a nick, with include_joined_member_list: the
        // body names them, not the bouncer.
        let nickless = from_hub(MessageType::Parted)
            .with_room("a")
            .with_body(Raw::from_value(&Value::Array(vec![Value::Bytes(vec![3; 16])])));
        assert_eq!(s.handle(&nickless, t0), Inbound::Relay);
        assert_eq!(s.rooms().joined, ["a"]);
        let mine = from_hub(MessageType::Parted)
            .with_room("a")
            .with_body(Raw::from_value(&Value::Array(vec![Value::Bytes(USER.to_vec())])));
        assert_eq!(s.handle(&mine, t0), Inbound::PartConfirmed("a".into()));
        assert!(s.rooms().joined.is_empty() && s.rooms().wanted.is_empty());
        s.tick(t0).unwrap();
        assert!(s.take_outbox().is_empty());
        // Without a member list, the bouncer's PARTED is the one without a
        // nick.
        let mut s = joined_session(&["a"], t0);
        s.unwant("a", t0);
        assert_eq!(
            s.handle(&from_hub(MessageType::Parted).with_room("a"), t0),
            Inbound::PartConfirmed("a".into())
        );
    }

    #[test]
    fn a_lost_parted_times_out_and_a_late_one_is_handled() {
        let t0 = Instant::now();
        let mut s = joined_session(&["a"], t0);
        s.unwant("a", t0);
        s.take_outbox();
        s.tick(t0 + timings().part_timeout - Duration::from_millis(1)).unwrap();
        assert_eq!(s.rooms().joined, ["a"]);
        s.tick(t0 + timings().part_timeout).unwrap();
        assert!(s.rooms().joined.is_empty());
        assert_eq!(
            s.handle(&from_hub(MessageType::Parted).with_room("a"), t0),
            Inbound::Handled
        );
        // Joining it again works as for any room.
        s.want("a");
        s.tick(t0 + timings().part_timeout).unwrap();
        assert_eq!(types(&s.take_outbox()), [(MessageType::Join, Some("a".into()))]);
    }

    #[test]
    fn rooms_over_the_limit_are_joined_when_there_is_room() {
        let t0 = Instant::now();
        let mut s = session(&["a", "b", "c"], t0);
        s.handle(&welcome(2), t0);
        for room in ["a", "b"] {
            s.tick(t0).unwrap();
            s.take_outbox();
            s.handle(&from_hub(MessageType::Joined).with_room(room), t0);
        }
        s.tick(t0).unwrap();
        assert_eq!(s.rooms().over_limit, ["c"]);
        assert!(s.take_outbox().is_empty());
        s.unwant("a", t0);
        s.handle(&from_hub(MessageType::Parted).with_room("a"), t0);
        s.take_outbox();
        s.tick(t0).unwrap();
        assert!(s.rooms().over_limit.is_empty());
        assert_eq!(types(&s.take_outbox()), [(MessageType::Join, Some("c".into()))]);
        // A kick makes room too.
        s.handle(&from_hub(MessageType::Joined).with_room("c"), t0);
        s.want("d");
        s.tick(t0).unwrap();
        assert_eq!(s.rooms().over_limit, ["d"]);
        s.handle(&error(Some("b"), "kicked from b"), t0);
        s.tick(t0).unwrap();
        assert_eq!(types(&s.take_outbox()), [(MessageType::Join, Some("d".into()))]);
    }

    #[test]
    fn client_sends_wait_for_the_welcome_and_their_room() {
        let t0 = Instant::now();
        let mut s = session(&["a", "b"], t0);
        assert!(!s.may_send(None));
        assert!(!s.may_send(Some("elsewhere")));
        s.handle(&welcome(32), t0);
        assert!(s.may_send(None));
        assert!(s.may_send(Some("elsewhere")));
        assert!(!s.may_send(Some("a")));
        assert!(!s.may_send(Some(" A ")));
        s.tick(t0).unwrap();
        s.handle(&from_hub(MessageType::Joined).with_room("a"), t0);
        assert!(s.may_send(Some("A")));
        assert!(!s.may_send(Some("b")));
        // Refused: the hub's answer to the send is for the client.
        s.tick(t0).unwrap();
        s.handle(&error(Some("b"), "invite-only (+i)"), t0);
        assert!(s.may_send(Some("b")));
    }

    #[test]
    fn messages_are_for_the_relay() {
        let t0 = Instant::now();
        let mut s = session(&[], t0);
        s.handle(&welcome(32), t0);
        for t in [
            MessageType::Msg,
            MessageType::Notice,
            MessageType::Action,
            MessageType::Unknown(99),
        ] {
            assert_eq!(s.handle(&from_hub(t).with_room("a"), t0), Inbound::Relay);
        }
        assert_eq!(s.handle(&welcome(1), t0), Inbound::Handled);
        assert_eq!(s.limits().max_rooms_per_session, 32);
    }

    #[test]
    fn backoff_doubles_to_the_cap_and_resets() {
        let mut b = Backoff::new(Duration::from_secs(5), Duration::from_secs(300));
        let delays: Vec<u64> = (0..9).map(|_| b.next_delay().as_secs()).collect();
        assert_eq!(delays, [5, 10, 20, 40, 80, 160, 300, 300, 300]);
        b.reset();
        assert_eq!(b.next_delay().as_secs(), 5);
    }

    #[test]
    fn status_reads_without_the_new_fields() {
        let status: UpstreamStatus = serde_json::from_str("{}").unwrap();
        assert_eq!(status, UpstreamStatus::default());
        assert_eq!(type_name(MessageType::Msg), "MSG");
        assert_eq!(type_name(MessageType::Unknown(99)), "t99");
    }

    #[test]
    fn the_event_channel_drops_traffic_but_never_state() {
        let (tx, mut rx) = event_channel();
        let traffic = || UpstreamEvent::Envelope(from_hub(MessageType::Msg).with_room("lobby"), Inbound::Relay);
        for _ in 0..EVENT_QUEUE {
            assert!(tx.send(traffic()));
        }
        assert!(!tx.send(traffic()));
        assert!(tx.send(UpstreamEvent::Down));
        let joined = from_hub(MessageType::Joined).with_room("lobby");
        assert!(tx.send(UpstreamEvent::Envelope(joined, Inbound::JoinConfirmed("lobby".into()))));
        let mut n = 0;
        while rx.try_recv().is_some() {
            n += 1;
        }
        assert_eq!(n, EVENT_QUEUE + 2);
        assert!(tx.send(traffic()));
    }
}
