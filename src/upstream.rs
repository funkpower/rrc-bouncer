// SPDX-License-Identifier: LicenseRef-Reticulum
//! The upstream session: one link to an upstream hub, as the user.
//!
//! [`Session`] is the protocol, with no I/O: it takes the envelopes the hub
//! sends and the passing of time, and says what to send. [`run`] is the
//! task that owns the link, keeps it up, and feeds the session.

use std::collections::{BTreeMap, VecDeque};
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
use crate::rooms::RoomSet;

/// The client name in the bouncer's HELLO.
pub const CLIENT_NAME: &str = "rrc-bouncer";

/// What the bouncer says it can do. `resource_envelope` stays off until
/// the bouncer relays Resources (M5). ACTION and direct NOTICE are plain
/// envelopes the relay passes on (M3). rrcd keeps a client's caps but does
/// not act on them.
pub const CAPS: Caps = Caps {
    resource_envelope: false,
    action: true,
    direct_notice: true,
};

/// The protocol's timings. Every one has a default for real hubs; the
/// environment variables below shorten them for tests/e2e.py.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timings {
    /// No WELCOME this long after the HELLO: the link is dropped.
    pub welcome_timeout: Duration,
    /// No JOINED or ERROR this long after a JOIN: the room is marked failed.
    pub join_timeout: Duration,
    /// Nothing heard from the hub this long: the bouncer sends a PING.
    pub ping_idle: Duration,
    /// No PONG this long after the bouncer's PING: the link is dropped.
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
            ping_idle: Duration::from_secs(120),
            pong_timeout: Duration::from_secs(60),
            backoff_initial: Duration::from_secs(5),
            backoff_max: Duration::from_secs(300),
            stable: Duration::from_secs(60),
        }
    }
}

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
    /// PING, PONG, a repeated WELCOME: nothing for anyone else.
    Handled,
    /// The WELCOME: the session is up.
    Welcomed,
    /// The hub's answer to the bouncer's JOIN.
    JoinConfirmed(String),
    /// An ERROR answering the bouncer's JOIN.
    JoinFailed { room: String, reason: String },
    /// The hub's answer to the bouncer's PART.
    PartConfirmed(String),
    /// An ERROR about a room the bouncer is in, which rrcd sends to a
    /// member it has kicked: the bouncer is no longer in the room.
    Removed { room: String, reason: String },
    /// Any other ERROR.
    HubError(String),
    /// Everything for the relay (M3) and history (M4): MSG, NOTICE, ACTION,
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
    pub failed: BTreeMap<String, String>,
    pub over_limit: Vec<String>,
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
    ping_sent: Option<Instant>,
    rooms: RoomsView,
    pending_join: Option<(String, Instant)>,
    parting: Vec<String>,
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

    /// Stay in `room` from now on (it is joined at the next `tick`).
    pub fn want(&mut self, room: &str) {
        if !self.rooms.wanted.iter().any(|r| r == room) {
            self.rooms.wanted.push(room.to_string());
        }
        // Asked for again: try again.
        self.rooms.failed.remove(room);
    }

    /// Leave `room`.
    pub fn unwant(&mut self, room: &str) {
        self.rooms.wanted.retain(|r| r != room);
        self.rooms.failed.remove(room);
        self.rooms.over_limit.retain(|r| r != room);
        let pending = self.pending_join.as_ref().is_some_and(|(r, _)| r == room);
        if (self.rooms.joined.iter().any(|r| r == room) || pending) && !self.parting.iter().any(|r| r == room) {
            let env = self.envelope(MessageType::Part).with_room(room);
            self.outbox.push(env);
            self.parting.push(room.to_string());
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
            MessageType::Joined => self.joined(env),
            MessageType::Parted => self.parted(env),
            MessageType::Error => self.error(env),
            _ => Inbound::Relay,
        }
    }

    fn joined(&mut self, env: &Envelope) -> Inbound {
        let Some(room) = env.room.as_deref() else {
            return Inbound::Relay;
        };
        // rrcd sends the joiner its JOINED without a nick and everyone
        // already in the room one with the joiner's nick; the bouncer is
        // not in a room it is still joining, so the first JOINED for it
        // is the answer.
        if self.pending_join.as_ref().is_some_and(|(r, _)| r == room) {
            self.pending_join = None;
            self.rooms.joined.push(room.to_string());
            return Inbound::JoinConfirmed(room.to_string());
        }
        Inbound::Relay
    }

    fn parted(&mut self, env: &Envelope) -> Inbound {
        let Some(room) = env.room.as_deref() else {
            return Inbound::Relay;
        };
        // The answer to the bouncer's PART has no nick; another member's
        // leaving has theirs.
        if env.nick.is_none() && self.parting.iter().any(|r| r == room) {
            self.parting.retain(|r| r != room);
            self.rooms.joined.retain(|r| r != room);
            return Inbound::PartConfirmed(room.to_string());
        }
        Inbound::Relay
    }

    fn error(&mut self, env: &Envelope) -> Inbound {
        let reason = env.text_body().unwrap_or_else(|| "(no text)".into());
        if let Some((pending, _)) = self.pending_join.clone() {
            // rrcd names the room in a JOIN error about the room (+i, +k,
            // ban) and leaves it out of one about the request (too many
            // rooms, a bad name). "rate limited" is about the packet,
            // whichever it was.
            let about_join = match env.room.as_deref() {
                Some(room) => room == pending,
                None => reason != "rate limited",
            };
            if about_join {
                self.pending_join = None;
                self.rooms.failed.insert(pending.clone(), reason.clone());
                return Inbound::JoinFailed { room: pending, reason };
            }
        }
        if let Some(room) = env.room.as_deref()
            && self.rooms.joined.iter().any(|r| r == room)
        {
            self.rooms.joined.retain(|r| r != room);
            self.rooms.failed.insert(room.to_string(), reason.clone());
            return Inbound::Removed {
                room: room.to_string(),
                reason,
            };
        }
        Inbound::HubError(reason)
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
        if let Some(sent) = self.ping_sent {
            if now.saturating_duration_since(sent) >= self.timings.pong_timeout {
                return Err(Dead::NoPong);
            }
        } else if now.saturating_duration_since(self.last_heard) >= self.timings.ping_idle {
            let body = Raw::from_value(&Value::Float(crate::bouncer::now()));
            let env = self.envelope(MessageType::Ping).with_body(body);
            self.outbox.push(env);
            self.ping_sent = Some(now);
        }
        if let Some((room, sent)) = self.pending_join.clone()
            && now.saturating_duration_since(sent) >= self.timings.join_timeout
        {
            self.pending_join = None;
            self.rooms.failed.insert(room, "no answer from the hub".into());
        }
        self.join_next(limits, now);
        Ok(())
    }

    /// One JOIN at a time, so each answer can be told apart: rrcd's errors
    /// for a refused JOIN do not always name the room.
    fn join_next(&mut self, limits: Limits, now: Instant) {
        if self.pending_join.is_some() {
            return;
        }
        let next = self.rooms.wanted.iter().find(|room| {
            !self.rooms.joined.contains(room)
                && !self.rooms.failed.contains_key(*room)
                && !self.rooms.over_limit.contains(room)
                && !self.parting.contains(room)
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

/// What the rest of the bouncer can ask of an upstream session.
#[derive(Debug)]
pub enum Command {
    /// Send a client's envelope upstream, sharing the session's rate limit.
    /// Dropped (and counted) while the session is not up.
    Send(Envelope),
    /// Add a room to the hub's room set (saved) and join it.
    Join(String),
    /// Take a room out of the room set and leave it. A room from the config
    /// file is left until the next start.
    Part(String),
}

/// What an upstream session tells the rest of the bouncer.
#[derive(Debug, Clone)]
pub enum UpstreamEvent {
    /// The hub welcomed the session.
    Up(Welcome),
    /// The link went.
    Down,
    /// An envelope from the hub, and what the session made of it.
    Envelope(Envelope, Inbound),
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
    /// Rooms the hub refused, with its reason.
    pub rooms_failed: BTreeMap<String, String>,
    /// Rooms past the hub's `max_rooms_per_session`.
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
    /// Envelopes from the hub nothing in the bouncer consumes yet, by type.
    pub unhandled: BTreeMap<String, u64>,
    /// Client envelopes dropped because the session was not up.
    pub dropped_sends: u64,
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
/// How long to wait for the link to close after asking it to.
const CLOSE_WAIT: Duration = Duration::from_secs(5);
/// How often the session's clock runs.
const TICK: Duration = Duration::from_millis(250);
/// Client envelopes waiting for a token beyond this many are dropped.
const CLIENT_QUEUE: usize = 256;

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
    /// None until the relay (M3) consumes them.
    pub events: Option<mpsc::Sender<UpstreamEvent>>,
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
            s.rooms_over_limit = view.over_limit.clone();
        });
    }

    fn show_room_set(&self) {
        let rooms = self.rooms.rooms();
        self.update(|s| s.rooms = rooms);
    }

    async fn emit(&self, event: UpstreamEvent) {
        if let Some(events) = &self.events {
            let _ = events.send(event).await;
        }
    }

    fn count_unhandled(&self, t: MessageType) {
        self.tally(|s| *s.unhandled.entry(type_name(t)).or_default() += 1);
    }

    /// A command while there is no session: the room set still changes.
    fn command_offline(&mut self, command: Command) {
        match command {
            Command::Send(env) => {
                tracing::debug!("upstream {}: dropped a client {:?}: not connected", self.tag(), env.t);
                self.tally(|s| s.dropped_sends += 1);
            }
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
        match self.rooms.remove(room) {
            Ok(_) if self.rooms.is_configured(room) => tracing::info!(
                "upstream {}: room {room} is in the config file; it is left until the next start",
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
        // rrcd's bucket for a link is full when the link comes up and holds
        // its default until the WELCOME says otherwise.
        let mut bucket = TokenBucket::new(Limits::default().rate_limit_msgs_per_minute, started);
        let mut control: VecDeque<Envelope> = VecDeque::new();
        let mut client: VecDeque<Envelope> = VecDeque::new();
        let mut send_at: Option<Instant> = None;
        let mut commands_open = true;
        let mut ticker = tokio::time::interval(TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        self.update(|s| s.state = "linked".into());

        let reason = 'link: loop {
            control.extend(session.take_outbox());
            // Send what the bucket allows: the bouncer's own envelopes
            // first, then clients'.
            let now = Instant::now();
            while send_at.is_none_or(|at| now >= at) {
                let queue = if !control.is_empty() {
                    &mut control
                } else if welcomed_at.is_some() && !client.is_empty() {
                    &mut client
                } else {
                    break;
                };
                if let Err(wait) = bucket.take(now) {
                    send_at = Some(now + wait);
                    break;
                }
                send_at = None;
                let Some(env) = queue.pop_front() else { break };
                match link.handle.send_packet(env.encode()).await {
                    Ok(_) => tracing::debug!("upstream {tag}: sent {:?} room={:?}", env.t, env.room),
                    Err(LinkSessionError::PayloadTooLarge { actual, max }) => {
                        tracing::warn!(
                            "upstream {tag}: dropped a {:?} of {actual} bytes (link MDU {max})",
                            env.t
                        );
                    }
                    Err(e) => break 'link format!("send failed: {e}"),
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
                        let inbound = session.handle(&env, Instant::now());
                        match &inbound {
                            Inbound::Handled => {}
                            Inbound::Welcomed => if let Some(welcome) = session.welcome().cloned() {
                                welcomed_at = Some(Instant::now());
                                bucket.set_limit(welcome.limits.rate_limit_msgs_per_minute, Instant::now());
                                self.welcomed(&welcome).await;
                                self.emit(UpstreamEvent::Up(welcome)).await;
                            }
                            Inbound::JoinConfirmed(room) => tracing::info!("upstream {tag}: joined {room}"),
                            Inbound::JoinFailed { room, reason } => tracing::warn!("upstream {tag}: could not join {room}: {reason}"),
                            Inbound::PartConfirmed(room) => tracing::info!("upstream {tag}: left {room}"),
                            Inbound::Removed { room, reason } => tracing::warn!("upstream {tag}: out of {room}: {reason}"),
                            Inbound::HubError(reason) => tracing::warn!("upstream {tag}: hub error: {reason}"),
                            Inbound::Relay => tracing::debug!("upstream {tag}: {:?} room={:?} not relayed yet", env.t, env.room),
                        }
                        if !matches!(inbound, Inbound::Handled | Inbound::Welcomed) {
                            if self.events.is_some() {
                                self.emit(UpstreamEvent::Envelope(env, inbound)).await;
                            } else if matches!(inbound, Inbound::Relay | Inbound::HubError(_)) {
                                self.count_unhandled(env.t);
                            }
                        }
                        self.show_rooms(session.rooms());
                    }
                    Some(LinkSessionEvent::Closed { reason }) => break 'link format!("link closed ({reason:?})"),
                    Some(LinkSessionEvent::Stale) => tracing::debug!("upstream {tag}: link stale"),
                    Some(LinkSessionEvent::Recovered) => tracing::debug!("upstream {tag}: link recovered"),
                    Some(_) => {}
                    None => break 'link "link session ended".into(),
                },
                // The bouncer accepts no Resources yet (M5); an offer left
                // undecided expires.
                Some(offer) = link.resource_offers.recv() => {
                    tracing::debug!("upstream {tag}: ignored a resource offer of {} bytes", offer.data_size());
                }
                command = commands.recv(), if commands_open => match command {
                    Some(Command::Send(env)) => {
                        if welcomed_at.is_none() {
                            self.command_offline(Command::Send(env));
                        } else if client.len() >= CLIENT_QUEUE {
                            tracing::warn!("upstream {tag}: client queue full, dropped a {:?}", env.t);
                            self.tally(|s| s.dropped_sends += 1);
                        } else {
                            client.push_back(env);
                        }
                    }
                    Some(Command::Join(room)) => {
                        self.add_room(&room);
                        if let Some(room) = crate::config::normalize_room(&room) {
                            session.want(&room);
                        }
                        self.show_rooms(session.rooms());
                    }
                    Some(Command::Part(room)) => {
                        self.remove_room(&room);
                        if let Some(room) = crate::config::normalize_room(&room) {
                            session.unwant(&room);
                        }
                        self.show_rooms(session.rooms());
                    }
                    // Nobody can send commands any more; the session goes on.
                    None => commands_open = false,
                },
                _ = ticker.tick() => {
                    if let Err(dead) = session.tick(Instant::now()) {
                        link.handle.close().await;
                        drain_until_closed(&mut link).await;
                        break 'link dead.to_string();
                    }
                    self.show_rooms(session.rooms());
                }
                _ = async { tokio::time::sleep_until(sleep_until.unwrap_or_else(tokio::time::Instant::now)).await }, if sleep_until.is_some() => {}
                _ = shutdown.changed() => {
                    link.handle.close().await;
                    drain_until_closed(&mut link).await;
                    return Ended { welcomed_for: welcomed_at.map(|t| t.elapsed()), reason: "shutting down".into(), shutdown: true };
                }
            }
        };
        if reason.starts_with("send failed") {
            link.handle.close().await;
            drain_until_closed(&mut link).await;
        }
        Ended {
            welcomed_for: welcomed_at.map(|t| t.elapsed()),
            reason,
            shutdown: false,
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
            let name = format!("{name} (via rrc-bouncer)");
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
                    up.emit(UpstreamEvent::Down).await;
                }
                let welcomed_for = ended.welcomed_for;
                up.update(|s| {
                    s.state = if ended.shutdown { "stopped" } else { "disconnected" }.into();
                    s.last_disconnected_at = Some(now());
                    s.rooms_joined.clear();
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
        assert!(!body.caps.resource_envelope);
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

    #[test]
    fn join_errors_with_and_without_a_room() {
        let t0 = Instant::now();
        let mut s = session(&["a", "b", "c", "d"], t0);
        s.handle(&welcome(32), t0);
        s.tick(t0).unwrap();
        s.take_outbox();
        let err = from_hub(MessageType::Error)
            .with_room("a")
            .with_body(Raw::text("banned from room"));
        assert_eq!(
            s.handle(&err, t0),
            Inbound::JoinFailed {
                room: "a".into(),
                reason: "banned from room".into()
            }
        );
        s.tick(t0).unwrap();
        assert_eq!(types(&s.take_outbox()), [(MessageType::Join, Some("b".into()))]);
        // "rate limited" is not about the JOIN.
        let limited = from_hub(MessageType::Error).with_body(Raw::text("rate limited"));
        assert_eq!(s.handle(&limited, t0), Inbound::HubError("rate limited".into()));
        let err = from_hub(MessageType::Error).with_body(Raw::text("too many rooms"));
        assert!(matches!(s.handle(&err, t0), Inbound::JoinFailed { room, .. } if room == "b"));
        s.tick(t0).unwrap();
        assert_eq!(types(&s.take_outbox()), [(MessageType::Join, Some("c".into()))]);
        // No answer at all.
        s.tick(t0 + Duration::from_secs(20)).unwrap();
        assert_eq!(
            s.rooms().failed.get("c").map(String::as_str),
            Some("no answer from the hub")
        );
        assert_eq!(types(&s.take_outbox()), [(MessageType::Join, Some("d".into()))]);
        s.handle(&from_hub(MessageType::Joined).with_room("d"), t0);
        // A kick: rrcd sends an ERROR naming the room.
        let kicked = from_hub(MessageType::Error)
            .with_room("d")
            .with_body(Raw::text("kicked from d"));
        assert!(matches!(s.handle(&kicked, t0), Inbound::Removed { room, .. } if room == "d"));
        assert!(s.rooms().joined.is_empty());
        // Asking for a failed room again tries again.
        s.want("a");
        s.tick(t0 + Duration::from_secs(20)).unwrap();
        assert_eq!(types(&s.take_outbox()), [(MessageType::Join, Some("a".into()))]);
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
        assert_eq!(s.tick(idle + timings().pong_timeout), Err(Dead::NoPong));
        // A PONG in time keeps it up.
        let mut s = session(&[], t0);
        s.handle(&welcome(32), t0);
        s.tick(idle).unwrap();
        s.handle(&from_hub(MessageType::Pong), idle + Duration::from_secs(1));
        s.tick(idle + timings().pong_timeout).unwrap();
    }

    #[test]
    fn part_is_confirmed_by_a_parted_without_a_nick() {
        let t0 = Instant::now();
        let mut s = session(&["a"], t0);
        s.handle(&welcome(32), t0);
        s.tick(t0).unwrap();
        s.handle(&from_hub(MessageType::Joined).with_room("a"), t0);
        s.take_outbox();
        s.unwant("a");
        assert_eq!(types(&s.take_outbox()), [(MessageType::Part, Some("a".into()))]);
        let other = from_hub(MessageType::Parted).with_room("a").with_nick(Some("x"));
        assert_eq!(s.handle(&other, t0), Inbound::Relay);
        assert_eq!(s.rooms().joined, ["a"]);
        let mine = from_hub(MessageType::Parted).with_room("a");
        assert_eq!(s.handle(&mine, t0), Inbound::PartConfirmed("a".into()));
        assert!(s.rooms().joined.is_empty() && s.rooms().wanted.is_empty());
        s.tick(t0).unwrap();
        assert!(s.take_outbox().is_empty());
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
}
