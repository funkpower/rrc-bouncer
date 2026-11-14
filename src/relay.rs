// SPDX-License-Identifier: LicenseRef-Reticulum
//! The local hub: the user's clients link to it, and the bouncer relays
//! between them and the upstream session.
//!
//! [`Relay`] is the logic, with no I/O: it takes client packets, upstream
//! events and the passing of time, hands commands to the upstream session
//! (with `try_send`) and says what to send to which client link. [`run`] is
//! the task that owns the local hub's destination and feeds the relay.
//!
//! The rules (DESIGN.md § Local hub):
//!
//! - A client's room MSG, ACTION and NOTICE go upstream, and the hub sends
//!   each back to every member link, the bouncer's included, with the
//!   client's K_ID. The relay passes that echo to every client in the room,
//!   the sender included: its siblings show it once, and the sender, which
//!   drew the line itself when it sent it, drops the echo by its K_ID.
//! - rrcd sends a direct NOTICE only to its destination. The relay gives a
//!   copy (K_SRC the user) to the user's other clients when it passes the
//!   NOTICE upstream, unless it is addressed to the user, whose copy comes
//!   back from the hub; the sender has drawn its own.
//! - A Resource (a RESOURCE_ENVELOPE and the text it announces) goes where
//!   the NOTICE with that text would; a client without the
//!   `resource_envelope` cap gets the text as NOTICEs that each fit a
//!   packet (DESIGN.md § Resources).

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ciborium::Value;
use rns_runtime::link_manager::{LinkResourceConclusion, LinkResourceDirection, LinkResourceEvent};
use rns_runtime::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc};

use crate::bouncer::local_hub_name;
use crate::config::normalize_room;
use crate::envelope::{Caps, Envelope, Hello, Limits, MessageType, Raw, ResourceEnvelope, Welcome, now_ms};
use crate::history::{self, History, Query, Record};
use crate::identity::hub_address;
use crate::resource::{self, Expectations, Expected, LINK_MDU, MAX_RESOURCE_BYTES};
use crate::upstream::{CLIENT_HOLD, Command, EventReceiver, Inbound, SharedStatus, UpstreamEvent, local_name};

/// A link that has not identified this long after it was established is
/// closed.
pub const IDENTIFY_TIMEOUT: Duration = Duration::from_secs(30);
/// A link of an identity that is not allowed is closed this long after
/// the ERROR telling it so, so the ERROR goes out first.
pub const REFUSE_CLOSE_DELAY: Duration = Duration::from_secs(1);
/// How long a client envelope the hub answers only when it refuses it (a
/// direct NOTICE, a message to a room the bouncer is not in) or a command
/// waits for its answer: the session may hold it `CLIENT_HOLD`, and the
/// hub answers within seconds.
const ANSWER_WAIT: Duration = Duration::from_secs(CLIENT_HOLD.as_secs() + 15);
/// How long a client's room envelope waits for its echo, for routing an
/// ERROR to it.
const ECHO_WAIT: Duration = Duration::from_secs(120);
/// The most client envelopes, and the most commands, remembered for
/// routing the hub's answers.
const MAX_SENT: usize = 1024;
/// The hub's greeting is the room-less NOTICEs it sends this soon after
/// its WELCOME, before anything else.
const GREETING_WINDOW: Duration = Duration::from_secs(10);
/// The most greeting NOTICEs (or Resources) kept for clients that link
/// later.
const MAX_GREETING: usize = 16;
/// The most bytes of greeting Resources kept.
const MAX_GREETING_BYTES: usize = 2 * MAX_RESOURCE_BYTES as usize;
/// Resources waiting for a client link's sender task, most; later ones
/// are dropped.
const LINK_RESOURCES: usize = resource::MAX_EXPECTED;
/// A send to a client link that takes longer than this is given up, so a
/// slow link does not hold up the link's later envelopes for long.
const SEND_TIMEOUT: Duration = Duration::from_secs(2);
/// The gap between two replayed envelopes on a link, whichever replay they
/// are of. rsReticulum sends link packets best-effort, with no flow
/// control, so a burst could overrun the client's link; 20 ms puts a full
/// default replay (100) through in two seconds.
const REPLAY_PACE: Duration = Duration::from_millis(20);
/// Room envelopes held for a client while a replay of the room runs, most;
/// later ones are dropped.
const MAX_HELD: usize = 512;
/// A replay not done this long after it was asked for is ended, and what
/// was held for it is sent.
const REPLAY_WAIT: Duration = Duration::from_secs(120);
/// Envelopes waiting for a client link's sender task, most; later ones are
/// dropped.
const LINK_QUEUE: usize = 1024;
/// `/history` gives at most this many messages.
pub const MAX_HISTORY: usize = 500;
/// `/history` with no number, when `replay_max` is 0.
const DEFAULT_HISTORY: usize = 100;
/// The NOTICE that answers a bad `/history`.
pub const HISTORY_USAGE: &str = "usage: /history [N], N from 1 to 500";
/// The text of the ERROR a link of another identity gets.
pub const NOT_ALLOWED: &str = "identity not allowed on this hub";

pub type Link = [u8; 16];

/// What the relay asks of the task. Short-lived, so not boxed.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    /// Send an envelope to a client link: held while a replay of its room
    /// to that link runs.
    Client(Link, Envelope),
    /// Send a RESOURCE_ENVELOPE and then its Resource to a client link,
    /// held as `Client` is. A link's Resources go one at a time.
    Resource(Link, Envelope, Arc<Vec<u8>>),
    /// Send the local hub's own notice to a client link: never held.
    Now(Link, Envelope),
    /// Close a client link now.
    Close(Link),
    /// Close a client link after `REFUSE_CLOSE_DELAY`.
    CloseLater(Link),
    /// Send a client a room's history. The link's room envelopes are held
    /// from here until the replay ends.
    Replay(Replay),
    /// End a client's replay of `room` (of every room: None), sending what
    /// was held for it (`flush`) or not.
    EndReplay {
        link: Link,
        room: Option<String>,
        flush: bool,
    },
}

/// A replay of a room's history to one client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replay {
    /// Told back with the replay's end, so an old end is not taken for a
    /// newer replay's.
    pub id: u64,
    pub link: Link,
    pub query: Query,
    /// Say so when there is nothing (`/history`); a JOIN's replay says
    /// nothing then.
    pub say_empty: bool,
}

/// A replay the relay has asked for and not seen end.
#[derive(Debug)]
struct Running {
    id: u64,
    since: Instant,
}

/// Whether a client MSG body is `/history [N]`, and what it asks for:
/// Ok(N), capped at `MAX_HISTORY`, `default` with no number, Err for a bad
/// number. The command word is matched as rrcd matches its own commands,
/// whatever its case.
fn history_request(body: &str, default: usize) -> Option<Result<usize, ()>> {
    let mut words = body.trim().strip_prefix('/')?.split_whitespace();
    if !words.next()?.eq_ignore_ascii_case("history") {
        return None;
    }
    Some(match (words.next(), words.next()) {
        (None, _) => Ok(default.min(MAX_HISTORY)),
        (Some(n), None) => match n.parse::<usize>() {
            Ok(n) if n >= 1 => Ok(n.min(MAX_HISTORY)),
            _ => Err(()),
        },
        _ => Err(()),
    })
}

/// What a replay sends: a NOTICE, the envelopes oldest first, a NOTICE;
/// for an empty one, nothing, or with `say_empty` one NOTICE. Envelopes go
/// as the hub sent them, but with K_SRC the local hub's identity where it
/// was the upstream hub's (whose identity's hub address is `upstream`).
pub fn replay_envelopes(
    records: &[Record],
    hub: &[u8; 16],
    upstream: &[u8; 16],
    room: &str,
    say_empty: bool,
) -> Vec<Envelope> {
    let notice = |text: String| {
        Envelope::new(MessageType::Notice, hub)
            .with_room(room)
            .with_body(Raw::text(&text))
    };
    let mut envelopes: Vec<Envelope> = Vec::new();
    let mut oldest = None;
    for record in records {
        let Ok(mut env) = Envelope::decode(&record.envelope) else {
            continue;
        };
        if let Ok(src) = <[u8; 16]>::try_from(&env.src[..])
            && hub_address(&src) == *upstream
        {
            env.src = hub.to_vec();
        }
        oldest.get_or_insert(record.ts_ms);
        envelopes.push(env);
    }
    let Some(oldest) = oldest else {
        return if say_empty {
            vec![notice(format!("--- no history in {room} ---"))]
        } else {
            Vec::new()
        };
    };
    let n = envelopes.len();
    let what = if n == 1 { "message" } else { "messages" };
    let mut out = vec![notice(format!(
        "--- replay: {n} {what} since {} ---",
        history::utc(oldest)
    ))];
    out.extend(envelopes);
    out.push(notice("--- end of replay ---".into()));
    out
}

/// One local hub's clients and traffic in status.json. Every field has a
/// default, so a status file from an older bouncer still reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RelayStatus {
    /// Client links identified as an allowed identity.
    pub clients: u64,
    /// Their identities (hex), one per link, sorted.
    pub client_identities: Vec<String>,
    /// Links refused for their identity since start.
    pub refused_links: u64,
    /// Client envelopes handed to the upstream session.
    pub relayed_in: u64,
    /// Envelopes sent to clients, one per client.
    pub relayed_out: u64,
    /// Client envelopes the session's queue had no room for, and envelopes
    /// a client link could not take.
    pub relay_drops: u64,
    /// Client Resources received whole, matching their RESOURCE_ENVELOPE,
    /// and sent upstream as NOTICEs.
    pub client_resources_in: u64,
    /// Resources a client received whole, one per client.
    pub client_resources_out: u64,
    /// Resources sent as NOTICEs to a client without the
    /// `resource_envelope` cap, one per client.
    pub client_resources_split: u64,
    /// Resources dropped, each once, when it ended: from clients (refused,
    /// failed, announced by no RESOURCE_ENVELOPE, a sha256 other than
    /// announced, not a room notice, refused as its NOTICEs would be, too
    /// long), to clients (not taken, too many waiting, the link closed
    /// first), and the hub's `blob` Resources.
    pub client_resource_drops: u64,
}

pub type SharedRelayStatus = Arc<Mutex<RelayStatus>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientState {
    /// No identity yet: its packets are ignored.
    Unidentified,
    Allowed([u8; 16]),
    /// Another identity: told so and closed.
    Refused,
}

#[derive(Debug)]
struct Client {
    state: ClientState,
    linked_at: Instant,
    /// Said HELLO and got WELCOME.
    welcomed: bool,
    /// The rooms the client is in, as it sees them.
    rooms: BTreeSet<String>,
    /// Rooms the client asked to join that the bouncer is joining upstream.
    pending: BTreeSet<String>,
    /// Its HELLO had the `resource_envelope` cap.
    resources: bool,
    /// Its RESOURCE_ENVELOPEs waiting for their Resource.
    expected: Expectations,
}

impl Client {
    fn new(now: Instant) -> Self {
        Self {
            state: ClientState::Unidentified,
            linked_at: now,
            welcomed: false,
            rooms: BTreeSet::new(),
            pending: BTreeSet::new(),
            resources: false,
            expected: Expectations::default(),
        }
    }
}

/// Something from the hub on its way to clients: an envelope, or a
/// Resource with the NOTICE it stands for.
#[derive(Debug, Clone)]
enum Item {
    Env(Envelope),
    Resource {
        /// The RESOURCE_ENVELOPE.
        envelope: Envelope,
        /// The NOTICE with the Resource's text.
        notice: Box<Envelope>,
        data: Arc<Vec<u8>>,
    },
}

/// What kind of client envelope went upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// MSG, ACTION or NOTICE in a room.
    Room,
    /// A direct NOTICE.
    Direct,
    /// A MSG or NOTICE whose body, trimmed, starts with `/`: rrcd's slash
    /// command, whatever its room or K_DST.
    Command,
}

/// A client envelope handed upstream, until its echo, an ERROR for it, or
/// its wait is over.
#[derive(Debug)]
struct Sent {
    id: Vec<u8>,
    link: Link,
    t: MessageType,
    room: Option<String>,
    kind: Kind,
    at: Instant,
    wait: Duration,
    /// Sent on a link that has since gone: no answer can come for it on
    /// the next, but the session may still report it dropped.
    stale: bool,
}

fn kind_of(env: &Envelope) -> Kind {
    let command = matches!(env.t, MessageType::Msg | MessageType::Notice)
        && env.text_body().is_some_and(|b| b.trim().starts_with('/'));
    if command {
        Kind::Command
    } else if env.t == MessageType::Notice && env.dst.is_some() {
        Kind::Direct
    } else {
        Kind::Room
    }
}

/// Whose envelope an ERROR from the hub answers, by its text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Answers {
    /// A client's room envelope for the room the ERROR names.
    Room,
    /// A client's room envelope that named no room.
    NoRoom,
    /// A client's MSG or ACTION, in any room (the ERROR names none).
    RoomMessage,
    /// A client's direct NOTICE.
    Direct,
    /// A client's command.
    Command,
    /// The session's own JOIN, PART or HELLO: no client's.
    Session,
    /// Nobody's in particular: every client gets it.
    Everyone,
}

/// Whose envelope rrcd's ERROR text answers (`router.py`, `commands.py`,
/// `resource_manager.py`, `service.py` `_norm_room`).
fn error_answers(text: &str) -> Answers {
    match text {
        "destination not connected"
        | "direct notice must not include room"
        | "direct notice requires destination identity" => Answers::Direct,
        "unrecognized command"
        | "not authorized"
        | "not authorized (+t)"
        | "only the room founder can register"
        | "only the room founder can unregister" => Answers::Command,
        _ if text.starts_with("invite failed: ") => Answers::Command,
        "no such room" | "no outside messages (+n)" | "banned from room" | "room is moderated (+m)" => Answers::Room,
        "message requires room name" => Answers::NoRoom,
        _ if text.starts_with("message too large: ") => Answers::RoomMessage,
        "JOIN requires room name"
        | "too many rooms"
        | "invite-only (+i)"
        | "bad key (+k)"
        | "PART requires room name"
        | "send HELLO first"
        | "room name must not be empty" => Answers::Session,
        _ if text.starts_with("room name too long: ") => Answers::Session,
        // "rate limited" names no envelope, and the session cannot tell
        // whether its own was the one dropped. "kicked from <room>" and
        // "banned from <room>" for a room the session is not in, "bad
        // message: ...", the resource errors and texts this list does not
        // know go to every client.
        _ => Answers::Everyone,
    }
}

/// A client's command, until the hub answers it.
#[derive(Debug)]
struct PendingCommand {
    id: Vec<u8>,
    link: Link,
    /// The command envelope's room, normalised.
    room: Option<String>,
    at: Instant,
    stale: bool,
}

/// The local hub's side of one upstream hub.
pub struct Relay {
    /// The local hub's identity hash: K_SRC of its own envelopes.
    hub: [u8; 16],
    user: [u8; 16],
    upstream: [u8; 16],
    allowed: Vec<[u8; 16]>,
    part_upstream: bool,
    /// The hub's `nick`: K_NICK of every client envelope sent upstream.
    nick: Option<String>,
    commands: mpsc::Sender<Command>,
    /// The upstream session's status, for the room set.
    session: SharedStatus,
    status: SharedRelayStatus,
    changed: bool,
    clients: BTreeMap<Link, Client>,
    /// The upstream hub's last WELCOME.
    welcome: Option<Welcome>,
    /// The upstream hub's identity hash, from its WELCOME, while the
    /// session is up.
    upstream_hub: Option<Vec<u8>>,
    /// Rooms the bouncer is in upstream, with their members (identity
    /// hashes, each once, in the order they came).
    joined: BTreeMap<String, Vec<Vec<u8>>>,
    /// The hub sends member lists in JOINED (`include_joined_member_list`).
    member_lists: bool,
    /// Rooms a client's JOIN added to the room set that are not joined yet.
    added: BTreeSet<String>,
    sent: VecDeque<Sent>,
    /// Commands in the order they went upstream.
    pending_commands: VecDeque<PendingCommand>,
    /// Rooms just joined, whose JOINED rrcd follows with a NOTICE about the
    /// room for the joiner.
    join_notice: BTreeMap<String, Instant>,
    /// The hub's greeting, from the NOTICEs (and Resources) after its last
    /// WELCOME.
    greeting: Vec<Item>,
    /// Until when room-less hub NOTICEs are taken as the greeting.
    greeting_until: Option<Instant>,
    history: History,
    replay_window: Duration,
    replay_max: usize,
    /// Replays running, by client and room.
    replaying: BTreeMap<(Link, String), Running>,
    next_replay: u64,
}

/// Everything a relay needs.
pub struct RelayConfig {
    pub hub_identity: [u8; 16],
    pub user: [u8; 16],
    pub upstream: [u8; 16],
    /// The user's identity and `allowed_identities`.
    pub allowed: Vec<[u8; 16]>,
    pub part_upstream: bool,
    pub nick: Option<String>,
    pub commands: mpsc::Sender<Command>,
    pub session: SharedStatus,
    pub status: SharedRelayStatus,
    pub history: History,
    /// A JOIN replays what came in this long before it; zero: no replay.
    pub replay_window: Duration,
    /// A JOIN replays at most this many messages; 0: no replay.
    pub replay_max: usize,
}

impl Relay {
    pub fn new(config: RelayConfig) -> Self {
        Self {
            hub: config.hub_identity,
            user: config.user,
            upstream: config.upstream,
            allowed: config.allowed,
            part_upstream: config.part_upstream,
            nick: config.nick,
            commands: config.commands,
            session: config.session,
            status: config.status,
            changed: false,
            clients: BTreeMap::new(),
            welcome: None,
            upstream_hub: None,
            joined: BTreeMap::new(),
            member_lists: false,
            added: BTreeSet::new(),
            sent: VecDeque::new(),
            pending_commands: VecDeque::new(),
            join_notice: BTreeMap::new(),
            greeting: Vec::new(),
            greeting_until: None,
            history: config.history,
            replay_window: config.replay_window,
            replay_max: config.replay_max,
            replaying: BTreeMap::new(),
            next_replay: 0,
        }
    }

    /// Whether status.json should be written now (the clients changed).
    pub fn take_changed(&mut self) -> bool {
        std::mem::take(&mut self.changed)
    }

    fn tally(&self, f: impl FnOnce(&mut RelayStatus)) {
        if let Ok(mut status) = self.status.lock() {
            f(&mut status);
        }
    }

    fn show_clients(&mut self) {
        let mut identities: Vec<String> = self
            .clients
            .values()
            .filter_map(|c| match c.state {
                ClientState::Allowed(id) => Some(hex::encode(id)),
                _ => None,
            })
            .collect();
        identities.sort();
        self.tally(|s| {
            s.clients = identities.len() as u64;
            s.client_identities = identities;
        });
        self.changed = true;
    }

    fn limits(&self) -> Limits {
        self.welcome.as_ref().map(|w| w.limits).unwrap_or_default()
    }

    /// An envelope of the local hub's own.
    fn own(&self, t: MessageType) -> Envelope {
        Envelope::new(t, &self.hub)
    }

    fn error(&self, room: Option<&str>, text: &str) -> Envelope {
        let mut env = self.own(MessageType::Error).with_body(Raw::text(text));
        env.room = room.map(String::from);
        env
    }

    /// The WELCOME a client gets: the upstream hub's name, version, caps
    /// and limits; before the first upstream WELCOME, the local hub's name,
    /// rrcd's default limits and every cap on (the bouncer takes Resources
    /// whether or not the hub does, and refuses them while the hub does
    /// not).
    fn welcome_envelope(&self) -> Envelope {
        let (name, ver, caps, limits) = match &self.welcome {
            Some(w) => (
                w.hub
                    .as_deref()
                    .map_or_else(|| local_hub_name(&self.upstream), local_name),
                w.ver.clone(),
                w.caps,
                w.limits,
            ),
            None => (
                local_hub_name(&self.upstream),
                None,
                Caps {
                    resource_envelope: true,
                    action: true,
                    direct_notice: true,
                },
                Limits::default(),
            ),
        };
        let body = Welcome {
            hub: Some(name),
            ver: Some(ver.unwrap_or_else(|| Value::Text(env!("CARGO_PKG_VERSION").into()))),
            caps,
            limits,
        };
        self.own(MessageType::Welcome).with_body(body.to_body())
    }

    fn members_body(members: &[Vec<u8>]) -> Raw {
        Raw::from_value(&Value::Array(members.iter().map(|m| Value::Bytes(m.clone())).collect()))
    }

    /// The JOINED a client gets for a room the bouncer is in: with member
    /// lists, the user first, then the others in the order they came, as
    /// many as fit one packet.
    fn joined_envelope(&self, room: &str) -> Envelope {
        let mut env = self.own(MessageType::Joined).with_room(room);
        if !self.member_lists {
            return env;
        }
        let mut list = vec![self.user.to_vec()];
        let others = self
            .joined
            .get(room)
            .into_iter()
            .flatten()
            .filter(|m| m[..] != self.user[..]);
        for member in others {
            list.push(member.clone());
            env.body = Some(Self::members_body(&list));
            if env.encode().len() > LINK_MDU {
                list.pop();
                break;
            }
        }
        env.body = Some(Self::members_body(&list));
        env
    }

    /// A hub envelope on its way to clients: K_SRC the local hub's
    /// identity where it was the upstream hub's.
    fn outward(&self, env: &Envelope) -> Envelope {
        let mut env = env.clone();
        if self.upstream_hub.as_deref() == Some(&env.src[..]) {
            env.src = self.hub.to_vec();
        }
        env
    }

    fn is_upstream_hub(&self, env: &Envelope) -> bool {
        self.upstream_hub.as_deref() == Some(&env.src[..])
    }

    fn welcomed(&self) -> Vec<Link> {
        self.clients
            .iter()
            .filter(|(_, c)| c.welcomed && matches!(c.state, ClientState::Allowed(_)))
            .map(|(link, _)| *link)
            .collect()
    }

    fn in_room(&self, room: &str) -> Vec<Link> {
        self.clients
            .iter()
            .filter(|(_, c)| c.welcomed && c.rooms.contains(room))
            .map(|(link, _)| *link)
            .collect()
    }

    /// Send `env` to `links`; counted in `relayed_out` when it is sent.
    fn deliver(&self, out: &mut Vec<Output>, links: &[Link], env: &Envelope) {
        for link in links {
            out.push(Output::Client(*link, env.clone()));
        }
    }

    /// Send a hub item to `links`, K_SRC the local hub's where it was the
    /// upstream hub's: a Resource as one to a client with the
    /// `resource_envelope` cap, as NOTICEs that each fit a packet to the
    /// others.
    fn deliver_item(&self, out: &mut Vec<Output>, links: &[Link], item: &Item) {
        match item {
            Item::Env(env) => self.deliver(out, links, &self.outward(env)),
            Item::Resource { envelope, notice, data } => {
                let envelope = self.outward(envelope);
                let mut chunks = None;
                for link in links {
                    if self.clients.get(link).is_some_and(|c| c.resources) {
                        out.push(Output::Resource(*link, envelope.clone(), data.clone()));
                    } else {
                        let chunks = chunks.get_or_insert_with(|| resource::split(&self.outward(notice)));
                        self.tally(|s| s.client_resources_split += 1);
                        for chunk in chunks.iter() {
                            out.push(Output::Client(*link, chunk.clone()));
                        }
                    }
                }
            }
        }
    }

    /// End a client's replay of a room, if one runs; what was held for it
    /// is not sent.
    fn end_replay(&mut self, link: Link, room: &str, out: &mut Vec<Output>) {
        if self.replaying.remove(&(link, room.to_string())).is_some() {
            out.push(Output::EndReplay {
                link,
                room: Some(room.to_string()),
                flush: false,
            });
        }
    }

    /// The session's room set (the rooms it stays in, joined or not, and
    /// those over the limit), with the rooms clients' JOINs added since.
    fn room_set(&self) -> BTreeSet<String> {
        let mut rooms: BTreeSet<String> = self
            .session
            .lock()
            .map(|s| s.rooms.iter().chain(&s.rooms_joined).cloned().collect())
            .unwrap_or_default();
        rooms.extend(self.added.iter().cloned());
        rooms
    }

    fn over_limit(&self, room: &str) -> bool {
        self.session
            .lock()
            .is_ok_and(|s| s.rooms_over_limit.iter().any(|r| r == room))
    }

    pub fn link_established(&mut self, link: Link, now: Instant) {
        self.clients.entry(link).or_insert_with(|| Client::new(now));
    }

    pub fn link_closed(&mut self, link: Link) {
        if let Some(client) = self.clients.remove(&link) {
            // A Resource that came whole and waits for its envelope; an
            // envelope alone is not a Resource.
            let waiting = client.expected.early() as u64;
            if waiting > 0 {
                self.tally(|s| s.client_resource_drops += waiting);
            }
            self.show_clients();
        }
        self.replaying.retain(|(l, _), _| *l != link);
        // Its commands' answers have nobody to go to; they still take
        // their place in the order.
        self.sent.retain(|s| s.link != link);
    }

    /// A link identified. Called again for the same link, it does nothing.
    pub fn link_identified(&mut self, link: Link, identity: [u8; 16], now: Instant) -> Vec<Output> {
        let allowed = self.allowed.contains(&identity);
        let client = self.clients.entry(link).or_insert_with(|| Client::new(now));
        if client.state != ClientState::Unidentified {
            return Vec::new();
        }
        if allowed {
            client.state = ClientState::Allowed(identity);
            self.show_clients();
            Vec::new()
        } else {
            client.state = ClientState::Refused;
            self.tally(|s| s.refused_links += 1);
            self.changed = true;
            vec![
                Output::Client(link, self.error(None, NOT_ALLOWED)),
                Output::CloseLater(link),
            ]
        }
    }

    /// Whether a link's packets are handled: it identified as an allowed
    /// identity.
    pub fn is_allowed(&self, link: &Link) -> bool {
        self.clients
            .get(link)
            .is_some_and(|c| matches!(c.state, ClientState::Allowed(_)))
    }

    /// A packet from a client link. Packets of a link that is not
    /// identified as an allowed identity are ignored (see `is_allowed`).
    pub fn client_packet(&mut self, link: Link, data: &[u8], now: Instant) -> Vec<Output> {
        let mut out = Vec::new();
        let Some(client) = self.clients.get(&link) else {
            return out;
        };
        let ClientState::Allowed(identity) = client.state else {
            return out;
        };
        let welcomed = client.welcomed;
        let env = match Envelope::decode(data) {
            Ok(env) => env,
            Err(e) => {
                tracing::debug!("client {}: undecodable packet: {e}", hex::encode(link));
                return out;
            }
        };
        match env.t {
            // Answered before HELLO too, as the local hub did before the
            // relay (a client may PING to check the link).
            MessageType::Ping => {
                let mut pong = self.own(MessageType::Pong);
                pong.body = env.body;
                out.push(Output::Client(link, pong));
            }
            MessageType::Pong => {}
            MessageType::Hello => {
                let caps = env.body.as_ref().map(|b| Hello::from_body(b).caps).unwrap_or_default();
                let mut forgotten = 0;
                if let Some(client) = self.clients.get_mut(&link) {
                    client.welcomed = true;
                    client.rooms.clear();
                    client.pending.clear();
                    client.resources = caps.resource_envelope;
                    // The old session's envelopes, and its Resources still
                    // waiting for theirs, go with it.
                    forgotten = client.expected.clear() as u64;
                }
                if forgotten > 0 {
                    self.tally(|s| s.client_resource_drops += forgotten);
                }
                // A new session on the link: it is in no room, so its
                // replays end.
                let before = self.replaying.len();
                self.replaying.retain(|(l, _), _| *l != link);
                if self.replaying.len() < before {
                    out.push(Output::EndReplay {
                        link,
                        room: None,
                        flush: false,
                    });
                }
                out.push(Output::Client(link, self.welcome_envelope()));
                for item in &self.greeting {
                    self.deliver_item(&mut out, &[link], item);
                }
            }
            _ if !welcomed => out.push(Output::Client(link, self.error(None, "send HELLO first"))),
            MessageType::ResourceEnvelope => self.resource_envelope(link, env, now, &mut out),
            MessageType::Join => self.join(link, &env, now, &mut out),
            MessageType::Part => self.part(link, identity, &env, &mut out),
            MessageType::Msg if self.history_command(link, &env, now, &mut out) => {}
            MessageType::Msg | MessageType::Action | MessageType::Notice => {
                self.message(link, env, now, &mut out);
            }
            other => tracing::debug!("client {}: ignored a {other:?}", hex::encode(link)),
        }
        out
    }

    /// A client's `/history [N]`, answered here, never sent upstream:
    /// false if `env` is not one.
    fn history_command(&mut self, link: Link, env: &Envelope, now: Instant, out: &mut Vec<Output>) -> bool {
        let default = if self.replay_max == 0 {
            DEFAULT_HISTORY
        } else {
            self.replay_max
        };
        let Some(request) = env.text_body().and_then(|b| history_request(&b, default)) else {
            return false;
        };
        let room = env.room.as_deref().and_then(normalize_room);
        let notice = |relay: &Self, text: &str| {
            let mut env = relay.own(MessageType::Notice).with_body(Raw::text(text));
            env.room = room.clone();
            env
        };
        match (request, &room) {
            (Ok(max), Some(room)) => {
                let query = Query {
                    room: room.clone(),
                    since_ms: None,
                    max,
                };
                if !self.start_replay(link, query, true, now, out) {
                    let env = notice(self, &format!("--- a replay of {room} is running ---"));
                    out.push(Output::Now(link, env));
                }
            }
            _ => {
                let env = notice(self, HISTORY_USAGE);
                out.push(Output::Now(link, env));
            }
        }
        true
    }

    /// Ask for a replay to a client, unless one of the room to that client
    /// runs: false then.
    fn start_replay(&mut self, link: Link, query: Query, say_empty: bool, now: Instant, out: &mut Vec<Output>) -> bool {
        let key = (link, query.room.clone());
        if self.replaying.contains_key(&key) {
            return false;
        }
        let id = self.next_replay;
        self.next_replay += 1;
        self.replaying.insert(key, Running { id, since: now });
        out.push(Output::Replay(Replay {
            id,
            link,
            query,
            say_empty,
        }));
        true
    }

    /// A client's replay of a room is over (its sender task says so). An
    /// end of an older replay than the one running is ignored.
    pub fn replay_done(&mut self, link: Link, room: &str, id: u64) {
        let key = (link, room.to_string());
        if self.replaying.get(&key).is_some_and(|r| r.id == id) {
            self.replaying.remove(&key);
        }
    }

    /// A room name, checked as rrcd's `_norm_room` checks it, or the text
    /// of the ERROR rrcd would send (naming no room).
    fn room_name(&self, room: &str) -> Result<String, String> {
        let Some(room) = normalize_room(room) else {
            return Err("room name must not be empty".into());
        };
        let max = self.limits().max_room_name_bytes;
        if room.len() as u64 > max {
            return Err(format!("room name too long: {} bytes > {max} bytes", room.len()));
        }
        Ok(room)
    }

    fn join(&mut self, link: Link, env: &Envelope, now: Instant, out: &mut Vec<Output>) {
        // rrcd's order: no room (or an empty one), the room limit, then
        // the name.
        let Some(raw) = env.room.as_deref().filter(|r| !r.is_empty()) else {
            return out.push(Output::Client(link, self.error(None, "JOIN requires room name")));
        };
        let normalized = normalize_room(raw);
        if let Some(room) = &normalized
            && self.joined.contains_key(room)
        {
            let joined = self.joined_envelope(room);
            if let Some(client) = self.clients.get_mut(&link) {
                client.pending.remove(room);
                client.rooms.insert(room.clone());
            }
            self.deliver(out, &[link], &joined);
            self.replay(link, room, now, out);
            return;
        }
        let rooms = self.room_set();
        let new = normalized.as_ref().is_none_or(|r| !rooms.contains(r));
        if (new && rooms.len() as u64 >= self.limits().max_rooms_per_session)
            || normalized.as_deref().is_some_and(|r| self.over_limit(r))
        {
            return out.push(Output::Client(
                link,
                self.error(normalized.as_deref(), "too many rooms"),
            ));
        }
        let room = match self.room_name(raw) {
            Ok(room) => room,
            Err(text) => return out.push(Output::Client(link, self.error(None, &text))),
        };
        if !self.connected() {
            let text = format!("hub not connected: {room} not joined");
            return out.push(Output::Client(link, self.error(Some(&room), &text)));
        }
        match self.commands.try_send(Command::Join(room.clone())) {
            Ok(()) => {
                if new {
                    self.added.insert(room.clone());
                }
                if let Some(client) = self.clients.get_mut(&link) {
                    client.pending.insert(room);
                }
            }
            Err(_) => {
                self.tally(|s| s.relay_drops += 1);
                out.push(Output::Client(
                    link,
                    self.error(Some(&room), "the bouncer is busy: try the JOIN again"),
                ));
            }
        }
    }

    /// Replay the room's history to a client that just joined it: the
    /// newest `replay_max` messages of the last `replay_window`.
    fn replay(&mut self, link: Link, room: &str, now: Instant, out: &mut Vec<Output>) {
        if self.replay_max == 0 || self.replay_window.is_zero() {
            return;
        }
        let window = u64::try_from(self.replay_window.as_millis()).unwrap_or(u64::MAX);
        let query = Query {
            room: room.to_string(),
            since_ms: Some(now_ms().saturating_sub(window)),
            max: self.replay_max,
        };
        self.start_replay(link, query, false, now, out);
    }

    /// Keep an envelope from the hub in the room's history. It gets every
    /// envelope the hub sends to the room as a whole (see DESIGN.md
    /// § Local hub, History).
    fn record(&mut self, room: &str, env: &Envelope) {
        self.history.record(room, env);
    }

    fn part(&mut self, link: Link, identity: [u8; 16], env: &Envelope, out: &mut Vec<Output>) {
        let Some(raw) = env.room.as_deref().filter(|r| !r.is_empty()) else {
            return out.push(Output::Client(link, self.error(None, "PART requires room name")));
        };
        let room = match self.room_name(raw) {
            Ok(room) => room,
            Err(text) => return out.push(Output::Client(link, self.error(None, &text))),
        };
        // rrcd answers a PART with PARTED, whether the link was in the room
        // or not; with member lists the body is the parting identity.
        let parted = |relay: &Self, who: [u8; 16]| {
            let mut env = relay.own(MessageType::Parted).with_room(&room);
            if relay.member_lists {
                env.body = Some(Self::members_body(&[who.to_vec()]));
            }
            env
        };
        let member = self
            .clients
            .get_mut(&link)
            .is_some_and(|c| c.rooms.remove(&room) | c.pending.remove(&room));
        // The replay of the room ends, and what it held goes with it: the
        // PARTED goes out at once.
        self.end_replay(link, &room, out);
        let answer = parted(self, identity);
        self.deliver(out, &[link], &answer);
        if !member || !self.part_upstream {
            return;
        }
        if self.commands.try_send(Command::Part(room.clone())).is_err() {
            self.tally(|s| s.relay_drops += 1);
            tracing::warn!("the upstream session's queue is full: room {room} stays joined upstream");
            return;
        }
        // The session leaves the room whatever the hub answers (PARTED, or
        // nothing before its part timeout); a JOIN from now on goes
        // upstream again.
        self.joined.remove(&room);
        self.added.remove(&room);
        self.join_notice.remove(&room);
        // The bouncer leaves the room for every client.
        let others: Vec<(Link, [u8; 16])> = self
            .clients
            .iter()
            .filter(|(l, c)| **l != link && (c.rooms.contains(&room) || c.pending.contains(&room)))
            .filter_map(|(l, c)| match c.state {
                ClientState::Allowed(id) => Some((*l, id)),
                _ => None,
            })
            .collect();
        for (other, id) in others {
            if let Some(client) = self.clients.get_mut(&other) {
                client.rooms.remove(&room);
                client.pending.remove(&room);
            }
            self.end_replay(other, &room, out);
            let env = parted(self, id);
            self.deliver(out, &[other], &env);
        }
    }

    /// What rrcd would refuse a client's MSG, ACTION or NOTICE for before
    /// it looks at the room's state (`router.py` `_handle_message`, in its
    /// order), answered here: Err(None) for what it drops without a word.
    fn check(&self, env: &Envelope, kind: Kind) -> Result<Option<String>, Option<String>> {
        if kind == Kind::Command {
            return Ok(env.room.as_deref().and_then(normalize_room));
        }
        let room = env.room.as_deref().filter(|r| !r.is_empty());
        match env.t {
            MessageType::Msg | MessageType::Action => {
                if room.is_none() {
                    return Err(Some("message requires room name".into()));
                }
                let max = self.limits().max_msg_body_bytes;
                if let Some(body) = env.text_body()
                    && body.len() as u64 > max
                {
                    return Err(Some(format!("message too large: {} bytes > {max} bytes", body.len())));
                }
            }
            _ if env.dst.is_some() => {
                if env.room.is_some() {
                    return Err(Some("direct notice must not include room".into()));
                }
                return Ok(None);
            }
            _ if room.is_none() => return Err(None),
            _ => {}
        }
        match room.map(|r| self.room_name(r)) {
            Some(Ok(room)) => Ok(Some(room)),
            Some(Err(text)) => Err(Some(text)),
            None => Ok(None),
        }
    }

    fn message(&mut self, link: Link, mut env: Envelope, now: Instant, out: &mut Vec<Output>) {
        let kind = kind_of(&env);
        let room = match self.check(&env, kind) {
            Ok(room) => room,
            Err(Some(text)) => return out.push(Output::Client(link, self.error(None, &text))),
            Err(None) => {
                tracing::debug!("client {}: dropped a NOTICE with no room", hex::encode(link));
                return;
            }
        };
        if let Some(nick) = &self.nick {
            env.nick = Some(nick.clone());
        }
        // A direct NOTICE to someone other than the user, while there is a
        // session to send it: the hub does not echo it.
        let copy = kind == Kind::Direct && env.dst.as_deref() != Some(&self.user[..]) && self.connected();
        let id = env.id.clone();
        let t = env.t;
        let local = copy.then(|| {
            let mut copy = env.clone();
            copy.src = self.user.to_vec();
            copy
        });
        if self.commands.try_send(Command::Send(env)).is_err() {
            self.tally(|s| s.relay_drops += 1);
            return out.push(Output::Client(
                link,
                self.error(room.as_deref(), "not sent to the hub: the bouncer's queue is full"),
            ));
        }
        self.tally(|s| s.relayed_in += 1);
        if kind == Kind::Command {
            if self.pending_commands.len() >= MAX_SENT {
                self.pending_commands.pop_front();
            }
            self.pending_commands.push_back(PendingCommand {
                id,
                link,
                room,
                at: now,
                stale: false,
            });
            // The greeting is over by the time a command is answered.
            self.greeting_until = None;
        } else {
            // A room envelope is echoed when the bouncer is in the room or
            // will be (the session holds it until then); one for another
            // room is answered only when the hub refuses it.
            let echoed = kind == Kind::Room && room.as_ref().is_some_and(|r| self.room_set().contains(r));
            if self.sent.len() >= MAX_SENT {
                self.sent.pop_front();
            }
            self.sent.push_back(Sent {
                id,
                link,
                t,
                room,
                kind,
                at: now,
                wait: if echoed { ECHO_WAIT } else { ANSWER_WAIT },
                stale: false,
            });
        }
        if let Some(copy) = local {
            let links: Vec<Link> = self.welcomed().into_iter().filter(|l| *l != link).collect();
            self.deliver(out, &links, &copy);
        }
    }

    fn connected(&self) -> bool {
        self.upstream_hub.is_some()
    }

    /// A client's RESOURCE_ENVELOPE, checked as rrcd checks one: kept
    /// until its Resource comes (or matched with one that came first), or
    /// refused with rrcd's ERROR. An envelope is not counted: its
    /// Resource is, when it ends.
    fn resource_envelope(&mut self, link: Link, env: Envelope, now: Instant, out: &mut Vec<Output>) {
        let checked = if self.welcome.as_ref().is_some_and(|w| !w.caps.resource_envelope) {
            Err("resource transfer disabled".to_string())
        } else {
            resource::check(&env)
        };
        let refused = match checked {
            Ok(body) => match self.clients.get_mut(&link) {
                Some(client) => match client.expected.add(env.clone(), body, now) {
                    Ok(None) => None,
                    Ok(Some((expected, data))) => return self.announced_resource(link, expected, data, now, out),
                    Err(resource::TooMany) => Some("too many pending resource expectations".to_string()),
                },
                None => return,
            },
            Err(text) => Some(text),
        };
        if let Some(text) = refused {
            tracing::info!("client {}: refused a RESOURCE_ENVELOPE: {text}", hex::encode(link));
            out.push(Output::Client(link, self.error(env.room.as_deref(), &text)));
        }
    }

    /// A Resource a client link sent, whole. One a RESOURCE_ENVELOPE of
    /// the link announced (its size and sha256) goes on as
    /// [`Self::announced_resource`]; one that came before its envelope
    /// waits for it `OFFER_GRACE`; anything else is dropped and counted.
    pub fn client_resource(&mut self, link: Link, data: Vec<u8>, now: Instant) -> Vec<Output> {
        let mut out = Vec::new();
        let size = data.len();
        let drop = |relay: &Self, why: &str| {
            tracing::info!(
                "client {}: dropped a resource of {size} bytes: {why}",
                hex::encode(link)
            );
            relay.tally(|s| s.client_resource_drops += 1);
        };
        let Some(client) = self.clients.get_mut(&link) else {
            drop(self, "the link is gone");
            return out;
        };
        if !client.welcomed || !matches!(client.state, ClientState::Allowed(_)) {
            drop(self, "the link said no HELLO");
            return out;
        }
        match client.expected.take(data, now) {
            Ok(Some((expected, data))) => self.announced_resource(link, expected, data, now, &mut out),
            Ok(None) => tracing::debug!(
                "client {}: a resource of {size} bytes waits for its envelope",
                hex::encode(link)
            ),
            Err(why) => drop(self, &why),
        }
        out
    }

    /// A client's Resource and the RESOURCE_ENVELOPE that announced it: a
    /// `notice` for a room, in UTF-8. Its text is not sent upstream as a
    /// Resource: it goes as the NOTICEs [`resource::parts`] cuts it into,
    /// each as a client's NOTICE goes ([`Self::message`]), at most
    /// `MAX_PARTS`; a longer text is refused with an ERROR and nothing is
    /// sent. Counted once: in when its NOTICEs go, or dropped.
    fn announced_resource(
        &mut self,
        link: Link,
        expected: Expected,
        data: Vec<u8>,
        now: Instant,
        out: &mut Vec<Output>,
    ) {
        let drop = |relay: &Self, why: &str| {
            tracing::info!(
                "client {}: dropped a resource of {} bytes: {why}",
                hex::encode(link),
                data.len()
            );
            relay.tally(|s| s.client_resource_drops += 1);
        };
        if expected.body.kind != resource::KIND_NOTICE {
            return drop(
                self,
                &format!("a {:?} resource: only notices are relayed", expected.body.kind),
            );
        }
        if expected
            .body
            .encoding
            .as_deref()
            .is_some_and(|e| !e.eq_ignore_ascii_case("utf-8") && !e.eq_ignore_ascii_case("utf8"))
        {
            return drop(self, "not UTF-8");
        }
        let Ok(text) = std::str::from_utf8(&data) else {
            return drop(self, "not UTF-8");
        };
        let envelope = expected.envelope;
        if envelope.dst.is_some() || envelope.room.as_deref().is_none_or(str::is_empty) {
            return drop(self, "rrcd relays only a room notice");
        }
        let mut notice = resource::notice_of(&envelope, text);
        // The nick goes on before the cut, so each part still fits.
        if let Some(nick) = &self.nick {
            notice.nick = Some(nick.clone());
        }
        let max = usize::try_from(self.limits().max_msg_body_bytes).unwrap_or(usize::MAX);
        let room = envelope.room.as_deref().and_then(normalize_room);
        let parts = match resource::parts(&notice, max) {
            (Some(parts), _) if !parts.is_empty() => parts,
            (Some(_), _) => return drop(self, "no text"),
            (None, most) => {
                drop(self, "too long");
                let error = self.error(
                    room.as_deref(),
                    &format!("notice too long: {} bytes > {most}", text.len()),
                );
                return out.push(Output::Client(link, error));
            }
        };
        // What the hub would refuse every part for is answered once, and
        // the parts go all or none.
        match self.check(&parts[0], kind_of(&parts[0])) {
            Ok(_) => {}
            Err(None) => return drop(self, "no room"),
            Err(Some(text)) => {
                drop(self, &text);
                return out.push(Output::Client(link, self.error(None, &text)));
            }
        }
        if self.commands.capacity() < parts.len() {
            drop(self, "the bouncer's queue is full");
            self.tally(|s| s.relay_drops += 1);
            let error = self.error(room.as_deref(), "not sent to the hub: the bouncer's queue is full");
            return out.push(Output::Client(link, error));
        }
        tracing::debug!(
            "client {}: a notice of {} bytes goes as {} NOTICEs",
            hex::encode(link),
            text.len(),
            parts.len()
        );
        self.tally(|s| s.client_resources_in += 1);
        for part in parts {
            self.message(link, part, now, out);
        }
    }

    /// A Resource from a client link that will not come whole (it failed,
    /// or was refused): counted.
    pub fn client_resource_failed(&mut self, link: Link, why: &str) {
        tracing::info!("client {}: a resource did not come: {why}", hex::encode(link));
        self.tally(|s| s.client_resource_drops += 1);
    }

    /// The client whose envelope most likely caused a hub ERROR (see
    /// DESIGN.md § Local hub), taken off the list; None if none fits.
    fn blame(&mut self, room: Option<&str>, answers: Answers) -> Option<Link> {
        let room = room.and_then(normalize_room);
        let fits = |s: &Sent| {
            !s.stale
                && match answers {
                    Answers::Room => s.kind == Kind::Room && room.is_some() && s.room == room,
                    Answers::NoRoom => s.kind == Kind::Room && s.room.is_none(),
                    Answers::RoomMessage => {
                        s.kind == Kind::Room && matches!(s.t, MessageType::Msg | MessageType::Action)
                    }
                    Answers::Direct => s.kind == Kind::Direct,
                    _ => false,
                }
        };
        // Room envelopes leave the list at their echo, so the oldest left
        // is the one the hub is answering; direct NOTICEs leave it only by
        // time, so the newest is the likeliest.
        let pos = if answers == Answers::Direct {
            self.sent.iter().rposition(fits)
        } else {
            self.sent.iter().position(fits)
        };
        pos.and_then(|p| self.sent.remove(p)).map(|s| s.link)
    }

    /// The command a hub NOTICE or ERROR naming `room` answers: the oldest
    /// waiting, if it named no room or the same one. Taken off the list.
    fn command_answered(&mut self, room: Option<&str>) -> Option<PendingCommand> {
        let room = room.and_then(normalize_room);
        let pos = self.pending_commands.iter().position(|c| !c.stale)?;
        let head = &self.pending_commands[pos];
        if room.is_some() && head.room != room {
            return None;
        }
        self.pending_commands.remove(pos)
    }

    pub fn upstream(&mut self, event: UpstreamEvent, now: Instant) -> Vec<Output> {
        let mut out = Vec::new();
        match event {
            UpstreamEvent::Up { welcome, hub } => {
                self.welcome = Some(welcome);
                self.upstream_hub = Some(hub);
                self.member_lists = false;
                self.greeting.clear();
                self.greeting_until = Some(now + GREETING_WINDOW);
            }
            UpstreamEvent::Down => self.down(&mut out),
            UpstreamEvent::SendDropped { envelope, reason } => {
                let id = &envelope.id;
                let owner = match self.sent.iter().position(|s| &s.id == id) {
                    Some(p) => self.sent.remove(p).map(|s| s.link),
                    None => self
                        .pending_commands
                        .iter()
                        .position(|c| &c.id == id)
                        .and_then(|p| self.pending_commands.remove(p))
                        .map(|c| c.link),
                };
                match owner {
                    Some(link) if self.clients.contains_key(&link) => {
                        let room = envelope.room.as_deref().and_then(normalize_room);
                        let error = self.error(room.as_deref(), &format!("not sent to the hub: {reason}"));
                        self.deliver(&mut out, &[link], &error);
                    }
                    _ => tracing::debug!("a dropped client envelope's client is gone"),
                }
            }
            UpstreamEvent::Envelope(env, inbound) => self.hub_envelope(env, inbound, now, &mut out),
            UpstreamEvent::Resource { envelope, data } => self.hub_resource(envelope, data, now, &mut out),
        }
        out
    }

    /// A Resource from the hub: a `notice` goes where the NOTICE with its
    /// text would, and is kept as NOTICEs in history (each fits a history
    /// record); a room-less `motd` is the greeting. A `blob` has nowhere
    /// to go: dropped and counted.
    fn hub_resource(&mut self, envelope: Envelope, data: Arc<Vec<u8>>, now: Instant, out: &mut Vec<Output>) {
        let Some(body) = envelope.body.as_ref().and_then(ResourceEnvelope::from_body) else {
            return;
        };
        if body.kind != resource::KIND_NOTICE && body.kind != resource::KIND_MOTD {
            tracing::info!(
                "dropped a {:?} resource from the hub: only notices and the MOTD are relayed",
                body.kind
            );
            self.tally(|s| s.client_resource_drops += 1);
            return;
        }
        let notice = resource::notice_of(&envelope, &resource::text(&data));
        let roomless = notice.room.is_none() && notice.dst.is_none();
        if !roomless {
            self.greeting_until = None;
        }
        // rrcd sends a long greeting as a room-less `motd` Resource and
        // nothing else so: it is the greeting whenever it ends, and no
        // command's answer.
        let (links, record, greeting) = if roomless && body.kind == resource::KIND_MOTD {
            (self.welcomed(), None, true)
        } else {
            self.route(&notice, now)
        };
        if let Some(room) = record {
            for chunk in resource::split(&notice) {
                self.record(&room, &chunk);
            }
        }
        let item = Item::Resource {
            envelope,
            notice: Box::new(notice),
            data,
        };
        if greeting {
            self.keep_greeting(item.clone());
        }
        self.deliver_item(out, &links, &item);
    }

    /// Keep a greeting item for clients that link later, within
    /// `MAX_GREETING` and `MAX_GREETING_BYTES`.
    fn keep_greeting(&mut self, item: Item) {
        let bytes: usize = self
            .greeting
            .iter()
            .chain([&item])
            .map(|i| match i {
                Item::Resource { data, .. } => data.len(),
                Item::Env(_) => 0,
            })
            .sum();
        if self.greeting.len() < MAX_GREETING && bytes <= MAX_GREETING_BYTES {
            self.greeting.push(item);
        } else {
            tracing::info!(
                "the hub's greeting is longer than the bouncer keeps: clients that link later miss the rest"
            );
        }
    }

    /// The session went down: what was joined through it is gone.
    fn down(&mut self, out: &mut Vec<Output>) {
        self.upstream_hub = None;
        self.joined.clear();
        self.join_notice.clear();
        self.greeting_until = None;
        // Nothing sent on the old link can be answered on the next; the
        // session may still report envelopes it held as dropped.
        for sent in &mut self.sent {
            sent.stale = true;
        }
        for command in &mut self.pending_commands {
            command.stale = true;
        }
        let waiting: Vec<(Link, String)> = self
            .clients
            .iter_mut()
            .flat_map(|(l, c)| std::mem::take(&mut c.pending).into_iter().map(|r| (*l, r)))
            .collect();
        for (link, room) in waiting {
            let error = self.error(Some(&room), &format!("hub not connected: {room} not joined"));
            self.deliver(out, &[link], &error);
        }
        // A room a client's JOIN added, never joined, leaves the room set.
        for room in std::mem::take(&mut self.added) {
            if self.commands.try_send(Command::Part(room.clone())).is_err() {
                tracing::warn!("the upstream session's queue is full: room {room} stays in the room set");
            }
        }
    }

    fn hub_envelope(&mut self, env: Envelope, inbound: Inbound, now: Instant, out: &mut Vec<Output>) {
        if !(env.t == MessageType::Notice && env.room.is_none() && env.dst.is_none()) {
            self.greeting_until = None;
        }
        match inbound {
            Inbound::Handled | Inbound::Welcomed => {}
            Inbound::JoinConfirmed(room) => {
                // A JOINED with no body (the session's own answer to a JOIN
                // for a room it is in, or a hub without member lists)
                // leaves the list as it is.
                let mut list = self.joined.get(&room).cloned().unwrap_or_default();
                if let Some(members) = env.members() {
                    self.member_lists = true;
                    list.clear();
                    for member in members {
                        if !list.contains(&member) {
                            list.push(member);
                        }
                    }
                }
                if self.joined.insert(room.clone(), list).is_none() {
                    self.join_notice.insert(room.clone(), now);
                }
                self.added.remove(&room);
                let answer = self.joined_envelope(&room);
                let waiting: Vec<Link> = self
                    .clients
                    .iter()
                    .filter(|(_, c)| c.pending.contains(&room))
                    .map(|(l, _)| *l)
                    .collect();
                for link in waiting {
                    if let Some(client) = self.clients.get_mut(&link) {
                        client.pending.remove(&room);
                        client.rooms.insert(room.clone());
                    }
                    self.deliver(out, &[link], &answer);
                    self.replay(link, &room, now, out);
                }
            }
            Inbound::JoinFailed { room, .. } => {
                let answer = self.outward(&env);
                let waiting: Vec<Link> = self
                    .clients
                    .iter_mut()
                    .filter_map(|(l, c)| c.pending.remove(&room).then_some(*l))
                    .collect();
                self.deliver(out, &waiting, &answer);
                // A room a client's JOIN added goes out of the room set
                // again, so it is not tried at every connect.
                if self.added.remove(&room) && self.commands.try_send(Command::Part(room.clone())).is_err() {
                    tracing::warn!("the upstream session's queue is full: room {room} stays in the room set");
                }
            }
            Inbound::PartConfirmed(room) => {
                self.joined.remove(&room);
            }
            Inbound::Removed { room, .. } => {
                self.joined.remove(&room);
                self.record(&room, &env);
                let answer = self.outward(&env);
                let members = self.in_room(&room);
                for link in &members {
                    if let Some(client) = self.clients.get_mut(link) {
                        client.rooms.remove(&room);
                    }
                    self.end_replay(*link, &room, out);
                }
                self.deliver(out, &members, &answer);
            }
            Inbound::HubError(text) => self.hub_error(env, &text, out),
            Inbound::Relay => self.relay(env, now, out),
        }
    }

    fn hub_error(&mut self, env: Envelope, text: &str, out: &mut Vec<Output>) {
        let answer = self.outward(&env);
        let room = env.room.as_deref();
        let answers = error_answers(text);
        let owner = match answers {
            Answers::Session => {
                tracing::info!("hub error for the session: {text} (room={room:?})");
                return;
            }
            Answers::Everyone => None,
            Answers::Command => self.command_answered(room).map(|c| c.link),
            _ => self.blame(room, answers),
        };
        match owner {
            Some(link) if self.clients.contains_key(&link) => self.deliver(out, &[link], &answer),
            Some(_) => tracing::debug!("hub error {text:?} for a client that is gone"),
            None => {
                let links = self.welcomed();
                self.deliver(out, &links, &answer);
            }
        }
    }

    fn relay(&mut self, env: Envelope, now: Instant, out: &mut Vec<Output>) {
        let (links, record, greeting) = self.route(&env, now);
        if let Some(room) = record {
            self.record(&room, &env);
        }
        let item = Item::Env(env);
        if greeting {
            self.keep_greeting(item.clone());
        }
        self.deliver_item(out, &links, &item);
    }

    /// Where an envelope from the hub goes: the links, the room whose
    /// history keeps it, and whether it is part of the greeting.
    fn route(&mut self, env: &Envelope, now: Instant) -> (Vec<Link>, Option<String>, bool) {
        let room = env.room.as_deref().and_then(normalize_room);
        let from_hub = self.is_upstream_hub(env);
        let mut record = None;
        let mut greeting = false;
        match env.t {
            MessageType::Joined | MessageType::Parted => {
                if let (Some(room), Some(changed)) = (&room, env.members())
                    && let Some(members) = self.joined.get_mut(room)
                {
                    if env.t == MessageType::Joined {
                        for member in changed {
                            if !members.contains(&member) {
                                members.push(member);
                            }
                        }
                    } else {
                        members.retain(|m| !changed.contains(m));
                    }
                }
            }
            MessageType::Msg | MessageType::Action | MessageType::Notice if env.src[..] == self.user[..] => {
                self.sent.retain(|s| s.id != env.id);
            }
            _ => {}
        }
        let links = match (&room, &env.dst) {
            (Some(room), _) if env.t == MessageType::Notice && from_hub => {
                // The NOTICE rrcd follows a JOINED with, about the room
                // ("room <r>: registered; mode=...; topic=...").
                let about = format!("room {room}: ");
                let fresh = self.join_notice.contains_key(room)
                    && env.text_body().is_some_and(|b| b.starts_with(&about))
                    && self
                        .join_notice
                        .remove(room)
                        .is_some_and(|at| now.saturating_duration_since(at) < GREETING_WINDOW);
                let reply = if fresh { None } else { self.command_answered(Some(room)) };
                let mut links = self.in_room(room);
                match reply {
                    // A command's answer, or a change it made that the hub
                    // tells the whole room about.
                    Some(command) => {
                        if self.clients.contains_key(&command.link) && !links.contains(&command.link) {
                            links.push(command.link);
                        }
                    }
                    // The NOTICE about the room is for the joiner: not kept.
                    None if fresh => {}
                    None => record = Some(room.clone()),
                }
                links
            }
            (Some(room), _) => {
                record = Some(room.clone());
                self.in_room(room)
            }
            (None, Some(_)) => self.welcomed(),
            (None, None) if env.t == MessageType::Notice && from_hub => {
                if self.greeting_until.is_some_and(|until| now < until) {
                    greeting = true;
                    self.welcomed()
                } else {
                    match self.command_answered(None) {
                        Some(command) if self.clients.contains_key(&command.link) => vec![command.link],
                        Some(_) => Vec::new(),
                        None => self.welcomed(),
                    }
                }
            }
            (None, None) => self.welcomed(),
        };
        (links, record, greeting)
    }

    /// Time passing: close links that never identified, forget old sends,
    /// end replays that never said they were done.
    pub fn tick(&mut self, now: Instant) -> Vec<Output> {
        self.tick_busy(now, &std::collections::HashSet::new())
    }

    /// [`Self::tick`], given the sizes of the client Resources each link
    /// is still sending: their envelopes wait on.
    pub fn tick_busy(&mut self, now: Instant, busy: &std::collections::HashSet<(Link, u64)>) -> Vec<Output> {
        let stuck: Vec<(Link, String)> = self
            .replaying
            .iter()
            .filter(|(_, r)| now.saturating_duration_since(r.since) >= REPLAY_WAIT)
            .map(|(k, _)| k.clone())
            .collect();
        let mut out = Vec::new();
        for (link, room) in stuck {
            tracing::warn!("a replay of {room} did not end in {}s: ended", REPLAY_WAIT.as_secs());
            self.replaying.remove(&(link, room.clone()));
            out.push(Output::EndReplay {
                link,
                room: Some(room),
                flush: true,
            });
        }
        let late: Vec<Link> = self
            .clients
            .iter()
            .filter(|(_, c)| {
                c.state == ClientState::Unidentified && now.saturating_duration_since(c.linked_at) >= IDENTIFY_TIMEOUT
            })
            .map(|(l, _)| *l)
            .collect();
        for link in &late {
            self.clients.remove(link);
        }
        let mut dropped = Vec::new();
        let mut forgotten = 0;
        for (link, client) in &mut self.clients {
            let (gone, envelopes) = client.expected.expire(now, |size| busy.contains(&(*link, size)));
            dropped.extend(gone.into_iter().map(|why| (*link, why)));
            forgotten += envelopes;
        }
        if forgotten > 0 {
            tracing::info!("{forgotten} announced client resources did not come in time");
        }
        for (link, why) in &dropped {
            tracing::info!("client {}: dropped a resource: {why}", hex::encode(link));
        }
        if !dropped.is_empty() {
            let n = dropped.len() as u64;
            self.tally(|s| s.client_resource_drops += n);
        }
        self.sent.retain(|s| now.saturating_duration_since(s.at) < s.wait);
        self.pending_commands
            .retain(|c| now.saturating_duration_since(c.at) < ANSWER_WAIT);
        self.join_notice
            .retain(|_, at| now.saturating_duration_since(*at) < GREETING_WINDOW);
        if self.greeting_until.is_some_and(|until| now >= until) {
            self.greeting_until = None;
        }
        out.extend(late.into_iter().map(Output::Close));
        out
    }
}

/// The identity each link of a local hub identified as, filled in by the
/// hub's identity gate.
pub type LinkIdentities = Arc<Mutex<HashMap<[u8; 16], [u8; 16]>>>;

/// The identity gate of a local hub. rsReticulum calls it on the link
/// manager's task while it handles a link's LinkIdentify packet, before it
/// handles any later packet on that link (`IdentityGatePolicy`,
/// `rns-runtime/src/destination_runtime.rs:88-93`, called from
/// `link_manager.rs:2973-2977`). So when a packet reaches the hub's task
/// the link's identity is already here, whatever order the separate
/// `links_identified` and `link_packets` channels are read in. The gate
/// also tells the task on `identified`, which (unlike `links_identified`,
/// sent with `try_send` on a bounded channel) drops nothing, so a link of
/// another identity is refused even if it sends nothing. Every identity is
/// let through the gate (false would close the link at once, before the
/// ERROR saying why); the relay refuses the others.
pub fn identity_gate(
    identities: LinkIdentities,
    identified: mpsc::UnboundedSender<(Link, [u8; 16])>,
) -> IdentityGatePolicy {
    IdentityGatePolicy::new(move |link, identity| {
        if let Ok(mut map) = identities.lock() {
            map.insert(link, identity);
        }
        let _ = identified.send((link, identity));
        true
    })
}

fn link_identity(identities: &LinkIdentities, link: &[u8; 16]) -> Option<[u8; 16]> {
    identities.lock().ok().and_then(|map| map.get(link).copied())
}

/// Everything a local hub's task needs.
pub struct LocalHub {
    pub registered: RegisteredDestination,
    pub identities: LinkIdentities,
    pub identified: mpsc::UnboundedReceiver<(Link, [u8; 16])>,
    pub events: EventReceiver,
    pub relay: Relay,
    /// Woken when the clients change, for status.json.
    pub changed: Arc<Notify>,
    /// The client Resources the accept policy let in.
    pub slots: ResourceSlots,
}

/// The client Resources a local hub accepted and is receiving, by link and
/// resource hash, with their size. One is kept until its transfer ends
/// (rsReticulum ends a stalled one with its watchdog, and all of a link's
/// with the link): the local hub cannot cancel a transfer.
pub type ResourceSlots = Arc<Mutex<HashMap<(Link, [u8; 32]), u64>>>;

/// The sizes of the client Resources each link is sending.
fn busy_sizes(slots: &ResourceSlots) -> std::collections::HashSet<(Link, u64)> {
    slots
        .lock()
        .map(|slots| slots.iter().map(|((link, _), size)| (*link, *size)).collect())
        .unwrap_or_default()
}

/// The most client Resources a local hub receives at once, all links.
const HUB_RESOURCES: usize = 32;

/// A local hub's Resource accept policy, called by rsReticulum on the link
/// manager's task for every Resource advertisement from a client link. It
/// takes a Resource of 1 to `MAX_RESOURCE_BYTES` bytes, in one segment,
/// not a request or response, from an allowed identity, while that link
/// receives fewer than `MAX_EXPECTED` and the hub fewer than
/// `HUB_RESOURCES`; the rest are refused (and counted: rsReticulum tells
/// nothing more of a refused advertisement). Whether a RESOURCE_ENVELOPE
/// announced it is checked when it is whole.
pub fn resource_policy(
    identities: LinkIdentities,
    allowed: Vec<[u8; 16]>,
    slots: ResourceSlots,
    status: SharedRelayStatus,
) -> ResourceAcceptPolicy {
    ResourceAcceptPolicy::new(move |link, adv| {
        let refused = if !link_identity(&identities, &link).is_some_and(|id| allowed.contains(&id)) {
            Some("from a link of no allowed identity".to_string())
        } else if adv.data_size == 0 || adv.data_size as u64 > MAX_RESOURCE_BYTES {
            Some(format!("{} bytes", adv.data_size))
        } else if adv.total_segments > 1 {
            Some(format!("in {} segments", adv.total_segments))
        } else if adv.request_id.is_some() || adv.flags.is_request || adv.flags.is_response {
            Some("a request or response".to_string())
        } else {
            let Ok(mut slots) = slots.lock() else { return false };
            if slots.contains_key(&(link, adv.resource_hash)) {
                return true;
            }
            let on_link = slots.keys().filter(|(l, _)| *l == link).count();
            if on_link >= resource::MAX_EXPECTED {
                Some(format!("{on_link} come on the link already"))
            } else if slots.len() >= HUB_RESOURCES {
                Some(format!("{} come already", slots.len()))
            } else {
                slots.insert((link, adv.resource_hash), adv.data_size as u64);
                None
            }
        };
        match refused {
            None => true,
            Some(why) => {
                tracing::info!("client {}: refused a resource: {why}", hex::encode(link));
                if let Ok(mut s) = status.lock() {
                    s.client_resource_drops += 1;
                }
                false
            }
        }
    })
}

fn release_slot(slots: &ResourceSlots, link: Link, hash: [u8; 32]) {
    if let Ok(mut slots) = slots.lock() {
        slots.remove(&(link, hash));
    }
}

/// Resources sent to client links waiting for their end, by link and
/// resource hash. The hub's task fills it from the destination's events;
/// an end that comes before its waiter is kept for it a while.
#[derive(Clone, Default)]
pub struct ResourceWaiters(Arc<Mutex<Waiters>>);

#[derive(Default)]
struct Waiters {
    waiting: HashMap<(Link, [u8; 32]), tokio::sync::oneshot::Sender<bool>>,
    early: HashMap<(Link, [u8; 32]), (bool, Instant)>,
}

/// The most ends kept for no waiter.
const MAX_EARLY: usize = 256;

impl ResourceWaiters {
    fn wait(&self, link: Link, hash: [u8; 32]) -> tokio::sync::oneshot::Receiver<bool> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if let Ok(mut w) = self.0.lock() {
            w.waiting.retain(|_, tx| !tx.is_closed());
            match w.early.remove(&(link, hash)) {
                Some((ok, _)) => {
                    let _ = tx.send(ok);
                }
                None => {
                    w.waiting.insert((link, hash), tx);
                }
            }
        }
        rx
    }

    /// A client link closed: nothing it was sent will be waited for.
    fn forget(&self, link: Link) {
        if let Ok(mut w) = self.0.lock() {
            w.waiting.retain(|(l, _), _| *l != link);
            w.early.retain(|(l, _), _| *l != link);
        }
    }

    /// A Resource sent to a client link ended: taken whole, or not.
    pub fn concluded(&self, link: Link, hash: [u8; 32], ok: bool) {
        let Ok(mut w) = self.0.lock() else { return };
        match w.waiting.remove(&(link, hash)) {
            Some(tx) => {
                let _ = tx.send(ok);
            }
            None => {
                let now = Instant::now();
                w.early
                    .retain(|_, (_, at)| now.duration_since(*at) < resource::RESOURCE_TIMEOUT);
                if w.early.len() < MAX_EARLY {
                    w.early.entry((link, hash)).or_insert((ok, now));
                }
            }
        }
    }
}

/// A local hub's client links, through its destination.
#[derive(Clone)]
struct HubSink {
    handle: DestinationHandle,
    waiters: ResourceWaiters,
}

/// How often the relay's clock runs.
const TICK: Duration = Duration::from_secs(1);

/// Where a client link's envelopes and Resources go: the local hub's
/// destination, or a fake in the tests.
pub trait LinkSink: Clone + Send + Sync + 'static {
    fn send(&self, link: Link, data: Vec<u8>) -> impl std::future::Future<Output = Result<(), String>> + Send;
    /// Send a Resource and wait until the link has it whole, or not.
    fn send_resource(&self, link: Link, data: Vec<u8>) -> impl std::future::Future<Output = Result<(), String>> + Send;
}

impl LinkSink for HubSink {
    async fn send(&self, link: Link, data: Vec<u8>) -> Result<(), String> {
        self.handle
            .send_link_packet(link, data)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn send_resource(&self, link: Link, data: Vec<u8>) -> Result<(), String> {
        let receipt = self
            .handle
            .send_link_resource(link, data, false)
            .await
            .map_err(|e| e.to_string())?;
        match self.waiters.wait(link, receipt.resource_hash).await {
            Ok(true) => Ok(()),
            Ok(false) => Err("the client did not take it".into()),
            Err(_) => Err("the hub stopped".into()),
        }
    }
}

/// What the relay's task hands a client link's sender task.
#[derive(Debug)]
enum ToLink {
    /// An envelope, held while a replay of its room runs.
    Send(Envelope),
    /// The local hub's own notice, never held.
    Now(Envelope),
    /// A RESOURCE_ENVELOPE and its Resource, held as `Send` is.
    Resource(Envelope, Arc<Vec<u8>>),
    /// A replay; its room's envelopes are held from here.
    Replay {
        id: u64,
        room: String,
        say_empty: bool,
        records: tokio::sync::oneshot::Receiver<Vec<Record>>,
    },
    /// End the replays of a room (of every room: None).
    End { room: Option<String>, flush: bool },
}

/// A replay waiting its turn on a link, or sending.
struct LinkReplay {
    id: u64,
    room: String,
    say_empty: bool,
    records: Records,
}

enum Records {
    Asked(tokio::sync::oneshot::Receiver<Vec<Record>>),
    Got(Vec<Record>),
    Sending(VecDeque<Envelope>),
}

/// Something held for a link while its room's replay runs.
enum Held {
    Env(Envelope),
    Resource(Envelope, Arc<Vec<u8>>),
}

/// A link's Resources waiting their turn.
type ResourceQueue = VecDeque<(Envelope, Arc<Vec<u8>>)>;

/// The room an item held for a link is for.
fn held_room(item: &Held) -> Option<String> {
    let env = match item {
        Held::Env(env) | Held::Resource(env, _) => env,
    };
    env.room.as_deref().and_then(normalize_room)
}

/// The most replay commands (a replay asked for, or ended) waiting for a
/// link's sender task; a replay asked for beyond it is refused.
const CONTROL_QUEUE: usize = 32;

/// The most items waiting to follow those released after a replay.
const MAX_FLUSHING: usize = 2 * MAX_HELD;

/// A link's count of Resources once the link has closed and what was left
/// was counted (see [`Links::close`]).
const CLOSED: usize = usize::MAX;

/// What a sender task shares: who it is, what it tells, where it counts.
#[derive(Clone)]
struct LinkTask<S> {
    sink: S,
    link: Link,
    ids: ([u8; 16], [u8; 16]),
    status: SharedRelayStatus,
    done: mpsc::UnboundedSender<(Link, String, u64)>,
    /// The Resources given to the task and not ended yet.
    resources: Arc<std::sync::atomic::AtomicUsize>,
}

impl<S: LinkSink> LinkTask<S> {
    fn tally(&self, f: impl FnOnce(&mut RelayStatus)) {
        if let Ok(mut s) = self.status.lock() {
            f(&mut s);
        }
    }

    /// Send one envelope, given `SEND_TIMEOUT`: false if it was not sent.
    async fn send(&self, env: &Envelope) -> bool {
        let failure = match tokio::time::timeout(SEND_TIMEOUT, self.sink.send(self.link, env.encode())).await {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(e),
            Err(_) => Some(format!("no room in {}s", SEND_TIMEOUT.as_secs())),
        };
        match failure {
            None => {
                self.tally(|s| s.relayed_out += 1);
                true
            }
            Some(e) => {
                self.tally(|s| s.relay_drops += 1);
                tracing::debug!("could not send a {:?} to link {}: {e}", env.t, hex::encode(self.link));
                false
            }
        }
    }

    /// One of the link's Resources ended: the client took it, or it was
    /// dropped. Counted under the status lock, so it is never counted
    /// again by [`Links::close`].
    fn resource_ended(&self, taken: bool) {
        use std::sync::atomic::Ordering;
        let Ok(mut s) = self.status.lock() else { return };
        let left = self.resources.load(Ordering::Relaxed);
        if left == CLOSED {
            return;
        }
        self.resources.store(left.saturating_sub(1), Ordering::Relaxed);
        if taken {
            s.client_resources_out += 1;
        } else {
            s.client_resource_drops += 1;
        }
    }

    /// Items held for the link that will not go: its Resources are
    /// counted dropped.
    fn forget(&self, items: impl IntoIterator<Item = Held>) {
        for item in items {
            if let Held::Resource(..) = item {
                self.resource_ended(false);
            }
        }
    }

    /// Send a RESOURCE_ENVELOPE, then its Resource, and wait until the
    /// transfer ends: the local hub cannot cancel one, so the next is not
    /// offered while it runs (rsReticulum's watchdog ends a stalled one).
    async fn resource(&self, env: Envelope, data: Arc<Vec<u8>>) {
        let failure = if !self.send(&env).await {
            Some("its RESOURCE_ENVELOPE was not sent".to_string())
        } else {
            self.sink.send_resource(self.link, data.to_vec()).await.err()
        };
        if let Some(e) = &failure {
            tracing::info!(
                "a resource of {} bytes to link {} was dropped: {e}",
                data.len(),
                hex::encode(self.link)
            );
        }
        self.resource_ended(failure.is_none());
    }

    /// Queue a Resource for the link, or drop it (counted) when
    /// `LINK_RESOURCES` wait already.
    fn queue(&self, resources: &mut ResourceQueue, env: Envelope, data: Arc<Vec<u8>>) {
        if resources.len() >= LINK_RESOURCES {
            self.resource_ended(false);
            tracing::info!(
                "link {}: {LINK_RESOURCES} resources wait: one dropped",
                hex::encode(self.link)
            );
        } else {
            resources.push_back((env, data));
        }
    }

    /// A replay is over: what was held for its room follows, one item a
    /// turn, then the relay is told.
    fn finish(&self, replay: LinkReplay, held: &mut BTreeMap<String, Vec<Held>>, flushing: &mut VecDeque<Held>) {
        flushing.extend(held.remove(&replay.room).unwrap_or_default());
        let _ = self.done.send((self.link, replay.room, replay.id));
    }

    fn envelopes(&self, replay: &LinkReplay, records: &[Record]) -> VecDeque<Envelope> {
        let (hub, upstream) = self.ids;
        replay_envelopes(records, &hub, &upstream, &replay.room, replay.say_empty).into()
    }
}

/// Where a link's sender puts an envelope or Resource for a room: held
/// while the room's replay runs, after what of the room is still being
/// released, or out now.
enum Place {
    Held,
    Flushing,
    Now,
}

fn place(room: Option<&String>, held: &BTreeMap<String, Vec<Held>>, flushing: &VecDeque<Held>) -> Place {
    match room {
        Some(r) if held.contains_key(r) => Place::Held,
        Some(r) if flushing.iter().any(|h| held_room(h).as_ref() == Some(r)) => Place::Flushing,
        _ => Place::Now,
    }
}

/// One client link's sender: sends what the relay's task gives it in
/// order, never holding the relay's task up. Its replays go one after
/// another, one envelope every `REPLAY_PACE`; while a room's replay waits
/// or runs, the room's other envelopes are held (`MAX_HELD`, the rest
/// dropped) and released after it, one a turn, so a PART, a new HELLO or
/// a replay's end is read in between. The bouncer's own notices are never
/// held. Resources go one at a time (a client takes one at a time:
/// weechat-rrc cancels the one it is taking when another is offered);
/// envelopes go on meanwhile.
async fn link_sender<S: LinkSink>(
    task: LinkTask<S>,
    mut rx: mpsc::UnboundedReceiver<ToLink>,
    queued: Arc<std::sync::atomic::AtomicUsize>,
    controls: Arc<std::sync::atomic::AtomicUsize>,
) {
    use std::sync::atomic::Ordering;
    let mut waiting: VecDeque<LinkReplay> = VecDeque::new();
    let mut current: Option<LinkReplay> = None;
    let mut held: BTreeMap<String, Vec<Held>> = BTreeMap::new();
    // Released items, in order, sent one a turn.
    let mut flushing: VecDeque<Held> = VecDeque::new();
    let mut resources: ResourceQueue = VecDeque::new();
    let mut sending: Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>> = None;
    let mut next_at = tokio::time::Instant::now();
    loop {
        if sending.is_none()
            && let Some((env, data)) = resources.pop_front()
        {
            let task = task.clone();
            sending = Some(Box::pin(async move { task.resource(env, data).await }));
        }
        // A waiting replay with nothing to send ends at once: its "no
        // history" goes out without waiting its turn.
        let mut i = 0;
        while i < waiting.len() {
            if let Records::Asked(rx) = &mut waiting[i].records {
                match rx.try_recv() {
                    Ok(records) => waiting[i].records = Records::Got(records),
                    Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                        waiting[i].records = Records::Got(Vec::new());
                    }
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
                }
            }
            if matches!(&waiting[i].records, Records::Got(r) if r.is_empty()) {
                let Some(replay) = waiting.remove(i) else { break };
                for env in task.envelopes(&replay, &[]) {
                    task.send(&env).await;
                }
                task.finish(replay, &mut held, &mut flushing);
            } else {
                i += 1;
            }
        }
        // The next replay starts once the last one's held items are out.
        if current.is_none() && flushing.is_empty() {
            current = waiting.pop_front();
        }
        if let Some(replay) = &mut current
            && let Records::Got(records) = &replay.records
        {
            let envelopes = task.envelopes(replay, records);
            replay.records = Records::Sending(envelopes);
        }
        enum Step {
            Message(Option<ToLink>),
            Records(Vec<Record>),
            Pace,
            Sent,
            Flush,
        }
        let step = tokio::select! {
            message = rx.recv() => Step::Message(message),
            () = async {
                match sending.as_mut() {
                    Some(sent) => sent.await,
                    None => std::future::pending().await,
                }
            } => Step::Sent,
            step = async {
                match current.as_mut().map(|r| &mut r.records) {
                    Some(Records::Asked(records)) => Step::Records(records.await.unwrap_or_default()),
                    Some(Records::Sending(_)) => {
                        tokio::time::sleep_until(next_at).await;
                        Step::Pace
                    }
                    _ => std::future::pending().await,
                }
            } => step,
            () = std::future::ready(()), if !flushing.is_empty() => Step::Flush,
        };
        match step {
            Step::Message(None) => return,
            Step::Message(Some(ToLink::Send(env))) => {
                queued.fetch_sub(1, Ordering::Relaxed);
                let room = env.room.as_deref().and_then(normalize_room);
                match place(room.as_ref(), &held, &flushing) {
                    Place::Held => match room.and_then(|r| held.get_mut(&r)) {
                        Some(items) if items.len() < MAX_HELD => items.push(Held::Env(env)),
                        _ => task.tally(|s| s.relay_drops += 1),
                    },
                    Place::Flushing if flushing.len() < MAX_FLUSHING => flushing.push_back(Held::Env(env)),
                    Place::Flushing => task.tally(|s| s.relay_drops += 1),
                    Place::Now => {
                        task.send(&env).await;
                    }
                }
            }
            Step::Message(Some(ToLink::Resource(env, data))) => {
                queued.fetch_sub(1, Ordering::Relaxed);
                let room = env.room.as_deref().and_then(normalize_room);
                match place(room.as_ref(), &held, &flushing) {
                    Place::Held => match room.and_then(|r| held.get_mut(&r)) {
                        Some(items) if items.len() < MAX_HELD => items.push(Held::Resource(env, data)),
                        _ => task.resource_ended(false),
                    },
                    Place::Flushing if flushing.len() < MAX_FLUSHING => flushing.push_back(Held::Resource(env, data)),
                    Place::Flushing => task.resource_ended(false),
                    Place::Now => task.queue(&mut resources, env, data),
                }
            }
            Step::Sent => sending = None,
            Step::Flush => match flushing.pop_front() {
                Some(Held::Env(env)) => {
                    task.send(&env).await;
                }
                Some(Held::Resource(env, data)) => task.queue(&mut resources, env, data),
                None => {}
            },
            Step::Message(Some(ToLink::Now(env))) => {
                queued.fetch_sub(1, Ordering::Relaxed);
                task.send(&env).await;
            }
            Step::Message(Some(ToLink::Replay {
                id,
                room,
                say_empty,
                records,
            })) => {
                controls.fetch_sub(1, Ordering::Relaxed);
                held.entry(room.clone()).or_default();
                waiting.push_back(LinkReplay {
                    id,
                    room,
                    say_empty,
                    records: Records::Asked(records),
                });
            }
            Step::Message(Some(ToLink::End { room, flush })) => {
                controls.fetch_sub(1, Ordering::Relaxed);
                let ends = |r: &str| room.as_deref().is_none_or(|room| room == r);
                waiting.retain(|r| !ends(&r.room));
                if current.as_ref().is_some_and(|r| ends(&r.room)) {
                    current = None;
                }
                let ended: Vec<String> = held.keys().filter(|r| ends(r)).cloned().collect();
                for r in ended {
                    let items = held.remove(&r).unwrap_or_default();
                    if flush {
                        flushing.extend(items);
                    } else {
                        task.forget(items);
                    }
                }
                // Not flushed: what of the room is still being released
                // goes too.
                if !flush {
                    let (gone, kept): (VecDeque<Held>, VecDeque<Held>) = std::mem::take(&mut flushing)
                        .into_iter()
                        .partition(|h| held_room(h).as_deref().is_some_and(ends) || room.is_none());
                    flushing = kept;
                    task.forget(gone);
                }
            }
            Step::Records(records) => {
                if let Some(replay) = &mut current {
                    replay.records = Records::Got(records);
                }
            }
            Step::Pace => {
                let Some(replay) = &mut current else { continue };
                let Records::Sending(envelopes) = &mut replay.records else {
                    continue;
                };
                if let Some(env) = envelopes.pop_front() {
                    let sent = task.send(&env).await;
                    next_at = tokio::time::Instant::now() + REPLAY_PACE;
                    if !sent {
                        tracing::debug!(
                            "replay of {} to link {}: send failed, given up",
                            replay.room,
                            hex::encode(task.link)
                        );
                        envelopes.clear();
                    }
                }
                if envelopes.is_empty()
                    && let Some(replay) = current.take()
                {
                    task.finish(replay, &mut held, &mut flushing);
                }
            }
        }
    }
}

/// A client link's sender task, and what is queued for it.
struct LinkSender {
    tx: mpsc::UnboundedSender<ToLink>,
    /// Envelopes and Resources in the channel.
    queued: Arc<std::sync::atomic::AtomicUsize>,
    /// Replay commands in the channel.
    controls: Arc<std::sync::atomic::AtomicUsize>,
    /// Resources given to the task and not ended.
    resources: Arc<std::sync::atomic::AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

/// The sender tasks of a local hub's client links, started at a link's
/// first envelope and stopped when it closes.
struct Links<S: LinkSink> {
    sink: S,
    ids: ([u8; 16], [u8; 16]),
    status: SharedRelayStatus,
    done: mpsc::UnboundedSender<(Link, String, u64)>,
    senders: HashMap<Link, LinkSender>,
}

impl<S: LinkSink> Links<S> {
    fn new(
        sink: S,
        ids: ([u8; 16], [u8; 16]),
        status: SharedRelayStatus,
        done: mpsc::UnboundedSender<(Link, String, u64)>,
    ) -> Self {
        Self {
            sink,
            ids,
            status,
            done,
            senders: HashMap::new(),
        }
    }

    fn sender(&mut self, link: Link) -> &LinkSender {
        self.senders.entry(link).or_insert_with(|| {
            use std::sync::atomic::AtomicUsize;
            let (tx, rx) = mpsc::unbounded_channel();
            let queued = Arc::new(AtomicUsize::new(0));
            let controls = Arc::new(AtomicUsize::new(0));
            let resources = Arc::new(AtomicUsize::new(0));
            let task = LinkTask {
                sink: self.sink.clone(),
                link,
                ids: self.ids,
                status: self.status.clone(),
                done: self.done.clone(),
                resources: resources.clone(),
            };
            let task = tokio::spawn(link_sender(task, rx, queued.clone(), controls.clone()));
            LinkSender {
                tx,
                queued,
                controls,
                resources,
                task,
            }
        })
    }

    /// Queue an envelope for a link, or drop it (counted) when
    /// `LINK_QUEUE` wait already. Never waits. False if it was dropped.
    fn envelope(&mut self, link: Link, message: ToLink) -> bool {
        use std::sync::atomic::Ordering;
        let status = self.status.clone();
        let sender = self.sender(link);
        let resource = matches!(message, ToLink::Resource(..));
        let count = |s: &mut RelayStatus| {
            if resource {
                s.client_resource_drops += 1;
            } else {
                s.relay_drops += 1;
            }
        };
        if sender.queued.load(Ordering::Relaxed) >= LINK_QUEUE {
            if let Ok(mut s) = status.lock() {
                count(&mut s);
            }
            tracing::debug!("link {}: {LINK_QUEUE} envelopes wait: one dropped", hex::encode(link));
            return false;
        }
        sender.queued.fetch_add(1, Ordering::Relaxed);
        if resource {
            sender.resources.fetch_add(1, Ordering::Relaxed);
        }
        if sender.tx.send(message).is_err() {
            sender.queued.fetch_sub(1, Ordering::Relaxed);
            if resource {
                sender.resources.fetch_sub(1, Ordering::Relaxed);
            }
            if let Ok(mut s) = status.lock() {
                count(&mut s);
            }
            return false;
        }
        true
    }

    /// Whether a link's sender has `CONTROL_QUEUE` replay commands
    /// waiting.
    fn controls_full(&mut self, link: Link) -> bool {
        self.sender(link).controls.load(std::sync::atomic::Ordering::Relaxed) >= CONTROL_QUEUE
    }

    /// Hand a link's sender a replay command. An end of a replay is never
    /// refused (there is at most one for each replay the relay asked for);
    /// see [`Self::controls_full`] for a new replay.
    fn control(&mut self, link: Link, message: ToLink) {
        let sender = self.sender(link);
        sender.controls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if sender.tx.send(message).is_err() {
            sender.controls.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Stop a link's sender. The Resources it had not ended (waiting, held
    /// or being sent) are counted dropped; a transfer under way ends with
    /// the link.
    fn close(&mut self, link: &Link) {
        if let Some(sender) = self.senders.remove(link) {
            sender.task.abort();
            if let Ok(mut s) = self.status.lock() {
                let left = sender.resources.swap(CLOSED, std::sync::atomic::Ordering::Relaxed);
                if left != CLOSED {
                    s.client_resource_drops += left as u64;
                }
            }
        }
    }
}

impl<S: LinkSink> Drop for Links<S> {
    fn drop(&mut self) {
        for sender in self.senders.values() {
            sender.task.abort();
        }
    }
}

/// Hand the relay's outputs on: envelopes and replays to the links'
/// sender tasks. Never waits; the links to close now are returned. A
/// replay goes only if the envelope before it for the link (its JOINED)
/// was queued, and while fewer than `CONTROL_QUEUE` replay commands wait
/// for the link; otherwise the relay is told it is over, and in the
/// second case the client is told it was skipped.
fn dispatch<S: LinkSink>(
    outputs: Vec<Output>,
    relay: &Relay,
    links: &mut Links<S>,
    close_later: impl Fn(Link),
) -> Vec<Link> {
    let mut close = Vec::new();
    // Links whose last envelope in this batch was dropped.
    let mut dropped: std::collections::HashSet<Link> = std::collections::HashSet::new();
    let queued = |dropped: &mut std::collections::HashSet<Link>, link: Link, ok: bool| {
        if ok {
            dropped.remove(&link);
        } else {
            dropped.insert(link);
        }
    };
    for output in outputs {
        match output {
            Output::Client(link, env) => {
                let ok = links.envelope(link, ToLink::Send(env));
                queued(&mut dropped, link, ok);
            }
            Output::Now(link, env) => {
                let ok = links.envelope(link, ToLink::Now(env));
                queued(&mut dropped, link, ok);
            }
            Output::Resource(link, env, data) => {
                let ok = links.envelope(link, ToLink::Resource(env, data));
                queued(&mut dropped, link, ok);
            }
            Output::Close(link) => {
                links.close(&link);
                close.push(link);
            }
            Output::CloseLater(link) => close_later(link),
            // Asked now, before the next event is recorded.
            Output::Replay(replay) => {
                let link = replay.link;
                let room = replay.query.room.clone();
                let refused = if dropped.contains(&link) {
                    Some("the envelope before it was dropped")
                } else if links.controls_full(link) {
                    Some("too many replay commands wait")
                } else {
                    None
                };
                match refused {
                    None => {
                        let records = relay.history.query(replay.query);
                        links.control(
                            link,
                            ToLink::Replay {
                                id: replay.id,
                                room,
                                say_empty: replay.say_empty,
                                records,
                            },
                        );
                    }
                    Some(why) => {
                        tracing::info!("link {}: replay of {room} skipped: {why}", hex::encode(link));
                        let _ = links.done.send((link, room.clone(), replay.id));
                        if !dropped.contains(&link) {
                            let mut env = relay.own(MessageType::Notice).with_body(Raw::text(&format!(
                                "--- replay of {room} skipped: the link is busy ---"
                            )));
                            env.room = Some(room);
                            links.envelope(link, ToLink::Now(env));
                        }
                    }
                }
            }
            Output::EndReplay { link, room, flush } => links.control(link, ToLink::End { room, flush }),
        }
    }
    close
}

/// A packet from a client link.
fn link_packet(relay: &mut Relay, identities: &LinkIdentities, tag: &str, link: Link, data: &[u8]) -> Vec<Output> {
    let now = Instant::now();
    let mut out = Vec::new();
    if let Some(identity) = link_identity(identities, &link) {
        out = relay.link_identified(link, identity, now);
    }
    if relay.is_allowed(&link) {
        out.extend(relay.client_packet(link, data, now));
    } else {
        tracing::debug!("hub {tag}: ignored a packet on a link not identified as an allowed identity");
    }
    out
}

/// The local hub's task: client links in, upstream events in, envelopes to
/// clients out. The select is not biased, so a busy source (a chatty
/// upstream room, a flood of client packets) cannot starve the others.
pub async fn run(hub: LocalHub) {
    let LocalHub {
        mut registered,
        identities,
        mut identified,
        events: mut upstream,
        mut relay,
        changed,
        slots,
    } = hub;
    let handle = registered.handle.clone();
    let tag = hex::encode(handle.destination_hash());
    let events = &mut registered.events;
    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let (replayed, mut replays_done) = mpsc::unbounded_channel::<(Link, String, u64)>();
    let waiters = ResourceWaiters::default();
    let mut links = Links::new(
        HubSink {
            handle: handle.clone(),
            waiters: waiters.clone(),
        },
        (relay.hub, relay.upstream),
        relay.status.clone(),
        replayed,
    );
    loop {
        let now = Instant::now();
        let mut closed = None;
        let outputs = tokio::select! {
            Some(link) = events.links_established.recv() => {
                tracing::info!("hub {tag}: link {} established", hex::encode(link));
                relay.link_established(link, now);
                Vec::new()
            }
            Some((link, identity)) = events.links_identified.recv() => {
                tracing::info!(
                    "hub {tag}: link {} identified as {}{}",
                    hex::encode(link),
                    hex::encode(identity),
                    if relay.allowed.contains(&identity) { "" } else { " (not allowed)" }
                );
                Vec::new()
            }
            Some((link, identity)) = identified.recv() => {
                // A link that closed before this was read is gone from the
                // map: it is not brought back.
                if link_identity(&identities, &link).is_none() {
                    Vec::new()
                } else {
                    let out = relay.link_identified(link, identity, now);
                    if !out.is_empty() {
                        tracing::warn!(
                            "hub {tag}: refused link {}: identity {} is not allowed",
                            hex::encode(link),
                            hex::encode(identity)
                        );
                    }
                    out
                }
            }
            Some((data, link)) = events.link_packets.recv() => link_packet(&mut relay, &identities, &tag, link, &data),
            Some(event) = upstream.recv() => relay.upstream(event, now),
            Some(link) = events.links_closed.recv() => {
                // rsReticulum queues a link's last packets before its close
                // event: they are handled first.
                let mut out = Vec::new();
                while let Ok(link) = events.links_established.try_recv() {
                    relay.link_established(link, now);
                }
                while let Ok((data, from)) = events.link_packets.try_recv() {
                    out.extend(link_packet(&mut relay, &identities, &tag, from, &data));
                }
                if let Ok(mut map) = identities.lock() {
                    map.remove(&link);
                }
                if let Ok(mut slots) = slots.lock() {
                    slots.retain(|(l, _), _| *l != link);
                }
                relay.link_closed(link);
                closed = Some(link);
                tracing::info!("hub {tag}: link {} closed", hex::encode(link));
                out
            }
            _ = ticker.tick() => relay.tick_busy(now, &busy_sizes(&slots)),
            Some((link, room, id)) = replays_done.recv() => {
                relay.replay_done(link, &room, id);
                Vec::new()
            }
            Some(_) = events.packets.recv() => Vec::new(),
            Some(_) = events.link_packet_proofs.recv() => Vec::new(),
            Some(done) = events.resource_completions.recv() => {
                release_slot(&slots, done.link_id, done.resource_hash);
                // The RESOURCE_ENVELOPE that announced it came first.
                let mut out = Vec::new();
                while let Ok((data, from)) = events.link_packets.try_recv() {
                    out.extend(link_packet(&mut relay, &identities, &tag, from, &data));
                }
                if relay.is_allowed(&done.link_id) {
                    out.extend(relay.client_resource(done.link_id, done.data, now));
                }
                out
            }
            Some(proof) = events.resource_proofs.recv() => {
                waiters.concluded(proof.link_id, proof.resource_hash, true);
                Vec::new()
            }
            Some(event) = events.resource_events.recv() => {
                if let LinkResourceEvent::Concluded { link_id, resource_id, direction, conclusion } = event {
                    let complete = conclusion == LinkResourceConclusion::Complete;
                    match direction {
                        LinkResourceDirection::Outbound => waiters.concluded(link_id, resource_id, complete),
                        LinkResourceDirection::Inbound => {
                            release_slot(&slots, link_id, resource_id);
                            if !complete {
                                relay.client_resource_failed(link_id, &format!("{conclusion:?}"));
                            }
                        }
                    }
                }
                Vec::new()
            }
            Some(_) = events.channel_messages.recv() => Vec::new(),
        };
        let close_later = |link: Link| {
            let handle = handle.clone();
            tokio::spawn(async move {
                tokio::time::sleep(REFUSE_CLOSE_DELAY).await;
                let _ = handle.close_link(link, CloseReason::DestinationClosed, true).await;
            });
        };
        for link in dispatch(outputs, &relay, &mut links, close_later) {
            tracing::info!(
                "hub {tag}: closing link {}: not identified in {}s",
                hex::encode(link),
                IDENTIFY_TIMEOUT.as_secs()
            );
            let _ = handle.close_link(link, CloseReason::DestinationClosed, true).await;
        }
        // A closed link's sender stops, with what was left for it.
        if let Some(link) = closed {
            links.close(&link);
            waiters.forget(link);
        }
        if relay.take_changed() {
            changed.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HUB: [u8; 16] = [0x11; 16];
    const USER: [u8; 16] = [0x22; 16];
    const UPSTREAM: [u8; 16] = [0x33; 16];
    const UPSTREAM_HUB: [u8; 16] = [0x44; 16];
    const FRIEND: [u8; 16] = [0x55; 16];
    const STRANGER: [u8; 16] = [0x66; 16];
    const OTHER: [u8; 16] = [0x77; 16];
    const A: Link = [1; 16];
    const B: Link = [2; 16];
    const C: Link = [3; 16];

    struct Fixture {
        relay: Relay,
        commands: mpsc::Receiver<Command>,
        status: SharedRelayStatus,
        session: SharedStatus,
        now: Instant,
    }

    fn fixture_with(part_upstream: bool, queue: usize) -> Fixture {
        let (tx, rx) = mpsc::channel(queue);
        let status = SharedRelayStatus::default();
        let session = SharedStatus::default();
        let relay = Relay::new(RelayConfig {
            hub_identity: HUB,
            user: USER,
            upstream: UPSTREAM,
            allowed: vec![USER, FRIEND],
            part_upstream,
            nick: None,
            commands: tx,
            session: session.clone(),
            status: status.clone(),
            history: History::disabled(),
            // The M3 tests run without replays; `replays` turns them on.
            replay_window: Duration::from_secs(3600),
            replay_max: 0,
        });
        Fixture {
            relay,
            commands: rx,
            status,
            session,
            now: Instant::now(),
        }
    }

    fn fixture() -> Fixture {
        fixture_with(false, 64)
    }

    fn upstream_welcome() -> Welcome {
        Welcome {
            hub: Some("test hub".into()),
            ver: Some(Value::Text("1.2".into())),
            caps: Caps {
                resource_envelope: true,
                action: true,
                direct_notice: false,
            },
            limits: Limits {
                max_rooms_per_session: 3,
                max_room_name_bytes: 8,
                ..Limits::default()
            },
        }
    }

    fn members(list: &[&[u8; 16]]) -> Raw {
        Relay::members_body(&list.iter().map(|m| m.to_vec()).collect::<Vec<_>>())
    }

    fn from_hub(t: MessageType) -> Envelope {
        Envelope::new(t, &UPSTREAM_HUB)
    }

    fn client(t: MessageType) -> Envelope {
        Envelope::new(t, &USER)
    }

    fn sent_to(out: &[Output]) -> Vec<(Link, Envelope)> {
        out.iter()
            .filter_map(|o| match o {
                Output::Client(l, e) => Some((*l, e.clone())),
                _ => None,
            })
            .collect()
    }

    fn links_of(sent: &[(Link, Envelope)]) -> Vec<Link> {
        sent.iter().map(|(l, _)| *l).collect()
    }

    fn links(out: &[Output]) -> Vec<Link> {
        sent_to(out).into_iter().map(|(l, _)| l).collect()
    }

    impl Fixture {
        fn connect(&mut self, link: Link, identity: [u8; 16]) -> Vec<Output> {
            self.relay.link_established(link, self.now);
            self.relay.link_identified(link, identity, self.now)
        }

        fn packet(&mut self, link: Link, env: &Envelope) -> Vec<Output> {
            self.relay.client_packet(link, &env.encode(), self.now)
        }

        fn hello(&mut self, link: Link) -> Envelope {
            let out = self.packet(link, &client(MessageType::Hello));
            let sent = sent_to(&out);
            assert_eq!(sent.len(), 1);
            assert_eq!(sent[0].1.t, MessageType::Welcome);
            sent[0].1.clone()
        }

        fn up(&mut self) {
            let out = self.relay.upstream(
                UpstreamEvent::Up {
                    welcome: upstream_welcome(),
                    hub: UPSTREAM_HUB.to_vec(),
                },
                self.now,
            );
            assert!(out.is_empty());
        }

        fn hub(&mut self, env: Envelope, inbound: Inbound) -> Vec<Output> {
            self.relay.upstream(UpstreamEvent::Envelope(env, inbound), self.now)
        }

        /// The bouncer joins `room` upstream, with `list` as the members.
        fn joined_upstream(&mut self, room: &str, list: &[&[u8; 16]]) -> Vec<Output> {
            let env = from_hub(MessageType::Joined).with_room(room).with_body(members(list));
            if let Ok(mut s) = self.session.lock() {
                s.rooms.push(room.into());
            }
            self.hub(env, Inbound::JoinConfirmed(room.into()))
        }

        /// A welcomed client of the user in `rooms`, which are joined.
        fn in_rooms(&mut self, link: Link, rooms: &[&str]) {
            self.connect(link, USER);
            self.hello(link);
            for room in rooms {
                let out = self.packet(link, &client(MessageType::Join).with_room(room));
                assert_eq!(sent_to(&out)[0].1.t, MessageType::Joined, "{room}");
            }
        }

        fn packet_after_hello(&mut self, link: Link, env: Envelope) -> Vec<(Link, Envelope)> {
            self.connect(link, USER);
            self.hello(link);
            sent_to(&self.packet(link, &env))
        }

        fn command(&mut self) -> Option<Command> {
            self.commands.try_recv().ok()
        }
    }

    #[test]
    fn a_link_of_another_identity_gets_an_error_and_is_closed() {
        let mut f = fixture();
        let out = f.connect(A, STRANGER);
        let sent = sent_to(&out);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].1.t, MessageType::Error);
        assert_eq!(sent[0].1.src, HUB.to_vec());
        assert_eq!(sent[0].1.text_body().as_deref(), Some(NOT_ALLOWED));
        assert!(out.contains(&Output::CloseLater(A)));
        // Identified again (the packet path): nothing more.
        assert!(f.relay.link_identified(A, STRANGER, f.now).is_empty());
        assert!(f.packet(A, &client(MessageType::Ping)).is_empty());
        assert!(!f.relay.is_allowed(&A));
        let status = f.status.lock().unwrap().clone();
        assert_eq!(status.refused_links, 1);
        assert_eq!(status.clients, 0);
    }

    #[test]
    fn allowed_identities_are_clients() {
        let mut f = fixture();
        assert!(f.connect(A, USER).is_empty());
        assert!(f.connect(B, FRIEND).is_empty());
        assert!(f.relay.take_changed());
        let status = f.status.lock().unwrap().clone();
        assert_eq!(status.clients, 2);
        assert_eq!(status.client_identities, vec![hex::encode(USER), hex::encode(FRIEND)]);
        f.relay.link_closed(A);
        assert_eq!(f.status.lock().unwrap().clients, 1);
    }

    #[test]
    fn an_unidentified_link_is_ignored_and_closed_after_the_timeout() {
        let mut f = fixture();
        f.relay.link_established(A, f.now);
        assert!(f.packet(A, &client(MessageType::Ping)).is_empty());
        assert!(f.relay.tick(f.now + IDENTIFY_TIMEOUT / 2).is_empty());
        assert_eq!(f.relay.tick(f.now + IDENTIFY_TIMEOUT), vec![Output::Close(A)]);
        // An identified link is not closed.
        f.connect(B, USER);
        assert!(f.relay.tick(f.now + IDENTIFY_TIMEOUT * 2).is_empty());
    }

    #[test]
    fn ping_is_answered_before_hello_and_other_envelopes_are_not() {
        let mut f = fixture();
        f.connect(A, USER);
        let ping = client(MessageType::Ping).with_body(Raw::text("x"));
        let sent = sent_to(&f.packet(A, &ping));
        assert_eq!(sent[0].1.t, MessageType::Pong);
        assert_eq!(sent[0].1.src, HUB.to_vec());
        assert_eq!(sent[0].1.text_body().as_deref(), Some("x"));
        let sent = sent_to(&f.packet(A, &client(MessageType::Join).with_room("lobby")));
        assert_eq!(sent[0].1.t, MessageType::Error);
        assert_eq!(sent[0].1.text_body().as_deref(), Some("send HELLO first"));
        assert!(f.command().is_none());
    }

    #[test]
    fn welcome_before_and_after_the_upstream_welcome() {
        let mut f = fixture();
        f.connect(A, USER);
        let welcome = f.hello(A);
        assert_eq!(welcome.src, HUB.to_vec());
        let body = Welcome::from_body(welcome.body.as_ref().unwrap());
        assert_eq!(body.hub, Some(local_hub_name(&UPSTREAM)));
        assert_eq!(body.limits, Limits::default());
        assert!(body.caps.resource_envelope && body.caps.action && body.caps.direct_notice);

        f.up();
        let body = Welcome::from_body(f.hello(A).body.as_ref().unwrap());
        assert_eq!(body.hub.as_deref(), Some("test hub (via rrc-bouncer)"));
        assert_eq!(body.ver, Some(Value::Text("1.2".into())));
        assert_eq!(body.limits, upstream_welcome().limits);
        assert!(body.caps.resource_envelope && body.caps.action && !body.caps.direct_notice);

        // A hub that takes no Resources: the clients are told so.
        let mut welcome = upstream_welcome();
        welcome.caps.resource_envelope = false;
        f.relay.upstream(
            UpstreamEvent::Up {
                welcome,
                hub: UPSTREAM_HUB.to_vec(),
            },
            f.now,
        );
        let body = Welcome::from_body(f.hello(A).body.as_ref().unwrap());
        assert!(!body.caps.resource_envelope);
    }

    #[test]
    fn join_of_a_joined_room_is_answered_locally_with_the_members() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER, &OTHER]);
        f.connect(A, USER);
        f.hello(A);
        let sent = sent_to(&f.packet(A, &client(MessageType::Join).with_room(" Lobby ")));
        assert_eq!(sent.len(), 1);
        let joined = &sent[0].1;
        assert_eq!((joined.t, joined.room.as_deref()), (MessageType::Joined, Some("lobby")));
        assert_eq!(joined.src, HUB.to_vec());
        assert_eq!(joined.members(), Some(vec![USER.to_vec(), OTHER.to_vec()]));
        assert!(f.command().is_none());
        // Others joining and leaving upstream update the list.
        let other = from_hub(MessageType::Parted)
            .with_room("lobby")
            .with_body(members(&[&OTHER]));
        assert_eq!(links(&f.hub(other, Inbound::Relay)), vec![A]);
        f.connect(B, USER);
        f.hello(B);
        let sent = sent_to(&f.packet(B, &client(MessageType::Join).with_room("lobby")));
        assert_eq!(sent[0].1.members(), Some(vec![USER.to_vec()]));
    }

    #[test]
    fn join_of_a_new_room_goes_upstream_and_is_answered_when_joined() {
        let mut f = fixture();
        f.up();
        f.in_rooms(A, &[]);
        assert!(f.packet(A, &client(MessageType::Join).with_room("fresh")).is_empty());
        assert_eq!(f.command(), Some(Command::Join("fresh".into())));
        let out = f.joined_upstream("fresh", &[&USER]);
        let sent = sent_to(&out);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, A);
        assert_eq!(sent[0].1.t, MessageType::Joined);
        assert_eq!(sent[0].1.src, HUB.to_vec(), "the hub's K_SRC is the local hub's");
        // The room's NOTICE reaches the client, from the local hub.
        let notice = from_hub(MessageType::Notice)
            .with_room("fresh")
            .with_body(Raw::text("room fresh: unregistered"));
        let sent = sent_to(&f.hub(notice, Inbound::Relay));
        assert_eq!(sent[0].0, A);
        assert_eq!(sent[0].1.src, HUB.to_vec());
    }

    #[test]
    fn join_errors() {
        let mut f = fixture();
        f.up();
        f.in_rooms(A, &[]);
        let error = |f: &mut Fixture, env: Envelope| {
            let sent = sent_to(&f.packet(A, &env));
            assert_eq!(sent[0].1.t, MessageType::Error);
            sent[0].1.text_body().unwrap()
        };
        assert_eq!(error(&mut f, client(MessageType::Join)), "JOIN requires room name");
        assert_eq!(
            error(&mut f, client(MessageType::Join).with_room("")),
            "JOIN requires room name"
        );
        assert_eq!(
            error(&mut f, client(MessageType::Join).with_room("  ")),
            "room name must not be empty"
        );
        assert_eq!(
            error(&mut f, client(MessageType::Join).with_room("ninechars")),
            "room name too long: 9 bytes > 8 bytes"
        );
        for room in ["a", "b", "c"] {
            f.joined_upstream(room, &[&USER]);
        }
        let refusal = sent_to(&f.packet(A, &client(MessageType::Join).with_room("D")));
        assert_eq!(refusal[0].1.text_body().as_deref(), Some("too many rooms"));
        assert_eq!(refusal[0].1.room.as_deref(), Some("d"));
        // rrcd checks the room limit before the name.
        assert_eq!(
            error(&mut f, client(MessageType::Join).with_room("ninechars")),
            "too many rooms"
        );
        assert_eq!(error(&mut f, client(MessageType::Part)), "PART requires room name");
        assert_eq!(
            error(&mut f, client(MessageType::Part).with_room("")),
            "PART requires room name"
        );
        assert!(f.command().is_none());
    }

    #[test]
    fn a_refused_join_reaches_the_client_and_its_room_leaves_the_room_set() {
        let mut f = fixture();
        f.up();
        f.in_rooms(A, &[]);
        f.packet(A, &client(MessageType::Join).with_room("secret"));
        assert_eq!(f.command(), Some(Command::Join("secret".into())));
        let refusal = from_hub(MessageType::Error)
            .with_room("secret")
            .with_body(Raw::text("invite-only (+i)"));
        let sent = sent_to(&f.hub(
            refusal,
            Inbound::JoinFailed {
                room: "secret".into(),
                reason: "invite-only (+i)".into(),
            },
        ));
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, A);
        assert_eq!(sent[0].1.src, HUB.to_vec());
        assert_eq!(f.command(), Some(Command::Part("secret".into())));
    }

    #[test]
    fn part_keeps_the_bouncer_in_the_room_by_default() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.in_rooms(A, &["lobby"]);
        f.in_rooms(B, &["lobby"]);
        let sent = sent_to(&f.packet(A, &client(MessageType::Part).with_room("lobby")));
        assert_eq!(sent.len(), 1);
        assert_eq!((sent[0].0, sent[0].1.t), (A, MessageType::Parted));
        assert_eq!(sent[0].1.members(), Some(vec![USER.to_vec()]));
        assert!(f.command().is_none());
        // A no longer gets the room's envelopes; B does.
        let msg = from_hub(MessageType::Msg).with_room("lobby");
        assert_eq!(links(&f.hub(msg, Inbound::Relay)), vec![B]);
    }

    #[test]
    fn part_upstream_parts_every_client() {
        let mut f = fixture_with(true, 64);
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.in_rooms(A, &["lobby"]);
        f.in_rooms(B, &["lobby"]);
        let sent = sent_to(&f.packet(A, &client(MessageType::Part).with_room("lobby")));
        assert_eq!(
            sent.iter().map(|(l, e)| (*l, e.t)).collect::<Vec<_>>(),
            vec![(A, MessageType::Parted), (B, MessageType::Parted)]
        );
        assert_eq!(f.command(), Some(Command::Part("lobby".into())));
        // A PART from a client not in the room: PARTED to it alone.
        let sent = sent_to(&f.packet(B, &client(MessageType::Part).with_room("lobby")));
        assert_eq!(links_of(&sent), vec![B]);
        assert!(f.command().is_none());
        // The relay has left the room: a JOIN goes upstream again.
        assert!(f.packet(A, &client(MessageType::Join).with_room("lobby")).is_empty());
        assert_eq!(f.command(), Some(Command::Join("lobby".into())));
    }

    #[test]
    fn room_messages_are_shown_at_their_echo_once_per_client() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.in_rooms(A, &["lobby"]);
        f.in_rooms(B, &["lobby"]);
        f.connect(C, USER);
        f.hello(C); // not in the room
        let mut msg = client(MessageType::Msg).with_room("lobby").with_body(Raw::text("hi"));
        msg.nick = Some("me".into());
        msg.extra.push((64, Raw::text("fmt")));
        // Nothing to any client when the envelope goes upstream.
        assert!(f.packet(A, &msg).is_empty());
        assert_eq!(
            f.command(),
            Some(Command::Send(Envelope::decode(&msg.encode()).unwrap()))
        );
        // The hub's echo goes to every client in the room, the sender included.
        let echo = msg.clone();
        let sent = sent_to(&f.hub(echo, Inbound::Relay));
        assert_eq!(sent.iter().map(|(l, _)| *l).collect::<Vec<_>>(), vec![A, B]);
        for (_, env) in &sent {
            assert_eq!(env.id, msg.id);
            assert_eq!(env.src, USER.to_vec());
            assert_eq!(env.extra(64).map(Raw::value), Some(Value::Text("fmt".into())));
        }
        let status = f.status.lock().unwrap().clone();
        // relayed_out is counted by the links' sender tasks, as they send.
        assert_eq!((status.relayed_in, status.relayed_out), (1, 0));
        // Another member's ACTION too, with its K_SRC.
        let action = Envelope::new(MessageType::Action, &OTHER).with_room("lobby");
        let sent = sent_to(&f.hub(action, Inbound::Relay));
        assert_eq!(sent.len(), 2);
        assert!(sent.iter().all(|(_, e)| e.src == OTHER.to_vec()));
    }

    #[test]
    fn a_direct_notice_is_copied_to_every_client_unless_it_is_to_the_user() {
        let mut f = fixture();
        f.up();
        f.in_rooms(A, &[]);
        f.in_rooms(B, &[]);
        let mut notice = client(MessageType::Notice).with_body(Raw::text("psst"));
        notice.dst = Some(OTHER.to_vec());
        let sent = sent_to(&f.packet(A, &notice));
        assert_eq!(sent.iter().map(|(l, _)| *l).collect::<Vec<_>>(), vec![B]);
        assert!(sent.iter().all(|(_, e)| e.id == notice.id && e.src == USER.to_vec()));
        assert!(matches!(f.command(), Some(Command::Send(_))));
        // To the user: the hub delivers it, once, to every client.
        let mut to_user = client(MessageType::Notice).with_body(Raw::text("self"));
        to_user.dst = Some(USER.to_vec());
        assert!(f.packet(A, &to_user).is_empty());
        assert_eq!(links(&f.hub(to_user, Inbound::Relay)), vec![A, B]);
        // From someone else to the user: every client.
        let mut incoming = Envelope::new(MessageType::Notice, &OTHER).with_body(Raw::text("hey"));
        incoming.dst = Some(USER.to_vec());
        let sent = sent_to(&f.hub(incoming, Inbound::Relay));
        assert_eq!(sent.len(), 2);
        assert!(sent.iter().all(|(_, e)| e.src == OTHER.to_vec()));
        // Not connected: no copy; the session drops it and says so.
        f.relay.upstream(UpstreamEvent::Down, f.now);
        assert!(f.packet(B, &notice).is_empty());
    }

    #[test]
    fn a_dropped_send_is_reported_to_its_sender() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.in_rooms(A, &["lobby"]);
        f.in_rooms(B, &["lobby"]);
        let msg = client(MessageType::Msg).with_room("lobby");
        f.packet(B, &msg);
        let out = f.relay.upstream(
            UpstreamEvent::SendDropped {
                envelope: msg,
                reason: "not connected".into(),
            },
            f.now,
        );
        let sent = sent_to(&out);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, B);
        assert_eq!(
            sent[0].1.text_body().as_deref(),
            Some("not sent to the hub: not connected")
        );
        assert_eq!(sent[0].1.room.as_deref(), Some("lobby"));
    }

    #[test]
    fn a_full_queue_is_an_error_to_the_sender() {
        let mut f = fixture_with(false, 1);
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.in_rooms(A, &["lobby"]);
        let msg = client(MessageType::Msg).with_room("lobby");
        assert!(f.packet(A, &msg).is_empty());
        let sent = sent_to(&f.packet(A, &msg));
        assert_eq!(sent[0].1.t, MessageType::Error);
        assert_eq!(f.status.lock().unwrap().relay_drops, 1);
    }

    fn hub_error(room: Option<&str>, text: &str) -> Envelope {
        let mut env = from_hub(MessageType::Error).with_body(Raw::text(text));
        env.room = room.map(String::from);
        env
    }

    #[test]
    fn hub_errors_go_to_the_client_whose_envelope_they_answer() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.joined_upstream("side", &[&USER]);
        f.in_rooms(A, &["lobby", "side"]);
        f.in_rooms(B, &["lobby", "side"]);
        f.in_rooms(C, &["lobby"]);
        // A in lobby, B in side: a refusal naming side is B's.
        f.packet(A, &client(MessageType::Msg).with_room("lobby"));
        f.packet(B, &client(MessageType::Msg).with_room("side"));
        let text = "room is moderated (+m)";
        let out = f.hub(hub_error(Some("side"), text), Inbound::HubError(text.into()));
        assert_eq!(links(&out), vec![B]);
        assert_eq!(sent_to(&out)[0].1.src, HUB.to_vec());
        // A direct NOTICE refusal is the direct NOTICE's sender's, not A's.
        let mut notice = client(MessageType::Notice);
        notice.dst = Some(OTHER.to_vec());
        f.packet(C, &notice);
        let text = "destination not connected";
        assert_eq!(
            links(&f.hub(hub_error(None, text), Inbound::HubError(text.into()))),
            vec![C]
        );
        // A command's.
        f.packet(
            B,
            &client(MessageType::Msg)
                .with_room("lobby")
                .with_body(Raw::text("/bogus")),
        );
        let text = "unrecognized command";
        assert_eq!(
            links(&f.hub(hub_error(Some("lobby"), text), Inbound::HubError(text.into()))),
            vec![B]
        );
        // Two room envelopes in lobby: the older one is answered first.
        f.packet(C, &client(MessageType::Msg).with_room("lobby"));
        let text = "message too large: 9 bytes > 8 bytes";
        assert_eq!(
            links(&f.hub(hub_error(None, text), Inbound::HubError(text.into()))),
            vec![A]
        );
        let text = "no such room";
        assert_eq!(
            links(&f.hub(hub_error(Some(" Lobby"), text), Inbound::HubError(text.into()))),
            vec![C],
            "the ERROR's room is normalised"
        );
        // Nothing fits: every client.
        let text = "rate limited";
        assert_eq!(
            links(&f.hub(hub_error(None, text), Inbound::HubError(text.into()))),
            vec![A, B, C]
        );
    }

    #[test]
    fn an_echo_clears_the_envelope_from_error_routing() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.in_rooms(A, &["lobby"]);
        f.in_rooms(B, &["lobby"]);
        let first = client(MessageType::Msg).with_room("lobby");
        f.packet(A, &first);
        f.packet(B, &client(MessageType::Msg).with_room("lobby"));
        f.hub(first, Inbound::Relay);
        let text = "room is moderated (+m)";
        assert_eq!(
            links(&f.hub(hub_error(Some("lobby"), text), Inbound::HubError(text.into()))),
            vec![B]
        );
        // And old sends are forgotten.
        f.packet(A, &client(MessageType::Msg).with_room("lobby"));
        f.relay.tick(f.now + ECHO_WAIT);
        assert_eq!(
            links(&f.hub(hub_error(Some("lobby"), text), Inbound::HubError(text.into()))),
            vec![A, B]
        );
    }

    #[test]
    fn command_replies_go_to_the_client_that_gave_the_command() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.in_rooms(A, &["lobby"]);
        f.in_rooms(B, &["lobby"]);
        let command = |body: &str| client(MessageType::Msg).with_room("lobby").with_body(Raw::text(body));
        f.packet(A, &command("/list"));
        f.packet(B, &command(" /who lobby"));
        let reply = || from_hub(MessageType::Notice).with_body(Raw::text("members: x"));
        // One reply per command, in the order they went upstream.
        let sent = sent_to(&f.hub(reply(), Inbound::Relay));
        assert_eq!(sent.len(), 1);
        assert_eq!((sent[0].0, sent[0].1.src.clone()), (A, HUB.to_vec()));
        assert_eq!(links(&f.hub(reply(), Inbound::Relay)), vec![B]);
        // No command waiting: every client.
        assert_eq!(links(&f.hub(reply(), Inbound::Relay)), vec![A, B]);
        // A command unanswered in ANSWER_WAIT is forgotten.
        f.packet(A, &command("/motd"));
        f.relay.tick(f.now + ANSWER_WAIT);
        assert_eq!(links(&f.hub(reply(), Inbound::Relay)), vec![A, B]);
        // A command's ERROR takes it off the list.
        f.packet(A, &command("/bogus"));
        let text = "unrecognized command";
        assert_eq!(
            links(&f.hub(hub_error(Some("lobby"), text), Inbound::HubError(text.into()))),
            vec![A]
        );
        assert_eq!(links(&f.hub(reply(), Inbound::Relay)), vec![A, B]);
    }

    #[test]
    fn removal_from_a_room_reaches_its_clients() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.in_rooms(A, &["lobby"]);
        f.in_rooms(B, &[]);
        let kick = hub_error(Some("lobby"), "kicked from lobby");
        let out = f.hub(
            kick,
            Inbound::Removed {
                room: "lobby".into(),
                reason: "kicked from lobby".into(),
            },
        );
        assert_eq!(links(&out), vec![A]);
        assert!(
            f.hub(from_hub(MessageType::Msg).with_room("lobby"), Inbound::Relay)
                .is_empty()
        );
    }

    /// A HELLO with the `resource_envelope` cap.
    fn hello_with_resources() -> Envelope {
        let hello = Hello {
            name: Some("test".into()),
            ver: None,
            caps: Caps {
                resource_envelope: true,
                action: false,
                direct_notice: false,
            },
            legacy_nick: None,
        };
        client(MessageType::Hello).with_body(hello.to_body())
    }

    /// A long text: more than a packet, in lines.
    fn long_text(lines: usize) -> String {
        (0..lines)
            .map(|i| format!("line {i}: {}", "x".repeat(60)))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn resources_to(out: &[Output]) -> Vec<(Link, Envelope, Arc<Vec<u8>>)> {
        out.iter()
            .filter_map(|o| match o {
                Output::Resource(l, e, d) => Some((*l, e.clone(), d.clone())),
                _ => None,
            })
            .collect()
    }

    impl Fixture {
        /// A welcomed client of the user with the `resource_envelope` cap,
        /// in `rooms`.
        fn with_resources(&mut self, link: Link, rooms: &[&str]) {
            self.connect(link, USER);
            let out = self.packet(link, &hello_with_resources());
            assert_eq!(sent_to(&out)[0].1.t, MessageType::Welcome);
            for room in rooms {
                self.packet(link, &client(MessageType::Join).with_room(room));
            }
        }

        fn hub_resource(&mut self, kind: &str, room: Option<&str>, text: &str) -> Vec<Output> {
            let envelope = resource::announce(&UPSTREAM_HUB, kind, room, text.as_bytes());
            self.relay.upstream(
                UpstreamEvent::Resource {
                    envelope,
                    data: Arc::new(text.as_bytes().to_vec()),
                },
                self.now,
            )
        }

        fn drops(&self) -> u64 {
            self.status.lock().unwrap().client_resource_drops
        }
    }

    #[test]
    fn a_hub_resource_goes_whole_to_clients_with_the_cap_and_as_notices_to_the_others() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.with_resources(A, &["lobby"]);
        f.in_rooms(B, &["lobby"]);
        f.in_rooms(C, &[]);
        let text = long_text(20);
        let out = f.hub_resource(resource::KIND_NOTICE, Some("lobby"), &text);
        let whole = resources_to(&out);
        assert_eq!(whole.len(), 1);
        let (link, envelope, data) = &whole[0];
        assert_eq!(*link, A);
        assert_eq!(envelope.t, MessageType::ResourceEnvelope);
        assert_eq!(envelope.src, HUB.to_vec(), "K_SRC is the local hub's");
        assert_eq!(envelope.room.as_deref(), Some("lobby"));
        let body = ResourceEnvelope::from_body(envelope.body.as_ref().unwrap()).unwrap();
        assert_eq!(body.size, text.len() as u64);
        assert_eq!(body.sha256, Some(resource::sha256(text.as_bytes())));
        assert_eq!(data.as_slice(), text.as_bytes());

        let notices = sent_to(&out);
        assert!(notices.len() > 1);
        assert!(notices.iter().all(|(l, e)| *l == B && e.t == MessageType::Notice));
        assert!(
            notices
                .iter()
                .all(|(_, e)| e.encode().len() <= LINK_MDU && e.src == HUB.to_vec())
        );
        let joined: Vec<String> = notices.iter().filter_map(|(_, e)| e.text_body()).collect();
        assert_eq!(joined.join("\n"), text);
        let status = f.status.lock().unwrap().clone();
        assert_eq!((status.client_resources_split, status.client_resource_drops), (1, 0));
    }

    #[test]
    fn a_hub_resource_from_another_user_keeps_its_src_and_blobs_are_dropped() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER, &OTHER]);
        f.with_resources(A, &["lobby"]);
        let text = long_text(10);
        let envelope = resource::announce(&OTHER, resource::KIND_NOTICE, Some("lobby"), text.as_bytes());
        let out = f.relay.upstream(
            UpstreamEvent::Resource {
                envelope,
                data: Arc::new(text.into_bytes()),
            },
            f.now,
        );
        assert_eq!(resources_to(&out)[0].1.src, OTHER.to_vec());
        assert!(f.hub_resource("blob", Some("lobby"), "data").is_empty());
        assert_eq!(f.drops(), 1);
    }

    #[test]
    fn a_greeting_resource_is_kept_for_clients_that_link_later() {
        let mut f = fixture();
        f.up();
        let motd = long_text(12);
        assert!(f.hub_resource(resource::KIND_MOTD, None, &motd).is_empty());
        f.connect(A, USER);
        let out = f.packet(A, &hello_with_resources());
        assert_eq!(sent_to(&out)[0].1.t, MessageType::Welcome);
        let whole = resources_to(&out);
        assert_eq!(whole.len(), 1);
        assert_eq!(whole[0].2.as_slice(), motd.as_bytes());
        assert_eq!(whole[0].1.src, HUB.to_vec());
        // A client without the cap gets it as NOTICEs, after the WELCOME.
        f.connect(B, USER);
        let sent = sent_to(&f.packet(B, &client(MessageType::Hello)));
        assert_eq!(sent[0].1.t, MessageType::Welcome);
        let text: Vec<String> = sent[1..].iter().filter_map(|(_, e)| e.text_body()).collect();
        assert_eq!(text.join("\n"), motd);
    }

    #[tokio::test]
    async fn a_hub_room_resource_is_kept_in_history_as_notices() {
        let dir = tempfile::tempdir().unwrap();
        let history = History::start(
            dir.path().join("h"),
            crate::config::HistorySettings {
                retention: Duration::from_secs(86_400),
                max_bytes: 1_000_000,
                replay_window: Duration::from_secs(3600),
                replay_max: 100,
            },
            Default::default(),
        )
        .unwrap();
        let mut f = fixture();
        f.relay.history = history.clone();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        let text = long_text(20);
        f.hub_resource(resource::KIND_NOTICE, Some("lobby"), &text);
        let records = history
            .query(Query {
                room: "lobby".into(),
                since_ms: None,
                max: 100,
            })
            .await
            .unwrap();
        let notices: Vec<String> = records
            .iter()
            .map(|r| Envelope::decode(&r.envelope).unwrap())
            .inspect(|e| assert_eq!(e.t, MessageType::Notice))
            .filter_map(|e| e.text_body())
            .collect();
        assert!(notices.len() > 1);
        assert_eq!(notices.join("\n"), text);
    }

    impl Fixture {
        /// Every command handed to the session so far.
        fn commands_sent(&mut self) -> Vec<Command> {
            std::iter::from_fn(|| self.command()).collect()
        }

        /// A client's notice Resource: its RESOURCE_ENVELOPE, then the
        /// Resource.
        fn client_notice(&mut self, link: Link, text: &str) -> Vec<Output> {
            let env = resource::announce(&USER, resource::KIND_NOTICE, Some("lobby"), text.as_bytes());
            let mut out = self.packet(link, &env);
            out.extend(self.relay.client_resource(link, text.as_bytes().to_vec(), self.now));
            out
        }

        fn counts(&self) -> (u64, u64, u64) {
            let s = self.status.lock().unwrap();
            (s.client_resources_in, s.relayed_in, s.client_resource_drops)
        }
    }

    fn sends(commands: &[Command]) -> Vec<Envelope> {
        commands
            .iter()
            .map(|c| match c {
                Command::Send(env) => env.clone(),
                other => panic!("{other:?}"),
            })
            .collect()
    }

    #[test]
    fn a_client_resource_goes_upstream_as_ordered_notices() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.with_resources(A, &["lobby"]);
        f.in_rooms(B, &["lobby"]);
        f.connect(C, USER);
        f.hello(C); // not in the room
        let text = long_text(20);
        // Nothing to any client until the hub echoes the NOTICEs.
        assert!(f.client_notice(A, &text).is_empty());
        let parts = sends(&f.commands_sent());
        assert!(parts.len() > 1 && parts.len() <= resource::MAX_PARTS, "{}", parts.len());
        for p in &parts {
            assert_eq!(
                (p.t, p.room.as_deref(), p.dst.as_deref()),
                (MessageType::Notice, Some("lobby"), None)
            );
            assert!(p.text_body().unwrap().len() <= 350);
            assert!(p.encode().len() <= LINK_MDU);
        }
        let texts: Vec<String> = parts.iter().filter_map(Envelope::text_body).collect();
        assert_eq!(texts.join("\n"), text);
        assert_eq!(f.counts(), (1, parts.len() as u64, 0));
        // Each part waits for its echo like any room NOTICE, and the echoes
        // go to every client in the room, in order.
        assert!(
            parts
                .iter()
                .all(|p| f.relay.sent.iter().any(|s| s.id == p.id && s.kind == Kind::Room))
        );
        let mut seen: Vec<(Link, String)> = Vec::new();
        for p in &parts {
            for (link, env) in sent_to(&f.hub(p.clone(), Inbound::Relay)) {
                seen.push((link, env.text_body().unwrap()));
            }
        }
        for link in [A, B] {
            let got: Vec<String> = seen
                .iter()
                .filter(|(l, _)| *l == link)
                .map(|(_, t)| t.clone())
                .collect();
            assert_eq!(got, texts, "{link:?}");
        }
        assert!(seen.iter().all(|(l, _)| *l != C));
        assert!(f.relay.sent.is_empty());
    }

    #[test]
    fn a_client_resource_before_its_envelope_waits_for_it() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.with_resources(A, &["lobby"]);
        let text = long_text(10);
        let env = resource::announce(&USER, resource::KIND_NOTICE, Some("lobby"), text.as_bytes());
        assert!(f.relay.client_resource(A, text.clone().into_bytes(), f.now).is_empty());
        assert!(f.command().is_none());
        assert!(f.packet(A, &env).is_empty());
        let texts: Vec<String> = sends(&f.commands_sent())
            .iter()
            .filter_map(Envelope::text_body)
            .collect();
        assert_eq!(texts.join("\n"), text);
        assert_eq!(f.counts().0, 1);
        assert_eq!(f.counts().2, 0);
    }

    #[test]
    fn a_notice_of_more_than_sixteen_parts_is_refused_and_nothing_goes() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.with_resources(A, &["lobby"]);
        let text = "y".repeat(350 * resource::MAX_PARTS + 1);
        let sent = sent_to(&f.client_notice(A, &text));
        assert_eq!(sent.len(), 1);
        assert_eq!((sent[0].0, sent[0].1.t), (A, MessageType::Error));
        let (_, most) = resource::parts(
            &client(MessageType::Notice)
                .with_room("lobby")
                .with_body(Raw::text(&text)),
            350,
        );
        assert_eq!(
            sent[0].1.text_body().unwrap(),
            format!("notice too long: {} bytes > {most}", text.len())
        );
        assert!(f.command().is_none());
        assert_eq!(f.counts(), (0, 0, 1));
    }

    #[test]
    fn a_client_resource_goes_all_or_nothing_when_the_queue_is_short() {
        let mut f = fixture_with(false, 2);
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.with_resources(A, &["lobby"]);
        let sent = sent_to(&f.client_notice(A, &long_text(20)));
        assert_eq!(
            sent[0].1.text_body().as_deref(),
            Some("not sent to the hub: the bouncer's queue is full")
        );
        assert!(f.command().is_none());
        assert_eq!(f.counts(), (0, 0, 1));
        assert_eq!(f.status.lock().unwrap().relay_drops, 1);
    }

    #[test]
    fn a_client_resource_before_the_upstream_welcome_goes_as_a_short_notice_does() {
        // No session yet: the NOTICEs go to the session (which holds them
        // or reports them dropped) as a short NOTICE would, cut to the
        // default max_msg_body_bytes.
        let mut f = fixture();
        f.with_resources(A, &[]);
        let text = long_text(10);
        assert!(f.client_notice(A, &text).is_empty());
        let parts = sends(&f.commands_sent());
        assert!(parts.len() > 1);
        assert!(
            parts
                .iter()
                .all(|p| p.text_body().unwrap().len() as u64 <= Limits::default().max_msg_body_bytes)
        );
        let short = client(MessageType::Notice)
            .with_room("lobby")
            .with_body(Raw::text("hi"));
        assert!(f.packet(A, &short).is_empty());
        assert_eq!(sends(&f.commands_sent()).len(), 1);
    }

    #[test]
    fn client_resources_that_do_not_match_are_dropped_and_counted_once() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.with_resources(A, &["lobby"]);
        let text = long_text(20);
        // Unannounced: it waits OFFER_GRACE for an envelope, then goes.
        assert!(f.relay.client_resource(A, text.clone().into_bytes(), f.now).is_empty());
        assert_eq!(f.drops(), 0);
        f.relay.tick(f.now + resource::OFFER_GRACE);
        assert_eq!(f.drops(), 1);
        f.relay.tick(f.now + resource::OFFER_GRACE * 2);
        assert_eq!(f.drops(), 1);
        // The announced size, another sha256: dropped once, at OFFER_GRACE,
        // and the envelope with it.
        let env = resource::announce(&USER, resource::KIND_NOTICE, Some("lobby"), text.as_bytes());
        f.packet(A, &env);
        let mut other = text.clone().into_bytes();
        other[0] = b'L';
        assert!(f.relay.client_resource(A, other, f.now).is_empty());
        f.relay.tick(f.now + resource::OFFER_GRACE);
        assert_eq!(f.drops(), 2);
        assert!(f.relay.clients[&A].expected.is_empty());
        // Too big: refused with rrcd's ERROR. The envelope is not a
        // Resource: not counted (the policy refuses and counts the
        // Resource if it is offered).
        let mut big = env.clone();
        let mut body = ResourceEnvelope::from_body(env.body.as_ref().unwrap()).unwrap();
        body.size = MAX_RESOURCE_BYTES + 1;
        big.body = Some(body.to_body());
        let sent = sent_to(&f.packet(A, &big));
        assert_eq!(sent[0].1.t, MessageType::Error);
        assert_eq!(
            sent[0].1.text_body().as_deref(),
            Some(format!("resource too large: {} > {MAX_RESOURCE_BYTES}", MAX_RESOURCE_BYTES + 1).as_str())
        );
        assert_eq!(f.drops(), 2);
        // A blob, announced and sent: not relayed.
        let blob = resource::announce(&USER, "blob", Some("lobby"), b"data");
        f.packet(A, &blob);
        f.relay.client_resource(A, b"data".to_vec(), f.now);
        assert_eq!(f.drops(), 3);
        assert!(f.command().is_none());
        // Announced and never sent: the envelope is forgotten, uncounted;
        // not while its Resource is on its way.
        f.packet(A, &env);
        let busy: std::collections::HashSet<(Link, u64)> = [(A, text.len() as u64)].into();
        f.relay.tick_busy(f.now + resource::RESOURCE_TIMEOUT, &busy);
        assert_eq!(f.relay.clients[&A].expected.len(), 1);
        f.relay.tick(f.now + resource::RESOURCE_TIMEOUT);
        assert_eq!(f.relay.clients[&A].expected.len(), 0);
        assert_eq!(f.drops(), 3);
        // A failed transfer is counted once, by its end.
        f.relay.client_resource_failed(A, "Failed");
        assert_eq!(f.drops(), 4);
        // The relay goes on.
        assert!(f.client_notice(A, &text).is_empty());
        assert!(!sends(&f.commands_sent()).is_empty());
        assert_eq!(f.counts().0, 1);
        assert_eq!(f.drops(), 4);
    }

    #[test]
    fn a_new_hello_or_a_closed_link_drops_only_the_resources_that_came() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.with_resources(A, &["lobby"]);
        let env = resource::announce(&USER, resource::KIND_NOTICE, Some("lobby"), b"text");
        f.packet(A, &env);
        f.relay.client_resource(A, b"early one".to_vec(), f.now);
        f.relay.client_resource(A, b"early two".to_vec(), f.now);
        // A new HELLO: the old session's envelopes and early Resources go.
        f.packet(A, &hello_with_resources());
        assert_eq!(f.drops(), 2);
        assert!(f.relay.clients[&A].expected.is_empty());
        // The Resource of the old envelope, if it comes, is unannounced.
        f.relay.client_resource(A, b"text".to_vec(), f.now);
        assert!(f.command().is_none());
        // A closed link: its early Resource counts, its envelopes do not.
        f.packet(
            A,
            &resource::announce(&USER, resource::KIND_NOTICE, Some("lobby"), b"other text"),
        );
        f.relay.link_closed(A);
        assert_eq!(f.drops(), 3);
    }

    #[test]
    fn resources_are_refused_while_the_hub_takes_none() {
        let mut f = fixture();
        let mut welcome = upstream_welcome();
        welcome.caps.resource_envelope = false;
        f.relay.upstream(
            UpstreamEvent::Up {
                welcome,
                hub: UPSTREAM_HUB.to_vec(),
            },
            f.now,
        );
        f.joined_upstream("lobby", &[&USER]);
        f.with_resources(A, &["lobby"]);
        let env = resource::announce(&USER, resource::KIND_NOTICE, Some("lobby"), b"text");
        let sent = sent_to(&f.packet(A, &env));
        assert_eq!(sent[0].1.text_body().as_deref(), Some("resource transfer disabled"));
        assert_eq!(f.drops(), 0);
    }

    #[test]
    fn a_client_resource_over_the_limit_of_expectations_is_refused() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.with_resources(A, &["lobby"]);
        for _ in 0..resource::MAX_EXPECTED {
            let env = resource::announce(&USER, resource::KIND_NOTICE, Some("lobby"), b"text");
            assert!(f.packet(A, &env).is_empty());
        }
        let env = resource::announce(&USER, resource::KIND_NOTICE, Some("lobby"), b"text");
        let sent = sent_to(&f.packet(A, &env));
        assert_eq!(
            sent[0].1.text_body().as_deref(),
            Some("too many pending resource expectations")
        );
        f.relay.link_closed(A);
        assert_eq!(f.drops(), 0);
    }

    #[test]
    fn a_greeting_resource_after_the_window_is_still_the_greeting() {
        let mut f = fixture();
        f.up();
        f.in_rooms(A, &[]);
        // A command waits for its answer.
        let who = client(MessageType::Msg).with_body(Raw::text("/who"));
        f.packet(A, &who);
        assert_eq!(f.relay.pending_commands.len(), 1);
        f.now += GREETING_WINDOW + Duration::from_secs(1);
        let motd = long_text(12);
        let out = f.hub_resource(resource::KIND_MOTD, None, &motd);
        assert!(!links(&out).is_empty() && links(&out).iter().all(|l| *l == A));
        // It answered no command, and is kept for a later client.
        assert_eq!(f.relay.pending_commands.len(), 1);
        f.connect(B, USER);
        let out = f.packet(B, &hello_with_resources());
        assert_eq!(resources_to(&out)[0].2.as_slice(), motd.as_bytes());
        // A room-less `notice` Resource then is a command's answer.
        let out = f.hub_resource(resource::KIND_NOTICE, None, "who: you");
        assert!(links(&out).iter().all(|l| *l == A));
        assert!(f.relay.pending_commands.is_empty());
    }

    #[test]
    fn the_rooms_of_a_down_session_are_joined_again_through_it() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.in_rooms(A, &["lobby"]);
        f.relay.upstream(UpstreamEvent::Down, f.now);
        // A JOIN while down is refused, even of a room in the room set.
        f.connect(B, USER);
        f.hello(B);
        for room in ["lobby", "new"] {
            let sent = sent_to(&f.packet(B, &client(MessageType::Join).with_room(room)));
            assert_eq!(sent.len(), 1);
            assert_eq!(
                sent[0].1.text_body(),
                Some(format!("hub not connected: {room} not joined"))
            );
            assert_eq!(sent[0].1.room.as_deref(), Some(room));
        }
        assert!(f.command().is_none());
        f.up();
        let out = f.joined_upstream("lobby", &[&USER]);
        assert!(out.is_empty(), "A was in the room all along");
        f.packet(B, &client(MessageType::Join).with_room("lobby"));
        let msg = from_hub(MessageType::Msg).with_room("lobby");
        assert_eq!(links(&f.hub(msg, Inbound::Relay)), vec![A, B]);
    }

    #[test]
    fn every_rrcd_error_text_has_an_owner() {
        use Answers::*;
        // rrcd 0.3.2: router.py, commands.py, resource_manager.py,
        // service.py `_norm_room`.
        let table: &[(&str, Answers)] = &[
            ("rate limited", Everyone),
            ("bad message: unexpected end of data", Everyone),
            ("send HELLO first", Session),
            ("resource transfer disabled", Everyone),
            ("invalid resource envelope body", Everyone),
            ("resource envelope missing id", Everyone),
            ("resource envelope missing kind", Everyone),
            ("resource envelope invalid size", Everyone),
            ("resource too large: 900000 > 262144", Everyone),
            ("resource envelope invalid sha256", Everyone),
            ("too many pending resource expectations", Everyone),
            ("JOIN requires room name", Session),
            ("too many rooms", Session),
            ("invite-only (+i)", Session),
            ("bad key (+k)", Session),
            ("banned from room", Room),
            ("PART requires room name", Session),
            ("room name must not be empty", Session),
            ("room name too long: 70 bytes > 64 bytes", Session),
            ("unrecognized command", Command),
            ("message requires room name", NoRoom),
            ("message too large: 400 bytes > 350 bytes", RoomMessage),
            ("direct notice must not include room", Direct),
            ("direct notice requires destination identity", Direct),
            ("destination not connected", Direct),
            ("no such room", Room),
            ("no outside messages (+n)", Room),
            ("room is moderated (+m)", Room),
            ("not authorized", Command),
            ("not authorized (+t)", Command),
            ("only the room founder can register", Command),
            ("only the room founder can unregister", Command),
            ("kicked from lobby", Everyone),
            ("banned from lobby", Everyone),
            ("invite failed: no such room", Command),
            ("invite failed: target not identified", Command),
            ("something new", Everyone),
        ];
        for (text, answers) in table {
            assert_eq!(error_answers(text), *answers, "{text}");
        }
    }

    #[test]
    fn a_command_is_a_command_whatever_its_room_or_destination() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.in_rooms(A, &["lobby"]);
        f.in_rooms(B, &["lobby"]);
        // A direct NOTICE that is a command: no copy, and the reply is A's.
        let mut notice = client(MessageType::Notice).with_body(Raw::text("/list"));
        notice.dst = Some(OTHER.to_vec());
        assert!(f.packet(A, &notice).is_empty());
        assert!(matches!(f.command(), Some(Command::Send(_))));
        let reply = from_hub(MessageType::Notice).with_body(Raw::text("No public rooms registered"));
        assert_eq!(links(&f.hub(reply, Inbound::Relay)), vec![A]);
        // A command with no room is not refused for it.
        let command = client(MessageType::Msg).with_body(Raw::text("/who lobby"));
        assert!(f.packet(B, &command).is_empty());
        let reply = from_hub(MessageType::Notice).with_body(Raw::text("members in lobby: x"));
        assert_eq!(links(&f.hub(reply, Inbound::Relay)), vec![B]);
    }

    #[test]
    fn a_command_answer_to_the_room_reaches_the_commander_and_the_room() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.in_rooms(A, &[]);
        f.in_rooms(B, &["lobby"]);
        f.packet(
            A,
            &client(MessageType::Msg)
                .with_room("Lobby")
                .with_body(Raw::text("/mode lobby +m")),
        );
        let notice = from_hub(MessageType::Notice)
            .with_room("lobby")
            .with_body(Raw::text("mode +m"));
        assert_eq!(links(&f.hub(notice.clone(), Inbound::Relay)), vec![B, A]);
        assert_eq!(links(&f.hub(notice, Inbound::Relay)), vec![B]);
    }

    #[test]
    fn messages_rrcd_would_refuse_are_refused_locally() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.in_rooms(A, &["lobby"]);
        let refusal = |f: &mut Fixture, env: Envelope| {
            let sent = sent_to(&f.packet(A, &env));
            assert_eq!(sent.len(), 1);
            sent[0].1.text_body().unwrap()
        };
        assert_eq!(
            refusal(&mut f, client(MessageType::Msg).with_room("")),
            "message requires room name"
        );
        assert_eq!(
            refusal(&mut f, client(MessageType::Action).with_body(Raw::text("waves"))),
            "message requires room name"
        );
        let big = "x".repeat(351);
        assert_eq!(
            refusal(
                &mut f,
                client(MessageType::Msg).with_room("lobby").with_body(Raw::text(&big))
            ),
            "message too large: 351 bytes > 350 bytes"
        );
        let mut notice = client(MessageType::Notice).with_room("lobby");
        notice.dst = Some(OTHER.to_vec());
        assert_eq!(refusal(&mut f, notice), "direct notice must not include room");
        assert_eq!(
            refusal(&mut f, client(MessageType::Msg).with_room("ninechars")),
            "room name too long: 9 bytes > 8 bytes"
        );
        // rrcd drops a NOTICE with no room or destination without a word.
        assert!(
            f.packet(A, &client(MessageType::Notice).with_body(Raw::text("hi")))
                .is_empty()
        );
        assert!(f.command().is_none());
    }

    #[test]
    fn the_hub_nick_is_put_on_every_envelope_sent_upstream() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut relay = Relay::new(RelayConfig {
            hub_identity: HUB,
            user: USER,
            upstream: UPSTREAM,
            allowed: vec![USER],
            part_upstream: false,
            nick: Some("bnc".into()),
            commands: tx,
            session: SharedStatus::default(),
            status: SharedRelayStatus::default(),
            history: History::disabled(),
            replay_window: Duration::ZERO,
            replay_max: 0,
        });
        let now = Instant::now();
        relay.upstream(
            UpstreamEvent::Up {
                welcome: upstream_welcome(),
                hub: UPSTREAM_HUB.to_vec(),
            },
            now,
        );
        relay.link_established(A, now);
        relay.link_identified(A, USER, now);
        relay.client_packet(A, &client(MessageType::Hello).encode(), now);
        for body in ["hello", "/list"] {
            let env = client(MessageType::Msg)
                .with_room("lobby")
                .with_body(Raw::text(body))
                .with_nick(Some("weechat"));
            relay.client_packet(A, &env.encode(), now);
            match rx.try_recv() {
                Ok(Command::Send(sent)) => assert_eq!(sent.nick.as_deref(), Some("bnc")),
                other => panic!("{other:?}"),
            }
        }
        // Without one, the client's nick goes as it is.
        let mut f = fixture();
        f.up();
        f.in_rooms(A, &[]);
        let env = client(MessageType::Msg).with_room("lobby").with_nick(Some("weechat"));
        f.packet(A, &env);
        match f.command() {
            Some(Command::Send(sent)) => assert_eq!(sent.nick.as_deref(), Some("weechat")),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn join_limits_count_the_room_set() {
        let mut f = fixture();
        f.up();
        f.in_rooms(A, &[]);
        // Two rooms in the session's set, one a client added: the third
        // fits, the fourth does not.
        if let Ok(mut s) = f.session.lock() {
            s.rooms = vec!["a".into(), "b".into()];
        }
        assert!(f.packet(A, &client(MessageType::Join).with_room("c")).is_empty());
        assert_eq!(f.command(), Some(Command::Join("c".into())));
        let sent = sent_to(&f.packet(A, &client(MessageType::Join).with_room("d")));
        assert_eq!(sent[0].1.text_body().as_deref(), Some("too many rooms"));
        // A JOIN of a room the set has again is not counted twice.
        assert!(f.packet(A, &client(MessageType::Join).with_room("a")).is_empty());
        // A room the session holds over the limit is refused too.
        if let Ok(mut s) = f.session.lock() {
            s.rooms_over_limit = vec!["b".into()];
        }
        let sent = sent_to(&f.packet(A, &client(MessageType::Join).with_room("b")));
        assert_eq!(sent[0].1.text_body().as_deref(), Some("too many rooms"));
        assert_eq!(sent[0].1.room.as_deref(), Some("b"));
    }

    #[test]
    fn members_are_kept_once_and_the_local_joined_fits_a_packet() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&FRIEND, &USER, &FRIEND, &USER]);
        let out = f.packet_after_hello(A, client(MessageType::Join).with_room("lobby"));
        assert_eq!(out[0].1.members(), Some(vec![USER.to_vec(), FRIEND.to_vec()]));
        // A JOINED of someone already there adds nothing.
        let joined = from_hub(MessageType::Joined)
            .with_room("lobby")
            .with_body(members(&[&FRIEND]));
        f.hub(joined, Inbound::Relay);
        assert_eq!(f.relay.joined["lobby"], vec![FRIEND.to_vec(), USER.to_vec()]);
        // Many members: the user first, then as many as fit.
        let many: Vec<[u8; 16]> = (0..40u8).map(|i| [i + 100; 16]).collect();
        let refs: Vec<&[u8; 16]> = many.iter().collect();
        f.joined_upstream("big", &refs);
        let out = f.packet_after_hello(B, client(MessageType::Join).with_room("big"));
        let env = &out[0].1;
        assert!(env.encode().len() <= LINK_MDU);
        let list = env.members().unwrap();
        assert_eq!(list[0], USER.to_vec());
        assert_eq!(list[1], many[0].to_vec());
        assert!(list.len() > 10 && list.len() < 41, "{}", list.len());
        // A JOINED with no body keeps the list.
        let bare = from_hub(MessageType::Joined).with_room("big");
        f.hub(bare, Inbound::JoinConfirmed("big".into()));
        assert_eq!(f.relay.joined["big"].len(), 40);
    }

    #[test]
    fn the_greeting_reaches_clients_that_link_later() {
        let mut f = fixture();
        f.in_rooms(A, &[]);
        f.up();
        let greeting = from_hub(MessageType::Notice).with_body(Raw::text("e2e"));
        assert_eq!(links(&f.hub(greeting, Inbound::Relay)), vec![A]);
        f.joined_upstream("lobby", &[&USER]);
        // Over: a room-less NOTICE now is no greeting.
        let other = from_hub(MessageType::Notice).with_body(Raw::text("later"));
        f.hub(other, Inbound::Relay);
        f.connect(B, USER);
        let out = sent_to(&f.packet(B, &client(MessageType::Hello)));
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].1.t, MessageType::Welcome);
        assert_eq!(out[1].1.text_body().as_deref(), Some("e2e"));
        assert_eq!(out[1].1.src, HUB.to_vec());
        // A new session's greeting replaces it.
        f.up();
        f.connect(C, USER);
        f.hello(C);
    }

    #[test]
    fn the_greeting_window_closes_at_a_command_or_in_time() {
        let mut f = fixture();
        f.in_rooms(A, &[]);
        f.up();
        f.packet(A, &client(MessageType::Msg).with_body(Raw::text("/list")));
        let reply = from_hub(MessageType::Notice).with_body(Raw::text("Registered public rooms:"));
        f.hub(reply, Inbound::Relay);
        assert!(f.relay.greeting.is_empty());
        f.up();
        f.relay.tick(f.now + GREETING_WINDOW);
        let late = from_hub(MessageType::Notice).with_body(Raw::text("late"));
        f.hub(late, Inbound::Relay);
        assert!(f.relay.greeting.is_empty());
    }

    #[test]
    fn a_join_of_a_room_already_joined_upstream_is_answered() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER, &FRIEND]);
        f.in_rooms(A, &[]);
        // The relay forgot the room (a PART with part_upstream it took
        // back); the session answers the JOIN with a JOINED of no body.
        f.relay.joined.remove("lobby");
        f.packet(A, &client(MessageType::Join).with_room("lobby"));
        let bare = from_hub(MessageType::Joined).with_room("lobby");
        let out = sent_to(&f.hub(bare, Inbound::JoinConfirmed("lobby".into())));
        assert_eq!(links_of(&out), vec![A]);
        assert_eq!(out[0].1.members(), Some(vec![USER.to_vec()]));
    }

    #[test]
    fn after_a_down_old_sends_are_not_blamed_but_still_reported_dropped() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.in_rooms(A, &["lobby"]);
        f.in_rooms(B, &["lobby"]);
        let msg = client(MessageType::Msg).with_room("lobby");
        f.packet(A, &msg);
        f.relay.upstream(UpstreamEvent::Down, f.now);
        f.up();
        let text = "room is moderated (+m)";
        assert_eq!(
            links(&f.hub(hub_error(Some("lobby"), text), Inbound::HubError(text.into()))),
            vec![A, B]
        );
        let out = f.relay.upstream(
            UpstreamEvent::SendDropped {
                envelope: msg,
                reason: "the link ended".into(),
            },
            f.now,
        );
        assert_eq!(links(&out), vec![A]);
    }

    #[test]
    fn a_rooms_join_notice_goes_to_the_room() {
        let mut f = fixture();
        f.in_rooms(A, &[]);
        f.up();
        f.packet(A, &client(MessageType::Msg).with_body(Raw::text("/list")));
        f.packet(A, &client(MessageType::Join).with_room("lobby"));
        f.joined_upstream("lobby", &[&USER]);
        // rrcd's NOTICE about the room after the JOINED is not the
        // command's answer.
        let topic = from_hub(MessageType::Notice)
            .with_room("lobby")
            .with_body(Raw::text("room lobby: unregistered; mode=+nt; topic=(none)"));
        assert_eq!(links(&f.hub(topic, Inbound::Relay)), vec![A]);
        let reply = from_hub(MessageType::Notice).with_body(Raw::text("Registered public rooms:"));
        assert_eq!(links(&f.hub(reply, Inbound::Relay)), vec![A]);
        assert!(f.relay.pending_commands.is_empty());
    }

    impl Fixture {
        fn replays(mut self, max: usize) -> Self {
            self.relay.replay_max = max;
            self
        }
    }

    fn replays_in(out: &[Output]) -> Vec<Replay> {
        out.iter()
            .filter_map(|o| match o {
                Output::Replay(r) => Some(r.clone()),
                _ => None,
            })
            .collect()
    }

    fn room_msg(room: &str, body: &str) -> Envelope {
        Envelope::new(MessageType::Msg, &OTHER)
            .with_room(room)
            .with_body(Raw::text(body))
    }

    #[test]
    fn history_requests() {
        assert_eq!(history_request("/history", 100), Some(Ok(100)));
        assert_eq!(history_request("  /HISTORY  ", 100), Some(Ok(100)));
        // As rrcd splits a command line.
        assert_eq!(history_request("/ history 3", 100), Some(Ok(3)));
        assert_eq!(history_request("/history 5", 100), Some(Ok(5)));
        assert_eq!(history_request("/history\t7 ", 100), Some(Ok(7)));
        assert_eq!(history_request("/history 9999", 100), Some(Ok(MAX_HISTORY)));
        assert_eq!(history_request("/history", 9999), Some(Ok(MAX_HISTORY)));
        for bad in [
            "/history 0",
            "/history -1",
            "/history x",
            "/history 5 6",
            "/history 99999999999999999999999",
        ] {
            assert_eq!(history_request(bad, 100), Some(Err(())), "{bad}");
        }
        for not in ["/historyx", "/list", "history", "hello /history"] {
            assert_eq!(history_request(not, 100), None, "{not}");
        }
    }

    #[test]
    fn a_replay_is_framed_and_keeps_the_envelopes_but_the_upstream_hubs_src() {
        let upstream = hub_address(&UPSTREAM_HUB);
        let mut said = room_msg("lobby", "hi").with_nick(Some("d"));
        said.extra.push((64, Raw::text("fmt")));
        let notice = from_hub(MessageType::Notice)
            .with_room("lobby")
            .with_body(Raw::text("topic changed"));
        let records = vec![
            Record {
                ts_ms: 1_794_567_845_123,
                envelope: said.encode(),
            },
            Record {
                ts_ms: 1_794_567_846_000,
                envelope: notice.encode(),
            },
        ];
        let out = replay_envelopes(&records, &HUB, &upstream, "lobby", false);
        assert_eq!(out.len(), 4);
        for frame in [&out[0], &out[3]] {
            assert_eq!((frame.t, frame.room.as_deref()), (MessageType::Notice, Some("lobby")));
            assert_eq!(frame.src, HUB.to_vec());
        }
        assert_eq!(
            out[0].text_body().as_deref(),
            Some("--- replay: 2 messages since 2026-11-13T11:04:05Z ---")
        );
        assert_eq!(out[3].text_body().as_deref(), Some("--- end of replay ---"));
        assert_eq!(out[1], said);
        assert_eq!(out[1].encode(), said.encode());
        let mut local = notice.clone();
        local.src = HUB.to_vec();
        assert_eq!(out[2], local);
        // One message; nothing.
        let one = replay_envelopes(&records[..1], &HUB, &upstream, "lobby", false);
        assert!(one[0].text_body().unwrap().starts_with("--- replay: 1 message since "));
        assert!(replay_envelopes(&[], &HUB, &upstream, "lobby", false).is_empty());
        let empty = replay_envelopes(&[], &HUB, &upstream, "lobby", true);
        assert_eq!(empty.len(), 1);
        assert_eq!(empty[0].text_body().as_deref(), Some("--- no history in lobby ---"));
    }

    fn ends_in(out: &[Output]) -> Vec<(Link, Option<String>, bool)> {
        out.iter()
            .filter_map(|o| match o {
                Output::EndReplay { link, room, flush } => Some((*link, room.clone(), *flush)),
                _ => None,
            })
            .collect()
    }

    fn now_to(out: &[Output]) -> Vec<(Link, Envelope)> {
        out.iter()
            .filter_map(|o| match o {
                Output::Now(l, e) => Some((*l, e.clone())),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_join_asks_for_a_replay_after_its_joined() {
        let mut f = fixture().replays(50);
        f.up();
        f.joined_upstream("lobby", &[&USER, &OTHER]);
        f.connect(A, USER);
        f.hello(A);
        let out = f.packet(A, &client(MessageType::Join).with_room("lobby"));
        assert!(matches!(&out[0], Output::Client(A, e) if e.t == MessageType::Joined));
        let replays = replays_in(&out);
        assert_eq!(replays.len(), 1);
        let r = &replays[0];
        assert_eq!(
            (r.link, r.query.room.as_str(), r.query.max, r.say_empty),
            (A, "lobby", 50, false)
        );
        let since = r.query.since_ms.unwrap();
        assert!(now_ms() - since >= 3_600_000 && now_ms() - since < 3_610_000);
        // A JOIN while the replay runs starts no second one.
        let again = f.packet(A, &client(MessageType::Join).with_room("lobby"));
        assert!(replays_in(&again).is_empty());
        // Room envelopes go out as ever: the link's sender holds them.
        assert_eq!(links(&f.hub(room_msg("lobby", "one"), Inbound::Relay)), vec![A]);
        // Done, the next JOIN replays again, with a new id.
        f.relay.replay_done(A, "lobby", r.id);
        let next = replays_in(&f.packet(A, &client(MessageType::Join).with_room("lobby")));
        assert!(next.len() == 1 && next[0].id != r.id);
    }

    #[test]
    fn a_stale_replay_done_is_ignored() {
        let mut f = fixture().replays(10);
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.connect(A, USER);
        f.hello(A);
        let join = client(MessageType::Join).with_room("lobby");
        let first = replays_in(&f.packet(A, &join))[0].id;
        // In time it is ended, and what it held is sent.
        let out = f.relay.tick(f.now + REPLAY_WAIT);
        assert_eq!(ends_in(&out), vec![(A, Some("lobby".into()), true)]);
        assert!(f.relay.replaying.is_empty());
        let second = replays_in(&f.packet(A, &join))[0].id;
        // The first replay's end, late: the second still runs.
        f.relay.replay_done(A, "lobby", first);
        assert!(f.relay.replaying.contains_key(&(A, "lobby".to_string())));
        assert_eq!(replays_in(&f.packet(A, &join)), vec![]);
        f.relay.replay_done(A, "lobby", second);
        assert!(f.relay.replaying.is_empty());
        // A close ends a link's replays too.
        replays_in(&f.packet(A, &join));
        f.relay.link_closed(A);
        assert!(f.relay.replaying.is_empty());
    }

    #[test]
    fn part_and_hello_end_replays_before_their_answers() {
        let mut f = fixture_with(true, 16).replays(10);
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.joined_upstream("side", &[&USER]);
        f.in_rooms(A, &["lobby", "side"]);
        f.in_rooms(B, &["lobby"]);
        // A's PART: A's replay of the room ends, then its PARTED; with
        // part_upstream, B's too, then B's.
        let out = f.packet(A, &client(MessageType::Part).with_room("lobby"));
        let order: Vec<(Link, &str)> = out
            .iter()
            .filter_map(|o| match o {
                Output::EndReplay { link, flush: false, .. } => Some((*link, "end")),
                Output::Client(link, e) if e.t == MessageType::Parted => Some((*link, "parted")),
                _ => None,
            })
            .collect();
        assert_eq!(order, [(A, "end"), (A, "parted"), (B, "end"), (B, "parted")]);
        assert_eq!(ends_in(&out)[0], (A, Some("lobby".into()), false));
        // A re-JOIN starts a fresh replay.
        f.joined_upstream("lobby", &[&USER]);
        let again = f.packet(A, &client(MessageType::Join).with_room("lobby"));
        assert_eq!(replays_in(&again).len(), 1);
        // HELLO ends every replay of the link, before the WELCOME.
        let out = f.packet(A, &client(MessageType::Hello));
        assert_eq!(ends_in(&out), vec![(A, None, false)]);
        assert!(matches!(&out[1], Output::Client(A, e) if e.t == MessageType::Welcome));
        assert!(f.relay.replaying.keys().all(|(l, _)| *l != A));
        // A removal ends the members' replays of the room.
        f.joined_upstream("side", &[&USER]);
        f.in_rooms(C, &["side"]);
        let kick = from_hub(MessageType::Error).with_room("side");
        let out = f.hub(
            kick,
            Inbound::Removed {
                room: "side".into(),
                reason: String::new(),
            },
        );
        assert_eq!(ends_in(&out), vec![(C, Some("side".into()), false)]);
    }

    #[test]
    fn no_replay_when_replay_max_is_zero() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.connect(A, USER);
        f.hello(A);
        let out = f.packet(A, &client(MessageType::Join).with_room("lobby"));
        assert!(replays_in(&out).is_empty());
        // A new room joined upstream: no replay either.
        f.packet(A, &client(MessageType::Join).with_room("side"));
        assert!(replays_in(&f.joined_upstream("side", &[&USER])).is_empty());
    }

    #[test]
    fn a_new_room_replays_when_joined_upstream() {
        let mut f = fixture().replays(5);
        f.up();
        f.connect(A, USER);
        f.hello(A);
        f.packet(A, &client(MessageType::Join).with_room("side"));
        let out = f.joined_upstream("side", &[&USER]);
        let sent = sent_to(&out);
        assert_eq!((sent[0].0, sent[0].1.t), (A, MessageType::Joined));
        assert_eq!(replays_in(&out).len(), 1);
    }

    #[test]
    fn history_is_answered_locally_to_its_client() {
        let mut f = fixture();
        f.in_rooms(A, &[]);
        f.in_rooms(B, &[]);
        // Works with the session down, and asks no time window.
        let ask = |body: &str, room: Option<&str>| {
            let mut env = client(MessageType::Msg).with_body(Raw::text(body));
            env.room = room.map(String::from);
            env
        };
        let out = f.packet(A, &ask("/history 7", Some("Lobby")));
        assert!(f.command().is_none());
        assert!(sent_to(&out).is_empty());
        let id = replays_in(&out)[0].id;
        assert_eq!(
            replays_in(&out),
            vec![Replay {
                id,
                link: A,
                query: Query {
                    room: "lobby".into(),
                    since_ms: None,
                    max: 7
                },
                say_empty: true
            }]
        );
        // While it runs, another is refused with a NOTICE, sent at once.
        let running = now_to(&f.packet(A, &ask("/history", Some("lobby"))));
        assert_eq!(running.len(), 1);
        assert_eq!(
            running[0].1.text_body().as_deref(),
            Some("--- a replay of lobby is running ---")
        );
        f.relay.replay_done(A, "lobby", id);
        // No number: replay_max, or 100 when that is 0.
        assert_eq!(
            replays_in(&f.packet(B, &ask("/history", Some("lobby"))))[0].query.max,
            100
        );
        // A bad one, or none in no room: the usage, to that client.
        for (body, room) in [("/history 0", Some("side")), ("/history", None)] {
            let sent = now_to(&f.packet(A, &ask(body, room)));
            assert_eq!(sent.len(), 1);
            let (link, notice) = &sent[0];
            assert_eq!(
                (*link, notice.t, notice.src.as_slice()),
                (A, MessageType::Notice, &HUB[..])
            );
            assert_eq!(notice.room.as_deref(), room);
            assert_eq!(notice.text_body().as_deref(), Some(HISTORY_USAGE));
        }
        assert!(f.command().is_none());
        // ACTION and NOTICE are not the command; nor is /historyx.
        f.up();
        f.packet(
            A,
            &client(MessageType::Notice)
                .with_room("lobby")
                .with_body(Raw::text("/history")),
        );
        assert!(matches!(f.command(), Some(Command::Send(_))));
        f.packet(A, &ask("/historyx", Some("lobby")));
        assert!(matches!(f.command(), Some(Command::Send(_))));
    }

    /// A client link that takes every envelope, or none.
    #[derive(Clone, Default)]
    struct FakeSink {
        sent: Arc<Mutex<Vec<(Link, Envelope, tokio::time::Instant)>>>,
        fail: Arc<std::sync::atomic::AtomicBool>,
        /// Resources taken: the link, the size, from when to when.
        resources: Arc<Mutex<Vec<Taken>>>,
    }

    type Taken = (Link, usize, tokio::time::Instant, tokio::time::Instant);

    /// How long the fake takes to send a Resource.
    const FAKE_RESOURCE_TIME: Duration = Duration::from_secs(3);

    impl LinkSink for FakeSink {
        async fn send(&self, link: Link, data: Vec<u8>) -> Result<(), String> {
            if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
                return Err("link down".into());
            }
            let env = Envelope::decode(&data).unwrap();
            self.sent.lock().unwrap().push((link, env, tokio::time::Instant::now()));
            Ok(())
        }

        async fn send_resource(&self, link: Link, data: Vec<u8>) -> Result<(), String> {
            let start = tokio::time::Instant::now();
            tokio::time::sleep(FAKE_RESOURCE_TIME).await;
            self.resources
                .lock()
                .unwrap()
                .push((link, data.len(), start, tokio::time::Instant::now()));
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_links_resources_go_one_at_a_time_and_envelopes_go_on_meanwhile() {
        let mut s = senders();
        for i in 0..3 {
            let data = vec![b'x'; 1000 + i];
            let env = resource::announce(&HUB, resource::KIND_NOTICE, Some("lobby"), &data);
            s.links.envelope(A, ToLink::Resource(env, Arc::new(data)));
        }
        s.links.envelope(A, ToLink::Send(room_msg("lobby", "short")));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(s.to(A).contains(&"short".to_string()), "not behind the Resources");
        settle().await;
        let taken = s.sink.resources.lock().unwrap().clone();
        assert_eq!(taken.iter().map(|r| r.1).collect::<Vec<_>>(), [1000, 1001, 1002]);
        for pair in taken.windows(2) {
            assert!(pair[1].2 >= pair[0].3, "one at a time");
        }
        let status = s.status.lock().unwrap().clone();
        assert_eq!((status.client_resources_out, status.client_resource_drops), (3, 0));
        // Each went after its RESOURCE_ENVELOPE.
        assert_eq!(s.to(A).iter().filter(|t| t.contains("ResourceEnvelope")).count(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn resources_wait_for_their_rooms_replay_and_too_many_are_dropped() {
        let mut s = senders();
        let records = s.replay_waiting(A, 0, "lobby");
        let data = vec![b'x'; 1000];
        let env = resource::announce(&HUB, resource::KIND_NOTICE, Some("lobby"), &data);
        s.links.envelope(A, ToLink::Resource(env, Arc::new(data.clone())));
        settle().await;
        assert!(s.sink.resources.lock().unwrap().is_empty());
        let _ = records.send(records_of("lobby", 1));
        settle().await;
        assert_eq!(s.sink.resources.lock().unwrap().len(), 1);

        for _ in 0..LINK_RESOURCES + 2 {
            let env = resource::announce(&HUB, resource::KIND_NOTICE, Some("side"), &data);
            s.links.envelope(B, ToLink::Resource(env, Arc::new(data.clone())));
        }
        tokio::time::sleep(FAKE_RESOURCE_TIME * (LINK_RESOURCES as u32 + 3)).await;
        let status = s.status.lock().unwrap().clone();
        // One was sending when the rest came: one more than the queue holds.
        assert_eq!(status.client_resource_drops, 1);
        assert_eq!(status.client_resources_out, 1 + LINK_RESOURCES as u64 + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_closed_links_resources_are_counted_dropped_once() {
        let mut s = senders();
        for i in 0..3 {
            let data = vec![b'x'; 1000 + i];
            let env = resource::announce(&HUB, resource::KIND_NOTICE, Some("lobby"), &data);
            assert!(s.links.envelope(A, ToLink::Resource(env, Arc::new(data))));
        }
        // The first is under way, two wait.
        tokio::time::sleep(Duration::from_secs(1)).await;
        s.links.close(&A);
        settle().await;
        let status = s.status.lock().unwrap().clone();
        assert_eq!((status.client_resources_out, status.client_resource_drops), (0, 3));
        assert!(s.sink.resources.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn held_items_go_in_order_and_an_unflushed_end_drops_them() {
        let mut s = senders();
        let _stuck = s.replay_waiting(A, 0, "lobby");
        for t in ["a", "b", "c"] {
            s.links.envelope(A, ToLink::Send(room_msg("lobby", t)));
        }
        let data = vec![b'x'; 1000];
        let env = resource::announce(&HUB, resource::KIND_NOTICE, Some("lobby"), &data);
        s.links.envelope(A, ToLink::Resource(env, Arc::new(data.clone())));
        s.end(A, Some("lobby"), true);
        // Sent while the held ones are let out: after them.
        s.links.envelope(A, ToLink::Send(room_msg("lobby", "d")));
        settle().await;
        let texts: Vec<String> = s.to(A).into_iter().filter(|t| t.len() == 1).collect();
        assert_eq!(texts, ["a", "b", "c", "d"]);
        assert_eq!(s.sink.resources.lock().unwrap().len(), 1);
        // Parted while its replay waits: what was held goes nowhere, and
        // its Resource is counted dropped.
        let _stuck = s.replay_waiting(B, 1, "side");
        s.links.envelope(B, ToLink::Send(room_msg("side", "x")));
        let env = resource::announce(&HUB, resource::KIND_NOTICE, Some("side"), &data);
        s.links.envelope(B, ToLink::Resource(env, Arc::new(data.clone())));
        s.end(B, Some("side"), false);
        settle().await;
        assert!(s.to(B).is_empty());
        let status = s.status.lock().unwrap().clone();
        assert_eq!((status.client_resources_out, status.client_resource_drops), (1, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn a_replay_goes_only_after_its_joined_and_while_the_link_has_room() {
        let f = fixture();
        let mut s = senders();
        let replay = |id: u64| {
            Output::Replay(Replay {
                id,
                link: A,
                query: history::Query {
                    room: "lobby".into(),
                    since_ms: None,
                    max: 10,
                },
                say_empty: true,
            })
        };
        let joined = |text: &str| Output::Client(A, room_msg("lobby", text));
        // Its JOINED dropped (the link's queue is full): no replay, the
        // relay is told it is over, and nothing is said on the link.
        for i in 0..LINK_QUEUE {
            assert!(s.links.envelope(A, ToLink::Send(room_msg("side", &i.to_string()))));
        }
        dispatch(vec![joined("joined"), replay(1)], &f.relay, &mut s.links, |_| {});
        assert_eq!(s.done(), done_of(&[(A, "lobby", 1)]));
        settle().await;
        assert!(!s.to(A).iter().any(|t| t.contains("replay")));
        // Its JOINED queued: it runs.
        dispatch(vec![joined("joined"), replay(2)], &f.relay, &mut s.links, |_| {});
        settle().await;
        assert_eq!(s.done(), done_of(&[(A, "lobby", 2)]));
        assert!(s.to(A).iter().any(|t| t.contains("no history") || t.contains("replay")));
        // Too many replay commands wait: skipped, and the client is told.
        // (Counted until the link's sender takes them in.)
        for i in 0..CONTROL_QUEUE {
            let _ = s.replay_waiting(A, 10 + i as u64, &format!("r{i}"));
        }
        let before = s.to(A).len();
        dispatch(vec![joined("joined"), replay(3)], &f.relay, &mut s.links, |_| {});
        assert!(s.done().contains(&(A, "lobby".to_string(), 3)));
        settle().await;
        assert!(s.to(A)[before..].contains(&"--- replay of lobby skipped: the link is busy ---".to_string()));
    }

    #[test]
    fn an_end_before_its_waiter_is_kept_for_it() {
        let waiters = ResourceWaiters::default();
        waiters.concluded(A, [9; 32], false);
        let mut rx = waiters.wait(A, [9; 32]);
        assert_eq!(rx.try_recv(), Ok(false));
        let mut rx = waiters.wait(A, [8; 32]);
        waiters.concluded(A, [8; 32], true);
        assert_eq!(rx.try_recv(), Ok(true));
    }

    struct Senders {
        links: Links<FakeSink>,
        sink: FakeSink,
        done: mpsc::UnboundedReceiver<(Link, String, u64)>,
        status: SharedRelayStatus,
    }

    fn senders() -> Senders {
        let sink = FakeSink::default();
        let status = SharedRelayStatus::default();
        let (tx, done) = mpsc::unbounded_channel();
        Senders {
            links: Links::new(sink.clone(), (HUB, UPSTREAM), status.clone(), tx),
            sink,
            done,
            status,
        }
    }

    fn records_of(room: &str, n: usize) -> Vec<Record> {
        (0..n)
            .map(|i| Record {
                ts_ms: 1_794_567_845_000 + i as u64,
                envelope: room_msg(room, &format!("{room} {i}")).encode(),
            })
            .collect()
    }

    impl Senders {
        /// A replay of `n` records of `room`, answered at once.
        fn replay(&mut self, link: Link, id: u64, room: &str, n: usize) {
            let tx = self.replay_waiting(link, id, room);
            let _ = tx.send(records_of(room, n));
        }

        /// A replay whose records have not come: their sender.
        fn replay_waiting(&mut self, link: Link, id: u64, room: &str) -> tokio::sync::oneshot::Sender<Vec<Record>> {
            let (tx, records) = tokio::sync::oneshot::channel();
            self.links.control(
                link,
                ToLink::Replay {
                    id,
                    room: room.into(),
                    say_empty: true,
                    records,
                },
            );
            tx
        }

        fn end(&mut self, link: Link, room: Option<&str>, flush: bool) {
            self.links.control(
                link,
                ToLink::End {
                    room: room.map(String::from),
                    flush,
                },
            );
        }

        /// What went to a link, as text bodies (or types).
        fn to(&self, link: Link) -> Vec<String> {
            self.sink
                .sent
                .lock()
                .unwrap()
                .iter()
                .filter(|(l, _, _)| *l == link)
                .map(|(_, e, _)| e.text_body().unwrap_or_else(|| format!("{:?}", e.t)))
                .collect()
        }

        fn done(&mut self) -> BTreeSet<(Link, String, u64)> {
            std::iter::from_fn(|| self.done.try_recv().ok()).collect()
        }
    }

    async fn settle() {
        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    fn done_of(list: &[(Link, &str, u64)]) -> BTreeSet<(Link, String, u64)> {
        list.iter().map(|(l, r, i)| (*l, r.to_string(), *i)).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn two_rooms_replayed_to_one_link_are_paced_as_one_stream() {
        let mut s = senders();
        s.replay(A, 0, "lobby", 3);
        s.replay(A, 1, "side", 3);
        // Another link's replay runs beside them, paced on its own.
        s.replay(B, 2, "lobby", 1);
        settle().await;
        let bodies = s.to(A);
        assert_eq!(bodies.len(), 10);
        assert!(bodies[0].starts_with("--- replay: 3 messages"));
        assert_eq!(bodies[1..4], ["lobby 0", "lobby 1", "lobby 2"]);
        assert_eq!(bodies[4], "--- end of replay ---");
        assert!(bodies[5].starts_with("--- replay: 3 messages"));
        assert_eq!(bodies[6..9], ["side 0", "side 1", "side 2"]);
        assert_eq!(bodies[9], "--- end of replay ---");
        let times: Vec<_> = s
            .sink
            .sent
            .lock()
            .unwrap()
            .iter()
            .filter(|(l, _, _)| *l == A)
            .map(|(_, _, t)| *t)
            .collect();
        for pair in times.windows(2) {
            assert!(pair[1] - pair[0] >= REPLAY_PACE, "one pace across both replays");
        }
        assert_eq!(s.to(B).len(), 3);
        assert_eq!(s.done(), done_of(&[(A, "lobby", 0), (A, "side", 1), (B, "lobby", 2)]));
        assert_eq!(s.status.lock().unwrap().relayed_out, 13);
    }

    #[tokio::test(start_paused = true)]
    async fn live_lines_wait_for_their_rooms_replay_but_own_notices_do_not() {
        let mut s = senders();
        let records = s.replay_waiting(A, 0, "lobby");
        s.links.envelope(A, ToLink::Send(room_msg("lobby", "live")));
        s.links.envelope(A, ToLink::Send(room_msg("side", "other room")));
        let own = Envelope::new(MessageType::Notice, &HUB)
            .with_room("lobby")
            .with_body(Raw::text("--- a replay of lobby is running ---"));
        s.links.envelope(A, ToLink::Now(own));
        settle().await;
        assert_eq!(s.to(A), ["other room", "--- a replay of lobby is running ---"]);
        let _ = records.send(records_of("lobby", 1));
        settle().await;
        assert_eq!(
            s.to(A)[2..],
            [
                "--- replay: 1 message since 2026-11-13T11:04:05Z ---".to_string(),
                "lobby 0".into(),
                "--- end of replay ---".into(),
                "live".into()
            ]
        );
        assert_eq!(s.done(), done_of(&[(A, "lobby", 0)]));
        // A failed send is a drop, counted where it failed.
        s.sink.fail.store(true, std::sync::atomic::Ordering::Relaxed);
        s.links.envelope(A, ToLink::Send(room_msg("lobby", "lost")));
        settle().await;
        let status = s.status.lock().unwrap().clone();
        assert_eq!((status.relayed_out, status.relay_drops), (6, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn an_ended_replay_stops_and_the_next_one_runs() {
        let mut s = senders();
        // Its records never come: the relay's timeout ends it, and what
        // was held is sent.
        let _stuck = s.replay_waiting(A, 0, "lobby");
        s.links.envelope(A, ToLink::Send(room_msg("lobby", "live")));
        s.replay(A, 1, "side", 1);
        settle().await;
        assert!(s.to(A).is_empty(), "the side replay waits its turn");
        s.end(A, Some("lobby"), true);
        settle().await;
        assert_eq!(s.to(A)[0], "live");
        assert_eq!(s.to(A)[2], "side 0");
        assert_eq!(s.done(), done_of(&[(A, "side", 1)]));
    }

    #[tokio::test(start_paused = true)]
    async fn part_ends_a_replay_and_parted_comes_before_any_later_room_line() {
        let mut s = senders();
        s.replay(A, 0, "lobby", 50);
        tokio::time::sleep(REPLAY_PACE * 5).await;
        s.links.envelope(A, ToLink::Send(room_msg("lobby", "live")));
        s.end(A, Some("lobby"), false);
        let parted = Envelope::new(MessageType::Parted, &HUB).with_room("lobby");
        s.links.envelope(A, ToLink::Send(parted));
        settle().await;
        let bodies = s.to(A);
        assert!(bodies.len() < 10, "the replay stopped: {bodies:?}");
        assert_eq!(bodies.last().map(String::as_str), Some("Parted"));
        assert!(
            !bodies.contains(&"live".to_string()),
            "held lines of a parted room are dropped"
        );
        // A JOIN again: a fresh replay, then live lines.
        let before = bodies.len();
        s.replay(A, 1, "lobby", 2);
        s.links.envelope(A, ToLink::Send(room_msg("lobby", "live again")));
        settle().await;
        assert_eq!(
            s.to(A)[before + 1..],
            ["lobby 0", "lobby 1", "--- end of replay ---", "live again"]
        );
        assert_eq!(s.done(), done_of(&[(A, "lobby", 1)]));
    }

    #[tokio::test(start_paused = true)]
    async fn hello_ends_every_replay_of_its_link() {
        let mut s = senders();
        s.replay(A, 0, "lobby", 50);
        let _waiting = s.replay_waiting(A, 1, "side");
        s.replay(B, 2, "lobby", 2);
        tokio::time::sleep(REPLAY_PACE * 3).await;
        s.end(A, None, false);
        let welcome = Envelope::new(MessageType::Welcome, &HUB);
        s.links.envelope(A, ToLink::Send(welcome));
        settle().await;
        let bodies = s.to(A);
        assert!(bodies.len() < 10);
        assert_eq!(bodies.last().map(String::as_str), Some("Welcome"));
        assert_eq!(s.to(B).len(), 4, "another link's replay runs on");
        assert_eq!(s.done(), done_of(&[(B, "lobby", 2)]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_closed_link_stops_its_sender() {
        let mut s = senders();
        s.replay(A, 0, "lobby", 50);
        tokio::time::sleep(REPLAY_PACE * 3).await;
        s.links.close(&A);
        settle().await;
        assert!(s.to(A).len() < 10);
        assert!(s.links.senders.is_empty());
    }

    #[tokio::test]
    async fn room_envelopes_are_recorded_with_no_client_but_not_the_join_notice() {
        let dir = tempfile::tempdir().unwrap();
        let history = History::start(
            dir.path().join("h"),
            crate::config::HistorySettings {
                retention: Duration::from_secs(86_400),
                max_bytes: 1_000_000,
                replay_window: Duration::from_secs(3600),
                replay_max: 100,
            },
            Default::default(),
        )
        .unwrap();
        let mut f = fixture();
        f.relay.history = history.clone();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        let about = from_hub(MessageType::Notice)
            .with_room("lobby")
            .with_body(Raw::text("room lobby: unregistered; mode=+nt; topic=(none)"));
        f.hub(about, Inbound::Relay);
        let said = room_msg("lobby", "hello");
        f.hub(said.clone(), Inbound::Relay);
        let joined = from_hub(MessageType::Joined)
            .with_room("lobby")
            .with_body(members(&[&OTHER]));
        f.hub(joined, Inbound::Relay);
        let mut echo = client(MessageType::Action)
            .with_room("lobby")
            .with_body(Raw::text("waves"));
        echo.nick = Some("me".into());
        f.hub(echo.clone(), Inbound::Relay);
        let records = history
            .query(Query {
                room: "lobby".into(),
                since_ms: None,
                max: 10,
            })
            .await
            .unwrap();
        let got: Vec<Envelope> = records.iter().map(|r| Envelope::decode(&r.envelope).unwrap()).collect();
        assert_eq!(got, vec![said, echo]);
        assert!(history.stop(Duration::from_secs(5)).await);
    }
}
