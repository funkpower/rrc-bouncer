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
//! - A client's room MSG, ACTION and NOTICE go upstream unchanged, and its
//!   clients see them only when the upstream hub sends them back: rrcd
//!   sends a room envelope to every member of the room, the sender's link
//!   included, so the bouncer's link gets each of its own back, with the
//!   client's K_ID, K_FMT and other keys, and K_SRC set to the user. The
//!   relay passes that copy to every client in the room, the sender
//!   included. A client sees each envelope once and the sender gets its
//!   echo, and nothing is shown that the hub refused.
//! - rrcd sends a direct NOTICE only to its destination. The relay gives a
//!   copy (K_SRC the user) to every client of the local hub, the sender
//!   included, when it passes the NOTICE upstream, unless it is addressed
//!   to the user, whose copy comes back from the hub.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ciborium::Value;
use rns_runtime::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc};

use crate::bouncer::local_hub_name;
use crate::config::normalize_room;
use crate::envelope::{Caps, Envelope, Limits, MessageType, Raw, Welcome};
use crate::upstream::{Command, Inbound, SharedStatus, UpstreamEvent, local_name};

/// A link that has not identified this long after it was established is
/// closed.
pub const IDENTIFY_TIMEOUT: Duration = Duration::from_secs(30);
/// A link of an identity that is not allowed is closed this long after
/// the ERROR telling it so, so the ERROR goes out first.
pub const REFUSE_CLOSE_DELAY: Duration = Duration::from_secs(1);
/// rrcd answers a slash command with NOTICEs that name no room, sent to
/// the link that gave the command. Such a NOTICE this soon after a client's
/// command goes to that client; any other goes to every client.
const COMMAND_REPLY_WINDOW: Duration = Duration::from_secs(10);
/// How long a client's room envelope waits for its echo, for routing an
/// ERROR to it. The session may hold it up to `CLIENT_HOLD` (60 s) while
/// its room is joined.
const ECHO_WAIT: Duration = Duration::from_secs(120);
/// How long a direct NOTICE or a command, which the hub answers only when
/// it refuses them, can be blamed for an ERROR.
const ANSWER_WAIT: Duration = Duration::from_secs(15);
/// The most client envelopes remembered for routing ERRORs.
const MAX_SENT: usize = 1024;
/// The text of the ERROR a link of another identity gets.
pub const NOT_ALLOWED: &str = "identity not allowed on this hub";

pub type Link = [u8; 16];

/// What the relay asks of the task. Short-lived, so not boxed.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    /// Send an envelope to a client link.
    Client(Link, Envelope),
    /// Close a client link now.
    Close(Link),
    /// Close a client link after `REFUSE_CLOSE_DELAY`.
    CloseLater(Link),
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
}

impl Client {
    fn new(now: Instant) -> Self {
        Self {
            state: ClientState::Unidentified,
            linked_at: now,
            welcomed: false,
            rooms: BTreeSet::new(),
            pending: BTreeSet::new(),
        }
    }
}

/// What kind of client envelope a hub's ERROR may answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// MSG, ACTION or NOTICE in a room: echoed by the hub.
    Room,
    /// A direct NOTICE.
    Direct,
    /// A MSG or NOTICE whose body starts with `/`: rrcd's slash command.
    Command,
}

/// A client envelope handed upstream, until its echo, an ERROR for it, or
/// its wait is over.
#[derive(Debug)]
struct Sent {
    id: Vec<u8>,
    link: Link,
    room: Option<String>,
    kind: Kind,
    at: Instant,
}

fn kind_of(env: &Envelope) -> Kind {
    let command = matches!(env.t, MessageType::Msg | MessageType::Notice)
        && env.dst.is_none()
        && env.text_body().is_some_and(|b| b.starts_with('/'));
    if command {
        Kind::Command
    } else if env.t == MessageType::Notice && env.dst.is_some() {
        Kind::Direct
    } else {
        Kind::Room
    }
}

/// The kind of client envelope rrcd's ERROR text answers (`router.py`
/// `_handle_message`, `_handle_direct_notice`, `commands.py`); None for
/// texts any of them may get (rate limited, message too large, ...).
fn error_kind(text: &str) -> Option<Kind> {
    match text {
        "destination not connected"
        | "direct notice must not include room"
        | "direct notice requires destination identity" => Some(Kind::Direct),
        "unrecognized command" => Some(Kind::Command),
        "message requires room name"
        | "no such room"
        | "no outside messages (+n)"
        | "banned from room"
        | "room is moderated (+m)" => Some(Kind::Room),
        _ => None,
    }
}

/// The local hub's side of one upstream hub.
pub struct Relay {
    /// The local hub's identity hash: K_SRC of its own envelopes.
    hub: [u8; 16],
    user: [u8; 16],
    upstream: [u8; 16],
    allowed: Vec<[u8; 16]>,
    part_upstream: bool,
    commands: mpsc::Sender<Command>,
    /// The upstream session's status, for the room set.
    session: SharedStatus,
    status: SharedRelayStatus,
    changed: bool,
    clients: BTreeMap<Link, Client>,
    /// The upstream hub's last WELCOME.
    welcome: Option<Welcome>,
    /// The upstream hub's identity hash, from its WELCOME.
    upstream_hub: Option<Vec<u8>>,
    /// Rooms the bouncer is in upstream, with their members (identity
    /// hashes, one per link, as rrcd lists them).
    joined: BTreeMap<String, Vec<Vec<u8>>>,
    /// The hub sends member lists in JOINED (`include_joined_member_list`).
    member_lists: bool,
    /// Rooms a client's JOIN added to the room set that are not joined yet.
    added: BTreeSet<String>,
    sent: VecDeque<Sent>,
    last_command: Option<(Link, Instant)>,
}

/// Everything a relay needs.
pub struct RelayConfig {
    pub hub_identity: [u8; 16],
    pub user: [u8; 16],
    pub upstream: [u8; 16],
    /// The user's identity and `allowed_identities`.
    pub allowed: Vec<[u8; 16]>,
    pub part_upstream: bool,
    pub commands: mpsc::Sender<Command>,
    pub session: SharedStatus,
    pub status: SharedRelayStatus,
}

impl Relay {
    pub fn new(config: RelayConfig) -> Self {
        Self {
            hub: config.hub_identity,
            user: config.user,
            upstream: config.upstream,
            allowed: config.allowed,
            part_upstream: config.part_upstream,
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
            last_command: None,
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

    /// The WELCOME a client gets: the upstream hub's name, version, ACTION
    /// and direct NOTICE caps and limits, with `resource_envelope` off
    /// until the bouncer relays Resources (M5); before the first upstream
    /// WELCOME, the local hub's name and rrcd's defaults.
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
                    resource_envelope: false,
                    action: true,
                    direct_notice: true,
                },
                Limits::default(),
            ),
        };
        let body = Welcome {
            hub: Some(name),
            ver: Some(ver.unwrap_or_else(|| Value::Text(env!("CARGO_PKG_VERSION").into()))),
            caps: Caps {
                resource_envelope: false,
                ..caps
            },
            limits,
        };
        self.own(MessageType::Welcome).with_body(body.to_body())
    }

    fn members_body(members: &[Vec<u8>]) -> Raw {
        Raw::from_value(&Value::Array(members.iter().map(|m| Value::Bytes(m.clone())).collect()))
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

    fn deliver(&self, out: &mut Vec<Output>, links: &[Link], env: &Envelope) {
        for link in links {
            out.push(Output::Client(*link, env.clone()));
        }
        self.tally(|s| s.relayed_out += links.len() as u64);
    }

    fn in_room_set(&self, room: &str) -> bool {
        self.session.lock().is_ok_and(|s| s.rooms.iter().any(|r| r == room))
    }

    pub fn link_established(&mut self, link: Link, now: Instant) {
        self.clients.entry(link).or_insert_with(|| Client::new(now));
    }

    pub fn link_closed(&mut self, link: Link) {
        if self.clients.remove(&link).is_some() {
            self.show_clients();
        }
        if self.last_command.is_some_and(|(l, _)| l == link) {
            self.last_command = None;
        }
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
                if let Some(client) = self.clients.get_mut(&link) {
                    client.welcomed = true;
                    client.rooms.clear();
                    client.pending.clear();
                }
                out.push(Output::Client(link, self.welcome_envelope()));
            }
            MessageType::ResourceEnvelope => {
                tracing::info!(
                    "client {}: dropped a RESOURCE_ENVELOPE: the bouncer does not relay Resources yet",
                    hex::encode(link)
                );
            }
            _ if !welcomed => out.push(Output::Client(link, self.error(None, "send HELLO first"))),
            MessageType::Join => self.join(link, &env, &mut out),
            MessageType::Part => self.part(link, identity, &env, &mut out),
            MessageType::Msg | MessageType::Action | MessageType::Notice => {
                self.message(link, env, now, &mut out);
            }
            other => tracing::debug!("client {}: ignored a {other:?}", hex::encode(link)),
        }
        out
    }

    /// The room a JOIN or PART names, checked as rrcd checks it, or the
    /// text of the ERROR rrcd would send (naming no room).
    fn room_arg(&self, env: &Envelope, verb: &str) -> Result<String, String> {
        let Some(room) = env.room.as_deref() else {
            return Err(format!("{verb} requires room name"));
        };
        let Some(room) = normalize_room(room) else {
            return Err("room name must not be empty".into());
        };
        let max = self.limits().max_room_name_bytes;
        if room.len() as u64 > max {
            return Err(format!("room name too long: {} bytes > {max} bytes", room.len()));
        }
        Ok(room)
    }

    fn join(&mut self, link: Link, env: &Envelope, out: &mut Vec<Output>) {
        let room = match self.room_arg(env, "JOIN") {
            Ok(room) => room,
            Err(text) => return out.push(Output::Client(link, self.error(None, &text))),
        };
        if let Some(members) = self.joined.get(&room) {
            let mut joined = self.own(MessageType::Joined).with_room(&room);
            if self.member_lists {
                joined.body = Some(Self::members_body(members));
            }
            if let Some(client) = self.clients.get_mut(&link) {
                client.pending.remove(&room);
                client.rooms.insert(room.clone());
            }
            self.deliver(out, &[link], &joined);
            self.replay(link, &room, out);
            return;
        }
        if self.joined.len() as u64 >= self.limits().max_rooms_per_session {
            return out.push(Output::Client(link, self.error(None, "too many rooms")));
        }
        let new = !self.in_room_set(&room);
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

    /// M4: replay the room's history to a client that just joined it.
    fn replay(&mut self, _link: Link, _room: &str, _out: &mut Vec<Output>) {}

    /// M4: keep an envelope from the hub in the room's history.
    fn record(&mut self, _env: &Envelope) {}

    fn part(&mut self, link: Link, identity: [u8; 16], env: &Envelope, out: &mut Vec<Output>) {
        let room = match self.room_arg(env, "PART") {
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
        if let Some(client) = self.clients.get_mut(&link) {
            client.rooms.remove(&room);
            client.pending.remove(&room);
        }
        let answer = parted(self, identity);
        self.deliver(out, &[link], &answer);
        if !self.part_upstream {
            return;
        }
        if self.commands.try_send(Command::Part(room.clone())).is_err() {
            self.tally(|s| s.relay_drops += 1);
            tracing::warn!("the upstream session's queue is full: room {room} stays joined upstream");
            return;
        }
        self.added.remove(&room);
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
            let env = parted(self, id);
            self.deliver(out, &[other], &env);
        }
    }

    fn message(&mut self, link: Link, env: Envelope, now: Instant, out: &mut Vec<Output>) {
        let kind = kind_of(&env);
        let room = env.room.as_deref().and_then(normalize_room);
        // A well-formed direct NOTICE to someone other than the user, while
        // there is a session to send it: the hub does not echo it.
        let copy = kind == Kind::Direct
            && env.room.is_none()
            && env.dst.as_deref() != Some(&self.user[..])
            && self.connected();
        let id = env.id.clone();
        let local = copy.then(|| {
            let mut copy = env.clone();
            copy.src = self.user.to_vec();
            copy
        });
        match self.commands.try_send(Command::Send(env)) {
            Ok(()) => {
                self.tally(|s| s.relayed_in += 1);
                if self.sent.len() >= MAX_SENT {
                    self.sent.pop_front();
                }
                self.sent.push_back(Sent {
                    id,
                    link,
                    room: room.clone(),
                    kind,
                    at: now,
                });
                if kind == Kind::Command {
                    self.last_command = Some((link, now));
                }
                if let Some(copy) = local {
                    let links = self.welcomed();
                    self.deliver(out, &links, &copy);
                }
            }
            Err(_) => {
                self.tally(|s| s.relay_drops += 1);
                out.push(Output::Client(
                    link,
                    self.error(room.as_deref(), "not sent to the hub: the bouncer's queue is full"),
                ));
            }
        }
    }

    fn connected(&self) -> bool {
        self.upstream_hub.is_some()
    }

    /// The client whose envelope most likely caused a hub ERROR (see
    /// DESIGN.md § Local hub), taken off the list; None if none fits.
    fn blame(&mut self, room: Option<&str>, text: &str) -> Option<Link> {
        let kind = error_kind(text);
        let fits = |s: &Sent| kind.is_none_or(|k| s.kind == k) && room.is_none_or(|r| s.room.as_deref() == Some(r));
        // Room envelopes leave the list at their echo, so the oldest left
        // is the one the hub is answering; direct NOTICEs and commands
        // leave it only by time, so the newest is the likeliest.
        let pos = if kind == Some(Kind::Room) {
            self.sent.iter().position(fits)
        } else {
            self.sent.iter().rposition(fits)
        };
        pos.and_then(|p| self.sent.remove(p)).map(|s| s.link)
    }

    pub fn upstream(&mut self, event: UpstreamEvent, now: Instant) -> Vec<Output> {
        let mut out = Vec::new();
        match event {
            UpstreamEvent::Up { welcome, hub } => {
                self.welcome = Some(welcome);
                self.upstream_hub = Some(hub);
            }
            UpstreamEvent::Down => {
                self.upstream_hub = None;
                self.joined.clear();
            }
            UpstreamEvent::SendDropped { envelope, reason } => {
                let owner = self
                    .sent
                    .iter()
                    .position(|s| s.id == envelope.id)
                    .and_then(|p| self.sent.remove(p))
                    .map(|s| s.link);
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
        }
        out
    }

    fn hub_envelope(&mut self, env: Envelope, inbound: Inbound, now: Instant, out: &mut Vec<Output>) {
        match inbound {
            Inbound::Handled | Inbound::Welcomed => {}
            Inbound::JoinConfirmed(room) => {
                let members = env.members();
                self.member_lists = members.is_some();
                self.joined.insert(room.clone(), members.unwrap_or_default());
                self.added.remove(&room);
                let answer = self.outward(&env);
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
                    self.replay(link, &room, out);
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
                let answer = self.outward(&env);
                let members = self.in_room(&room);
                for link in &members {
                    if let Some(client) = self.clients.get_mut(link) {
                        client.rooms.remove(&room);
                    }
                }
                self.deliver(out, &members, &answer);
            }
            Inbound::HubError(text) => {
                let answer = self.outward(&env);
                let room = env.room.as_deref();
                match self.blame(room, &text) {
                    Some(link) if self.clients.contains_key(&link) => self.deliver(out, &[link], &answer),
                    Some(_) => tracing::debug!("hub error {text:?} for a client that is gone"),
                    None => {
                        let links = self.welcomed();
                        self.deliver(out, &links, &answer);
                    }
                }
            }
            Inbound::Relay => self.relay(env, now, out),
        }
    }

    fn relay(&mut self, env: Envelope, now: Instant, out: &mut Vec<Output>) {
        let room = env.room.clone();
        match env.t {
            MessageType::ResourceEnvelope => {
                tracing::info!("dropped a RESOURCE_ENVELOPE from the hub: the bouncer does not relay Resources yet");
                return;
            }
            MessageType::Joined | MessageType::Parted => {
                if let (Some(room), Some(changed)) = (&room, env.members())
                    && let Some(members) = self.joined.get_mut(room)
                {
                    if env.t == MessageType::Joined {
                        members.extend(changed);
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
        self.record(&env);
        let links = match (&room, &env.dst) {
            (Some(room), _) => self.in_room(room),
            (None, Some(_)) => self.welcomed(),
            (None, None) => match self.last_command {
                Some((link, at))
                    if env.t == MessageType::Notice
                        && now.saturating_duration_since(at) < COMMAND_REPLY_WINDOW
                        && self.clients.contains_key(&link) =>
                {
                    vec![link]
                }
                _ => self.welcomed(),
            },
        };
        let env = self.outward(&env);
        self.deliver(out, &links, &env);
    }

    /// Time passing: close links that never identified, forget old sends.
    pub fn tick(&mut self, now: Instant) -> Vec<Output> {
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
        self.sent.retain(|s| {
            let wait = if s.kind == Kind::Room { ECHO_WAIT } else { ANSWER_WAIT };
            now.saturating_duration_since(s.at) < wait
        });
        late.into_iter().map(Output::Close).collect()
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
    pub events: mpsc::Receiver<UpstreamEvent>,
    pub relay: Relay,
    /// Woken when the clients change, for status.json.
    pub changed: Arc<Notify>,
}

/// How often the relay's clock runs.
const TICK: Duration = Duration::from_secs(1);

/// The local hub's task: client links in, upstream events in, envelopes to
/// clients out.
pub async fn run(hub: LocalHub) {
    let LocalHub {
        mut registered,
        identities,
        mut identified,
        events: mut upstream,
        mut relay,
        changed,
    } = hub;
    let handle = registered.handle.clone();
    let tag = hex::encode(handle.destination_hash());
    let events = &mut registered.events;
    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let now = Instant::now();
        let outputs = tokio::select! {
            // Established and identified before packets, so the log reads
            // in order (packets do not depend on it, see `identity_gate`).
            // Closed after packets: rsReticulum queues a link's last
            // packets before its close event, and they are still handled.
            biased;
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
            Some((data, link)) = events.link_packets.recv() => {
                let mut out = Vec::new();
                if let Some(identity) = link_identity(&identities, &link) {
                    out = relay.link_identified(link, identity, now);
                }
                if relay.is_allowed(&link) {
                    out.extend(relay.client_packet(link, &data, now));
                } else {
                    tracing::debug!("hub {tag}: ignored a packet on a link not identified as an allowed identity");
                }
                out
            }
            Some(event) = upstream.recv() => relay.upstream(event, now),
            Some(link) = events.links_closed.recv() => {
                if let Ok(mut map) = identities.lock() {
                    map.remove(&link);
                }
                relay.link_closed(link);
                tracing::info!("hub {tag}: link {} closed", hex::encode(link));
                Vec::new()
            }
            _ = ticker.tick() => relay.tick(now),
            Some(_) = events.packets.recv() => Vec::new(),
            Some(_) = events.link_packet_proofs.recv() => Vec::new(),
            Some(_) = events.resource_completions.recv() => Vec::new(),
            Some(_) = events.resource_proofs.recv() => Vec::new(),
            Some(_) = events.resource_events.recv() => Vec::new(),
            Some(_) = events.channel_messages.recv() => Vec::new(),
        };
        for output in outputs {
            match output {
                Output::Client(link, env) => {
                    if let Err(e) = handle.send_link_packet(link, env.encode()).await {
                        relay.tally(|s| s.relay_drops += 1);
                        tracing::debug!(
                            "hub {tag}: could not send a {:?} to link {}: {e}",
                            env.t,
                            hex::encode(link)
                        );
                    }
                }
                Output::Close(link) => {
                    tracing::info!(
                        "hub {tag}: closing link {}: not identified in {}s",
                        hex::encode(link),
                        IDENTIFY_TIMEOUT.as_secs()
                    );
                    let _ = handle.close_link(link, CloseReason::DestinationClosed, true).await;
                }
                Output::CloseLater(link) => {
                    let handle = handle.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(REFUSE_CLOSE_DELAY).await;
                        let _ = handle.close_link(link, CloseReason::DestinationClosed, true).await;
                    });
                }
            }
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
            commands: tx,
            session: session.clone(),
            status: status.clone(),
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
        assert!(!body.caps.resource_envelope && body.caps.action && body.caps.direct_notice);

        f.up();
        let body = Welcome::from_body(f.hello(A).body.as_ref().unwrap());
        assert_eq!(body.hub.as_deref(), Some("test hub (via rrc-bouncer)"));
        assert_eq!(body.ver, Some(Value::Text("1.2".into())));
        assert_eq!(body.limits, upstream_welcome().limits);
        assert!(!body.caps.resource_envelope && body.caps.action && !body.caps.direct_notice);
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
        assert_eq!(
            error(&mut f, client(MessageType::Join).with_room("d")),
            "too many rooms"
        );
        assert_eq!(error(&mut f, client(MessageType::Part)), "PART requires room name");
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
        // Two JOINED answers and two echoes.
        assert_eq!((status.relayed_in, status.relayed_out), (1, 4));
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
        assert_eq!(sent.iter().map(|(l, _)| *l).collect::<Vec<_>>(), vec![A, B]);
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
            links(&f.hub(hub_error(Some("lobby"), text), Inbound::HubError(text.into()))),
            vec![C],
            "the newest fitting envelope, for an error any kind may get"
        );
        let text = "no such room";
        assert_eq!(
            links(&f.hub(hub_error(Some("lobby"), text), Inbound::HubError(text.into()))),
            vec![A]
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
        f.packet(
            B,
            &client(MessageType::Msg).with_room("lobby").with_body(Raw::text("/who")),
        );
        let reply = || from_hub(MessageType::Notice).with_body(Raw::text("members: x"));
        let sent = sent_to(&f.hub(reply(), Inbound::Relay));
        assert_eq!(sent.len(), 1);
        assert_eq!((sent[0].0, sent[0].1.src.clone()), (B, HUB.to_vec()));
        let later = f.now + COMMAND_REPLY_WINDOW;
        assert_eq!(
            links(
                &f.relay
                    .upstream(UpstreamEvent::Envelope(reply(), Inbound::Relay), later)
            ),
            vec![A, B]
        );
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

    #[test]
    fn resource_envelopes_are_dropped() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.in_rooms(A, &["lobby"]);
        let env = client(MessageType::ResourceEnvelope).with_room("lobby");
        assert!(f.packet(A, &env).is_empty());
        assert!(f.command().is_none());
        let env = from_hub(MessageType::ResourceEnvelope).with_room("lobby");
        assert!(f.hub(env, Inbound::Relay).is_empty());
    }

    #[test]
    fn the_rooms_of_a_down_session_are_joined_again_through_it() {
        let mut f = fixture();
        f.up();
        f.joined_upstream("lobby", &[&USER]);
        f.in_rooms(A, &["lobby"]);
        f.relay.upstream(UpstreamEvent::Down, f.now);
        // A JOIN now waits for the session.
        f.connect(B, USER);
        f.hello(B);
        assert!(f.packet(B, &client(MessageType::Join).with_room("lobby")).is_empty());
        assert_eq!(f.command(), Some(Command::Join("lobby".into())));
        f.up();
        let out = f.joined_upstream("lobby", &[&USER]);
        assert_eq!(links(&out), vec![B], "A was in the room all along");
        let msg = from_hub(MessageType::Msg).with_room("lobby");
        assert_eq!(links(&f.hub(msg, Inbound::Relay)), vec![A, B]);
    }
}
