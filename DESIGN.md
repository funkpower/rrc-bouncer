# rrc-bouncer design

An IRC bouncer (ZNC, soju) for [RRC](https://rrc.kc1awv.net/), Reticulum Relay Chat. One always-on
process holds the user's RRC identity, keeps one link to each upstream hub, stays in the user's
rooms and keeps what is said there. Every client the user runs (NomadNet, MeshChatX, rBrowser2,
rettui, weechat-rrc) links to the bouncer instead of the hub, speaks plain RRC to it, and gets the
backlog when it joins a room. It is the RRC counterpart of
[lxmf-bouncer](https://git.pumpin.online/funkpower/lxmf-bouncer), and it is laid out the same way:
read that repository before writing code here.

## The two problems

1. **One identity on several devices.** RRC knows a user by the identity a link identified as.
   Two clients holding the same identity open two links to the hub. rrcd allows that, but its
   direct-notice routing keeps only the newest link per identity and drops the identity when that
   link closes, even with an older one still up (`rrcd/session.py:40,84,121-123`, rrcd 0.3.2), so
   direct messages fail with "destination not connected". MeshChatX-hosted hubs cap an identity at 3
   links (`backend/rrc/server.py:40,403-427`). Behind the bouncer there is exactly one upstream link
   per hub, whatever the number of clients.
2. **No history.** Hubs keep none (spec
   [state machine](https://rrc.kc1awv.net/4-RRC-state-machine.html#room-state-and-membership-semantics):
   membership is per link and ends with it; [optional](https://rrc.kc1awv.net/5-RRC-optional.html#logging-and-archival):
   "There is no standardized replay mechanism"). A client that was not linked missed it. The
   bouncer is always linked, so it keeps every room's history and replays it.

## Protocol facts the design rests on

Source of truth: the spec at <https://rrc.kc1awv.net/> and the reference hub
[kc1awv/rrcd](https://github.com/kc1awv/rrcd) (MIT; `rrcd/constants.py`, `envelope.py`,
`router.py`, `session.py`).

- Hub destination: `rrc.hub`, Reticulum `SINGLE` IN destination, aspects `rrc`, `hub`. Hubs
  announce with CBOR app data `{"proto": "rrc", "v": 1, "hub": NAME}`.
- Transport: a Reticulum Link. The client calls `link.identify()`, then sends `HELLO`, the hub
  answers `WELCOME`. rrcd ignores anything on a link that has not identified
  (`router.py:83-88`) and overwrites `K_SRC` with the link's identity (`router.py:800-805`).
- Envelope: a CBOR map with integer keys `K_V=0, K_T=1, K_ID=2, K_TS=3, K_SRC=4, K_ROOM=5,
  K_BODY=6, K_NICK=7, K_DST=8`. rBrowser2 adds `K_FMT=64` (colour spans): pass unknown keys
  through untouched.
- Types: `HELLO=1 WELCOME=2 JOIN=10 JOINED=11 PART=12 PARTED=13 MSG=20 NOTICE=21 ACTION=22
  PING=30 PONG=31 ERROR=40 RESOURCE_ENVELOPE=50`.
- HELLO body `{0: name, 1: ver, 2: caps}`; WELCOME body `{0: hub, 1: ver, 2: caps, 3: limits}`,
  limits `{0: max_nick_bytes, 1: max_room_name_bytes, 2: max_msg_body_bytes,
  3: max_rooms_per_session, 4: rate_limit_msgs_per_minute}`; caps `{0: resource_envelope,
  1: action, 2: direct_notice}`.
- A whole envelope fits one Link packet (about 481 bytes). Bigger hub text (greeting, MOTD, long
  notices) comes as `RESOURCE_ENVELOPE` (body `{0: id, 1: kind, 2: size, 3: sha256, 4: encoding}`,
  kinds `notice`, `motd`, `blob`) followed by a Reticulum Resource on the same link.
- rrcd defaults: body 350 bytes, nick 32, room name 64, 32 rooms per session, 240 messages a minute
  per link (`rrcd/config.py:29-34`). Use the limits the upstream WELCOME gives.
- Nicks are advisory and not unique ([optional](https://rrc.kc1awv.net/5-RRC-optional.html#nicknames)).
  Each envelope may carry `K_NICK`.

What the clients do, which the relay must respect:

- weechat-rrc counts a NOTICE as the hub's own only when `K_SRC` equals the identity of the hub
  destination it linked to (`plugin.c:960,1276` in funkpower/weechat-rrc). Hub-originated
  envelopes relayed to clients must carry the **bouncer's local hub identity** in `K_SRC`.
- weechat-rrc marks a line as the user's own when `K_SRC` is its identity and drops the hub's echo
  only for ids it sent itself (`rrc.c:724,791`). The bouncer must relay a client's MSG to the
  other clients and echo it back with the original `K_ID`.
- weechat-rrc and rBrowser2 stamp lines with arrival time and ignore `K_TS`; MeshChatX drops
  repeats by envelope id. Replayed envelopes keep their original `K_ID` and `K_TS` so a client can
  do either (patches to weechat-rrc and rBrowser2 are separate work).
- NomadNet signs in to RRC with its LXMF identity (`nomadnet/RRC.py:1423-1424`): for the user
  that is the same identity lxmf-bouncer holds, which is the identity this bouncer should hold.

Other implementations, for behaviour only: [rrc-hub](https://github.com/thatSFguy/reticulum-relay-chat)
(Go; replays the last 10 envelopes on JOIN between two NOTICEs), [rsRRC](https://github.com/reticulum-spb/rsRRC)
and [rsRRCD](https://github.com/reticulum-spb/rsRRCD) (Rust). **None of those three has a licence:
read them, never copy from them.** The codec here is our own, on `ciborium`.

## Architecture

```
  weechat-rrc ─┐                        ┌── link, identified as the user ──▶ hub A (rrcd)
  rBrowser2  ──┼─▶ local hub for A ─────┤
  NomadNet   ──┘   (rrc.hub, own key)   │   history: state_dir/history/<hub A>/<room>.log
                                        │
  weechat-rrc ───▶ local hub for B ─────┴── link, identified as the user ──▶ hub B
```

- **Upstream session, one per configured hub.** Opens a Link to the hub, identifies with the
  user's identity, sends HELLO (name `rrc-bouncer`, ver 1, caps it supports), stores WELCOME
  (hub name, caps, limits), JOINs every room in the hub's room set, answers PING, sends its own PING
  when idle, and reconnects with backoff (5 s doubling to 5 min) when the link drops, then rejoins.
  Details under [Upstream session](#upstream-session).
- **Local hub, one per configured hub.** A `rrc.hub` IN destination under its own identity, kept in
  `state_dir/hubs/<upstream hash>.identity` (created on first run), so its address is stable.
  It accepts Links, requires `identify()`, and accepts the user's identity and those in
  `allowed_identities` (default: none). A client from another identity gets `ERROR` and the link is closed.
  - HELLO from a client: answer WELCOME with the upstream's caps and limits and hub name
    `"<upstream name> (via rrc-bouncer)"`. Before the upstream's first WELCOME, answer with rrcd's
    defaults.
  - JOIN: add the room to the hub's room set (saved in state, so it survives a restart), JOIN
    upstream if not already in, answer the client with JOINED, then replay history (below).
  - PART: by default the bouncer stays in the room upstream and only the client's view ends
    (`part_upstream = false`, as ZNC detaches). With `part_upstream = true`, a PART from any client
    parts upstream and removes the room from the set.
  - MSG, ACTION, NOTICE (with or without `K_DST`): pass upstream unchanged (same `K_ID`, unknown
    keys; `K_NICK` the hub's `nick` when one is set), and relay the hub's echo to every client in
    that room; history records the echo.
  - Envelopes from upstream: record room traffic in history, then relay to every client linked to
    this local hub that is in the room (or, for a direct NOTICE, to every client). Rewrite `K_SRC`
    to the local hub's identity when the upstream envelope's `K_SRC` is the upstream hub's identity.
  - PING from a client: answer PONG locally.
  - Rate: the upstream rate limit is per link and every client shares it. Queue client envelopes and
    send them upstream no faster than the WELCOME limit allows (the upstream session's bucket,
    below).
  Details under [Local hub](#local-hub).
- **Announces.** The bouncer must not spread fake hubs across the network by default
  (`announce = false`). Clients reach it by the hash `hashes` prints. See [Announces](#announces).

## Announces

With `announce = false` (the default) the local hubs are never announced to the network; a client
reaches one by its hash, through a path request. M1 settled whether that works with the pinned
rsReticulum (`deps/rsReticulum` at 14241a3), from the code and on a private network on 127.0.0.1
(`tests/e2e.sh`, Python RNS 1.6.0 for rnsd and the clients).

- **rsReticulum answers a path request for a local IN destination that has never announced.**
  `rns-transport/src/actor/outbound.rs:566` (`handle_inbound_path_request`) finds the destination
  in `local_destinations` (:625) and sends its link manager
  `DestinationEvent::AnnounceRequested { path_response: true, tag, attached_interface }`
  (:631-640); `rns-runtime/src/link_manager.rs:2086-2094` answers with an announce of context
  `PATH_RESPONSE`, carrying the destination's `default_app_data`, on the interface the request came
  in on (unit test `path_response_announces_carry_default_app_data`, link_manager.rs:7952). In the
  e2e phase "direct", a client connected straight to a TCPServerInterface of the bouncer's asked
  for a path to the never-announced hub: the bouncer logged `answering path request — destination
  is local`, the client got the hub as a path response (no announce), linked, identified, and got
  PONG for PING.
- **A transport node between them passes a request for an unknown destination on only in some
  interface modes.** Python rnsd forwards it when the request came in on an interface in mode
  `gateway`, `access_point`, `roaming` or `internal`, or from one of its own shared-instance
  clients (`RNS/Transport.py` `path_request`, `should_search_for_unknown`;
  `Interface.DISCOVER_PATHS_FOR`). A client on a `full`-mode interface of the rnsd the bouncer
  uses never reaches a hub that rnsd has not heard of: in M1, before the bouncer sent anything in
  `tcp` mode, the client's path request through rnsd went unanswered for 30 s.
- **So the bouncer makes sure its own rnsd knows the path, without announcing.** A path response
  is not rebroadcast by transport nodes (`Transport.py:2603`), so it goes one hop.
  - `shared_instance`: rsReticulum does it. Registering a destination with a shared instance sends
    the instance a path-response announce (`rns-transport/src/actor/mod.rs:803-830`, logged as
    `auto-announce triggered for shared instance registration`), as Python RNS does for its own
    shared-instance clients. rnsd keeps the path, passes the path response on to its other local
    clients only (`Transport.py:2627-2647`), and answers path requests from any interface itself
    (e2e phase "shared": `Answering path request for <hub> … path is known`, PING/PONG through it,
    and a watcher on rnsd saw no announce).
  - `tcp`: the bouncer sends a path response for each hub on the uplink when it comes online
    (checked every 10 s through `interface_stats`), and again on every online uplink every 5
    minutes. e2e phase "tcp": rnsd answers the client's path request from its path table, the
    client links and gets PONG, and a watcher on the same rnsd saw no announce.
  - The refresh is needed because rnsd drops a path nobody has used after `UNUSED_PATH_LINGER`
    (1.75 h with transport) or `UNUSED_PATH_LINGER_LEAF` (12 min without), counted from the last
    path table update (`Transport.py:228-229,1145-1148`); answering a path request does not count
    as use. Five minutes is under both. It also covers an rnsd restart or a reconnect that falls
    between two 10 s checks, which never shows as offline. Each refresh is a new path response:
    sent without a tag, rsReticulum builds it afresh (`create_announce_at`,
    `rns-identity/src/destination.rs:976-1060`) with new random bytes and the current time in its
    random blob, and rnsd replaces a path entry when the blob is new and its emission time is later
    than the entry's (`Transport.py:2520-2523`), which resets the linger (`:2726`). e2e phases
    "tcp: rnsd TL drops never-used paths" and "tcp: the bouncer refreshes the path" run rnsd with
    the linger set to 12 s (`tests/rnsd_linger.py`) and the bouncer's refresh set to 4 s
    (`RRC_BOUNCER_UPLINK_REFRESH_SECS`, for tests): with no refresh rnsd logs `Removing never used
    path` after the linger; with it, every refresh shows as a path table update, the path stays for
    three linger times, and a client then gets the path and PONG.
  - `config_dir`: nothing is sent. The hub answers the path requests that reach it; the user's
    interfaces decide how far one gets (for example `mode = gateway` on the transport node's
    interface for clients). Path responses on every interface of an arbitrary config would put the
    hub on radio and other links nobody asked for.
- **Known issue in `shared_instance` mode: when rnsd restarts, rsReticulum announces the hubs to
  the network.** On reconnecting to the shared instance it asks every local destination for a
  *normal* announce (`SharedConnectionRestored` → `request_announces_from_local_destinations`,
  `rns-transport/src/actor/mod.rs:1004-1010,1444-1462`, `AnnounceRequest::normal`; with no
  announce handler the link manager sends it as it is, link_manager.rs:2089-2091), and rnsd
  rebroadcasts a normal announce from a local client at once. Python RNS sends a path response in
  that place (`Transport.shared_connection_reappeared`, `Transport.py:4203-4207`). Seen in e2e
  phase "shared: rnsd S restarts": rnsd logged `Rebroadcasting announce for <hub> with hop count 0`
  and a watcher on another interface received a normal announce of the hub with its app data. The
  runtime has no hook to stop it (`register_destination` sets no announce handler). The fix belongs
  in rsReticulum (`path_response: true` on restore); until the pinned version has it,
  `announce = false` holds in `shared_instance` mode only until the shared instance restarts, and
  `tcp` mode is the one to use where that matters. The e2e test prints it as `known`, and as `ok`
  once it stops happening.
- **Known issue in `shared_instance` mode: when the bouncer restarts and rnsd does not, rnsd may
  announce the hubs to the network.** When rnsd answers a path request for a hub from its path
  table and the path came from a local client, it records the request in
  `pending_local_path_requests` (`Transport.py:3788-3797`) and clears it only when a local client
  sends a path response for that hub. The registration path response of the restarted bouncer is
  that path response: rnsd puts it in the announce table without `block_rebroadcasts` and with no
  attached interface (`Transport.py:2627-2647`), and the retransmit loop sends it as a normal
  announce on every interface (`:920-951`). Seen in e2e phase "shared: the bouncer restarts, rnsd
  S stays up" (after a client had asked rnsd S for the path): rnsd logged `Rebroadcasting announce
  for <hub> with hop count 0` and the watcher received a normal announce of the hub. It happens
  once per restart, and only if a client asked rnsd for a hub's path since the bouncer last
  registered it. It is Python RNS behaviour that any shared-instance client restarting would see;
  the bouncer cannot avoid it short of not sending the registration path response, which
  rsReticulum sends by itself. `tcp` mode avoids it too. The e2e test prints it as `known`.
- `announce = true`: normal announces 5 s after start, then every `announce_interval_minutes`,
  with app data `{"proto": "rrc", "v": 1, "hub": NAME}` (cbor2's bytes, checked in a unit test).
- NAME is `rrc-bouncer for <upstream hash>` until the upstream hub's first WELCOME, then
  `"<upstream name> (via rrc-bouncer)"`, the upstream name cut on a character boundary so NAME is
  at most 96 bytes and the app data at most 120, well inside an announce. It is the local hub's `default_app_data`
  (`DestinationHandle::set_default_app_data`), and announces and path responses are sent without
  app data of their own, so both carry it. A WELCOME with a new name sets it and, at once, announces
  the hubs (`announce = true`) or sends the uplink its path responses (`tcp`), so the network does
  not wait for the next announce or refresh to learn the name. The name is not saved: after a
  restart it is the placeholder until the WELCOME. e2e phase "rrcd": a client asking rnsd for the
  local hub's path after the WELCOME gets `e2e hub (via rrc-bouncer)`.

## Upstream session

`src/upstream.rs`: a `Session` that is the protocol with no I/O (envelopes and the time in, what to
send out; unit-tested), and `run`, the task per hub that owns the link and feeds it.

- **Connect.** `connect_link` to the hub's `rrc.hub` with `identify: true` (path wait 30 s, the
  whole connect at most 60 s). A failed connect asks for the path again before the next try, in
  case it went stale. The first try is at once.
- **HELLO.** Body `{0: "rrc-bouncer", 1: 1, 2: caps}` with the hub's `nick` as `K_NICK`. Caps:
  `resource_envelope` false until M5 receives Resources; `action` and `direct_notice` true, as
  the relay (M3) passes both on. rrcd keeps a client's caps without acting on them. No WELCOME in
  30 s ends the link. A WELCOME is taken once per link; with no body, its limits are rrcd's
  defaults.
- **JOIN, one at a time.** The room set is joined in order (config rooms first), the next JOIN
  only after the last one's answer, because rrcd's errors for a refused JOIN do not always name
  the room. rrcd sends a joiner's JOINED and PARTED from the hub's identity, so `K_SRC` cannot tell
  the bouncer's own from another member's; the body can, when rrcd's
  `include_joined_member_list` puts the member's identity hash in it, and otherwise the bouncer's
  own is the one without a nick (other members' carry theirs). The answer is:
  - `JOINED` for the room: joined. A JOINED for a room whose JOIN timed out or was rate limited
    is a late answer and is taken too.
  - A refusal (`router.py` `_handle_join`, `service.py` `_norm_room`): failed with the hub's text,
    and not tried again on this link. Without a room: `JOIN requires room name`, `too many rooms`,
    or a text starting `room name` (empty, too long). Naming the pending room: `invite-only (+i)`,
    `bad key (+k)`, `banned from room`.
  - `ERROR "rate limited"` (no room) while the JOIN is pending, or no answer in 20 s: the JOIN was
    lost, not refused. The room is tried again after 5, 10 and 20 s, then every 5 minutes for as
    long as the link lasts, and shows as `trying again` with the reason and the count meanwhile.
    The next room goes while it waits.
  - A name over `max_room_name_bytes` fails without being sent. Rooms past
    `max_rooms_per_session` are not sent either and show as over the limit; when a part or kick
    makes room, they are joined.
  Failed and over-limit rooms are tried again on the next link, or at once when a client JOINs
  them (M3).
- **Other ERRORs.** Only two take the bouncer out of a joined room, both from rrcd's
  `commands.py` and naming the room: a text starting `kicked from ` (`/kick`) and
  `banned from <room>` (a `/ban` that removes members). The room counts as failed until the next
  link or a client's JOIN for it. Every other ERROR (`room is moderated (+m)`, `message too
  large: …`, `not authorized`, `rate limited` with no JOIN pending, …) changes no room state: it
  is a hub error, logged and handed to the relay (`Inbound::HubError`), which sends it to the
  client whose envelope caused it.
- **PART** (M3, `part_upstream = true`): sends PART and takes the room as left on the bouncer's
  own PARTED (matched as above). With none in 20 s the room is taken as left anyway; a PARTED that
  comes later is handled and not relayed. A JOINED for a room no longer wanted is answered with a PART. An added room is taken out of the room set file; a config room is kept out in
  memory until the process restarts (or a client JOINs it again), and the config file is not
  touched.
- **PING.** A PING from the hub gets PONG with the same body (rrcd PINGs only if its
  `ping_interval_s` is set, and drops a link whose PONG is `ping_timeout_s` late). After 120 s with
  nothing from the hub the bouncer sends PING (body: the time as a float, as rrcd does). No PONG in
  60 s sends one more PING, since one lost packet should not end a link; no PONG to that one
  either, or the link closing, ends the link. 120 s keeps an idle link's traffic small and is far
  inside rrcd's defaults; a Reticulum link that went silent is otherwise noticed only after its
  keepalive, minutes later.
- **Reconnect.** After the link ends, wait 5 s, doubling to 5 min, then connect again. A link that
  was welcomed and stayed up 60 s resets the wait to 5 s; a shorter one does not, so a hub that
  welcomes and then drops the link at once is not hammered. Every room is joined again on the new
  link, and the joined, failed, retrying and over-limit lists in the status are cleared when a link
  ends.
- **Rate.** rrcd's bucket is per link: `rate_limit_msgs_per_minute` tokens, full when the link
  opens, one token per packet of any type, and a packet into an empty bucket gets
  `ERROR "rate limited"` and is lost (`session.py` `refill_and_take`, `router.py:80-91`). The
  session keeps the same bucket (`src/rate.rs`) at 90 % of the limit (240 a minute until the
  WELCOME) and holds a packet until there is a token instead of losing it. Its own envelopes
  (PONG, PING, JOIN, PART) go first.
- **Client envelopes** (`Command::Send`, from M3):
  - No link up: dropped at once.
  - Link up but not welcomed yet, or its room is in the room set but not joined yet (JOIN pending
    or waiting to try again): held, in arrival order, in a queue of at most 256
    (`CLIENT_QUEUE`), each for at most 60 s (`CLIENT_HOLD`). A room that is joined, failed, over
    the limit or not in the room set does not hold its envelopes: they go, and rrcd's answer
    reaches the client.
  - Dropped: with the queue full, after 60 s held, when the link ends with it still queued, or
    when it is too large for the link.
  Every drop counts in `dropped_sends` and is reported as `UpstreamEvent::SendDropped { envelope,
  reason }`, the envelope as the client sent it (so its `K_ID` names it).
- **Events and commands (the contract with the relay, M3).** The session task never waits on the
  relay.
  - Events: `Upstream::events` is an unbounded `mpsc` channel with a count of the events in it
    (`upstream::event_channel`). Sending never waits. Traffic (`Envelope(_, Relay)`,
    `Envelope(_, HubError)`, `SendDropped`) is dropped once `EVENT_QUEUE` (4096) events wait,
    counted in `dropped_events` with a warning at the 1st, 2nd, 4th, 8th, … drop. State events
    (`Up`, `Down`, and `Envelope` with `JoinConfirmed`, `JoinFailed`, `PartConfirmed` or
    `Removed`) are never dropped, so the relay's view of the rooms cannot drift from the
    session's; they are few (one per JOIN, PART, kick or link), so the channel stays bounded in
    practice. Events: `Up { welcome, hub }` (`hub` the hub's identity hash, from the link),
    `Down`, `Envelope(env, Inbound)` for everything the session does not handle itself (`Relay`,
    `JoinConfirmed`, `JoinFailed`, `PartConfirmed`, `Removed`, `HubError`), and `SendDropped`.
    With no events channel (M2), the `Relay` and `HubError` envelopes are logged at debug and
    counted by type in `unhandled`.
  - A `Command::Join` for a room the session is in (joined and not being left) is answered at
    once with `Envelope(JOINED, JoinConfirmed)`: a JOINED from the hub's identity with the room
    and no body, so the relay keeps the member list it has. rrcd would answer a second JOIN on
    the same link with a JOINED too, but the session does not send one.
  - Commands: `Command` (`Send`, `Join`, `Part`) goes in through an `mpsc` channel of
    `COMMAND_QUEUE` (1024). The relay must send with `try_send`, never `send().await`, so a slow
    or reconnecting session cannot stall the clients of that hub; a full channel drops the
    command, and the relay counts it and tells the client (an `ERROR` for a `Send`, as for a
    `SendDropped`). `Join` and `Part` change the room set with or without a link.
- **Room set file.** `state_dir/rooms/<upstream hash>.json`, `{"rooms": [...]}`, mode 0600 in a
  0700 directory, written to a temporary file that is synced, renamed over it, and the directory
  synced, so a crash leaves the old or the new file; a failed write removes the temporary file. It
  holds the rooms added while the bouncer runs (a client's JOIN, M3); the config's `rooms` are not
  copied into it, and the config file is never written. On start the names are normalised as the
  config's are and the file is written back if that changed it; a file that does not parse stops
  the start with its path and is left as it is.
- **Shutdown.** On a signal every session closes its link (waiting at most 5 s, `CLOSE_WAIT`) and
  writes its status, within the bouncer's 10 s per session (`SESSION_STOP`, longer by a
  compile-time check); Reticulum is stopped only after that, so no link outlives the transport.

## Local hub

`src/relay.rs`: `Relay` is the logic, with no I/O (client packets, upstream events and the passing
of time in, commands to the upstream session and envelopes for client links out), and
`relay::run` is the task that owns the local hub's destination and feeds it. One per upstream hub.

- **Who is a client.** The identity gate (see [Commands](#commands), as of M1) records each link's
  identity and also sends it on an unbounded channel, so a link of another identity is refused
  even if it never sends a packet; the bounded `links_identified` event is only logged. The gate
  lets every identity through, because refusing there would close the link before an ERROR could
  say why.
  - The user's identity and `allowed_identities` are clients.
  - Any other identity gets `ERROR` `"identity not allowed on this hub"` (`K_SRC` the local hub,
    no room) and its link is closed 1 s later (`REFUSE_CLOSE_DELAY`), so the ERROR goes out first.
    It is counted in `refused_links`; its packets are ignored.
  - A link that has not identified 30 s after it was established (`IDENTIFY_TIMEOUT`) is closed;
    until then its packets are ignored.
- **HELLO.** Answered with WELCOME: `K_SRC` the local hub, hub name
  `"<upstream name> (via rrc-bouncer)"`, the upstream's version, caps and limits, with cap `0`
  (`resource_envelope`) false until M5. Before the upstream's first WELCOME: the local hub's name
  (`rrc-bouncer for <upstream hash>`), the bouncer's version and rrcd's default caps and limits. A
  later HELLO on the same link starts the client over with no rooms. Before its HELLO a client's
  envelopes get `ERROR` `"send HELLO first"`, as rrcd answers them, except PING.
- **PING** is answered with PONG by the local hub, body echoed, before HELLO too: the M1 local hub
  did so, and a client may PING to check a fresh link. This differs from rrcd, which wants HELLO
  first. PONGs from clients are ignored.
- **JOIN.** Checked in rrcd's order (`router.py` `_handle_join`, `service.py` `_norm_room`), with
  rrcd's texts, none naming a room unless said:
  - No room, or an empty string: `"JOIN requires room name"`.
  - A room the bouncer is in upstream: answered at once with a JOINED from the local hub (below).
  - The room set (the session's room set and joined rooms in `status`, with the rooms clients'
    JOINs added that the session has not taken yet) has `max_rooms_per_session` rooms and this is
    not one of them, or the session holds it over the limit (`rooms_over_limit`):
    `"too many rooms"`, naming the room (normalised) when it has a name. rrcd counts the rooms
    of the link; the room set is what the session will try to join on it.
  - Normalising: `"room name must not be empty"` (whitespace only), `"room name too long: <n>
    bytes > <max> bytes"`.
  - No upstream session (between `Down` and `Up`): `"hub not connected: <room> not joined"`,
    naming the room, and the room set is not changed. That holds for a room already in the room
    set too: the client is told now instead of waiting for a link that may be minutes away, and
    can JOIN again.
  - Otherwise `Command::Join` goes to the session, which adds the room to the room set file and
    joins it; the client waits. The JOINED (the hub's, or the session's own for a room it is in)
    goes to every client waiting for that room; a refusal (`ERROR` naming the room) goes to them
    as it is, and a room the client's JOIN added leaves the room set again, so it is not tried at
    every connect. A full command queue: `ERROR` `"the bouncer is busy: try the JOIN again"`,
    counted in `relay_drops`. When the session goes down, every client still waiting gets
    `"hub not connected: <room> not joined"`, and the rooms clients added that were never joined
    leave the room set.
  - The JOINED a client gets is the local hub's: `K_SRC` the local hub, and, when the hub sends
    member lists, the members (identity hashes) kept from the upstream JOINED and the JOINED and
    PARTED that follow. The list is a set, each identity once in the order it came: rrcd lists one
    hash per link and sends a PARTED only when an identity's last link leaves, so a list with
    repeats could never be emptied. The user comes first, then the others in order, as many as
    keep the envelope within one link packet (`LINK_MDU`, 431 bytes, about 20 members); rrcd
    itself puts no cap on its member list.
  - The room's history is replayed right after the JOINED ([History](#history)).
- **PART.** Checked as rrcd does: no room or an empty string is `"PART requires room name"`, then
  the name is normalised. Answered with PARTED (body the parting identity when the hub sends
  member lists), whether the client was in the room or not, as rrcd does.
  - `part_upstream = false` (the default): the bouncer stays in the room upstream and only that
    client's view ends; the other clients and the upstream room see nothing.
  - `part_upstream = true`, from a client in the room (joined or waiting for it): `Command::Part`
    to the session, PARTED to every other client in the room, and the relay takes the room as
    left at once, as the session does whether the hub answers or not, so a JOIN right after goes
    upstream again. A PART from a client not in the room is answered with PARTED and nothing else.
- **MSG, ACTION, NOTICE.** A MSG or NOTICE whose text body, trimmed, starts with `/` is a slash
  command, whatever its room or `K_DST`, as rrcd decides before anything else (`router.py`
  `_handle_message`). The others are checked as rrcd checks them before it looks at a room's
  state, in its order, and refused locally with rrcd's text (no room named): MSG or ACTION with
  no room or an empty one, `"message requires room name"`; a text body over `max_msg_body_bytes`,
  `"message too large: <n> bytes > <max> bytes"`; a NOTICE with `K_DST` and a room,
  `"direct notice must not include room"`; a room that does not normalise, the room name texts. A
  NOTICE with neither room nor `K_DST` is dropped without a word, as rrcd drops it. What passes
  goes upstream as `Command::Send`, counted in `relayed_in`: `K_ID`, `K_TS`, `K_FMT` and every
  unknown key byte for byte, and `K_NICK` replaced by the hub's `nick` when one is configured
  (the session's HELLO and JOIN carry it too). rrcd sets the session's nick from every MSG,
  ACTION and NOTICE, so with no `nick` configured each client's own `K_NICK` goes up as it is and
  the bouncer's nick upstream changes with whichever client spoke last. A full command queue drops
  the envelope with `ERROR` `"not sent to the hub: the bouncer's queue is full"` to the sender; a
  `SendDropped` from the session ([Upstream session](#upstream-session)) gives the sender
  `"not sent to the hub: <reason>"`. The upstream session's token bucket is the one rate limit all
  clients share.
- **Echo and once-only delivery.** Each client sees each envelope once, and the sender of a
  refused envelope is told.
  - Room envelopes: rrcd sends a room envelope to every member of the room, the sending link
    included, so the bouncer's own link gets every client's envelope back, with its `K_ID`,
    `K_FMT` and other keys and `K_SRC` the user. The relay sends nothing to anyone when a client
    sends, and relays that echo to every client in the room, the sender included. The other
    clients show it once. weechat-rrc (`rrc.c:792`), rBrowser2 (`rrc.py:1606-1608`, `1859`) and
    NomadNet (`RRC.py:667-671`, `1153`) draw their own MSG and ACTION when they send it and drop
    the hub's echo by its `K_ID`, so the sender shows it once too, at once, with no round trip;
    a line the hub refuses (`+m`, rate limited) still shows in the sender's own client, as it
    does for a direct client of rrcd, and the ERROR follows it. Relaying to the other clients at
    once instead would show them lines the hub refused.
  - Direct NOTICE (`K_DST`, no room): rrcd sends it only to its destination, with no echo. When
    the relay passes one upstream it gives a copy with `K_SRC` the user to the user's other
    clients, not the sender, which drew its own; so the user's other clients see what was said.
    No copy when it is addressed to the user (the hub sends that one back to the bouncer's link,
    and it is relayed to every client like any direct NOTICE), when it is a slash command, when
    it carries a room (refused above), or while there is no upstream session. A direct NOTICE the
    hub then refuses (`destination not connected`) has been shown already; the sender also gets
    the ERROR.
- **Envelopes from upstream.** `K_SRC` is rewritten from the upstream hub's identity to the local
  hub's, so a client can tell the hub's own envelopes from the user's; every other `K_SRC` is
  passed on as it is. Counted per client in `relayed_out`, with the local hub's own answers
  (WELCOME, JOINED, PARTED, ERROR). JOINED and PARTED of others update the member lists. They go
  to:
  - with a room: every client in that room. A NOTICE from the hub naming a room is first checked
    against the room's join and the commands waiting (below).
  - with `K_DST` and no room (a direct NOTICE to the user): every client;
  - a NOTICE from the hub with neither: the greeting while it is being taken (below), else the
    answer to the oldest command waiting (below), else every client;
  - anything else with neither: every client.
  A hub kicking the bouncer from a room (`Removed`) is passed to the clients in it, who leave it.
- **Greeting.** rrcd follows its WELCOME with the `greeting` as room-less NOTICEs from the hub,
  one per line. At `Up` the relay forgets the last greeting and, for 10 s (`GREETING_WINDOW`),
  takes room-less, `K_DST`-less NOTICEs from the hub as the greeting: each goes to every welcomed
  client and is kept (at most 16, `MAX_GREETING`). Any other envelope from the hub (the JOINED
  that answers the session's first JOIN comes next), or a client's command going upstream, ends
  it. A client's HELLO is answered with the WELCOME and then the kept greeting, `K_SRC` the local
  hub, as a client of rrcd gets it. A greeting rrcd sends as a Resource (over 512 bytes, with
  resources on) is not relayed until M5.
- **Commands.** rrcd answers a slash command on the link it came from, with one NOTICE per line of
  the reply (`commands.py` `emit_notice`; `/list` and `/who` send one, with no room), or with an
  ERROR; some (`/mode`, `/topic`) also tell the room with a NOTICE naming it. Replies carry no
  `K_ID`, so the relay keeps the commands it sent upstream in order (`pending_commands`, at most
  1024), as the hub answers them in order on the one link:
  - A room-less NOTICE from the hub, outside the greeting, answers the oldest command waiting and
    goes to its client alone; that command is done.
  - A NOTICE from the hub naming a room answers the oldest command waiting if that command named
    the same room (normalised): it goes to that client and to every client in the room, and the
    command is done. Otherwise it is the room's: every client in the room, and history.
  - The NOTICE rrcd sends after every JOINED (`room <r>: <registered>; mode=<m>; topic=<t>`) is
    not a command's answer: a NOTICE with that text within 10 s of the room being joined goes to
    the room.
  - An ERROR answering a command (below) answers the oldest command waiting, if it names no room
    or the command's room.
  - A command waits at most 75 s (`ANSWER_WAIT`: the session may hold it 60 s, `CLIENT_HOLD`, and
    the hub answers within seconds). A command dropped by the session (`SendDropped`) is done.
  A reply of several NOTICEs is taken as several answers: the first NOTICE goes to the command's
  client, the rest to the next commands waiting, or to every client when none is. rrcd's own
  commands answer with one NOTICE each; a multi-line reply and a later command waiting at the
  same time can send the later lines to the wrong client.
- **ERROR routing.** rrcd's ERRORs carry no `K_ID`, so the relay keeps the last 1024 client
  envelopes it sent upstream that are not commands (`sent`: id, link, type, room, kind) and finds
  the one an ERROR answers by its text (`error_answers`, every text rrcd 0.3.2 sends):
  - `SendDropped` names its envelope by `K_ID`: the ERROR goes to that envelope's sender.
  - A room envelope for the ERROR's room (normalised): `no such room`, `no outside messages (+n)`,
    `banned from room`, `room is moderated (+m)`. The oldest that fits: room envelopes leave the
    list at their echo, so the oldest left is the one the hub is answering.
  - A room envelope with no room: `message requires room name`. Any MSG or ACTION: `message too
    large: …`. The oldest. (The relay refuses both itself, so the hub sends them only for a limit
    that changed.)
  - A direct NOTICE: `destination not connected`, `direct notice must not include room`, `direct
    notice requires destination identity`. The newest: direct NOTICEs are answered only when
    refused, so they leave the list only by age.
  - A command (above): `unrecognized command`, `not authorized`, `not authorized (+t)`, `only the
    room founder can register`, `only the room founder can unregister`, `invite failed: …`.
  - The session's own JOIN, PART or HELLO: `JOIN requires room name`, `too many rooms`,
    `invite-only (+i)`, `bad key (+k)`, `PART requires room name`, `send HELLO first`, `room name
    …`. Logged; no client gets it (the session handles a refused JOIN, below the relay).
  - Every client: `rate limited`, `bad message: …`, the resource errors, `kicked from <r>` and
    `banned from <r>` that are not the bouncer's removal, and any text not listed. rrcd's
    `rate limited` names no envelope, and the session cannot tell which of its packets the hub
    dropped (one of its own, or a client's), so every client is told and no entry leaves the
    list.
  - The blamed envelope leaves the list. No envelope fits: the ERROR goes to every client, so it
    is never lost.
  - Entries leave by age: a room envelope 120 s after it was sent (`ECHO_WAIT`; the session may
    hold it 60 s) when the bouncer is in its room or about to be, and 75 s (`ANSWER_WAIT`) for a
    room envelope to a room outside the room set (it gets no echo) or a direct NOTICE.
  - When the session goes down, every entry and waiting command is marked stale: nothing sent on
    the old link is answered on the next, so no ERROR is blamed on it, but a `SendDropped` for it
    (the session reports what it still held) still reaches its sender. A client link that closes
    takes its entries with it.
- **RESOURCE_ENVELOPE**, either way, is logged and dropped until M5 relays Resources; the WELCOME's
  cap `0` tells clients not to send one.
- **The task.** `relay::run` selects over the client links' events, the session's events and a 1 s
  tick, unbiased, so a busy source (a chatty room, a flood of client packets) cannot starve the
  others.
  - rsReticulum queues a link's last packets before its close event, on separate channels, so on
    a close the task first takes every link establishment and every packet already queued, then
    closes the link.
  - An identity from the gate for a link no longer in the gate's map (it closed before the
    identity was read) is ignored, so a closed link is not brought back.
  - Each send to a client link is given 2 s (`SEND_TIMEOUT`); one that takes longer is given up,
    counted in `relay_drops` and logged, so a client link that does not drain cannot hold up the
    other clients or the session's events.
- **History hooks.** `Relay::record` is called with every envelope the hub sends to a room as
  a whole: room MSG, ACTION and NOTICE (the hub's room NOTICEs included, not a command's answer
  to one client), other members' JOINED and PARTED, and the ERROR that removes the bouncer from
  the room (kick, ban). It is not called for direct NOTICEs, room-less NOTICEs (greeting, command
  answers), ERRORs answering a client, the bouncer's own JOINED and PARTED (the session's), or the
  `room <r>: …` NOTICE rrcd sends after a JOIN (it goes to the joiner alone).
  The hub's echo of a client's envelope is a room envelope like any other, so each client
  envelope is recorded once, as the hub delivered it. `Relay::replay` is called after each JOINED
  a client gets.
- **Status.** Each `hubs` entry in `status.json` adds `clients`, `client_identities` (one per
  link, sorted), `refused_links`, `relayed_in`, `relayed_out` and `relay_drops`. A change of
  clients writes the file at once.
- **Limitations.**
  - A client's JOIN key (`+k`) is not passed upstream: the session joins with none.
  - A JOIN answered locally is not followed by the room status NOTICE rrcd sends after a JOIN.
  - Clients of the same local hub see no JOINED or PARTED for each other: upstream they are one
    link.
  - Clients are not told when the upstream link goes down; their envelopes get `SendDropped`
    ERRORs.
  - Two direct NOTICEs in flight from two clients: a refusal goes to the newer one's sender.
  - The greeting is taken by time and position; a hub that sends some other room-less NOTICE
    within 10 s of its WELCOME, before anything else, has it kept as greeting too.

## History

- **Store.** One append-only file per hub and room:
  `state_dir/history/<upstream hash>/<room file name>.log`, a sequence of CBOR records, each a map
  `{"ts_ms": <uint>, "envelope": <bytes>}`: the time the bouncer got the envelope (milliseconds
  since the epoch) and the envelope's bytes as the hub delivered them (`K_SRC` the original
  sender, the upstream hub for its own NOTICEs). Kept on the local disk unencrypted (README says
  so), files mode 0600, `history/` and each hub's directory mode 0700.
- **Room file names.** A room name is kept as it is when it is made of `a-z`, `0-9`, `_`, `-` and
  `.` (a `.` not first); every other byte (upper case, `%`, `/`, a leading `.`, UTF-8) is written
  `%XX`, upper-case hex, and `.log` is added. The mapping is one to one, and a file whose name does
  not decode back to the same name is ignored. A room whose file name would be over 255 bytes is
  not recorded (counted in `history_drops`).
- **Writer.** Each hub has one history thread (`src/history.rs`) fed by a bounded queue (4096
  jobs). `Relay::record` puts a job on it with `try_send` and never waits: when the queue is full
  the record is dropped and counted in `history_drops`, so a slow disk cannot hold up the relay.
  Replay queries go through the same queue, so a query sees every record queued before it.
- **Durability.** Each record is one `write` to a file opened with `O_APPEND`. The thread calls
  `fdatasync` on the files written to 5 s after a write and at shutdown (the bouncer gives it 5 s),
  not after each record: a power loss can take the last 5 s of history, a crash of the bouncer
  alone loses nothing written. A write that fails is cut back off the file (`set_len`) and counted
  as a drop.
- **A cut record.** Reading stops at the first record that does not decode, so a file whose last
  record was cut short (power loss, full disk) still replays everything before it. The prune at
  start and every hour takes such a tail off (logged), and removes `*.tmp` files a crash left.
- **Retention.** `history.days` (default 30) and `history.max_mb` per hub (default 100, in units of
  1,000,000 bytes). At start and every hour the thread reads the hub's room files, drops the
  records older than `days`, then, while the hub's files together are over `max_mb`, the oldest
  records of the hub, whichever room they are in. A file that changes is written to a `.tmp` file,
  synced and renamed over the old one (the directory synced after); a file left empty is removed.
  Between prunes a hub's history can grow past `max_mb` by an hour's traffic. Records are in the
  order the bouncer got them; a file is not sorted.
- **What is replayed.** MSG, ACTION and NOTICE only. JOINED, PARTED and the ERROR that removes the
  bouncer are recorded but not replayed: a client takes them as changes to the room it is in now
  (weechat-rrc updates its member list, an ERROR naming the room is taken as a kick).
- **Replay on JOIN.** After each JOINED a client gets (answered locally, or once the hub answers a
  new room's JOIN), the records of that room from the last `history.replay_hours` (default 24),
  at most `history.replay_max` (default 100, the newest kept), are sent to that client alone,
  oldest first:
  - framed by two NOTICEs from the local hub (`K_SRC` its identity, `K_ROOM` the room, no
    `K_NICK`): `--- replay: N messages since <time> ---` (`1 message` for one) before, and
    `--- end of replay ---` after. `<time>` is the oldest replayed record's `ts_ms` in UTC,
    `YYYY-MM-DDTHH:MM:SSZ`; clients show replayed lines at the time they arrive, so the opening
    NOTICE is what dates them;
  - each envelope as the hub delivered it, `K_ID`, `K_TS`, `K_SRC`, `K_NICK`, `K_FMT` and unknown
    keys kept, except a `K_SRC` that is the upstream hub's identity, which is made the local hub's
    (as for live envelopes);
  - nothing at all when there is nothing to replay;
  - none when `replay_max = 0` or `replay_hours = 0`.
- **Pacing.** A replay is sent by a task of its own, one envelope every 20 ms, each send given 2 s
  as any send to a client (counted in `relayed_out`, or in `relay_drops` and the rest of the
  replay given up). Other clients and the upstream session are not held up. While a client's
  replay of a room runs, the room's live envelopes for that client are held (up to 512, the rest
  dropped and counted in `relay_drops`) and sent after `--- end of replay ---`, so the client sees
  them in order and none twice: the query is queued when the client joins, before any live
  envelope that follows is recorded. A hold not released in 120 s is sent anyway (logged); a
  client that closes its link ends its replays.
- **Repeats.** The bouncer does not know what a client has already seen. A client that links
  again (or joins again) gets the room's last `replay_hours` once more, including what it saw
  before it went away; the opening NOTICE says where the replay starts. A client's own messages
  are recorded from the hub's echo, so they are replayed once, with the user's identity in
  `K_SRC`: see the client notes below.
- **`/history [N]`.** A client's MSG in a room whose body is `/history` with at most one argument
  (`/ history 3` too, and any case, as rrcd parses commands) is answered by the bouncer to that
  client alone and is not sent upstream. It works whether or not the client has joined the room
  and while the upstream link is down. It replays the last N records of the room (MSG, ACTION,
  NOTICE; no time window; N defaults to `replay_max`, or 100 when that is 0, and is at most 500),
  framed and paced as the replay on JOIN; with nothing to replay it gets one NOTICE,
  `--- no history in <room> ---`. An N that is not a whole number from 1 up gets the NOTICE
  `usage: /history [N], N from 1 to 500`, and a `/history` while a replay of that room to that
  client is still running gets `--- a replay of <room> is running ---`.
- **Clients and replayed own lines.** From the code of weechat-rrc, rBrowser2 and NomadNet:
  - weechat-rrc (`rrc.c`) marks an envelope as the user's own when `K_SRC` is the user's identity
    and drops it only when its `K_ID` is in the ids it sent in this session; a replayed own line
    is shown as the user's own line.
  - rBrowser2 (`rrc.py`, around line 1858) drops an envelope from the user only when its `K_ID` is
    in `hub.sent_ids`; otherwise it shows the line.
  - NomadNet (`RRC.py` 1145-1215) drops an own envelope only when its id is in `_sent_ids`;
    otherwise it keeps it as a message stamped with the time it arrived, not `K_TS`.
  - None of them fails on a replay; all show replayed lines at their arrival time.
- **Status.** Each `hubs` entry adds `history_bytes` and `history_rooms` (as of the last write or
  prune), `history_records` (records written since start) and `history_drops` (records not
  written: queue full, disk error, room name too long). They go out with the next write of
  `status.json` (every minute, a change of clients, shutdown).

## Configuration

TOML, like lxmf-bouncer's (relative paths relative to the config file, `~/` the home directory).
See `contrib/config.example.toml`.

| Key | Default | Meaning |
|---|---|---|
| `identity` | required | The user's Reticulum identity file (64 bytes) |
| `state_dir` | required | Local hub identities, room sets, history, `status.json`, generated Reticulum config; created mode 0700 |
| `log_level` | `info` | As lxmf-bouncer |
| `[reticulum]` | | `shared_instance` / `instance_name` / `tcp` / `config_dir`, exactly as lxmf-bouncer (copy its code) |
| `announce` | `false` | Announce the local hubs |
| `announce_interval_minutes` | `360` | With `announce = true`; at least 1 |
| `allowed_identities` | `[]` | Identity hashes allowed to use the local hubs besides the user's |
| `part_upstream` | `false` | A client's PART also parts upstream |
| `[history] days / max_mb / replay_hours / replay_max` | `30 / 100 / 24 / 100` | [History](#history); `days` and `max_mb` at least 1 (`max_mb` in units of 1,000,000 bytes), either `replay_*` 0 turns the replay on JOIN off |
| `[[hub]] address` | required | The upstream hub's `rrc.hub` destination hash |
| `[[hub]] rooms` | `[]` | Rooms to be in from the start (more are added by clients' JOINs and kept in the room set file). Trimmed and lowercased as rrcd does (`service.py:507`), duplicates dropped, an empty name refused |
| `[[hub]] nick` | none | `K_NICK` the bouncer uses on its own envelopes (HELLO, JOIN, PART, PING; not PONG, as rrcd's has none) and puts on every client MSG, ACTION and NOTICE it sends upstream; empty means none, and then each client's own nick goes up, so the bouncer's nick on the hub changes with whichever client spoke last |

At least one `[[hub]]` is required, and each address may appear once. Unknown keys are refused, so
a typo is an error rather than a silent default.

## Commands

As lxmf-bouncer: `run` (default), `status [--json]` (from `state_dir/status.json`, written every
minute: upstream link state per hub, rooms, clients linked, history size, envelopes relayed),
`hashes` (the user's identity hash and, per upstream hub, the local hub address to put in clients).

As of M1:

- `hashes` prints `identity <hash>`, then per hub `upstream hub <hash>` and, indented,
  `local hub <hash>`. It creates a missing hub identity, so the address it prints is the one `run`
  will use.
- `run` loads the identities, starts Reticulum, registers one local hub per upstream, answers path
  requests (above), logs links as they are established, identified and closed, and answers PING
  with PONG (body echoed, `K_SRC` the local hub's identity) on links identified as an allowed
  identity. Everything else is ignored until M2/M3. SIGTERM or SIGINT stops it.
- A packet that follows a client's `identify()` is handled as from that identity, never dropped as
  unidentified. The local hub does not take a link's identity from the `links_identified` event:
  that is a separate channel from `link_packets`, read in no fixed order against it, and sent with
  `try_send` on a bounded channel, so it can be late or dropped. It registers an
  `identity_gate` (`IdentityGatePolicy`), which rsReticulum calls on the link manager's task while
  it handles the LinkIdentify packet (`rns-runtime/src/link_manager.rs:2956-2977`), before it
  handles the link's next packet and puts it on `link_packets` (`:1277-1279`); the gate records
  the identity in a map the hub reads when a packet arrives. What this relies on: one link
  manager task per destination handles that destination's inbound link packets one at a time in
  order. The e2e clients send their PING in the same TCP segment as the identify; before the gate,
  about one in six of those PINGs went unanswered.
- `status.json` holds `pid`, `started_at`, `updated_at`, `identity_hash`, `announce` and `hubs`
  (`upstream`, `local_hub`), written at start, every minute and at shutdown; later milestones add
  the fields listed above. `status` prints it with the age of the last update.

As of M2:

- `run` also runs the upstream sessions ([Upstream session](#upstream-session)). On SIGTERM it
  closes the upstream links (at most 5 s) so the hubs see the user leave at once.
- Each `hubs` entry adds the upstream link: `state` (`connecting`, `linked` before the WELCOME,
  `connected`, `disconnected`, `waiting`, `stopped`), `hub_name`, `hub_version`, `hub_caps`,
  `hub_limits`, `rooms` (the room set), `rooms_joined`, `rooms_failed` (room → reason),
  `rooms_retrying` (room → reason and tries), `rooms_over_limit`, `last_connected_at`, `last_disconnected_at`, `last_error`, `connects`
  (welcomed links), `reconnects` (welcomed links after the first), `failed_attempts` (links that
  ended before a WELCOME, or never opened), `next_attempt_at`, `unhandled` (type → count),
  `dropped_sends` and `dropped_events`. A change of state or rooms writes the file at once; the counters go out with
  the next minute's write. A file without these fields (from M1) still reads.
- `status` prints, per hub, the link (`connected to "<name>" (version v), since …` or its state and
  last error), the room set with `joined N of M`, the rooms not joined and why (a room being tried
  again shows `trying again: <reason>; N tries`), the connect counts, the next attempt, the
  envelopes not relayed yet, the dropped sends and the dropped events.

As of M3:

- `run` also runs one local hub task per upstream hub ([Local hub](#local-hub)): clients link,
  say HELLO, JOIN and PART, and their envelopes are relayed both ways.
- Each `hubs` entry adds the clients: `clients`, `client_identities`, `refused_links`,
  `relayed_in` (client envelopes passed upstream), `relayed_out` (envelopes sent to clients, one
  per client) and `relay_drops`. A file without these fields still reads.
- `status` prints, per hub, the clients and their identities, the envelopes relayed in and out,
  and the relay drops and refused links when there are any.

As of M4:

- `run` also runs one history thread per upstream hub ([History](#history)): room envelopes are
  recorded, replayed after each JOINED and on `/history`.
- Each `hubs` entry adds `history_bytes`, `history_rooms`, `history_records` and
  `history_drops`. A file without these fields still reads.
- `status` prints, per hub, `history          <bytes> bytes in <n> rooms` (`1 room`) and
  `history written  <records> records, <drops> dropped`.

## Implementation notes

- **Crate layout.** `src/lib.rs` (`config`, `identity`, `envelope`, `history`, `bouncer`,
  `upstream`, `rate`, `rooms`, `relay`) and a thin
  `src/main.rs` (CLI, logging). The library split lets the codec and config be tested and used
  without the binary.
- **Local hub identities.** `state_dir/hubs/<upstream hash>.identity`, 64 bytes as Python RNS
  writes them, mode 0600 in a 0700 directory. A new one is written to a temporary file and
  hard-linked into place, so it appears whole, and when two processes (`hashes` and `run`) create
  it at once the second finds the link taken and loads the first one's. `state_dir` and `hubs/`
  are set to 0700 at every start, also when they already exist. A file that does not load is an
  error and is left alone, never overwritten. A hub identity equal to the user's is
  refused: the hub's identity in `K_SRC` is what tells its envelopes from the user's.
- **Envelope codec** (`src/envelope.rs`, on `ciborium`). The body and every unknown key are kept as
  the raw CBOR item they arrived as, so they are passed on byte for byte: cbor2 writes floats as
  float64 and a re-encode through `ciborium::Value` would shorten them (`PING` bodies are floats).
  Encoding writes the known keys in rrcd's `make_envelope` order (0, 1, 2, 3, 4, 8, 5, 6, 7), then
  unknown keys in the order they arrived; an envelope rrcd made comes out byte for byte the same.
  Decoding is strict where rrcd's `validate_envelope` is, and in two places beyond it: the top
  level must be one map with nothing after it, and a key that appears twice is refused (cbor2 would
  keep the last). Keys must be unsigned integers, `K_V` must be 1, `K_V`/`K_T`/`K_ID`/`K_TS`/`K_SRC`
  must be present and every known key must have its type. Unknown message types decode and pass
  through; `K_T` is any CBOR integer, negative included, as rrcd takes it. Nesting deeper than 64 is refused. Typed views of the HELLO (including the legacy nick at
  body key 64), WELCOME (missing limits fall back to rrcd's defaults) and RESOURCE_ENVELOPE bodies
  sit on top.
- **Fixtures.** `tests/fixtures/make_envelopes.py` builds 19 envelopes with rrcd 0.3.2's own
  `make_envelope`, `validate_envelope` and `encode` from fixed values and writes them to
  `tests/fixtures/envelopes/*.cbor`. The bytes are committed, so `cargo test` needs no Python;
  the tests decode each one, check its fields, and re-encode it to the same bytes.
- **End-to-end test.** `tests/e2e.sh` (`RRC_TEST_VENV` = a venv with `rns`, `cbor2` and `rrcd`;
  `RRC_TEST_WORK`, default `/tmp/rrc-bouncer-e2e`; `--only rrcd` runs the rrcd phases alone,
  `--only history` the history phases)
  builds the bouncer and runs `tests/e2e.py`: rnsd instances with unique instance names and
  TCPServerInterfaces on 127.0.0.1, ports 4370-4379, and Python peers (`tests/rrc_peer.py`) that
  watch announces, request paths, link, identify and PING, or act as an RRC client of a hub. It
  covers the phases cited under [Announces](#announces), the allow-list (a link identified as
  another identity gets no PONG, gets `ERROR` and is closed), a PING sent right behind the
  identify, `hashes`, `status` and a clean stop.
  The rrcd phases run rrcd 0.3.2 on its own RNS instance (config dir, identity, room registry and
  config all under the work directory, `max_rooms_per_session = 2`, PING every 3 s, timeout 4 s)
  behind an rnsd, a Python client in `lobby` under an identity of its own, and the bouncer with
  `rooms = ["lobby"]`, `nick = "bnc"` and a room set file seeded with `[" Side ", "side",
  "third"]`. They check: the client sees the bouncer's JOINED (nick `bnc`, the user's identity);
  `status.json` shows the hub's name, `lobby` and `side` joined and `third` over the limit; the file
  is normalised; rrcd logged the HELLO with nick `bnc`; `status` prints the link; the local hub's
  path response carries `e2e hub (via rrc-bouncer)`; the link outlives three rrcd ping rounds,
  answering rrcd's PINGs and sending its own; rrcd stopped (the link closes) and rrcd killed (no
  PONG) are each followed by a reconnect with backoff and a rejoin the client sees; and after a
  bouncer restart the persisted room set is joined again.
  The relay phases follow, on the same rrcd: the bouncer restarted with `max_rooms_per_session =
  4`, two driven Python clients A and B under the user's identity on the local hub (B says HELLO
  in the same breath as its identify, as NomadNet does) and one, D, directly on rrcd under an
  identity of its own, all in `lobby`. They check: A and B get the WELCOME with the upstream's
  name `(via rrc-bouncer)` and its caps and limits (`resource_envelope` off), then rrcd's greeting
  (`K_SRC` the local hub, no room), B though it linked after the greeting was sent; each JOIN gets a
  JOINED with the member list; MSG, ACTION and direct NOTICE from A, B and D, each reaching every
  client exactly once with its original `K_ID`, `K_FMT` kept, `K_SRC` the user or D, and the
  sender's echo, a direct NOTICE from A or B reaching the other and D but not its sender; D sees
  the nick `bnc` on everything A and B send; A's MSG to a room that does not exist and B's MSG to
  `lobby`, sent together, give the ERROR to A alone while B gets its echo; A's `/list` and B's
  `/who lobby`, sent together, each get their own answer and nothing of the other's; a direct NOTICE to an identity that is not connected gives an ERROR to its sender
  only; a JOIN of a new room joins upstream, gets the room's NOTICE with `K_SRC` the local hub and
  is saved in the room set file; a JOIN over the limit gets `too many rooms`; A's PART leaves the
  bouncer in the room (D sees no PART, B still gets D's MSG and A does not); `status.json` and
  `status` show the clients and the relayed counts; a client of another identity gets ERROR and
  is closed, and after a restart with that identity in `allowed_identities` it is welcomed; no
  driven client logs an error handling what it got.
  A `part_upstream = true` phase follows: A's PART gives A a PARTED and D a PARTED of the user; A's
  JOIN right after joins upstream again (D sees the JOINED); D's MSG reaches A; with rrcd stopped, a
  JOIN gets `hub not connected: side not joined`, and once rrcd is back and the session has
  rejoined, the same JOIN is answered with a JOINED.
  The history phases follow, on rrcd restarted with `max_rooms_per_session = 6` and the bouncer
  in `lobby` and `hist`. With no client of the bouncer linked, D3 (directly on rrcd) joins `hist`,
  E (another identity) joins and parts, and D3 says a MSG, an ACTION and a NOTICE; `hist.log`
  holds the three JOIN/PART envelopes and the three messages with D3's `K_ID`, `K_TS` and
  `K_SRC`, mode 0600 in 0700 directories. Then, each check comparing every replayed envelope's
  `K_ID`, `K_TS`, `K_SRC`, `K_NICK`, type and body with what D3 or A3 got live:
  - A3 links and joins: the three messages, in order, between the two NOTICEs from the local hub,
    the opening one dated by the first message; E's JOIN and PART are not replayed.
  - A3 says a MSG; A4 links and joins: four messages, A3's once, with the user's identity.
  - `status.json` and `status` show the history counts; `/history 2` from A3 gets the last two,
    framed, `/history 0` gets the usage NOTICE, A4 gets neither, and D3 never sees `/history`.
  - After a bouncer restart, A5's JOIN gets the same four.
  - With a record 40 days old, a hub NOTICE two hours old and half a record written into
    `hist.log` by hand, an `old.log` holding only a 31-day-old record, and `replay_hours = 1`: at
    start the old record and the cut tail are gone and `old.log` is removed; A6's JOIN gets the
    four (the two-hour-old NOTICE is outside the window); `/history 50` gets five, the hub's NOTICE
    with `K_SRC` the local hub.
  - With `replay_max = 2`, A7's JOIN gets the newest two; with `replay_max = 0`, A8's JOIN gets
    nothing and `/history 1` still answers.
  Pruning by `max_mb` is covered by the unit tests in `src/history.rs`.
  The phases' start waits are generous (Python peers 180 s, rrcd 120 s) so a loaded host does not
  fail them.
  `RRC_BOUNCER_PING_SECS` (the idle PING and the PONG wait) and `RRC_BOUNCER_BACKOFF_SECS` (the
  first wait; the longest is 60 times it, at most 5 min) shorten the session's timings for the
  test.

## Milestones

Each milestone ends with `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and
`cargo test` passing, and is landed on `main`. Unit tests next to the code; end-to-end tests in
`tests/` against a real rrcd, run the way lxmf-bouncer's `tests/e2e.sh` runs (a throwaway Reticulum
instance with no interfaces to the outside, so nothing reaches a real network).

1. **M1: foundations.** Config (with tests), identity loading, the Reticulum runtime and its three
   attachment modes (lift from lxmf-bouncer), the envelope codec (round-trip tests against envelopes
   produced by rrcd's own `envelope.py`), local hub identities, `hashes`. Answer the path request
   question above and write the answer into this file (done: [Announces](#announces)).
2. **M2: upstream session.** Link, identify, HELLO/WELCOME, JOIN the room set, PING/PONG,
   reconnect with backoff and rejoin. e2e: the bouncer joins a room on a local rrcd and a Python test
   client in the same room sees its JOINED (done: [Upstream session](#upstream-session)).
3. **M3: local hub and relay.** Accept clients, allow-list, HELLO/WELCOME, JOIN/PART, the relay both
   ways with `K_SRC` rewriting and echo, the shared rate limit. e2e: two clients on the bouncer and
   one directly on rrcd exchange MSG, ACTION and a direct NOTICE in every direction; a client of a
   foreign identity is refused (done: [Local hub](#local-hub)).
4. **M4: history.** Store, retention, replay on JOIN, `/history N`. e2e: messages said while no
   client is linked are replayed, in order, with their original ids, to a client that joins later;
   after a bouncer restart too (done: [History](#history)).
5. **M5: finish.** `RESOURCE_ENVELOPE` relay (MOTD and long notices both ways), `status`,
   README (setup, every client's settings, limits), `contrib/` unit and example config final.
   A manual check list for NomadNet, weechat-rrc and rBrowser2 against the bouncer goes in the README.

## Out of scope for 0.1

Patches to clients (`K_TS` display, dedupe by id), push notifications or LXMF mention forwarding,
several users on one bouncer, a web UI, hosting rooms of its own.
