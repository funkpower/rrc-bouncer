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
- A whole envelope fits one Link packet (about 481 bytes). Longer text can come as
  `RESOURCE_ENVELOPE` (body `{0: id, 1: kind, 2: size, 3: sha256, 4: encoding}`, kinds `notice`,
  `motd`, `blob`) followed by a Reticulum Resource on the same link. Stock rrcd 0.3.2 sends only
  its greeting this way, when it is over 512 bytes ([Resources](#resources)).
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
  not wait for the next announce or refresh to learn the name. That send waits for the next whole
  second first: an announce carries its emission time in whole seconds (rsReticulum stamps `now as
  u64`, `rns-identity/src/destination.rs:966`), and rnsd replaces a path it has only with a later
  one (`Transport.py:2519`), so a WELCOME in the same second as the first path response would
  leave rnsd with the placeholder until the next refresh or announce. The name is not saved: after a
  restart it is the placeholder until the WELCOME. e2e phase "rrcd": a client asking rnsd for the
  local hub's path after the WELCOME gets `e2e hub (via rrc-bouncer)`.

## Upstream session

`src/upstream.rs`: a `Session` that is the protocol with no I/O (envelopes and the time in, what to
send out; unit-tested), and `run`, the task per hub that owns the link and feeds it.

- **Connect.** `connect_link` to the hub's `rrc.hub` with `identify: true` (path wait 30 s, the
  whole connect at most 60 s). A failed connect asks for the path again before the next try, in
  case it went stale. The first try is at once.
- **HELLO.** Body `{0: "rrc-bouncer", 1: 1, 2: caps}` with the hub's `nick` as `K_NICK`. Caps:
  `resource_envelope`, `action` and `direct_notice` all true: the session takes Resources
  ([Resources](#resources)) and the relay passes the rest on. rrcd keeps a client's caps without
  acting on them: it sends its long greeting as a Resource whatever they say. No WELCOME in
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
- **Client envelopes** (`Command::Send`, from M3; `Command::SendPart` for the parts of a client's
  long notice, paced by the hub's echo, [Resources](#resources)):
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
  Then every hub's history thread is stopped at once, each given 5 s to write and sync what it
  was given (a full queue is not waited on past that), the last `status.json` gets 2 s
  (`STATUS_STOP`), Reticulum is given 5 s to stop, and tasks still running get 2 s
  (`Runtime::shutdown_timeout`): the process exits within 24 s of the signal whatever does
  not stop in user space. The bound does not cover a thread stuck in the kernel: an `fsync` or
  `write` on a stalled disk (a dead NFS mount, a failing drive) holds its thread, and the process
  with it, until the call returns, and no timeout in the bouncer can end it; systemd's
  `TimeoutStopSec` and its SIGKILL are what end such a process.
- **Signals and slow disks.** The kernel gives a signal sent to the process to its main thread
  first, and a thread in a blocking call such as an `fsync` on a busy disk takes it only when the
  call returns; until then the signal is not handled at all. So `run` runs on the runtime's
  workers and the main thread only waits for it, and `status.json` is written by a task of its
  own, one write at a time and only the latest status (`spawn_blocking`), never by the run loop.
  A disk that holds an `fsync` for seconds does not delay the signal, and the shutdown waits on
  `status.json` for at most 2 s.
- **One bouncer per `state_dir`.** `run` takes an exclusive `flock` on `state_dir/lock` (mode
  0600, created if missing, never removed) before it starts the history or Reticulum, and holds it
  until it exits; the kernel lets it go when the process ends, however it ends. A second `run` on
  the same `state_dir` exits 1 with `another rrc-bouncer is running with state_dir <dir> (it holds
  <dir>/lock)`. `status` and `hashes` do not take the lock and work while the bouncer runs.

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
  `"<upstream name> (via rrc-bouncer)"`, the upstream's version, caps and limits: cap `0`
  (`resource_envelope`) is on exactly when the hub's is (rrcd's default), and a client's
  `RESOURCE_ENVELOPE` is refused while it is off, as rrcd refuses one. Before the upstream's first WELCOME: the
  local hub's name (`rrc-bouncer for <upstream hash>`), the bouncer's version, every cap on and
  rrcd's default limits. A
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
- **MSG, ACTION, NOTICE.** A MSG or NOTICE whose text body, trimmed as Python's `strip()` trims
  (`envelope::is_command`), starts with `/` is a slash command, whatever its room or `K_DST`, as rrcd decides before anything else (`router.py`
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
  resources on; a room-less kind `motd` Resource) is the greeting whenever its transfer ends, in
  the window or after it, and is sent and kept the same way ([Resources](#resources)).
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
    hold it 60 s), and a long notice's part 10 s more for each part before it (each waits for the
    one before's echo), when the bouncer is in its room or about to be, and 75 s (`ANSWER_WAIT`) for a
    room envelope to a room outside the room set (it gets no echo) or a direct NOTICE.
  - When the session goes down, every entry and waiting command is marked stale: nothing sent on
    the old link is answered on the next, so no ERROR is blamed on it, but a `SendDropped` for it
    (the session reports what it still held) still reaches its sender. A client link that closes
    takes its entries with it.
- **RESOURCE_ENVELOPE**, either way, and the Resource it announces: [Resources](#resources).
- **The task.** `relay::run` selects over the client links' events, the session's events and a 1 s
  tick, unbiased, so a busy source (a chatty room, a flood of client packets) cannot starve the
  others.
  - rsReticulum queues a link's last packets before its close event, on separate channels, so on
    a close the task first takes every link establishment and every packet already queued, then
    closes the link.
  - An identity from the gate for a link no longer in the gate's map (it closed before the
    identity was read) is ignored, so a closed link is not brought back.
  - The task never waits on a client link. Each client link has a sender task of its own, started
    at the link's first envelope and stopped when the link closes; the relay's task hands it
    envelopes through a queue and goes on. At 1024 envelopes waiting for one link
    (`LINK_QUEUE`) the next is dropped and counted in `relay_drops` (the client is not told:
    it is the link that does not drain). Replay starts and ends are never dropped.
  - Each send to a client link is given 2 s (`SEND_TIMEOUT`), inside the link's sender; one that
    takes longer is given up, counted in `relay_drops` and logged, so a client link that does not
    drain holds up only itself. `relayed_out` counts envelopes as they are sent, `relay_drops`
    where they are dropped.
- **History hooks.** `Relay::record` is called with every envelope the hub sends to a room as
  a whole: room MSG, ACTION and NOTICE (the hub's room NOTICEs included, not a command's answer
  to one client), other members' JOINED and PARTED, and the ERROR that removes the bouncer from
  the room (kick, ban). It is not called for direct NOTICEs, room-less NOTICEs (greeting, command
  answers), ERRORs answering a client, the bouncer's own JOINED and PARTED (the session's), or the
  `room <r>: …` NOTICE rrcd sends after a JOIN (it goes to the joiner alone). A room NOTICE that
  came as a Resource, from the hub or from a client, is recorded as the NOTICEs it splits into
  ([Resources](#resources)).
  The hub's echo of a client's envelope is a room envelope like any other, so each client
  envelope is recorded once, as the hub delivered it. `Relay::replay` is called after each JOINED
  a client gets; the history is asked at once, in the relay's task, and the link's sender is
  handed the answer to wait for.
- **Status.** Each `hubs` entry in `status.json` adds `clients`, `client_identities` (one per
  link, sorted), `refused_links`, `relayed_in`, `relayed_out`, `relay_drops` and the
  `client_resources_*` counters ([Resources](#resources)). A change of clients writes the file at
  once.
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

## Resources

`src/resource.rs` (bounds, checks, matching, splitting), with the hub side in `src/upstream.rs`
and the client side in `src/relay.rs`. A text too long for one link packet travels as a
`RESOURCE_ENVELOPE` (`K_SRC`, `K_ROOM`, `K_NICK` as the NOTICE would have them; body
`{0: id, 1: kind, 2: size, 3: sha256, 4: encoding}`) followed by a Reticulum Resource of the text's
bytes on the same link. Stock rrcd 0.3.2 sends one only for its greeting, when it is over 512
bytes (kind `motd`, no room, `K_SRC` the hub; `session.py:302`, `messages.py:207`
`send_text_smart`, `resources.py` `send_via_resource`). The bouncer takes kind `notice` too, room
or room-less, from a hub that sends one. It takes one from a client for a long room NOTICE and
sends that upstream as ordinary NOTICEs, never as a Resource.

- **Bounds**, rrcd's, on both sides: a Resource is 1 to 262144 bytes (`MAX_RESOURCE_BYTES`,
  rrcd's `max_resource_bytes`), in one segment, not a request or response, and sent uncompressed
  (`auto_compress = false`, as rrcd sends). rrcd takes a size of 0 in an envelope; the bouncer
  refuses it as malformed (`resource::check`, with rrcd's text `resource envelope invalid size`),
  since a Resource of no bytes cannot be sent. At most 8 announced and not yet whole per link
  (`MAX_EXPECTED`, rrcd's `max_pending_resource_expectations`). A local hub receives at most 32
  client Resources at once over all its links (`HUB_RESOURCES`).
- **Counting.** A `RESOURCE_ENVELOPE` is never counted. Each Resource is counted once, when it
  ends: taken whole and matched (`*_in`, or `client_resources_out` when a client took it), or
  dropped (`*_drops`): refused, failed, unannounced, a sha256 that is not the announced one, a
  kind or text the bouncer does not relay, or left when its link closed or its session started
  again.
- **Matching.** An envelope's body is checked as rrcd checks it (`resource::check`): id bytes,
  kind a non-empty text, size from 1 to the limit, sha256 32 bytes when present. A Resource that
  comes whole is matched to a waiting envelope of the same link by size and sha256 (rrcd and the
  clients match by size, then check the hash), so two of one size sent together match whatever
  order they end in. The envelope and the Resource come on different channels, so either may be
  read first. A whole Resource with no envelope waits 5 s (`OFFER_GRACE`) for one, at most 8 and
  512 KiB of them per link (`MAX_EARLY`, `MAX_EARLY_BYTES`); then it is dropped, and if an envelope
  of its size waits, that envelope goes with it as a sha256 mismatch. An envelope whose Resource
  has not come in 120 s (`RESOURCE_TIMEOUT`) is forgotten, uncounted, unless a Resource of its
  size is still on the way on that link.
- **Transfers are not cancelled.** rsReticulum's local hub (`DestinationHandle`) can neither cancel
  a Resource it sends nor one it receives, and a link session's inbound handle cannot be cancelled
  while its end is awaited (`concluded` takes it). So no transfer is given up by the bouncer: each
  keeps its place (an inbound slot, a link's one outbound Resource) until it ends. rsReticulum ends
  a stalled transfer with its watchdog, and every transfer of a link when the link closes, each
  with `Concluded` failed. rsReticulum hands the local hub's resource events over a bounded
  channel and drops one when it is full. A lost end of a Resource to a client leaves its link's
  next Resource waiting until the link closes. A client Resource's slot is given up instead: one
  not heard of (accepted, `Started`, `Progress`) for 120 s (`RESOURCE_TIMEOUT`) is freed
  (`relay::expire_slots`), counted once in `client_resource_drops` and logged (`no word of it in
  120 s`); its envelope, no longer waiting on a transfer, then expires as any other. If its end
  does come after all, it is not counted again and its text is dropped (the last 64 given up are
  remembered). The transfer itself, if it still runs, keeps going in rsReticulum until its
  watchdog or the link ends it; only the bouncer's count of it is freed.
- **From the hub** (the upstream session). The session's link takes advertisements itself
  (`ResourceStrategy::AcceptApp`, offers on `LinkSession::resource_offers`). An offer outside the
  bounds, or with 8 already on the way, is refused. One whose envelope is there is accepted; one
  with none waits 5 s for it, then is refused. A whole Resource that matches its envelope is the
  event `UpstreamEvent::Resource { envelope, data }` (`hub_resources_in`). Dropped and counted in
  `hub_resource_drops`, with a log line: a refused offer, an offer nobody announced, a Resource
  whose sha256 is not the announced one or that matches no envelope in 5 s, a transfer that
  failed, one left when the link ended. A malformed envelope, or one past the 8, is logged and
  ignored. Transfers run in tasks of their own; the session's loop only hands out and collects, so
  it never waits on one.
- **To clients** (the relay). A Resource from the hub goes where the NOTICE with its text would
  ([Local hub](#local-hub): a room's clients; a room-less one is the greeting in its window, else
  a command's answer, else every client), with `K_SRC` rewritten as for any hub envelope. A room-less kind `motd` Resource
  is always the greeting (below), whenever it comes, and never answers a command. A kind other than
  `notice` or `motd` (`blob`) has nowhere to go and is dropped (`client_resource_drops`).
  - A client whose HELLO had cap `0` true gets the `RESOURCE_ENVELOPE` (`K_ID` and body as the hub
    sent them) and then the Resource, byte for byte (`client_resources_out` when the client has it
    whole).
  - A client without the cap gets the text as NOTICEs that each fit a packet (`resource::split`,
    counted once per client in `client_resources_split`): one per line, a line too long cut on
    character boundaries, blank lines left out, each with a new 8-byte id and the rest of the
    envelope (`K_SRC`, `K_ROOM`, `K_NICK`, `K_TS`) as the hub sent it. That is how rrcd sends its
    greeting to everyone with resources off, and what any client can show.
  - Sends go through the client link's sender task and its queue, like every envelope, and are
    held during a replay of the room the same way. A link's Resources go one at a time (weechat-rrc
    cancels the transfer it is taking when a second is advertised); envelopes go on meanwhile. At
    most 8 wait per link (`LINK_RESOURCES`), the next is dropped. The sender learns the end from
    the hub destination's `LinkResourceEvent` (`Concluded`, by resource hash) and only then offers
    the next. One not taken, refused, or still waiting or held when the link closes counts in
    `client_resource_drops`, once.
- **The greeting.** A greeting Resource is kept with the greeting NOTICEs (at most 16 items and
  512 KiB of Resources, `MAX_GREETING_BYTES`) and given to every client that says HELLO later, as a
  Resource or as NOTICEs by that client's cap. A greeting over those bounds is still sent to the
  clients linked at the time; later clients get what was kept.
- **History.** A room Resource from the hub is recorded as the NOTICEs the split makes of it, so
  every record stays one packet's envelope and a replay sends packets only, to every client alike.
  The split NOTICEs have new ids; a client that saw the Resource live sees the text again, split,
  in a replay.
- **From clients.** A client may send a `RESOURCE_ENVELOPE` and a Resource for a long room NOTICE,
  as rrcd takes one. The bouncer sends it upstream as ordinary NOTICEs:
  - The envelope is checked as rrcd checks it and answered with rrcd's ERROR texts:
    `resource transfer disabled` while the hub has cap `0` off, `resource too large: <n> > 262144`,
    the malformed-body texts, `too many pending resource expectations` past 8.
  - The local hub's destination decides on each advertisement on the link manager's task
    (`ResourceAcceptPolicy`, `ResourceStrategy::AcceptApp`): it takes one from a link of an
    allowed identity, within the bounds, while the link has fewer than 8 and the hub fewer than 32
    on the way, and refuses the rest (counted). Whether an envelope announced it is checked when
    it is whole, in the relay's task. A new HELLO on the link forgets its envelopes and the
    Resources waiting for one (those are counted dropped); a transfer under way keeps its slot
    until it ends.
  - A whole Resource that matches an envelope of its link is a NOTICE: kind `notice`, UTF-8, a
    room and no `K_DST`, as rrcd relays only that. Its text is cut into NOTICEs
    (`resource::parts`): each body at most the hub's `max_msg_body_bytes` (350 before the first
    WELCOME, rrcd's default) and each envelope within one packet (`LINK_MDU`) as the hub sends it
    on to the room: rrcd puts in the sender's 16-byte peer hash for `K_SRC` (a client's may be
    shorter), the room trimmed and lowercased, and keeps `K_NICK` (the hub's `nick` when one is
    set) and `K_TS` as sent, so each part is sized against a 16-byte (or longer) source, the longer
    of the two room spellings, the nick, the time and an 8-byte id. Cuts fall on UTF-8 boundaries,
    at the last newline in the second half of a part, else the last whitespace, else as full as
    the part goes; the separator is left out. Parts are in order, each with a new 8-byte id. If 16
    parts (`MAX_PARTS`) cannot hold the text cut that way, it is cut as full as each part goes; if
    that still needs more than 16, nothing is sent and the sender gets
    `ERROR "notice too long: <n> bytes > <m>"`, `<m>` 16 times the most one part holds.
  - **No part is a command.** rrcd takes a MSG or NOTICE whose text, stripped, starts with `/` as
    a slash command: it runs it or answers `unrecognized command`, and does not relay it
    (`router.py` `_handle_message`, `body.strip().startswith("/")`). It has no escape for a text
    that starts with `/`, and Python's `strip()` takes off every Unicode space and U+001C to U+001F
    (`envelope::is_command` follows it exactly). So a cut that would start the next part that way
    is passed over for the next best one. A part that starts that way all the same (the first,
    when the text does, or one no cut can spare, a run of `/` longer than a part) starts with
    U+200B ZERO WIDTH SPACE (`resource::COMMAND_GUARD`), counted in the part's size: rrcd does not
    strip it, so the part is relayed, and clients show it as nothing. That is the smallest change
    rrcd does not parse; a leading space would be stripped. A client's short MSG or NOTICE is not
    touched: one that starts with `/` is a command, as the user meant.
  - A long notice goes only to a room in the bouncer's room set (joined or being joined): each
    part waits for the echo of the one before, and the hub echoes only into a room the bouncer is
    in. To another room the sender gets `ERROR "notice not sent: a long notice goes only to a room
    the bouncer is in"` and nothing is sent.
  - The parts go through the path of a client's NOTICE (`message`), each in turn: membership, the
    rate limit, the session's queue and `CLIENT_HOLD` before the WELCOME, ERROR blame, echo and
    history. They go upstream as `Command::SendPart`, each with the relay's number for the notice,
    its index and the count (`upstream::Part`): rrcd handles each packet on a thread of its own
    (`service.py:450`, `_on_packet`), and parts sent back to back reached the room out of order in
    the e2e test. So once a part is sent, the next client envelope of its room (rooms compared as
    the hub names them, `normalize_room`) waits until the hub echoes the part (rrcd forwards it to
    the room, the bouncer's link included, with its `K_ID`); other rooms go on meanwhile. What the
    hub would refuse each part for is answered once, and the parts go all or none: if the relay's
    queue to the session has no room for all of them the sender gets
    `ERROR "not sent to the hub: the bouncer's queue is full"`. Before the upstream's first
    WELCOME a client's `RESOURCE_ENVELOPE` is taken like a short NOTICE: the parts are queued for
    the session or dropped as below.
  - **A notice cut short.** The parts after one that failed are not sent, and the sender hears so
    once: `ERROR "<u> of <n> parts of the notice not sent: <reason>"`, with the notice's room. A
    part fails when:
    - the hub refuses it: an ERROR the relay puts on that part (blame, as for any envelope; the
      hub's own ERROR goes to the sender as always). The relay tells the session
      (`Command::CutNotice`); reason `the hub refused part <i>`, and `<u>` counts the parts after
      it;
    - no echo of it comes in 10 s (`upstream::ECHO_WAIT`, long enough for a slow path's round
      trip): reason `no echo of part <i> from the hub in 10 s`, `<u>` the parts after it. Whether
      that part itself reached the room is not known;
    - the session does not send it (held past `CLIENT_HOLD`, larger than the link carries, the
      queue full, not connected, the link ended): that part and the rest, with the reason a short
      NOTICE would get.

    The session drops the notice's parts it holds and remembers the last 64 notices cut short, so
    parts that come later are dropped too; each dropped part counts in `dropped_sends`, and the
    report is `UpstreamEvent::PartsDropped`. No ERROR is sent when nothing is left unsent (the
    last part refused or late). While a room waits for a part's echo, that time does not count
    towards the `CLIENT_HOLD` of anything waiting for the room (`upstream::pause_hold`), so a
    notice going out slowly does not age out what is behind it; each wait is bounded by
    `ECHO_WAIT`, so 16 parts hold a room at most 160 s.
  - rrcd echoes each NOTICE to the room, the bouncer's link included, so the user's clients in the
    room (the sender too) get the parts at their echo, in order, as for any room NOTICE, and
    history records them there. A direct client of rrcd gets the parts as ordinary NOTICEs.
  - `client_resources_in` counts the text once when its parts go; `relayed_in` counts each part.
    Dropped silently and counted in `client_resource_drops`: a Resource no envelope announced, one
    whose sha256 is not the announced one (rrcd drops it silently too), a kind other than
    `notice`, text that is not UTF-8, a NOTICE with `K_DST` or no room, a transfer that fails or
    is given up on. A refusal with an ERROR (too long, a full queue, a room the bouncer is not in)
    counts it dropped too.
  - Sending the text on as NOTICEs keeps it readable to every client of the hub. rrcd 0.3.2 with
    RNS 1.6 takes no client Resource: its accept callback reads `resource.total_size` or
    `resource.size` (`resources.py:253`) of a `ResourceAdvertisement`, which has neither, so the
    advertisement times out; and its forward to the room calls `link.packet(payload)`
    (`resources.py:420`), which is `None` on an `RNS.Link`.
- **Status.** Per hub: `hub_resources_in` and `hub_resource_drops` (the upstream session),
  `client_resources_in` (client texts sent on as NOTICEs), `client_resources_out` (Resources
  clients took whole), `client_resources_split` (Resources sent to a client as NOTICEs) and
  `client_resource_drops` (the relay: client Resources dropped or refused, Resources to clients
  not taken, hub `blob`s). The keys are distinct across the parts of a hub entry, which
  `status.json` flattens into one object (a unit test checks it).

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
  not decode back to the same name is ignored. A room is not recorded (counted in
  `history_drops`, warned once) when its file name with `.tmp` added, the longest name the history
  makes from it, would be over 255 bytes: 247 bytes of plain name, or 82 bytes that each take
  `%XX`.
- **Writer.** Each hub has one history thread (`src/history.rs`) fed by a bounded queue (4096
  jobs). `Relay::record` puts a job on it with `try_send` and never waits: when the queue is full
  the record is dropped and counted in `history_drops`, so a slow disk cannot hold up the relay.
  A warning is logged when the queue fills and one when it has room again, with how many records
  were dropped in between. Replay queries go through the same queue, so a query sees every record
  queued before it. At stop the thread syncs, then closes the queue before it answers: a record
  queued after the stop is counted in `history_drops`, a query after it gets no answer (its
  replay is empty), so nothing is taken after the stop is acknowledged.
- **Open files.** At most 64 room files are open at once; at 64 the least recently written is
  synced and closed. A file not written for 60 s is closed too.
- **Index.** For each room the thread keeps where its newest 500 replayable records are and their
  times, built when the file is first read and kept up as records are written. A replay reads
  just the records it sends. Only when the index cannot answer (a time window reaching back past
  records it has let go, as when the clock went back) is the file read through, a record at a
  time; memory stays at most 500 entries per room either way.
- **Durability.** Each record is one `write` to a file opened with `O_APPEND`. The thread calls
  `fdatasync` on the files written to 5 s after a write, before a file is closed and at shutdown
  (the bouncer gives it 5 s), not after each record: a power loss can take the last 5 s of
  history, a crash of the bouncer alone loses nothing written. A file stays to be synced until a
  sync of it succeeds: one that fails is tried again 5 s later, counted in `history_sync_errors`
  and warned about at most once a minute.
- **Damage.** A record counts only if it decodes as a record and its envelope decodes as an
  envelope (a record cut short reads on into the next one, and its envelope then mostly does
  not), and if it ends at the end of the file, at the start of a record, or with no record
  starting inside it. A record cut inside an envelope whose last field is a byte string can read
  on into the next record and still decode; it does not end where a record starts, and a record
  that reads starts inside it, so it is taken as damage and the record it ran into is kept, and
  never rewritten away by a prune.
  - Before a room file is first appended to, it is read through and the thread knows its good
    length: where its last whole record ends. Before each append the file's length is checked
    against that; bytes past it that are no record (a write cut short by a crash or a full disk,
    or written behind the bouncer's back) are cut off in place (`set_len`, then `fdatasync`),
    which needs no free space. Nothing is appended after bytes that are no record: if the cut
    fails the room is marked broken, the record is dropped and counted, and the cut is tried
    again before the next record of the room.
  - A write that fails is cut back off the file the same way and counted as a drop.
  - Damage in the middle of a file (a bad first record included) is read past: reading goes on
    at the next place a record starts (the bytes of a record's first key, `a2 65 ts_ms`) and reads
    from there, however far on that is. The records on either side are kept and replayed. The
    next prune writes the file again without the bytes that are no record (logged with their
    size); they are not kept anywhere else.
  - The prune at start and every hour cuts a bad tail off too (logged), and removes `*.tmp` files
    a crash left. A file is never removed for damage; only one left with no record is.
  - **Known limitation.** A record cut inside a last byte-string field that reads on to swallow
    whole records and ends exactly where another record starts (or at the end of the file) is
    taken as one record, and the records it swallowed are lost with it. Its bytes are the same as
    a sound record whose byte-string body holds a whole record, which any peer can send, so no
    check on the bytes alone tells them apart, and taking such a body as damage would let a peer
    hide or forge records through it. Closing it needs a check the format does not carry (a
    length or checksum per record), so it is left. It takes damage that is not at the file's end:
    a cut tail is cut off before the next append, and a text body mostly fails to decode as
    UTF-8 when it runs on into a record.
- **Retention.** `history.days` (default 30) and `history.max_mb` per hub (default 100, in units of
  1,000,000 bytes). At start and every hour the thread reads the hub's room files a record at a
  time and judges each record by its own `ts_ms`: it drops the records older than `days`, then,
  while the hub's records together are over `max_mb`, the hub's oldest records, whichever room
  they are in (one time for the whole hub, below which every record goes). A file that changes is
  written to a `.tmp` file, synced and renamed over the old one (the directory synced after); a
  file left empty is removed. Between prunes a hub's history can grow past `max_mb` by an hour's
  traffic. Records are in the order the bouncer got them, and a file is not sorted: a clock that
  went back leaves newer records before older ones, and each is still judged by its own time.
- **Times ahead of the clock.** A record stamped more than a day ahead of the clock at a prune
  (written while the clock was wrong) is taken as received at that prune: its `ts_ms` is
  rewritten to the prune's time, so it is neither kept forever nor replayed as the newest record
  for ever after. Replay windows judge each record's time as it is in the file.
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
- **Pacing.** Replays are sent by the client link's sender task ([Local hub](#local-hub)), one
  envelope every 20 ms per link: a link's replays go one after another, never interleaved, so two
  rooms joined at once are one paced stream, the second room's after the first's. Each send is
  given 2 s as any send to a client (counted in `relayed_out`, or in `relay_drops` and the rest of
  that replay given up). Other clients and the upstream session are not held up.
  - The client sees the room's JOINED, then the replay, then the room's live lines. From the
    moment a replay of a room is asked until it ends, the room's live envelopes for that client
    are held (up to 512, the rest dropped and counted in `relay_drops`) and sent after
    `--- end of replay ---`, so the client sees them in order and none twice: the query is queued
    when the client joins, before any live envelope that follows is recorded.
  - What was held is let out one item per turn of the link's sender, so the link's other
    envelopes and replay commands are not held up behind it. Until the last of it is out, the
    room's new envelopes queue after it (at most 1024 together, `MAX_FLUSHING`, the rest dropped
    and counted), and the link's next replay waits.
  - A replay is asked for only if the client's JOINED for it was queued: when that was dropped
    (the link's queue full) the replay is not run, nothing is said, and the relay is told it is
    over. At most 32 replay commands wait for a link's sender (`CONTROL_QUEUE`); one more is not
    run and the client gets `--- replay of <room> skipped: the link is busy ---` from the local
    hub. The relay keeps at most one replay per client and room, so a JOIN repeated while one runs
    asks for none; an end of a replay is never refused, and drops the waiting replays of its room.
  - The bouncer's own notices (the `/history` usage, `--- a replay of <room> is running ---`) are
    never held. A replay with nothing to send says `--- no history in <room> ---` at once, without
    waiting for the link's other replays.
  - Each replay has an id. A replay still running 120 s after it was asked is ended: what is left
    of it is not sent, what was held is (logged). Its late end is told apart by the id and does
    not end a newer replay of the room.
  - A PART of the room ends the client's replay of it and drops what was held for it or is still
    being let out (its Resources counted in `client_resource_drops`), and the
    PARTED goes out at once, before any later line of the room; a JOIN after that starts a fresh
    replay. A HELLO ends all of the link's replays (before its WELCOME). A link that closes stops
    its sender and everything queued for it.
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
- **Clients and `/history`.** The client has to send `/history N` to the room as a MSG:
  - weechat-rrc: `/history` is a WeeChat core command (the input history), and the plugin hooks
    only its own commands (`plugin.c:2376`, `:2519`), so WeeChat takes `/history 50` itself. Type
    `//history 50` in the room's buffer: WeeChat sends a line that starts with `//` as text, with
    one `/` taken off.
  - rBrowser2 (`rrc.py:1563`, `:1463-1466`): a line starting with `/` that is not one of its own
    commands is sent to the hub as a MSG, so `/history 50` works as typed.
  - NomadNet (`ui/textui/Channels.py:869`, `:982-989`, `:997-998`, `:1109-1118`): every line
    that starts with `/` is a command, and it sends to the hub only the commands in its own list,
    which has no `history`. `/history 50` gets "Unknown command" and `//history 50` is the
    command `/history`, unknown too. A NomadNet user cannot ask for history; the replay on JOIN
    still comes.
- **Status.** Each `hubs` entry adds `history_bytes` and `history_rooms` (as of the last write or
  prune), `history_records` (records written since start), `history_drops` (records not
  written: queue full, disk error, a broken file, room name too long) and `history_sync_errors`
  (syncs that failed). They go out with the next write of `status.json` (every minute, a change of
  clients, shutdown).

## Configuration

TOML, like lxmf-bouncer's (relative paths relative to the config file, `~/` the home directory).
See `contrib/config.example.toml`, which comments every key and uses `tcp`: the mode with no
announce leak ([Announces](#announces)), and the one the README recommends. A unit test parses it.

`contrib/rrc-bouncer.service` is a systemd user unit (`Restart=always`, `UMask=0077`) that makes
the file system read-only to the bouncer (`ProtectSystem=strict`, `ProtectHome=read-only`) except
the default `state_dir` (`ReadWritePaths`), which holds the `state_dir/lock` flock, and adds
`PrivateTmp`, `NoNewPrivileges`, `RestrictSUIDSGID`, `LockPersonality`, `RestrictRealtime` and
`RestrictAddressFamilies` (Unix, IP, netlink). lxmf-bouncer's unit has none of these; they fit a
daemon that writes only its state directory. `ProtectSystem=strict` alone leaves `/home` writable
in a user unit (systemd 262), hence `ProtectHome`. The flock was checked under these settings in a
transient unit: taken in the state directory, refused while another process holds it.
The unit has no `After=` or `Wants=` on `rnsd.service`: a user unit cannot order itself after a
system unit, and rnsd is often run by hand. `Restart=always` covers a bouncer in
`shared_instance` mode that exits while rnsd is down; in `tcp` mode it reconnects by itself.

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
| `[history] days / max_mb / replay_hours / replay_max` | `30 / 100 / 24 / 100` | [History](#history); `days` and `max_mb` at least 1 (`max_mb` in units of 1,000,000 bytes), `replay_max` at most 500, either `replay_*` 0 turns the replay on JOIN off |
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
- Each `hubs` entry adds `history_bytes`, `history_rooms`, `history_records`, `history_drops`
  and `history_sync_errors`. A file without these fields still reads.
- `status` prints, per hub, `history          <bytes> bytes in <n> rooms` (`1 room`) and
  `history written  <records> records, <drops> dropped`, and `history syncs    <n> failed` when
  any failed.
- `run` holds `state_dir/lock` while it runs, so a second bouncer on the same `state_dir` does
  not start ([Upstream session](#upstream-session)).

As of M5:

- `run` relays Resources from the hub to clients, and sends a client's long notice upstream as
  NOTICEs ([Resources](#resources)).
- Each `hubs` entry adds `hub_resources_in`, `hub_resource_drops`, `client_resources_in`,
  `client_resources_out`, `client_resources_split` and `client_resource_drops`. A file without
  these fields still reads, and so does one with fields it does not know.

### `status`

`status` reads `state_dir/status.json` and prints it; `status --json` prints the file as it is. The
bouncer writes the file at start, at once on a change of link state, rooms or clients, every
minute (`STATUS_INTERVAL`) and at shutdown, so the counters can be up to a minute old. `status`
says `not running` when the pid in the file is gone or the file is over 3 minutes old. The output,
per hub (lines in brackets only when they apply):

```
rrc-bouncer status (updated 12 s ago)
  identity           <the user's identity hash>
  started            3 h ago
  announce           off
  hub <upstream hash>  local hub <local hub hash>
    upstream         connected to "<name>" (version v), since 3 h ago
    rooms            lobby, side (joined 2 of 2)
   [not joined       <room> (<reason>); …]
    connects         1 (0 reconnects), 0 failed attempts
   [last disconnect  <when>: <why>]
   [last error       <text>]
   [next attempt     in <n> s]
   [not relayed      <type> <n>, …]
   [dropped sends    <n>]
   [dropped events   <n>]
    clients          2 (<identity hash>, <identity hash>)
    relayed          120 in, 2400 out
   [relay drops      <n>]
   [refused links    <n>]
    hub resources    1 received, 0 dropped
    client resources 0 received, 2 sent, 1 as notices, 0 dropped
    history          88950 bytes in 2 rooms
    history written  630 records, 0 dropped
   [history syncs    <n> failed]
```

`status.json`:

| Key | Meaning |
|---|---|
| `pid`, `started_at`, `updated_at` | The bouncer's process id; when it started and last wrote the file (seconds since the epoch) |
| `identity_hash`, `announce` | The user's identity hash; whether the local hubs are announced |
| `hubs[]` `upstream`, `local_hub` | The upstream hub's destination hash and the local hub's |
| `state` | `starting`, `connecting`, `linked` (waiting for the WELCOME), `connected`, `waiting` (to connect again), `stopped` |
| `hub_name`, `hub_version`, `hub_caps`, `hub_limits` | From the hub's WELCOME |
| `rooms`, `rooms_joined`, `rooms_failed`, `rooms_retrying`, `rooms_over_limit` | The room set and how each room stands on the current link ([Upstream session](#upstream-session)) |
| `last_connected_at`, `last_disconnected_at`, `last_error`, `next_attempt_at` | The link's history |
| `connects`, `reconnects`, `failed_attempts` | Welcomed links, welcomed links after the first, attempts with no WELCOME |
| `unhandled`, `dropped_sends`, `dropped_events` | Hub envelopes nothing took (by type), client envelopes not sent upstream, events the relay did not take |
| `hub_resources_in`, `hub_resource_drops` | Resources from the hub taken whole and matched, Resources from the hub dropped ([Resources](#resources)) |
| `clients`, `client_identities`, `refused_links` | Client links, their identities, links of other identities refused |
| `relayed_in`, `relayed_out`, `relay_drops` | Client envelopes passed upstream, envelopes sent to clients, envelopes dropped |
| `client_resources_in`, `client_resources_out`, `client_resources_split`, `client_resource_drops` | [Resources](#resources) |
| `history_bytes`, `history_rooms`, `history_records`, `history_drops`, `history_sync_errors` | [History](#history) |

## Implementation notes

- **Crate layout.** `src/lib.rs` (`config`, `identity`, `envelope`, `history`, `bouncer`,
  `upstream`, `rate`, `rooms`, `relay`, `resource`) and a thin
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
  `--only history` the history phases, `--only resource` the Resource phases)
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
  name `(via rrc-bouncer)` and its caps and limits (`resource_envelope` on, as rrcd's), then rrcd's greeting
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
  - With 400 records written into `hist2.log` by hand and `replay_max = 500`, A9 joins `hist2`
    and `hist` at once: each JOINED comes before its room's replay, all 400 of `hist2` come framed
    and then all five of `hist`, neither replay interleaved with the other.
  - A9 parts `hist2` and joins it again; a second after its replay starts, A9 parts it: the
    PARTED comes within 3 s, the replay stops there and nothing of `hist2` follows the PARTED; a
    JOIN after that gets a fresh replay of all 400.
  - With 1500 records of about 1.1 kB written by hand into `p1.log`, `p2.log` and `p3.log`, their
    times shuffled across the rooms and out of order in each, and `max_mb = 1`: at start the hub's
    history is cut to at most 1,000,000 bytes by taking the oldest records whichever room they are
    in (every record that went is older than every one that stayed, each room keeps some, and no
    more went than had to); `hist` and `hist2`, newer, are kept whole.
  - A second `run` on the same `state_dir` exits 1 with `another rrc-bouncer is running`; the
    first runs on, and `status` works beside it.
  The Resource phases follow, on stock rrcd restarted with a greeting of 24 lines and 1871 bytes
  (UTF-8 with `ü` and `✓`), the bouncer in `lobby` and `res`. The driven peers number each packet
  as it comes off the link (`RNS.Link.receive`, before Python RNS hands it to a thread of its own),
  so order is checked as it was on the wire:
  - rrcd sends the greeting as a Resource (its log: `kind=motd size=1871`) and the bouncer takes it
    (`hub_resources_in` 1, no drops). RA (cap `0` on) and RN (no caps) link afterwards and get it
    from the cache: RA the `RESOURCE_ENVELOPE` (kind `motd`, room-less, `K_SRC` the local hub,
    size and sha256 of the greeting) and then a Resource byte-identical to the greeting; RN the
    greeting's 24 lines as NOTICEs from the local hub, in order, and no envelope or Resource. rrcd
    restarted, both get it live the same way. `client_resources_out` 2 and
    `client_resources_split` 2.
  - RD joins `res` directly on rrcd. RA sends a 30-line, 1799-byte NOTICE to `res` as an envelope
    and Resource. RD gets it as 6 NOTICEs of at most 350 bytes from the user, in order, each with
    an id of its own and the bouncer's nick, and no envelope or Resource; rrcd forwarded 6 NOTICEs
    and logged no resource expectation. RA and RN get the 6 NOTICEs at the hub's echo, in order;
    RA gets no Resource back. The history of `res` holds the 6 NOTICEs in order.
    `client_resources_in` goes up by exactly one, `relayed_in` by exactly 6, no drops.
  - RA sends a 6299-byte notice, more than 16 parts hold: it gets
    `ERROR "notice too long: 6299 bytes > 5600"` from the local hub; RD gets nothing, rrcd forwards
    nothing, `client_resource_drops` goes up by exactly one and nothing is relayed.
  - RA sends a long notice to `res` whose text starts `/topic res` and whose 24 other lines each
    start `/kick dee,`. RD gets it as NOTICEs, the first starting with U+200B then `/topic`, none
    starting with `/` once stripped as rrcd strips, and together the text in order with one space
    or newline left out at each cut; rrcd forwards them all and nobody gets an ERROR (no
    `unrecognized command`); `relayed_in` goes up by the parts, `dropped_sends` not at all.
  - RD founds `mod` and sets it `+m` (`/mode mod +m` as a MSG); RA joins it through the bouncer and
    sends the 6-part notice there. RA gets the hub's `ERROR "room is moderated (+m)"` and then one
    `ERROR "5 of 6 parts of the notice not sent: the hub refused part 1"`, both in `mod`, and
    nothing more in the next 12 s; RD gets no part, rrcd forwards nothing, `dropped_sends` goes up
    by exactly 5, and `res` goes on (RD's next MSG reaches RA).
  - RA sends the notice to `elsewhere`, a room the bouncer is not in: it gets
    `ERROR "notice not sent: a long notice goes only to a room the bouncer is in"` in `elsewhere`,
    rrcd forwards nothing, `client_resource_drops` goes up by exactly one and nothing is relayed.
  - From RA: an envelope of size 0 gets `ERROR "resource envelope invalid size"`; one announcing
    262145 bytes gets `ERROR "resource too large: 262145 > 262144"` and its Resource is refused;
    a Resource whose sha256 is not the announced one reaches the bouncer and is dropped with no
    ERROR. `client_resource_drops` goes up by exactly two (envelopes are not counted); nothing is
    relayed and RD sees nothing, and RA's next MSG reaches RD. `status` prints both Resource
    lines, the bouncer stops cleanly, and its status at shutdown shows no later drop.
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
5. **M5: finish.** `RESOURCE_ENVELOPE` relay (the hub's greeting and long notices to clients, a
   client's long notice to the hub as NOTICEs), `status`,
   README (setup, every client's settings, limits), `contrib/` unit and example config final.
   A manual check list for NomadNet, weechat-rrc and rBrowser2 against the bouncer goes in the README
   (done: [Resources](#resources), [`status`](#status)).

## Out of scope for 0.1

Patches to clients (`K_TS` display, dedupe by id), push notifications or LXMF mention forwarding,
several users on one bouncer, a web UI, hosting rooms of its own.
