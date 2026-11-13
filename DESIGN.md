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
  It accepts Links, requires `identify()`, and accepts only identities in `allowed_identities`
  (default: the user's own). A client from another identity gets `ERROR` and the link is closed.
  - HELLO from a client: answer WELCOME with the upstream's caps and limits and hub name
    `"<upstream name> (via rrc-bouncer)"`. Before the upstream's first WELCOME, answer with rrcd's
    defaults.
  - JOIN: add the room to the hub's room set (saved in state, so it survives a restart), JOIN
    upstream if not already in, answer the client with JOINED, then replay history (below).
  - PART: by default the bouncer stays in the room upstream and only the client's view ends
    (`part_upstream = false`, as ZNC detaches). With `part_upstream = true`, a PART from any client
    parts upstream and removes the room from the set.
  - MSG, ACTION, NOTICE (with or without `K_DST`): pass upstream unchanged (same `K_ID`, `K_NICK`,
    unknown keys), record it in history, and relay it to the other clients in that room.
  - Envelopes from upstream: record room traffic in history, then relay to every client linked to
    this local hub that is in the room (or, for a direct NOTICE, to every client). Rewrite `K_SRC`
    to the local hub's identity when the upstream envelope's `K_SRC` is the upstream hub's identity.
  - PING from a client: answer PONG locally.
  - Rate: the upstream rate limit is per link and every client shares it. Queue client envelopes and
    send them upstream no faster than the WELCOME limit allows (the upstream session's bucket,
    below).
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
  `"<upstream name> (via rrc-bouncer)"`. It is the local hub's `default_app_data`
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
  the room (`too many rooms`, a bad name: no room; `+i`, `+k`, ban: the room). The answer is:
  - `JOINED` for the room: joined (rrcd sends the joiner its JOINED without a nick; the bouncer is
    not yet a member, so it cannot be another member's).
  - `ERROR` naming the room, or naming no room unless it is `rate limited`: the room failed, with
    the hub's text. It is not tried again on this link.
  - No answer in 20 s: failed, `no answer from the hub`.
  - A name over `max_room_name_bytes` fails without being sent. Rooms past
    `max_rooms_per_session` are not sent either and show as over the limit.
  Failed and over-limit rooms are tried again on the next link. An `ERROR` naming a room the
  bouncer is in (a kick: `kicked from <room>`) takes it out of the joined list until the next link
  or a new JOIN for it from a client (M3).
- **PART** (M3, `part_upstream = true`): sends PART and takes the room as left on a `PARTED`
  without a nick (other members' PARTED carry theirs).
- **PING.** A PING from the hub gets PONG with the same body (rrcd PINGs only if its
  `ping_interval_s` is set, and drops a link whose PONG is `ping_timeout_s` late). After 120 s with
  nothing from the hub the bouncer sends PING (body: the time as a float, as rrcd does); no PONG in
  60 s, or the link closing, ends the link. 120 s keeps an idle link's traffic small and is far
  inside rrcd's defaults; a Reticulum link that went silent is otherwise noticed only after its
  keepalive, minutes later.
- **Reconnect.** After the link ends, wait 5 s, doubling to 5 min, then connect again. A link that
  was welcomed and stayed up 60 s resets the wait to 5 s; a shorter one does not, so a hub that
  welcomes and then drops the link at once is not hammered. Every room is joined again on the new
  link.
- **Rate.** rrcd's bucket is per link: `rate_limit_msgs_per_minute` tokens, full when the link
  opens, one token per packet of any type, and a packet into an empty bucket gets
  `ERROR "rate limited"` and is lost (`session.py` `refill_and_take`, `router.py:80-91`). The
  session keeps the same bucket (`src/rate.rs`) at 90 % of the limit (240 a minute until the
  WELCOME) and holds a packet until there is a token instead of losing it. Its own envelopes
  (PONG, PING, JOIN, PART) go first; client envelopes (M3) wait in a queue of 256 behind them and
  are sent only once the link is welcomed. A client envelope with the queue full or no link up is
  dropped and counted (`dropped_sends`).
- **Hooks for M3/M4.** `Command` (`Send`, `Join`, `Part`) goes in through the channel `run` gets;
  `UpstreamEvent` (`Up(welcome)`, `Down`, `Envelope(env, Inbound)`) comes out through
  `Upstream::events`, with backpressure (`send().await`). With no events channel (M2), the
  envelopes the session does not handle itself (MSG, NOTICE, ERROR it cannot place, …) are logged
  at debug and counted by type in `unhandled`.
- **Room set file.** `state_dir/rooms/<upstream hash>.json`, `{"rooms": [...]}`, mode 0600 in a
  0700 directory, written through a temporary file and a rename. It holds the rooms added while
  the bouncer runs (a client's JOIN, M3); the config's `rooms` are not copied into it, and the
  config file is never written. On start the names are normalised as the config's are and the file
  is written back if that changed it; a file that does not parse stops the start with its path and
  is left as it is. A config room taken out at run time (M3) comes back on the next start.

## History

- One append-only file per hub and room: `state_dir/history/<upstream hash>/<room>.log`, a sequence
  of CBOR records `{ts_ms, envelope}` where `envelope` is the envelope bytes as received or sent.
  Kept on the local disk unencrypted, mode 0600, directory 0700 (README says so).
- Retention: `history.days` (default 30) and `history.max_mb` per hub (default 100); prune at start
  and hourly, oldest first.
- Replay on JOIN: the envelopes of the last `history.replay_hours` (default 24), at most
  `history.replay_max` (default 100), sent between two NOTICEs from the local hub
  (`"--- replay: N messages since <time> ---"` and `"--- end of replay ---"`), each with its
  original `K_ID`, `K_TS`, `K_SRC` and `K_NICK`.
- A `/history N` MSG from a client is not forwarded upstream; the bouncer answers it with the last
  N envelopes in that room, framed the same way.

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
| `[history] days / max_mb / replay_hours / replay_max` | `30 / 100 / 24 / 100` | Above; `days` and `max_mb` at least 1, `replay_*` 0 turns replay off |
| `[[hub]] address` | required | The upstream hub's `rrc.hub` destination hash |
| `[[hub]] rooms` | `[]` | Rooms to be in from the start (more are added by clients' JOINs and kept in the room set file). Trimmed and lowercased as rrcd does (`service.py:507`), duplicates dropped, an empty name refused |
| `[[hub]] nick` | none | `K_NICK` the bouncer uses on its own envelopes (HELLO, JOIN, PART, PING; not PONG, as rrcd's has none); empty means none |

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
  `rooms_over_limit`, `last_connected_at`, `last_disconnected_at`, `last_error`, `connects`
  (welcomed links), `reconnects` (welcomed links after the first), `failed_attempts` (links that
  ended before a WELCOME, or never opened), `next_attempt_at`, `unhandled` (type → count) and
  `dropped_sends`. A change of state or rooms writes the file at once; the counters go out with
  the next minute's write. A file without these fields (from M1) still reads.
- `status` prints, per hub, the link (`connected to "<name>" (version v), since …` or its state and
  last error), the room set with `joined N of M`, the rooms not joined and why, the connect
  counts, the next attempt, the envelopes not relayed yet and the dropped sends.

## Implementation notes

- **Crate layout.** `src/lib.rs` (`config`, `identity`, `envelope`, `bouncer`, `upstream`,
  `rate`, `rooms`) and a thin
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
  `RRC_TEST_WORK`, default `/tmp/rrc-bouncer-e2e`; `--only rrcd` runs the rrcd phases alone)
  builds the bouncer and runs `tests/e2e.py`: rnsd instances with unique instance names and
  TCPServerInterfaces on 127.0.0.1, ports 4370-4379, and Python peers (`tests/rrc_peer.py`) that
  watch announces, request paths, link, identify and PING, or act as an RRC client of a hub. It
  covers the phases cited under [Announces](#announces), the allow-list (a link identified as
  another identity gets no PONG), a PING sent right behind the identify, `hashes`, `status` and a
  clean stop.
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
   foreign identity is refused.
4. **M4: history.** Store, retention, replay on JOIN, `/history N`. e2e: messages said while no
   client is linked are replayed, in order, with their original ids, to a client that joins later;
   after a bouncer restart too.
5. **M5: finish.** `RESOURCE_ENVELOPE` relay (MOTD and long notices both ways), `status`,
   README (setup, every client's settings, limits), `contrib/` unit and example config final.
   A manual check list for NomadNet, weechat-rrc and rBrowser2 against the bouncer goes in the README.

## Out of scope for 0.1

Patches to clients (`K_TS` display, dedupe by id), push notifications or LXMF mention forwarding,
several users on one bouncer, a web UI, hosting rooms of its own.
