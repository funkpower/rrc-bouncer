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
    send them upstream no faster than the WELCOME limit allows.
- **Announces.** The bouncer must not spread fake hubs across the network by default
  (`announce = false`). Clients reach it by the hash `hashes` prints; a path request from a client on
  the same transport node must still resolve. **Verify in M1 whether rsReticulum answers a path
  request for a local destination that never announced.** If it does not, announce once at start and
  then only in answer to path requests, and document it. `announce = true` with
  `announce_interval_minutes` turns on normal announces with the app data above.

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
| `state_dir` | required | Local hub identities, room sets, history, `status.json`, generated Reticulum config |
| `log_level` | `info` | As lxmf-bouncer |
| `[reticulum]` | | `shared_instance` / `instance_name` / `tcp` / `config_dir`, exactly as lxmf-bouncer (copy its code) |
| `announce` | `false` | Announce the local hubs |
| `announce_interval_minutes` | `360` | With `announce = true` |
| `allowed_identities` | `[]` | Identity hashes allowed to use the local hubs besides the user's |
| `part_upstream` | `false` | A client's PART also parts upstream |
| `[history] days / max_mb / replay_hours / replay_max` | `30 / 100 / 24 / 100` | Above |
| `[[hub]] address` | required | The upstream hub's `rrc.hub` destination hash |
| `[[hub]] rooms` | `[]` | Rooms to be in from the start (more are added by clients' JOINs) |
| `[[hub]] nick` | none | `K_NICK` the bouncer uses on its own envelopes (JOIN, PART, PING) |

## Commands

As lxmf-bouncer: `run` (default), `status [--json]` (from `state_dir/status.json`, written every
minute: upstream link state per hub, rooms, clients linked, history size, envelopes relayed),
`hashes` (the user's identity hash and, per upstream hub, the local hub address to put in clients).

## Milestones

Each milestone ends with `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and
`cargo test` passing, and is landed on `main`. Unit tests next to the code; end-to-end tests in
`tests/` against a real rrcd, run the way lxmf-bouncer's `tests/e2e.sh` runs (a throwaway Reticulum
instance with no interfaces to the outside, so nothing reaches a real network).

1. **M1: foundations.** Config (with tests), identity loading, the Reticulum runtime and its three
   attachment modes (lift from lxmf-bouncer), the envelope codec (round-trip tests against envelopes
   produced by rrcd's own `envelope.py`), local hub identities, `hashes`. Answer the path request
   question above and write the answer into this file.
2. **M2: upstream session.** Link, identify, HELLO/WELCOME, JOIN the room set, PING/PONG,
   reconnect with backoff and rejoin. e2e: the bouncer joins a room on a local rrcd and a Python test
   client in the same room sees its JOINED.
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
