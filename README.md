# rrc-bouncer

One RRC identity, several RRC clients, and the history of every room. Like an
IRC bouncer, but for [RRC](https://rrc.kc1awv.net/) (Reticulum Relay Chat) on
[Reticulum](https://reticulum.network).

## Why

RRC knows you by the identity your client links to a hub with. Run the same
identity in two clients and the hub sees two links for one person: direct
messages go to whichever linked last, and stop when that one leaves even if
the other is still there. Run a different identity in each and you are a
different person on every device. And hubs keep no history: whatever was said
while none of your clients was linked is gone.

rrc-bouncer is a small always-on daemon that holds your identity and keeps one
link to each hub, in your rooms, all the time. Your clients link to the
bouncer instead of the hub and speak plain RRC to it. Everything said in your
rooms is kept, and a client that joins a room gets what it missed.

```
  weechat-rrc ─┐
  rBrowser2  ──┼──▶ rrc-bouncer ──(one link, your identity)──▶ hub
  NomadNet   ──┘        │
                        └─ history of every room, replayed on JOIN
```

## Status

Under development. [DESIGN.md](DESIGN.md) describes how it works and what
0.1 will do. So far the bouncer reads its config, creates a local hub for each
upstream hub, prints their addresses (`rrc-bouncer hashes`) and answers path
requests and PING on them. It links to each upstream hub as you, joins your
rooms, keeps the link up and reconnects when it drops. Your clients link to
the local hub under your identity (or one in `allowed_identities`), join and
part rooms, and their messages are relayed to the hub and to each other;
`rrc-bouncer status` shows each link, its rooms, its clients and its history.

Everything the hub sends to your rooms is kept in `state_dir/history/`, one
file per hub and room, for 30 days and up to 100 MB per hub (`[history]`
`days` and `max_mb`). It is stored unencrypted, readable by your user only:
anyone who can read your files can read your rooms. A client that joins a room
gets the last 24 hours, at most 100 messages (`replay_hours` and `replay_max`),
between a `--- replay: N messages since <time> ---` and an
`--- end of replay ---` notice. The bouncer does not know what a client has
already seen, so a client that comes back gets those lines again, and your own
messages from other clients are shown as yours. Clients show replayed lines at
the time they arrive; the first notice says when they start. Say
`/history N` in a room to get its last N messages (at most 500); it stays on
the bouncer and never reaches the hub.

Set `nick` on each `[[hub]]`. The hub takes the nick of every message it gets,
and all your clients share one link, so without it your nick on the hub
changes to whichever client spoke last.

## Build

Needs Rust 1.89 or newer. rsReticulum is a git submodule.

```
git clone --recursive <this repository> rrc-bouncer
cd rrc-bouncer
cargo build --release      # target/release/rrc-bouncer
```

## Test

```
cargo test
RRC_TEST_VENV=/path/to/venv tests/e2e.sh   # venv with: pip install rns cbor2 rrcd
```

The end-to-end test runs its own rnsd and rrcd instances on 127.0.0.1 (ports
4370-4379) and never uses the host's own.

## License

[Reticulum License](LICENSE).
