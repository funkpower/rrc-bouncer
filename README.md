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
0.1 will do.

## Build

Needs Rust 1.89 or newer. rsReticulum is a git submodule.

```
git clone --recursive <this repository> rrc-bouncer
cd rrc-bouncer
cargo build --release      # target/release/rrc-bouncer
```

## License

[Reticulum License](LICENSE).
