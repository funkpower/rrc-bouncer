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

For each hub you configure, the bouncer runs a **local hub**: an RRC hub of
its own, at an address of its own, that your clients link to. It answers them
as the real hub would (same rooms, limits and capabilities, name
`<hub name> (via rrc-bouncer)`), passes what they say to the hub, and passes
the hub's traffic back to every client in the room.
[DESIGN.md](DESIGN.md) has the details.

## Build

Needs Rust 1.89 or newer. rsReticulum is a git submodule.

```
git clone --recursive <this repository> rrc-bouncer
cd rrc-bouncer
cargo build --release      # target/release/rrc-bouncer
install -Dm755 target/release/rrc-bouncer ~/.local/bin/rrc-bouncer
```

It needs a Reticulum network to reach the hubs on: normally the `rnsd` already
running on the machine.

## Setting up

1. **Pick the identity.** Use the one a client already speaks as, so the hubs
   (and their operators, bans and registered rooms) keep knowing you. NomadNet
   keeps it at `~/.nomadnetwork/storage/identity`; weechat-rrc at
   `rrc.network.identity_file` (`~/.local/share/weechat/rrc/identity` by
   default); rBrowser2 at `settings/rrc_identity` in its directory. Or make a
   new one with `rnid -g <file>`. Copy it for the bouncer and keep it private:

   ```
   install -dm700 ~/.local/share/rrc-bouncer
   install -m600 ~/.nomadnetwork/storage/identity ~/.local/share/rrc-bouncer/identity
   ```

2. **Configure** `~/.config/rrc-bouncer/config.toml` from
   [`contrib/config.example.toml`](contrib/config.example.toml): your hubs,
   their rooms, your nick, and how to reach Reticulum (below).

3. **Get the addresses** to give the clients:

   ```
   rrc-bouncer --config ~/.config/rrc-bouncer/config.toml hashes
   identity           <your identity hash>
   upstream hub       082f8e4293d19281108a5d4faa52dcbe
     local hub        <the address your clients use for that hub>
   ```

   Each local hub has an identity of its own, made on first use in
   `state_dir/hubs/`, so its address stays the same.

4. **Run it** (see [systemd](#running-under-systemd)), then point each client
   at the local hub (see [The clients](#the-clients)).

### Reaching Reticulum

Set exactly one of these in `[reticulum]`:

- **`tcp = "127.0.0.1:4242"`** (recommended): the bouncer connects to a
  `TCPServerInterface` of the rnsd on this machine. Add one to rnsd's config
  if it has none:

  ```
  [[rrc-bouncer]]
    type = TCPServerInterface
    enabled = yes
    listen_ip = 127.0.0.1
    listen_port = 4242
  ```

  The bouncer tells that rnsd the path to each local hub (a path response,
  which goes one hop and is not passed on) when it connects and every 5
  minutes. Your clients ask the same rnsd for the path and get it; nothing
  announces the local hubs to the network.

- **`shared_instance = true`** (with `instance_name` if rnsd's isn't
  `default`): the bouncer attaches to rnsd as its clients do. It works, but
  the local hubs can leak onto the network as announces: when rnsd restarts,
  the bouncer's Reticulum library announces them, and when the bouncer
  restarts while rnsd stays up, rnsd itself may announce them if a client
  asked for their path in between. See [DESIGN.md § Announces](DESIGN.md#announces).
  The bouncer will not start while no shared instance is running; under
  systemd it is restarted until rnsd is up.

- **`config_dir = "<dir>"`**: a complete Reticulum config directory of your
  own; the bouncer runs its own Reticulum instance with its interfaces and
  sends no path responses. How far the local hubs can be found is up to
  those interfaces.

With `shared_instance` or `tcp` the bouncer writes its own Reticulum config to
`state_dir/reticulum/config` on each start.

`announce = true` announces the local hubs, every
`announce_interval_minutes`. Leave it off unless clients that can only find
hubs from announces need it: anyone on the network then sees them, though
only your identity and `allowed_identities` can use them.

## Configuration

TOML. Relative paths are relative to the config file; `~/` is the home
directory. Unknown keys are refused.
[`contrib/config.example.toml`](contrib/config.example.toml) has every key.

| Key | Default | Meaning |
|---|---|---|
| `identity` | required | Your Reticulum identity file (64 bytes). |
| `state_dir` | required | Local hub identities, room sets, history, `status.json`, generated Reticulum config. Created mode 0700. |
| `log_level` | `info` | `error`, `warn`, `info`, `debug`, `trace`, or a tracing filter. `RUST_LOG` overrides it. Logs go to stderr (journald). |
| `announce` | `false` | Announce the local hubs. |
| `announce_interval_minutes` | `360` | Time between announces, with `announce = true`. |
| `allowed_identities` | `[]` | Identity hashes (not addresses) allowed to use the local hubs besides yours. |
| `part_upstream` | `false` | A client's PART also leaves the room on the hub. Off: the bouncer stays and keeps the history; only that client leaves. |
| `[reticulum] tcp` | none | Connect to an rnsd `TCPServerInterface`, `host:port`. |
| `[reticulum] shared_instance` | `false` | Instead: attach to this host's rnsd shared instance. |
| `[reticulum] instance_name` | `default` | That instance's `instance_name`. |
| `[reticulum] config_dir` | none | Instead: a complete Reticulum config directory, used as is. |
| `[history] days` | `30` | How long a room's history is kept. |
| `[history] max_mb` | `100` | Most history per hub, in MB; the oldest goes first. |
| `[history] replay_hours` | `24` | How far back the replay on JOIN reaches (0: no replay). |
| `[history] replay_max` | `100` | Most messages replayed on JOIN, at most 500 (0: no replay). |
| `[[hub]] address` | required | The hub's destination hash. One table per hub. |
| `[[hub]] rooms` | `[]` | Rooms to be in from the start. Rooms your clients join are added and kept. |
| `[[hub]] nick` | none | Your nick on this hub. |

Set `nick` on each `[[hub]]`. The hub takes the nick of every message it gets,
and all your clients share one link, so without it your nick on the hub
changes to whichever client spoke last.

## The clients

Every client links to the **local hub** address `hashes` prints, never to the
hub itself, and speaks as **your identity**, the one in the bouncer's
`identity`. A client that can't hold that identity can keep its own: add its
identity hash to `allowed_identities`. Everything it says still goes to the
hub as you. Any other identity gets `ERROR "identity not allowed on this hub"`
and its link is closed.

Turn off any setting that connects a client to the real hub as well, or that
hub sees two links from you again.

### weechat-rrc

```
/set rrc.network.identity_file "~/.local/share/rrc-bouncer/identity"
/rrc add home rrc://<local hub>/lobby -nick yournick -autoconnect
/rrc connect home
```

The default address `unix:default` reaches the local rnsd, which knows the
local hub's path. Ask for a room's history with `//history 50`: `/history`
is WeeChat's own command, and a line starting with `//` goes out as text with
one `/` taken off.

### rBrowser2

Stop rBrowser2 and copy the identity to `settings/rrc_identity` in its
directory (keep the old file if you want it back), then start it, turn on
Settings > RRC Client, and open `rrc://<local hub>/lobby` in the address bar.
Star the hub to keep it, and ⚡ to connect at start. `/history 50` in a room
works as typed: rBrowser2 sends any `/` command it doesn't know to the hub.

### NomadNet

NomadNet speaks RRC as its own identity, `~/.nomadnetwork/storage/identity`.
Use that one for the bouncer (as in [Setting up](#setting-up)), or add
NomadNet's identity hash to `allowed_identities`. In Channels, open the New
Hub dialog, give it the local hub's address and a name, and connect.
NomadNet has no way to send `/history`: it sends the hub only the commands
it knows, and answers anything else with "Unknown command". The replay on
JOIN still comes.

### Others

Any RRC client works the same way: local hub address, your identity (or one
in `allowed_identities`). A client that finds hubs only from announces needs
`announce = true`, or a link to the local hub (`rrc://<local hub>/<room>`).

## History

Everything the hub sends to your rooms is kept in `state_dir/history/`, one
file per hub and room, for `days` (30) and up to `max_mb` (100 MB) per hub.
It is stored **unencrypted**, readable by your user only: anyone who can read
your files can read your rooms.

A client that joins a room gets the last `replay_hours` (24) of it, at most
`replay_max` (100) messages, between a
`--- replay: N messages since <time> ---` and an `--- end of replay ---`
notice. Clients show replayed lines at the time they arrive; the first notice
says when they start. The bouncer does not know what a client has already
seen, so a client that comes back gets those lines again, and your own
messages from other clients are shown as yours.

`/history N` in a room gets its last N messages (at most 500), whether or not
the hub is reachable. It stays on the bouncer and never reaches the hub.

## Long messages

A text too long for one packet (a long greeting, a long notice) travels as an
RRC `RESOURCE_ENVELOPE` followed by a Reticulum Resource. The bouncer takes
them from the hub up to 256 KiB, checks the size and sha256, and passes them
on:

- to a client that says it takes them (weechat-rrc, rBrowser2 and NomadNet
  all do), as a Resource, byte for byte;
- to a client that doesn't, as ordinary notices, one per line;
- into the history as those notices.

The hub's greeting is kept and given to every client that links later, the
long one too. weechat-rrc shows a long notice in its room with its sender;
rBrowser2 and NomadNet show it in the room as the hub's, without one. A long
greeting goes to rBrowser2's hub tab and becomes NomadNet's MOTD.

A client may send a long notice to a room as a Resource too; the bouncer
checks it (at most 256 KiB, sha256, room joined) and passes it to the hub,
and to your other clients in the room. One that is too large is refused with
an ERROR; one whose sha256 doesn't match is dropped. weechat-rrc, rBrowser2
and NomadNet never send one: they refuse a line over the hub's message limit
or split it themselves. A hub has to take Resources from clients for this to
work: rrcd 0.3.2 under RNS 1.6 doesn't, and the client gets
`ERROR "not sent to the hub: the resource was not sent: …"`.

## Running under systemd

[`contrib/rrc-bouncer.service`](contrib/rrc-bouncer.service) is a user unit:

```
install -Dm644 contrib/rrc-bouncer.service ~/.config/systemd/user/rrc-bouncer.service
systemctl --user daemon-reload
systemctl --user enable --now rrc-bouncer
loginctl enable-linger "$USER"   # keep it running while logged out
journalctl --user -u rrc-bouncer -f
```

The unit runs `~/.local/bin/rrc-bouncer` with
`~/.config/rrc-bouncer/config.toml` and makes the whole file system, your
home included, read-only to it except `~/.local/share/rrc-bouncer`, which must
exist before it starts.
With another `state_dir`, or with `[reticulum] config_dir` (Reticulum writes
there), change or add `ReadWritePaths=` to match.

One bouncer runs per `state_dir`: a second one started on the same directory
exits with "another rrc-bouncer is running". `rrc-bouncer status` and
`rrc-bouncer hashes` still work while it runs. On SIGTERM it closes its links,
writes its history to disk and exits within about 25 s.

## Status

`rrc-bouncer status` reads `state_dir/status.json`. The bouncer writes it at
start, at once when a link, its rooms or its clients change, every minute and
at shutdown, so the counters can be a minute old.

```
rrc-bouncer status (updated 12 s ago)
  identity           <your identity hash>
  started            3 h ago
  announce           off
  hub <hub address>  local hub <local hub address>
    upstream         connected to "Permanet Chat" (version 0.3.2), since 3 h ago
    rooms            lobby, side (joined 2 of 2)
    connects         1 (0 reconnects), 0 failed attempts
    clients          2 (<identity hash>, <identity hash>)
    relayed          120 in, 2400 out
    hub resources    1 received, 0 sent, 0 dropped
    client resources 0 received, 2 sent, 1 as notices, 0 dropped
    history          88950 bytes in 2 rooms
    history written  630 records, 0 dropped
```

Per hub:

| Line | |
|---|---|
| `upstream` | `connected` (to the hub's name and version, since when), `connecting`, `linked` (waiting for the hub's WELCOME), `waiting` (to try again), `stopped`; `(when it stopped)` once the bouncer is not running |
| `rooms` | The rooms the bouncer is in or will join, and how many it is in now |
| `not joined` | Rooms it is not in, and why (refused, trying again, over the hub's room limit) |
| `connects` | Links the hub welcomed, how many were reconnects, attempts it did not welcome |
| `last disconnect`, `last error`, `next attempt` | Why the last link ended or failed, and when the next try is |
| `not relayed` | Hub messages of a type the bouncer passes to no one, by type |
| `dropped sends`, `dropped events` | Client messages not sent to the hub (queue full, link down for over a minute); hub messages the relay could not take |
| `clients` | Client links and their identities |
| `relayed` | Client messages passed to the hub, messages sent to clients |
| `relay drops` | Messages to a client dropped because its link does not keep up |
| `refused links` | Links of identities not allowed |
| `hub resources` | Long messages from the hub received whole; your long notices the hub took; ones from the hub dropped |
| `client resources` | Long notices from your clients received; Resources sent to clients whole; times one went to a client as notices; ones dropped (too large, bad hash, not taken in time) |
| `history` | Size on disk and rooms (as of the last write or hourly prune) |
| `history written` | Records written since start; records dropped (disk too slow or failing) |
| `history syncs` | Writes to disk that failed |

Lines that would say nothing (no drops, no error) are left out.
`rrc-bouncer status --json` prints the file itself; its fields are listed in
[DESIGN.md § `status`](DESIGN.md#status).

## Limits

- **Your clients share one link and one nick.** The hub sees one of you.
  Clients of the same bouncer see no join or part for each other.
- **Clients aren't told when the hub's link is down.** They stay linked to
  the local hub; what they send meanwhile is answered with
  `ERROR "not sent to the hub: …"`. The bouncer reconnects (5 s, doubling to
  5 min) and rejoins every room by itself.
- **A room key (`+k`) is not passed on.** The bouncer joins without one.
- **Replays repeat.** A client that comes back gets the last `replay_hours`
  again, including lines it saw.
- **History is unencrypted** on the bouncer's disk, and it holds only what
  was said while the bouncer was in the room.
- **Long messages** are relayed up to 256 KiB; the hub's binary Resources
  (`blob`) are not relayed.
- **NomadNet** can't ask for `/history`. NomadNet and rBrowser2 show long
  notices without their sender.
- **`shared_instance` mode can announce the local hubs** when rnsd or the
  bouncer restarts ([Reaching Reticulum](#reaching-reticulum)). Use `tcp`.

## Checking it by hand

With the bouncer running and `rrc-bouncer status` showing the hub
`connected` and its rooms joined. Wherever a step names a second client, any
client of the bouncer will do; "a direct client" is one linked to the hub
itself under another identity.

### weechat-rrc

1. `/rrc add home rrc://<local hub>/lobby -nick yournick` and
   `/rrc connect home`.
   The server buffer shows the path found, the link, and the welcome from
   `<hub name> (via rrc-bouncer)`, then the hub's greeting. `lobby` opens.
2. Look at `lobby`. It starts with `--- replay: N messages since <time> ---`,
   the room's recent lines, and `--- end of replay ---` (or nothing, on a
   fresh bouncer).
3. Say something. It shows once, as yours. A direct client sees it from your
   nick; rBrowser2 or NomadNet on the bouncer see it too.
4. Have the direct client say something. It shows in `lobby`.
5. `//history 5`. The last five lines come between the replay notices.
   `//history x` gets `usage: /history [N], N from 1 to 500`.
6. `/part lobby`, then `/join lobby`. The room closes and opens again with
   the replay; `rrc-bouncer status` still shows `lobby` joined.
7. Have the direct client send a direct notice to you. It shows in every
   client of the bouncer.
8. `/rrc disconnect home`. The other clients carry on, the bouncer stays in
   `lobby`, and `status` shows one client fewer.

### rBrowser2

1. With the identity in place (see [rBrowser2](#rbrowser2)), open
   `rrc://<local hub>/lobby`.
   The hub tab shows the welcome from `<hub name> (via rrc-bouncer)` and the
   greeting; `lobby` opens with the replay between its two notices.
2. The head shows your identity hash: the bouncer's identity hash from
   `hashes`.
3. Say something, and have weechat-rrc say something. Each shows once in
   both, with the right nick.
4. `/history 5` in `lobby`. The last five lines, between the replay notices.
5. `/notice hello` in `lobby`. weechat-rrc and the direct client show it as a
   notice from you.
6. Quit rBrowser2, have weechat-rrc say a few lines in `lobby`, and start
   rBrowser2 again. `lobby` opens with those lines in its replay.

### NomadNet

1. Channels, New Hub: the local hub's address, name `home`. Connect.
   The hub shows `Connected`, and the hub's greeting shows (as its MOTD when
   the greeting is long).
2. `/join lobby`. The room opens with the replay between its two notices,
   then live lines.
3. Say something. weechat-rrc and rBrowser2 show it once, as you.
4. `/history 5` gets "Unknown command" from NomadNet itself, and nothing
   reaches the bouncer.
5. `/who lobby`. The answer comes to NomadNet only, not to the other clients.
6. Have a direct client send a notice longer than one packet to `lobby`
   (rrcd sends it on as a Resource). NomadNet shows it whole, without a
   sender; weechat-rrc shows it with the sender.

### For every client

- `rrc-bouncer status`: `clients` lists one link per client running, each
  with your identity hash (or the allowed one); `relayed` grows as you talk;
  `client resources` counts the greeting sent to each client that linked
  after a long greeting.
- A client under an identity that is not yours nor allowed gets
  `identity not allowed on this hub` and is dropped; `refused links` goes up.

## Tests

```
cargo test
RRC_TEST_VENV=/path/to/venv tests/e2e.sh   # venv with: pip install rns cbor2 rrcd
```

The end-to-end test runs its own rnsd and rrcd instances on 127.0.0.1 (ports
4370-4379) and never uses the host's own. It drives the bouncer against rrcd
with Python RNS clients: linking, rooms, relaying, history, long messages,
restarts and shutdown.

## License

[Reticulum License](LICENSE).
