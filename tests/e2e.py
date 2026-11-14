# SPDX-License-Identifier: LicenseRef-Reticulum
"""End-to-end test of rrc-bouncer on a private Reticulum network.

Starts its own rnsd instances (TCPServerInterface on 127.0.0.1, port
4370-4379, unique instance names), the bouncer and Python RNS peers, all
with throwaway identities under the work directory. Nothing touches this
host's own rnsd or any other network. Run through tests/e2e.sh.

  T  an rnsd with transport on and no shared instance (the bouncer
     connects to it over TCP)
  TL T again, with never-used paths dropped after LINGER seconds
  S  an rnsd with a shared instance of its own (the bouncer attaches to it)
  W  a peer watching rrc.hub announces
  C  a client holding the user's identity: asks for a path to the local
     hub, links, identifies, and expects PONG for PING
  X  a client holding an identity the bouncer does not allow
  R  an rnsd with transport on for the rrcd phases
  H  rrcd, the reference hub, on R (its own RNS instance and identity)
  RC an RRC client of H under an identity of its own, in the bouncer's room
  A, B  RRC clients of the bouncer's local hub holding the user's identity
        (B says HELLO in the same breath as its identify, as NomadNet does)
  D  an RRC client of H under an identity of its own, in the same room
  Y  an RRC client of the local hub holding an identity it does not allow
  D3, E  RRC clients of H under identities of their own, in room hist
  A3-A8  RRC clients of the local hub holding the user's identity, each
         joining hist to see what is replayed
  RA, RA2  RRC clients of the local hub holding the user's identity, with
           the resource_envelope cap
  RN  an RRC client of the local hub holding the user's identity, without it
  RD  an RRC client of H under an identity of its own, in room res
"""

import argparse
import calendar
import hashlib
import io
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

import cbor2
import RNS

HERE = os.path.dirname(os.path.abspath(__file__))
PEER = os.path.join(HERE, "rrc_peer.py")
RNSD_LINGER = os.path.join(HERE, "rnsd_linger.py")
RRCD_SHIM = os.path.join(HERE, "rrcd_shim.py")
UPSTREAM = "082f8e4293d19281108a5d4faa52dcbe"
# The bouncer's first announce comes 5 s after start; wait past it.
QUIET = 10
# The never-used path linger of rnsd TL, and the bouncer's refresh interval
# against it (both seconds).
LINGER = 12
REFRESH = 4
# rrcd H pings every RRCD_PING s and drops a link whose PONG is RRCD_TIMEOUT
# s late; the bouncer pings after BOUNCER_PING s of quiet and waits as long
# for the PONG, and waits BOUNCER_BACKOFF s (doubling) before connecting
# again.
RRCD_PING = 3
RRCD_TIMEOUT = 4
BOUNCER_PING = 2
BOUNCER_BACKOFF = 1
HUB_NAME = "e2e hub"
# status.json is written at every change of state and, for the counters,
# once a minute.
STATUS_TICK = 65


class Failure(Exception):
    pass


class Done(Exception):
    pass


def check(condition, what):
    if not condition:
        raise Failure(what)
    print(f"  ok  {what}", flush=True)


def free_port(taken=()):
    for port in range(4370, 4380):
        if port in taken:
            continue
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise Failure("no free port in 4370-4379")


def wait_port(port, timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.2)
    raise Failure(f"nothing listening on 127.0.0.1:{port}")


def rns_config(path, port, server=False, shared=None, transport=None):
    os.makedirs(path, exist_ok=True)
    if server:
        interface = (f"  [[e2e hub]]\n    type = TCPServerInterface\n    enabled = Yes\n"
                     f"    listen_ip = 127.0.0.1\n    listen_port = {port}\n")
    else:
        interface = (f"  [[e2e uplink]]\n    type = TCPClientInterface\n    enabled = Yes\n"
                     f"    target_host = 127.0.0.1\n    target_port = {port}\n")
    name = shared or f"rrcm1-e2e-{os.getpid()}-{os.path.basename(path)}"
    with open(os.path.join(path, "config"), "w") as f:
        transport = server if transport is None else transport
        f.write(f"[reticulum]\n  enable_transport = {'Yes' if transport else 'No'}\n"
                f"  share_instance = {'Yes' if shared else 'No'}\n"
                f"  instance_name = {name}\n  panic_on_interface_error = No\n\n"
                f"[logging]\n  loglevel = {7 if server else 4}\n\n[interfaces]\n{interface}")
    return path


class Run:
    def __init__(self, work, python, rnsd, binary):
        self.work, self.python, self.rnsd_bin, self.binary = work, python, rnsd, binary
        self.procs = {}

    def path(self, *parts):
        return os.path.join(self.work, *parts)

    def start(self, name, argv, env=None):
        log = open(self.path(f"{name}.log"), "ab")
        env = dict(os.environ, PYTHONUNBUFFERED="1", **(env or {}))
        self.procs[name] = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT, env=env)

    def stop(self, name, timeout=15):
        proc = self.procs.pop(name, None)
        if proc is None or proc.poll() is not None:
            return proc.returncode if proc else None
        proc.send_signal(signal.SIGTERM)
        try:
            return proc.wait(timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            return proc.wait()

    def stop_all(self):
        for name in list(self.procs):
            self.stop(name, timeout=5)

    def log(self, name):
        with open(self.path(f"{name}.log"), "rb") as f:
            return f.read().decode("utf-8", "replace")

    def wait_log(self, name, text, timeout):
        end = time.time() + timeout
        while time.time() < end:
            with open(self.path(f"{name}.log"), "rb") as f:
                if text in f.read().decode("utf-8", "replace"):
                    return True
            proc = self.procs.get(name)
            if proc is not None and proc.poll() is not None:
                raise Failure(f"{name} exited ({proc.returncode}) waiting for {text!r}")
            time.sleep(0.3)
        return False

    def events(self, name):
        try:
            with open(self.path(f"{name}.events")) as f:
                # A client may be part way through a line: read whole lines only.
                return [json.loads(line) for line in f if line.endswith("\n") and line.strip()]
        except FileNotFoundError:
            return []

    def wait_event(self, name, predicate, timeout):
        end = time.time() + timeout
        while time.time() < end:
            for event in self.events(name):
                if predicate(event):
                    return event
            proc = self.procs.get(name)
            if proc is not None and proc.poll() is not None:
                raise Failure(f"{name} exited ({proc.returncode})")
            time.sleep(0.3)
        return None

    def identity(self, name):
        path = self.path("ids", name)
        if not os.path.exists(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
            RNS.Identity().to_file(path)
        return path

    def rnsd(self, name, port, shared=None, linger=None):
        config = rns_config(self.path("rns", name), port, server=True, shared=shared)
        if linger is None:
            self.start(name, [self.rnsd_bin, "--config", config])
        else:
            self.start(name, [self.python, RNSD_LINGER, "--config", config],
                       env={"RRC_TEST_PATH_LINGER": str(linger)})
        wait_port(port)

    def peer(self, name, port, identity, *extra):
        config = rns_config(self.path("rns", name), port)
        self.start(name, [self.python, PEER, "--rnsconfig", config, "--identity", self.identity(identity),
                          "--events", self.path(f"{name}.events"), *extra])
        # Python RNS can take a while to start on a loaded host.
        if not self.wait_event(name, lambda e: e["event"] == "ready", 180):
            raise Failure(f"{name} did not start")

    def config(self, name, reticulum, announce=False, upstream=UPSTREAM, nick=None, allowed=(), part_upstream=False,
               rooms=("lobby",), history=None):
        path = self.path(f"{name}.toml")
        with open(path, "w") as f:
            f.write(f'identity = "ids/user"\nstate_dir = "state"\nlog_level = "debug"\n'
                    f"announce = {'true' if announce else 'false'}\n"
                    f"part_upstream = {'true' if part_upstream else 'false'}\n"
                    f'allowed_identities = {json.dumps(list(allowed))}\n\n[reticulum]\n{reticulum}\n\n')
            if history:
                f.write("[history]\n" + "".join(f"{k} = {v}\n" for k, v in history.items()) + "\n")
            f.write(f'[[hub]]\naddress = "{upstream}"\nrooms = {json.dumps(list(rooms))}\n')
            if nick:
                f.write(f'nick = "{nick}"\n')
        return path

    def rrcd(self, binary, port, max_rooms=2, greeting="e2e", shim=False):
        """Start rrcd H on rnsd R; every file it uses is under the work
        directory. With shim, rrcd forwards a client's long NOTICE (see
        rrcd_shim.py)."""
        rns = rns_config(self.path("rns", "H"), port)
        os.makedirs(self.path("rrcd"), exist_ok=True)
        registry = self.path("rrcd", "rooms.toml")
        if not os.path.exists(registry):
            with open(registry, "w") as f:
                f.write("[rooms]\n")
        config = self.path("rrcd", "rrcd.toml")
        with open(config, "w") as f:
            f.write(f'[hub]\nconfigdir = "{rns}"\nidentity_path = "{self.identity("rrcd-hub")}"\n'
                    f'room_registry_path = "{registry}"\nannounce_on_start = true\n'
                    f'hub_name = "{HUB_NAME}"\ngreeting = {json.dumps(greeting)}\ninclude_joined_member_list = true\n'
                    f"max_rooms_per_session = {max_rooms}\nrate_limit_msgs_per_minute = 120\n"
                    f"ping_interval_s = {RRCD_PING}\nping_timeout_s = {RRCD_TIMEOUT}\n\n"
                    f'[logging]\nlevel = "DEBUG"\nconsole = true\nfile = ""\n')
        argv = [self.python, RRCD_SHIM] if shim else [binary]
        self.start("H", [*argv, "--config", config, "--identity", self.identity("rrcd-hub"),
                         "--room-registry", registry, "--configdir", rns, "--log-level", "DEBUG"])
        if not self.wait_log("H", "Hub running at", 120):
            raise Failure("rrcd did not start")

    def status(self, upstream):
        try:
            with open(self.path("state", "status.json")) as f:
                status = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return None
        return next((h for h in status["hubs"] if h["upstream"] == upstream), None)

    def wait_status(self, upstream, predicate, timeout):
        end = time.time() + timeout
        while time.time() < end:
            hub = self.status(upstream)
            if hub is not None and predicate(hub):
                return hub
            time.sleep(0.3)
        return self.status(upstream)

    def bouncer(self, *argv):
        return subprocess.run([self.binary, *argv], capture_output=True, text=True, timeout=30)


def hub_announces(events, hub, path_response):
    return [e for e in events if e["event"] == "announce" and e["dest"] == hub
            and e["path_response"] == path_response]


def ping(run, name, port, identity, hub, timeout=30):
    run.peer(name, port, identity, "--ping", hub, "--timeout", str(timeout))
    done = run.wait_event(name, lambda e: e["event"] == "done", timeout + 10)
    run.stop(name)
    events = run.events(name)
    pong = next((e for e in events if e["event"] == "pong"), None)
    return done, pong, events


def joins(run, name, user, since_session=1):
    """The client's sightings of the user in the room: a JOINED for the
    user, or the user in the member list of the client's own JOINED."""
    return [e for e in run.events(name) if e["event"] == "env" and e["t"] == 11 and e["session"] >= since_session
            and e["room"] == "lobby" and (e["nick"] == "bnc" or user in (e["body"] or []))]


def rrcd_phases(run, args, ports, found):
    print("rrcd: the bouncer joins rooms on a real hub", flush=True)
    port_r = free_port(ports)
    run.rnsd("R", port_r)
    upstream = RNS.Destination.hash(RNS.Identity.from_file(run.identity("rrcd-hub")), "rrc", "hub").hex()
    user = RNS.Identity.from_file(run.path("ids/user")).hash.hex()
    run.rrcd(args.rrcd, port_r)
    check(run.wait_log("H", f"Hub running at dest_hash={upstream}", 1), "rrcd H runs at the expected address")
    run.peer("RC", port_r, "rc", "--room-client", upstream, "--room", "lobby", "--nick", "rc", "--idle", "10")
    check(run.wait_event("RC", lambda e: e["event"] == "env" and e["t"] == 11 and e["nick"] is None, 30)
          is not None, "the client joins lobby on rrcd H")
    config = run.config("rrcd", f'tcp = "127.0.0.1:{port_r}"', upstream=upstream, nick="bnc")
    rooms_file = run.path("state", "rooms", f"{upstream}.json")
    os.makedirs(os.path.dirname(rooms_file), exist_ok=True)
    with open(rooms_file, "w") as f:
        json.dump({"rooms": [" Side ", "side", "third"]}, f)
    env = {"RRC_BOUNCER_PING_SECS": str(BOUNCER_PING), "RRC_BOUNCER_BACKOFF_SECS": str(BOUNCER_BACKOFF)}
    local = {k.strip(): v for k, v in (line.rsplit(None, 1) for line in
                                       run.bouncer("--config", config, "hashes").stdout.splitlines())}["local hub"]
    run.start("BR", [args.bouncer, "--config", config, "run"], env=env)
    seen = run.wait_event("RC", lambda e: e in joins(run, "RC", user), 40)
    check(seen is not None and seen["nick"] == "bnc" and seen["body"] == [user],
          "the client sees the bouncer join lobby as nick bnc, with the user's identity")
    hub = run.wait_status(upstream, lambda h: sorted(h["rooms_joined"]) == ["lobby", "side"]
                          and h["rooms_over_limit"], 20)
    found["rrcd_status"] = hub
    check(hub["state"] == "connected" and hub["hub_name"] == HUB_NAME, "status.json: connected to the hub by name")
    check(sorted(hub["rooms_joined"]) == ["lobby", "side"] and hub["rooms_over_limit"] == ["third"],
          "status.json: lobby and side joined, third over the hub's limit of 2 rooms")
    check(hub["connects"] == 1 and hub["reconnects"] == 0, "status.json: one connect")
    with open(rooms_file) as f:
        check(json.load(f) == {"rooms": ["side", "third"]}, "the room set file was normalised")
    check(oct(os.stat(rooms_file).st_mode & 0o777) == "0o600", "the room set file is mode 0600")
    hello = [line for line in run.log("H").splitlines() if "HELLO peer=" in line and user[:12] in line]
    check(hello and "nick='bnc'" in hello[-1], "rrcd saw the bouncer's HELLO with nick bnc")
    out = run.bouncer("--config", config, "status")
    found["rrcd_status_out"] = out.stdout
    check(out.returncode == 0 and f'connected to "{HUB_NAME}"' in out.stdout, "status prints the upstream link")
    app_data = cbor2.dumps({"proto": "rrc", "v": 1, "hub": f"{HUB_NAME} (via rrc-bouncer)"}).hex()
    check(run.wait_log("BR", "the local hub is now", 5), "the bouncer renames its local hub after the WELCOME")
    done, pong, events = ping(run, "CR", port_r, "user", local)
    names = [e["app_data"] for e in events if e["event"] == "announce" and e["dest"] == local]
    found["rrcd_local_app_data"] = names
    check(pong is not None and app_data in names,
          f"a client of the local hub gets PONG and the name \"{HUB_NAME} (via rrc-bouncer)\"")

    wait = 3 * (RRCD_PING + RRCD_TIMEOUT)
    print(f"rrcd: the link stays up for {wait} s of rrcd pings", flush=True)
    time.sleep(wait)
    hub = run.status(upstream)
    rx = [line for line in run.log("H").splitlines() if f"RX peer={user[:12]}" in line]
    pongs = sum(" t=31 " in line for line in rx)
    pings = sum(" t=30 " in line for line in rx)
    found["rrcd_pings"] = {"pongs_from_bouncer": pongs, "pings_from_bouncer": pings}
    check(pongs >= wait // RRCD_PING - 1, f"the bouncer answered {pongs} PINGs from rrcd")
    check(pings >= 1, f"the bouncer sent {pings} PINGs of its own")
    check(hub["state"] == "connected" and hub["connects"] == 1 and hub["reconnects"] == 0,
          f"still the first link after {wait} s, past rrcd's {RRCD_TIMEOUT} s ping timeout")

    for how, sig in (("stopped", signal.SIGTERM), ("killed", signal.SIGKILL)):
        print(f"rrcd: rrcd H is {how} and started again", flush=True)
        before = run.status(upstream)["connects"]
        waits_from = run.log("BR").count("connecting again in")
        session = max([e["session"] for e in run.events("RC") if e["event"] == "env"], default=0)
        proc = run.procs.pop("H")
        proc.send_signal(sig)
        proc.wait(15)
        down = run.wait_status(upstream, lambda h: h["state"] != "connected", 30)
        check(down["state"] != "connected", f"the bouncer saw the link go ({down['last_error']})")
        found[f"rrcd_{how}_down"] = down
        time.sleep(2)
        run.rrcd(args.rrcd, port_r)
        hub = run.wait_status(upstream, lambda h: h["connects"] > before and sorted(h["rooms_joined"]) == ["lobby", "side"], 90)
        found[f"rrcd_{how}_up"] = hub
        check(hub["connects"] == before + 1 and hub["reconnects"] >= 1, f"the bouncer connected again ({hub['connects']} connects)")
        check(sorted(hub["rooms_joined"]) == ["lobby", "side"], "and joined lobby and side again")
        delays = [line.split("connecting again in ")[1] for line in run.log("BR").splitlines()
                  if "connecting again in" in line][waits_from:]
        found[f"rrcd_{how}_delays"] = delays
        print(f"  waits before connecting again: {', '.join(delays)}", flush=True)
        seen = run.wait_event("RC", lambda e: e in joins(run, "RC", user, session + 1), 60)
        check(seen is not None, "the client, back in lobby, sees the bouncer there "
              + ("(its JOINED)" if seen and seen["nick"] == "bnc" else "(in the member list)"))
        if sig == signal.SIGKILL:
            check(down["last_error"] == "no PONG from the hub", "a killed hub is noticed by its missing PONG")
            check(delays and delays[0] != f"{float(BOUNCER_BACKOFF)}s",
                  "the wait did not start over: the previous link was up for less than a minute")

    print("rrcd: the bouncer restarts", flush=True)
    seen = len(joins(run, "RC", user))
    check(run.stop("BR") == 0, "the bouncer stops cleanly")
    with open(rooms_file) as f:
        check(json.load(f) == {"rooms": ["side", "third"]}, "the room set file is unchanged")
    run.start("BR", [args.bouncer, "--config", config, "run"], env=env)
    hub = run.wait_status(upstream, lambda h: h["state"] == "connected" and sorted(h["rooms_joined"]) == ["lobby", "side"]
                          and h["rooms_over_limit"], 40)
    check(sorted(hub["rooms_joined"]) == ["lobby", "side"] and hub["rooms_over_limit"] == ["third"],
          "the restarted bouncer joins the persisted room set")
    check(run.wait_event("RC", lambda e: len(joins(run, "RC", user)) > seen, 20) is not None,
          "the client sees the bouncer join lobby again")
    run.stop("RC")
    relay_phases(run, args, port_r, upstream, user, local, config, env, found)
    run.stop("H")
    run.stop("R")


def driven(run, name, port, identity, hub, nick, hello_delay=0.5, resource_cap=False):
    """An RRC client the test drives with `command`."""
    run.peer(name, port, identity, "--driven", hub, "--commands", run.path(f"{name}.commands"),
             "--nick", nick, "--hello-delay", str(hello_delay), *(["--resource-cap"] if resource_cap else []))


def command(run, name, **cmd):
    with open(run.path(f"{name}.commands"), "a") as f:
        f.write(json.dumps(cmd) + "\n")


def envs(run, name, **match):
    return [e for e in run.events(name) if e["event"] == "env" and all(e.get(k) == v for k, v in match.items())]


def wait_env(run, name, timeout=20, **match):
    return run.wait_event(name, lambda e: e["event"] == "env" and all(e.get(k) == v for k, v in match.items()),
                          timeout)


def relay_phases(run, args, port_r, upstream, user, local, config, env, found):
    print("relay: rrcd H allows four rooms; the bouncer joins lobby, side and third", flush=True)
    before = run.status(upstream)["connects"]
    run.stop("H")
    run.rrcd(args.rrcd, port_r, max_rooms=4)
    hub = run.wait_status(upstream, lambda h: h["connects"] > before
                          and sorted(h["rooms_joined"]) == ["lobby", "side", "third"], 90)
    check(sorted(hub["rooms_joined"]) == ["lobby", "side", "third"], "the bouncer is in lobby, side and third")
    direct = RNS.Identity.from_file(run.identity("direct")).hash.hex()

    print("relay: two clients of the local hub, one of rrcd H", flush=True)
    driven(run, "A", port_r, "user", local, "a")
    driven(run, "B", port_r, "user", local, "b", hello_delay=0)
    driven(run, "D", port_r, "direct", upstream, "dee")
    welcomes = {name: wait_env(run, name, 40, t=2) for name in ("A", "B", "D")}
    found["relay_welcomes"] = welcomes
    check(all(welcomes.values()), "A, B and D get WELCOME")
    check(welcomes["B"] is not None, "B, which said HELLO in the same breath as its identify, gets WELCOME")
    hub_id = next(e["hub_identity"] for e in run.events("A") if e["event"] == "link")
    upstream_id = next(e["hub_identity"] for e in run.events("D") if e["event"] == "link")
    a, d = welcomes["A"]["body"], welcomes["D"]["body"]
    check(welcomes["A"]["src"] == hub_id and welcomes["B"]["src"] == hub_id,
          "the WELCOME's K_SRC is the local hub's identity")
    check(a["0"] == f"{HUB_NAME} (via rrc-bouncer)", f"the WELCOME names the hub \"{a['0']}\"")
    check(a["2"] == d["2"] and d["2"].get("0") is True and a["3"] == d["3"] and a["3"]["3"] == 4,
          f"it carries rrcd's caps (resource_envelope on) and limits: {a['2']} {a['3']}")
    for name in ("A", "B"):
        greeting = wait_env(run, name, 10, t=21, body="e2e")
        check(greeting is not None and greeting["src"] == hub_id and greeting["room"] is None,
              f"{name}, linked after the bouncer's WELCOME, gets rrcd's greeting \"e2e\" from the local hub")

    for name in ("A", "B"):
        command(run, name, op="join", room="lobby")
        joined = wait_env(run, name, t=11, room="lobby")
        check(joined is not None and joined["src"] == hub_id and user in (joined["body"] or []),
              f"{name} joins lobby: JOINED from the local hub, the user in the member list")
    command(run, "D", op="join", room="lobby")
    check(wait_env(run, "D", t=11, room="lobby") is not None, "D joins lobby on rrcd H")
    for name in ("A", "B"):
        seen = wait_env(run, name, t=11, room="lobby", nick="dee")
        check(seen is not None and seen["src"] == hub_id and seen["body"] == [direct],
              f"{name} sees D join, from the local hub")

    print("relay: MSG, ACTION and direct NOTICE in every direction", flush=True)
    fmt = {"0": "md"}
    messages = [
        # (id, sender, command, receivers, K_SRC)
        ("A", dict(t=20, room="lobby", body="hello from a", fmt=fmt), "ABD", user),
        ("B", dict(t=22, room="lobby", body="waves"), "ABD", user),
        ("D", dict(t=20, room="lobby", body="hello from dee"), "ABD", direct),
        ("D", dict(t=22, room="lobby", body="nods"), "ABD", direct),
        ("A", dict(t=21, dst=direct, body="psst from a"), "BD", user),
        ("D", dict(t=21, dst=user, body="psst from dee"), "AB", direct),
        ("B", dict(t=21, dst=direct, body="psst from b"), "AD", user),
        ("B", dict(t=21, room="lobby", body="a room notice"), "ABD", user),
    ]
    sent = []
    for sender, cmd, receivers, src in messages:
        ident = os.urandom(8).hex()
        command(run, sender, op="send", id=ident, **cmd)
        for name in receivers:
            wait_env(run, name, 20, id=ident)
        sent.append((ident, sender, cmd, receivers, src))
    time.sleep(3)
    counts = {}
    for ident, sender, cmd, receivers, src in sent:
        for name in "ABD":
            got = envs(run, name, id=ident)
            counts[f"{sender}->{name} t={cmd['t']} {cmd['body']}"] = len(got)
            expected = 1 if name in receivers else 0
            if len(got) != expected:
                raise Failure(f"{name} got {cmd['body']!r} from {sender} {len(got)} times, not {expected}")
            if got and (got[0]["src"] != src or got[0]["t"] != cmd["t"] or got[0]["body"] != cmd["body"]):
                raise Failure(f"{name} got {cmd['body']!r} with K_SRC {got[0]['src']}, t {got[0]['t']}")
    found["relay_counts"] = counts
    check(True, f"each client got each envelope once, with its K_ID, type, body and K_SRC ({len(sent)} envelopes)")
    first = sent[0][0]
    check(all(envs(run, name, id=first)[0]["fmt"] == fmt for name in "ABD"), "K_FMT came through on A's MSG")
    check(all(envs(run, sender, id=ident) for ident, sender, cmd, receivers, src in sent
              if sender in "AB" and "dst" not in cmd),
          "the sender of each room MSG, ACTION and NOTICE got its echo")
    nicks = {envs(run, "D", id=ident)[0]["nick"] for ident, sender, cmd, receivers, src in sent if sender in "AB"}
    found["relay_nicks_at_d"] = sorted(nicks, key=str)
    check(nicks == {"bnc"}, "A (K_NICK a) and B (K_NICK b) both reach D as nick bnc")

    print("relay: errors go to the client that caused them", flush=True)
    nobody = os.urandom(16).hex()
    command(run, "A", op="send", t=21, dst=nobody, body="to nobody")
    error = wait_env(run, "A", t=40, body="destination not connected")
    time.sleep(2)
    check(error is not None and error["src"] == hub_id and not envs(run, "B", t=40),
          "a direct NOTICE to an identity not connected: A gets rrcd's ERROR, B does not")
    good = os.urandom(8).hex()
    command(run, "A", op="send", t=20, room="nosuch", body="anyone?")
    command(run, "B", op="send", id=good, t=20, room="lobby", body="all good")
    error = wait_env(run, "A", t=40, body="no such room")
    check(wait_env(run, "B", id=good) is not None and wait_env(run, "D", id=good) is not None,
          "B's MSG to lobby, sent alongside, reaches B and D")
    time.sleep(2)
    check(error is not None and error["room"] == "nosuch" and not envs(run, "B", t=40),
          "A's MSG to nosuch at the same time: A alone gets \"no such room\"")

    print("relay: command replies go to the client that gave the command", flush=True)
    command(run, "A", op="send", t=20, body="/list")
    command(run, "B", op="send", t=20, body="/who lobby")
    lists = ("No public rooms registered", "Registered public rooms:")
    is_list = lambda e: e["t"] == 21 and isinstance(e["body"], str) and e["body"].startswith(lists)
    is_who = lambda e: e["t"] == 21 and isinstance(e["body"], str) and e["body"].startswith("members in lobby: ")
    a_list = run.wait_event("A", lambda e: e["event"] == "env" and is_list(e), 20)
    b_who = run.wait_event("B", lambda e: e["event"] == "env" and is_who(e), 20)
    time.sleep(2)
    found["relay_commands"] = {"A": a_list and a_list["body"], "B": b_who and b_who["body"]}
    check(a_list is not None and a_list["src"] == hub_id and a_list["room"] is None,
          f"A gets the /list reply ({a_list and a_list['body']!r})")
    check(b_who is not None and b_who["src"] == hub_id, f"B gets the /who reply ({b_who and b_who['body']!r})")
    check(not [e for e in envs(run, "A") if is_who(e)] and not [e for e in envs(run, "B") if is_list(e)],
          "neither gets the other's reply")

    print("relay: a client joins a new room through the bouncer", flush=True)
    command(run, "A", op="join", room="Fresh")
    joined = wait_env(run, "A", t=11, room="fresh")
    notice = wait_env(run, "A", t=21, room="fresh")
    check(joined is not None and joined["src"] == hub_id, "A gets JOINED for fresh, from the local hub")
    check(notice is not None and notice["src"] == hub_id and "fresh" in str(notice["body"]),
          f"and rrcd's room NOTICE, from the local hub ({notice and notice['body']!r})")
    check(not envs(run, "B", room="fresh"), "B, not in fresh, sees none of it")
    hub = run.wait_status(upstream, lambda h: "fresh" in h["rooms_joined"], 10)
    check("fresh" in hub["rooms_joined"], "status.json: the bouncer is in fresh")
    with open(run.path("state", "rooms", f"{upstream}.json")) as f:
        check("fresh" in json.load(f)["rooms"], "fresh is in the room set file")
    command(run, "A", op="join", room="fifth")
    error = wait_env(run, "A", t=40, body="too many rooms")
    check(error is not None and error["src"] == hub_id, "a fifth room: ERROR \"too many rooms\"")

    print("relay: a client parts lobby; the bouncer stays (part_upstream = false)", flush=True)
    parted_at = time.time()
    command(run, "A", op="part", room="lobby")
    parted = wait_env(run, "A", t=13, room="lobby")
    check(parted is not None and parted["src"] == hub_id and parted["body"] == [user], "A gets PARTED for lobby")
    time.sleep(3)
    check(not [e for e in envs(run, "D", t=13) if e["at"] >= parted_at], "D sees no PART")
    check("lobby" in run.status(upstream)["rooms_joined"], "status.json: the bouncer is still in lobby")
    ident = os.urandom(8).hex()
    command(run, "D", op="send", id=ident, t=20, room="lobby", body="still there?")
    check(wait_env(run, "B", id=ident) is not None, "D's next MSG in lobby reaches B")
    time.sleep(2)
    check(not envs(run, "A", id=ident), "and not A")
    command(run, "D", op="part", room="lobby")
    seen = wait_env(run, "B", t=13, room="lobby")
    check(seen is not None and seen["body"] == [direct] and seen["src"] == hub_id, "B sees D part lobby")

    hub = run.wait_status(upstream, lambda h: h.get("clients") == 2, 10)
    found["relay_status"] = hub
    check(hub["clients"] == 2 and hub["client_identities"] == [user, user],
          "status.json: two clients, both the user's identity")
    check(hub["relayed_in"] >= 6 and hub["relayed_out"] >= 20 and hub["relay_drops"] == 0,
          f"status.json: {hub['relayed_in']} envelopes in, {hub['relayed_out']} out, no drops")
    out = run.bouncer("--config", config, "status")
    found["relay_status_out"] = out.stdout
    check(f"clients          2 ({user}, {user})" in out.stdout and "relayed          " in out.stdout,
          "status prints the clients and the envelopes relayed")
    check(not [e for name in "ABD" for e in run.events(name) if e["event"] == "error"],
          "the driven clients raised nothing")
    for name in ("A", "B", "D"):
        run.stop(name)

    print("relay: an identity that is not allowed", flush=True)
    stranger = RNS.Identity.from_file(run.identity("stranger")).hash.hex()
    driven(run, "Y", port_r, "stranger", local, "y")
    error = wait_env(run, "Y", 40, t=40)
    closed = run.wait_event("Y", lambda e: e["event"] == "closed", 15)
    check(error is not None and error["body"] == "identity not allowed on this hub" and error["src"] == hub_id,
          "the stranger gets ERROR \"identity not allowed on this hub\" from the local hub")
    check(closed is not None and not envs(run, "Y", t=2), "and its link is closed, without a WELCOME")
    hub = run.wait_status(upstream, lambda h: h.get("refused_links", 0) >= 1, 10)
    check(hub["refused_links"] >= 1, "status.json counts the refused link")
    run.stop("Y")
    check(run.stop("BR") == 0, "the bouncer stops cleanly")
    config = run.config("rrcd", f'tcp = "127.0.0.1:{port_r}"', upstream=upstream, nick="bnc", allowed=[stranger])
    run.start("BR", [args.bouncer, "--config", config, "run"], env=env)
    run.wait_status(upstream, lambda h: h["state"] == "connected" and "lobby" in h["rooms_joined"], 40)
    driven(run, "Y2", port_r, "stranger", local, "y")
    check(wait_env(run, "Y2", 40, t=2) is not None, "with its hash in allowed_identities it gets WELCOME")
    hub = run.wait_status(upstream, lambda h: h.get("client_identities") == [stranger], 10)
    check(hub["client_identities"] == [stranger], "status.json lists it as a client")
    run.stop("Y2")
    check(run.stop("BR") == 0, "the bouncer stops cleanly")
    part_upstream_phases(run, args, port_r, upstream, user, local, env, found, hub_id)
    history_phases(run, args, port_r, upstream, user, local, env, found)
    resource_phases(run, args, port_r, upstream, user, local, env, found)


def part_upstream_phases(run, args, port_r, upstream, user, local, env, found, hub_id):
    print("relay: part_upstream = true", flush=True)
    config = run.config("rrcd", f'tcp = "127.0.0.1:{port_r}"', upstream=upstream, nick="bnc", part_upstream=True)
    run.start("BR", [args.bouncer, "--config", config, "run"], env=env)
    run.wait_status(upstream, lambda h: h["state"] == "connected" and "lobby" in h["rooms_joined"], 40)
    driven(run, "A2", port_r, "user", local, "a")
    driven(run, "D2", port_r, "direct", upstream, "dee")
    check(wait_env(run, "A2", 40, t=2) and wait_env(run, "D2", 40, t=2), "A2 and D2 get WELCOME")
    command(run, "D2", op="join", room="lobby")
    command(run, "A2", op="join", room="lobby")
    check(wait_env(run, "D2", t=11, room="lobby") and wait_env(run, "A2", t=11, room="lobby"),
          "A2 (local hub) and D2 (rrcd H) are in lobby")
    command(run, "A2", op="part", room="lobby")
    parted = wait_env(run, "A2", t=13, room="lobby")
    seen = wait_env(run, "D2", t=13, room="lobby")
    check(parted is not None and parted["src"] == hub_id, "A2 gets PARTED for lobby")
    check(seen is not None and seen["body"] == [user], "D2 sees the bouncer part lobby")
    rejoin_at = time.time()
    command(run, "A2", op="join", room="lobby")
    joined = run.wait_event("A2", lambda e: e["event"] == "env" and e["t"] == 11 and e["room"] == "lobby"
                            and e["at"] >= rejoin_at, 40)
    check(joined is not None and joined["src"] == hub_id, "A2 joins lobby again at once: JOINED")
    check(run.wait_event("D2", lambda e: e["event"] == "env" and e["t"] == 11 and e["room"] == "lobby"
                         and e["body"] == [user] and e["at"] >= rejoin_at, 20) is not None,
          "and the JOIN went upstream: D2 sees the bouncer join lobby")
    ident = os.urandom(8).hex()
    command(run, "D2", op="send", id=ident, t=20, room="lobby", body="welcome back")
    check(wait_env(run, "A2", id=ident) is not None, "A2 gets lobby's traffic again")
    run.stop("D2")

    print("relay: a JOIN while rrcd H is down", flush=True)
    run.stop("H")
    down = run.wait_status(upstream, lambda h: h["state"] != "connected", 30)
    check(down["state"] != "connected", f"the bouncer saw the link go ({down['last_error']})")
    time.sleep(1)
    command(run, "A2", op="join", room="side")
    error = wait_env(run, "A2", t=40, room="side")
    check(error is not None and error["body"] == "hub not connected: side not joined" and error["src"] == hub_id,
          f"A2's JOIN of side: ERROR {error and error['body']!r}, room side")
    run.rrcd(args.rrcd, port_r, max_rooms=4)
    run.wait_status(upstream, lambda h: h["state"] == "connected" and "side" in h["rooms_joined"], 90)
    command(run, "A2", op="join", room="side")
    check(wait_env(run, "A2", 40, t=11, room="side") is not None, "with rrcd H back, the JOIN of side gets JOINED")
    errors = [e for e in run.events("A2") if e["event"] == "error"] + [e for e in run.events("D2") if e["event"] == "error"]
    check(not errors, "the driven clients raised nothing")
    run.stop("A2")
    check(run.stop("BR") == 0, "the bouncer stops cleanly")


HISTORY_USAGE = "usage: /history [N], N from 1 to 500"
END_OF_REPLAY = "--- end of replay ---"


def history_file(run, upstream, room):
    return run.path("state", "history", upstream, f"{room}.log")


def read_history(path):
    """A room's history file: its records, and whether it ends on a whole record."""
    with open(path, "rb") as f:
        data = f.read()
    stream = io.BytesIO(data)
    records = []
    while stream.tell() < len(data):
        try:
            record = cbor2.load(stream)
        except Exception:
            return records, False
        records.append(dict(ts_ms=record["ts_ms"], env=cbor2.loads(record["envelope"])))
    return records, True


def history_record(ts_ms, t, src, body, nick=None, room="hist"):
    """A history record written by hand, and its envelope's K_ID."""
    env = {0: 1, 1: t, 2: os.urandom(8), 3: ts_ms, 4: bytes.fromhex(src), 5: room, 6: body}
    if nick is not None:
        env[7] = nick
    return cbor2.dumps({"ts_ms": ts_ms, "envelope": cbor2.dumps(env)}), env[2].hex()


def utc(ts_ms):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts_ms // 1000))


def replayed(run, name, room, since, timeout=30):
    """The next replay a client got in a room after `since`: its opening
    NOTICE, the envelopes in between, and its closing NOTICE; None if no
    replay ended in time."""
    if run.wait_event(name, lambda e: e["event"] == "env" and e["at"] >= since and e["t"] == 21
                      and e["room"] == room and e["body"] == END_OF_REPLAY, timeout) is None:
        return None
    got = [e for e in envs(run, name, room=room) if e["at"] >= since]
    start = next(i for i, e in enumerate(got) if e["t"] == 21 and str(e["body"]).startswith("--- replay: "))
    end = next(i for i, e in enumerate(got) if i > start and e["body"] == END_OF_REPLAY)
    return got[start], got[start + 1:end], got[end]


def same(env, expected):
    keys = ("id", "t", "ts", "src", "nick", "body", "room")
    return all(env.get(k) == expected.get(k) for k in keys)


def check_replay(run, name, room, since, hub_id, expected, what):
    """A replay of `expected` (env events as first delivered) framed by the
    two NOTICEs from the local hub; returns the opening NOTICE."""
    got = replayed(run, name, room, since)
    check(got is not None, f"{name}: a replay ends in {room} ({what})")
    start, middle, end = got
    n = len(expected)
    check(start["src"] == hub_id and end["src"] == hub_id and start["nick"] is None,
          f"{name}: both framing NOTICEs come from the local hub")
    check(start["body"].startswith(f"--- replay: {n} message{'' if n == 1 else 's'} since ")
          and start["body"].endswith(" ---"), f"{name}: \"{start['body']}\"")
    ids = [e["id"] for e in middle]
    if [e["id"] for e in expected] != ids:
        raise Failure(f"{name} got {[e['body'] for e in middle]}, not {[e['body'] for e in expected]}")
    bad = [e["body"] for e, x in zip(middle, expected) if not same(e, x)]
    check(not bad, f"{name}: {n} envelope{'' if n == 1 else 's'} in order, each with its K_ID, K_TS, K_SRC, "
                   "K_NICK and body")
    return start


def no_replay(run, name, room, since, wait=4):
    time.sleep(wait)
    return not [e for e in envs(run, name, room=room) if e["at"] >= since
                and (e["t"] in (20, 22) or str(e["body"]).startswith("--- replay"))]


def history_phases(run, args, port_r, upstream, user, local, env, found):
    print("history: the bouncer records hist with no client linked", flush=True)
    run.stop("H")
    run.rrcd(args.rrcd, port_r, max_rooms=6)
    direct = RNS.Identity.from_file(run.identity("direct")).hash.hex()
    eve = RNS.Identity.from_file(run.identity("eve")).hash.hex()
    upstream_id = RNS.Identity.from_file(run.identity("rrcd-hub")).hash.hex()
    hub_id = None
    reticulum = f'tcp = "127.0.0.1:{port_r}"'
    rooms = ("lobby", "hist")

    def bouncer(pruned=False, **history):
        config = run.config("history", reticulum, upstream=upstream, nick="bnc", rooms=rooms, history=history)
        mark = len(run.log("BR")) if os.path.exists(run.path("BR.log")) else 0
        run.start("BR", [args.bouncer, "--config", config, "run"], env=env)
        if pruned:
            # The prune at start runs on the history thread, beside the
            # upstream session: the files are read only once it is done.
            end = time.time() + 60
            while "history: pruned" not in run.log("BR")[mark:] and time.time() < end:
                time.sleep(0.3)
            check("history: pruned" in run.log("BR")[mark:], "the prune at start is done")
        hub = run.wait_status(upstream, lambda h: h["state"] == "connected" and "hist" in h["rooms_joined"], 60)
        check(hub["state"] == "connected" and "hist" in hub["rooms_joined"],
              f"the bouncer is in hist ({', '.join(f'{k} = {v}' for k, v in history.items()) or 'defaults'})")
        return config

    def client(name):
        nonlocal hub_id
        driven(run, name, port_r, "user", local, name.lower())
        check(wait_env(run, name, 40, t=2) is not None, f"{name} links to the local hub")
        linked = next(e["hub_identity"] for e in run.events(name) if e["event"] == "link")
        hub_id = hub_id or linked
        check(linked == hub_id, f"{name}: the local hub's identity is {hub_id}")

    def join(name, room="hist"):
        since = time.time()
        command(run, name, op="join", room=room)
        joined = wait_env(run, name, 40, t=11, room=room)
        check(joined is not None and joined["src"] == hub_id, f"{name} joins {room}: JOINED")
        return since

    def history(name, n, room="hist"):
        since = time.time()
        command(run, name, op="send", t=20, room=room, body=f"/history {n}")
        return since

    bouncer()
    driven(run, "D3", port_r, "direct", upstream, "dee")
    driven(run, "E", port_r, "eve", upstream, "eve")
    check(wait_env(run, "D3", 40, t=2) and wait_env(run, "E", 40, t=2), "D3 and E link to rrcd H")
    command(run, "D3", op="join", room="hist")
    check(wait_env(run, "D3", t=11, room="hist") is not None, "D3 joins hist on rrcd H")
    command(run, "E", op="join", room="hist")
    check(wait_env(run, "D3", t=11, room="hist", body=[eve]) is not None, "D3 sees E join hist")
    command(run, "E", op="part", room="hist")
    check(wait_env(run, "D3", t=13, room="hist") is not None, "D3 sees E part hist")
    said = []
    for t, body in ((20, "first, before anyone linked"), (22, "waves at nobody"), (21, "a notice to hist")):
        ident = os.urandom(8).hex()
        command(run, "D3", op="send", id=ident, t=t, room="hist", body=body)
        echo = wait_env(run, "D3", id=ident)
        check(echo is not None, f"D3 says t={t} \"{body}\" in hist")
        said.append(echo)
    path = history_file(run, upstream, "hist")
    end = time.time() + 10
    while time.time() < end and not (os.path.exists(path) and len(read_history(path)[0]) >= 6):
        time.sleep(0.3)
    records, whole = read_history(path)
    found["history_file"] = records
    kinds = [(r["env"][1], [m.hex() for m in r["env"][6]] if r["env"][1] in (11, 13) else None) for r in records]
    check(whole and kinds == [(11, [direct]), (11, [eve]), (13, [eve]), (20, None), (22, None), (21, None)],
          "hist.log holds D3's and E's JOIN, E's PART and D3's three messages")
    check([r["env"][2].hex() for r in records[3:]] == [e["id"] for e in said]
          and all(r["env"][4].hex() == direct and r["env"][3] == e["ts"] for r, e in zip(records[3:], said)),
          "as the hub sent them: D3's K_ID, K_TS and K_SRC")
    mode = lambda p: oct(os.stat(p).st_mode & 0o777)
    check(mode(path) == "0o600" and mode(os.path.dirname(path)) == "0o700"
          and mode(run.path("state", "history")) == "0o700",
          "the history file is mode 0600, its directories 0700")

    print("history: a client links and joins hist", flush=True)
    client("A3")
    since = join("A3")
    start = check_replay(run, "A3", "hist", since, hub_id, said, "D3's MSG, ACTION and NOTICE")
    oldest = start["body"].split(" since ")[1].rstrip(" -")
    check(abs(calendar.timegm(time.strptime(oldest, "%Y-%m-%dT%H:%M:%SZ")) - said[0]["at"]) < 30,
          f"the replay is since the first message's time ({oldest})")
    check(not envs(run, "A3", room="hist", t=11, nick="eve") and not envs(run, "A3", room="hist", t=13),
          "E's JOIN and PART are not replayed")
    mine = os.urandom(8).hex()
    command(run, "A3", op="send", id=mine, t=20, room="hist", body="mine, through the bouncer")
    check(wait_env(run, "D3", id=mine) is not None and wait_env(run, "A3", id=mine) is not None,
          "A3's MSG reaches D3 and comes back to A3")
    said.append(wait_env(run, "A3", id=mine))
    check(said[-1]["src"] == user, "with the user's identity")
    client("A4")
    since = join("A4")
    check_replay(run, "A4", "hist", since, hub_id, said, "A3's own MSG among them")
    check(len(envs(run, "A4", id=mine)) == 1 and len(envs(run, "A3", id=mine)) == 1,
          "A3's MSG is in the replay once, with the user's identity")
    hub = run.wait_status(upstream, lambda h: h.get("clients") == 2 and h.get("history_records", 0) >= 6, 10)
    found["history_status"] = hub
    check(hub["history_records"] >= 6 and hub["history_rooms"] >= 1 and hub["history_bytes"] > 0
          and hub["history_drops"] == 0,
          f"status.json: {hub['history_records']} records written, {hub['history_bytes']} bytes "
          f"in {hub['history_rooms']} room(s), no drops")
    out = run.bouncer("--config", run.path("history.toml"), "status")
    found["history_status_out"] = out.stdout
    kept_in = f"{hub['history_rooms']} room{'' if hub['history_rooms'] == 1 else 's'}"
    check(f"history          {hub['history_bytes']} bytes in {kept_in}" in out.stdout
          and f"history written  {hub['history_records']} records, 0 dropped" in out.stdout,
          "status prints the history")

    print("history: /history N", flush=True)
    asked = since = history("A3", 2)
    check_replay(run, "A3", "hist", since, hub_id, said[-2:], "/history 2")
    since = history("A3", 0)
    usage = run.wait_event("A3", lambda e: e["event"] == "env" and e["at"] >= since and e["body"] == HISTORY_USAGE, 20)
    check(usage is not None and usage["src"] == hub_id and usage["room"] == "hist", f"/history 0: \"{HISTORY_USAGE}\"")
    time.sleep(3)
    check(not [e for e in envs(run, "A4") if e["at"] >= asked and str(e["body"]).startswith(("--- replay", "usage"))],
          "A4 gets neither answer")
    check(not [e for e in envs(run, "D3") if "/history" in str(e["body"])], "D3 sees no /history: it went nowhere upstream")
    for name in ("A3", "A4"):
        run.stop(name)
    check(run.stop("BR") == 0, "the bouncer stops cleanly")
    hub = run.status(upstream)
    check(hub["history_drops"] == 0 and hub["history_records"] >= 6, "status.json at shutdown: no drops")

    print("history: the bouncer restarts", flush=True)
    bouncer()
    client("A5")
    check_replay(run, "A5", "hist", join("A5"), hub_id, said, "the same history after a restart")
    run.stop("A5")
    check(run.stop("BR") == 0, "the bouncer stops cleanly")

    print("history: days, replay_hours and a cut record (replay_hours = 1)", flush=True)
    now = int(time.time() * 1000)
    day = 24 * 3600 * 1000
    stale, stale_id = history_record(now - 40 * day, 20, direct, "forty days ago", "dee")
    earlier, earlier_id = history_record(now - 2 * 3600 * 1000, 21, upstream_id, "two hours ago")
    cut, cut_id = history_record(now, 20, direct, "cut short", "dee")
    with open(path, "rb") as f:
        kept = f.read()
    with open(path, "wb") as f:
        f.write(stale + earlier + kept + cut[:len(cut) // 2])
    gone = history_file(run, upstream, "old")
    with open(gone, "wb") as f:
        f.write(history_record(now - 31 * day, 20, direct, "a month ago", "dee", "old")[0])
    bouncer(pruned=True, replay_hours=1)
    records, whole = read_history(path)
    ids = [r["env"][2].hex() for r in records]
    check(whole and stale_id not in ids and earlier_id in ids and cut_id not in ids,
          "at start the record older than days = 30 is pruned and the cut record cut off")
    check(not os.path.exists(gone), "old.log, all of it older than 30 days, is gone")
    client("A6")
    check_replay(run, "A6", "hist", join("A6"), hub_id, said, "the two-hour-old NOTICE is outside replay_hours")
    since = history("A6", 50)
    earlier_env = dict(id=earlier_id, t=21, ts=now - 2 * 3600 * 1000, src=hub_id, nick=None,
                       body="two hours ago", room="hist")
    start = check_replay(run, "A6", "hist", since, hub_id, [earlier_env] + said, "/history 50")
    check(start["body"] == f"--- replay: 5 messages since {utc(now - 2 * 3600 * 1000)} ---",
          "/history has no time window; the upstream hub's NOTICE is from the local hub")
    run.stop("A6")
    check(run.stop("BR") == 0, "the bouncer stops cleanly")

    print("history: replay_max = 2", flush=True)
    bouncer(replay_max=2)
    client("A7")
    check_replay(run, "A7", "hist", join("A7"), hub_id, said[-2:], "the newest two")
    run.stop("A7")
    check(run.stop("BR") == 0, "the bouncer stops cleanly")

    print("history: replay_max = 0", flush=True)
    bouncer(replay_max=0)
    client("A8")
    since = join("A8")
    check(no_replay(run, "A8", "hist", since), "a JOIN replays nothing")
    check_replay(run, "A8", "hist", history("A8", 1), hub_id, said[-1:], "/history 1 still answers")
    errors = [e for name in ("A3", "A4", "A5", "A6", "A7", "A8", "D3", "E") for e in run.events(name)
              if e["event"] == "error"]
    check(not errors, "the driven clients raised nothing")
    for name in ("A8", "D3", "E"):
        run.stop(name)
    check(run.stop("BR") == 0, "the bouncer stops cleanly")

    print("history: two rooms joined at once (replay_max = 500)", flush=True)
    now = int(time.time() * 1000)
    many = 400
    with open(history_file(run, upstream, "hist2"), "wb") as f:
        for i in range(many):
            f.write(history_record(now - (many - i) * 1000, 20, direct, f"hist2 line {i}", "dee", "hist2")[0])
    rooms = ("lobby", "hist", "hist2")
    config = bouncer(replay_max=500)
    check(run.wait_status(upstream, lambda h: "hist2" in h["rooms_joined"], 30) is not None, "the bouncer is in hist2")
    client("A9")
    since = time.time()
    command(run, "A9", op="join", room="hist2")
    command(run, "A9", op="join", room="hist")
    for room in ("hist2", "hist"):
        check(replayed(run, "A9", room, since, 60) is not None, f"A9: a replay ends in {room}")
    got = [e for e in run.events("A9") if e["event"] == "env" and e["at"] >= since and e["room"] in ("hist", "hist2")]
    spans = {}
    for room in ("hist2", "hist"):
        start = next(i for i, e in enumerate(got) if e["room"] == room and str(e["body"]).startswith("--- replay: "))
        end = next(i for i, e in enumerate(got) if i > start and e["room"] == room and e["body"] == END_OF_REPLAY)
        joined = next(i for i, e in enumerate(got) if e["room"] == room and e["t"] == 11)
        check(joined < start, f"A9: JOINED {room} comes before its replay")
        check(all(e["room"] == room for e in got[start:end + 1]),
              f"A9: the {room} replay ({end - start - 1} messages) is not interleaved with the other room's")
        spans[room] = (start, end)
    # hist: what was said, and the two-hour-old NOTICE, inside replay_hours = 24.
    check(spans["hist2"][1] - spans["hist2"][0] - 1 == many
          and spans["hist"][1] - spans["hist"][0] - 1 == len(said) + 1,
          f"A9: all {many} of hist2, then all {len(said) + 1} of hist")

    print("history: PART during a replay, then JOIN again", flush=True)
    command(run, "A9", op="part", room="hist2")
    check(wait_env(run, "A9", 20, t=13, room="hist2") is not None, "A9 parts hist2")
    since = time.time()
    command(run, "A9", op="join", room="hist2")
    opening = run.wait_event("A9", lambda e: e["event"] == "env" and e["at"] >= since and e["room"] == "hist2"
                             and str(e["body"]).startswith("--- replay: "), 30)
    check(opening is not None, "A9 joins hist2 again and its replay starts")
    time.sleep(1)
    parted_at = time.time()
    command(run, "A9", op="part", room="hist2")
    parted = run.wait_event("A9", lambda e: e["event"] == "env" and e["at"] >= parted_at and e["t"] == 13
                            and e["room"] == "hist2", 20)
    check(parted is not None, "A9 parts hist2 mid-replay: PARTED")
    found["parted_after"] = parted["at"] - parted_at
    check(parted["at"] - parted_at < 3, f"PARTED comes {parted['at'] - parted_at:.1f} s after the PART")
    time.sleep(3)
    sent = [e for e in envs(run, "A9", room="hist2") if e["at"] >= since and e["t"] == 20]
    after = [e for e in envs(run, "A9", room="hist2") if e["at"] > parted["at"]]
    check(not after and 0 < len(sent) < many,
          f"the replay stopped at the PART ({len(sent)} of {many} sent), nothing of hist2 after PARTED")
    since = time.time()
    command(run, "A9", op="join", room="hist2")
    got = replayed(run, "A9", "hist2", since, 60)
    check(got is not None and len(got[1]) == many and got[0]["body"].startswith(f"--- replay: {many} messages"),
          f"a JOIN again: a fresh replay, all {many}")
    errors = [e for e in run.events("A9") if e["event"] == "error"]
    check(not errors, "A9 raised nothing")
    run.stop("A9")
    check(run.stop("BR") == 0, "the bouncer stops cleanly")

    print("history: max_mb = 1 across rooms", flush=True)
    # 1500 records of about 1.1 kB in three rooms, their times shuffled
    # across the rooms and out of order in each; all older than hist's.
    now = int(time.time() * 1000)
    total = 1500
    written = {}
    files = {room: open(history_file(run, upstream, room), "wb") for room in ("p1", "p2", "p3")}
    for i in range(total):
        ts = now - 3600 * 1000 - ((i * 7919) % total) * 1000
        room = f"p{i % 3 + 1}"
        record, ident = history_record(ts, 20, direct, f"{i} " + "x" * 1000, "dee", room)
        files[room].write(record)
        written[ident] = ts
    for f in files.values():
        f.close()
    config = bouncer(pruned=True, max_mb=1)
    hub_dir = os.path.dirname(history_file(run, upstream, "hist"))
    kept, size = {}, 0
    for name in os.listdir(hub_dir):
        if name.endswith(".log"):
            path_ = os.path.join(hub_dir, name)
            size += os.path.getsize(path_)
            records, whole = read_history(path_)
            check(whole, f"{name} reads to its end")
            kept[name[:-4]] = records
    found["pruned_sizes"] = {room: len(r) for room, r in kept.items()}
    check(size <= 1_000_000, f"the hub's history is {size} bytes, at most max_mb = 1")
    check(len(kept.get("hist2", [])) == many and len(kept.get("hist", [])) >= len(said),
          "the newest rooms are kept whole")
    left = {r["env"][2].hex() for room in ("p1", "p2", "p3") for r in kept.get(room, [])}
    gone = [ts for ident, ts in written.items() if ident not in left]
    stay = [ts for ident, ts in written.items() if ident in left]
    check(gone and stay and max(gone) < min(stay),
          f"the {len(gone)} oldest records went, whichever room they were in; {len(stay)} newer stay")
    check(all(kept.get(room) for room in ("p1", "p2", "p3")), "each of the three rooms keeps some")
    check(size > 1_000_000 - 3 * 1200, "no more went than had to")

    print("history: a second bouncer on the same state_dir", flush=True)
    out = run.bouncer("--config", config, "run")
    found["second_bouncer"] = out.stderr
    check(out.returncode == 1 and "another rrc-bouncer is running" in out.stderr,
          f"it refuses to start: {out.stderr.strip().splitlines()[-1]}")
    check(run.procs["BR"].poll() is None, "the first one runs on")
    out = run.bouncer("--config", config, "status")
    check(out.returncode == 0 and "connected" in out.stdout, "status works beside it")
    check(run.stop("BR") == 0, "the bouncer stops cleanly")


# rrcd sends a greeting longer than 512 bytes as a Resource.
GREETING = "\n".join(f"{i:02d} The e2e hub's greeting is too long for one packet: ünïcödé ✓ ü ✓."
                     for i in range(1, 25))
# A NOTICE too long for one packet, from either side.
LONG = "\n".join(f"line {i:02d} of a long notice to res, sent as a Resource — ✓ ü" for i in range(1, 31))


def resources(run, name, since=0):
    return [e for e in run.events(name) if e["event"] == "resource" and e["at"] >= since]


def resource_phases(run, args, port_r, upstream, user, local, env, found):
    print("resource: rrcd H greets with a text too long for one packet", flush=True)
    run.stop("H")
    run.rrcd(args.rrcd, port_r, max_rooms=16, greeting=GREETING, shim=True)
    direct = RNS.Identity.from_file(run.identity("direct")).hash.hex()
    reticulum = f'tcp = "127.0.0.1:{port_r}"'
    config = run.config("resource", reticulum, upstream=upstream, nick="bnc", rooms=("lobby", "res"))
    greeting = GREETING.encode()
    greeting_sha = hashlib.sha256(greeting).hexdigest()
    run.start("BR", [args.bouncer, "--config", config, "run"], env=env)
    hub = run.wait_status(upstream, lambda h: h["state"] == "connected" and "res" in h["rooms_joined"]
                          and h["hub_resources_in"] >= 1, 60)
    found["resource_status_greeting"] = hub
    check(hub["state"] == "connected" and "res" in hub["rooms_joined"], "the bouncer is in res")
    check(f"kind=motd size={len(greeting)}" in run.log("H"), f"rrcd sent its {len(greeting)}-byte greeting as a Resource")
    check(hub["hub_resources_in"] == 1 and hub["hub_resource_drops"] == 0,
          "status.json: the bouncer took it whole (hub_resources_in 1, no drops)")

    def client(name, cap):
        driven(run, name, port_r, "user", local, name.lower(), resource_cap=cap)
        check(wait_env(run, name, 40, t=2) is not None, f"{name} ({'with' if cap else 'without'} the cap) links to the local hub")
        return next(e["hub_identity"] for e in run.events(name) if e["event"] == "link")

    def got_greeting(name, hub_id, copies):
        """The greeting `copies` times as a Resource, after its RESOURCE_ENVELOPE."""
        run.wait_event(name, lambda e: len([r for r in resources(run, name) if r["sha256"] == greeting_sha]) >= copies, 30)
        announced = envs(run, name, t=50)
        got = resources(run, name)
        found[f"resource_{name}"] = {"envelopes": announced, "resources": [dict(r, text=r["text"][:80]) for r in got]}
        check(len(announced) == copies and all(e["src"] == hub_id and e["room"] is None and e["body"]["1"] == "motd"
                                               and e["body"]["2"] == len(greeting) and e["body"]["3"] == greeting_sha
                                               for e in announced),
              f"{name}: {copies} RESOURCE_ENVELOPE(s) from the local hub: motd, room-less, size and sha256 of the greeting")
        check(len(got) == copies and all(r["complete"] and r["text"] == GREETING and r["sha256"] == greeting_sha
                                         for r in got),
              f"{name}: and {copies} Resource(s), byte for byte the greeting")
        check(all(e["at"] <= r["at"] for e, r in zip(announced, got)), f"{name}: each envelope came before its Resource")

    def got_notices(name, hub_id, copies):
        lines = GREETING.splitlines()
        run.wait_event(name, lambda e: len(envs(run, name, t=21, room=None)) >= len(lines) * copies, 30)
        notices = [e["body"] for e in envs(run, name, t=21, room=None) if e["src"] == hub_id]
        found[f"resource_{name}_notices"] = len(notices)
        # In any order: Python RNS hands each packet to a thread of its own
        # (RNS/Link.py:1044).
        check(sorted(notices) == sorted(lines * copies),
              f"{name}: the greeting {copies} time(s) as {len(lines)} NOTICEs, one per line, from the local hub")
        check(not envs(run, name, t=50) and not resources(run, name), f"{name}: no RESOURCE_ENVELOPE, no Resource")

    hub_id = client("RA", True)
    client("RN", False)
    print("resource: the greeting from the bouncer's cache", flush=True)
    got_greeting("RA", hub_id, 1)
    got_notices("RN", hub_id, 1)

    print("resource: rrcd H restarts; the greeting reaches linked clients as it comes", flush=True)
    before = run.status(upstream)
    run.stop("H")
    run.rrcd(args.rrcd, port_r, max_rooms=16, greeting=GREETING, shim=True)
    hub = run.wait_status(upstream, lambda h: h["connects"] > before["connects"] and h["state"] == "connected"
                          and h["hub_resources_in"] > before["hub_resources_in"], 90)
    check(hub["connects"] > before["connects"] and hub["hub_resources_in"] == before["hub_resources_in"] + 1,
          "the bouncer connected again and took the greeting")
    got_greeting("RA", hub_id, 2)
    got_notices("RN", hub_id, 2)
    hub = run.wait_status(upstream, lambda h: h["client_resources_out"] >= 2 and h["client_resources_split"] >= 2,
                          STATUS_TICK)
    check(hub["client_resources_out"] == 2 and hub["client_resources_split"] == 2,
          f"status.json: client_resources_out {hub['client_resources_out']}, "
          f"client_resources_split {hub['client_resources_split']}")

    print("resource: a long notice from a client of the bouncer to res", flush=True)
    driven(run, "RD", port_r, "direct", upstream, "dee", resource_cap=True)
    check(wait_env(run, "RD", 40, t=2) is not None, "RD links to rrcd H")
    for name in ("RA", "RN", "RD"):
        command(run, name, op="join", room="res")
        check(wait_env(run, name, 40, t=11, room="res") is not None, f"{name} joins res")
    long = LONG.encode()
    long_sha = hashlib.sha256(long).hexdigest()
    before = run.status(upstream)
    since = time.time()
    command(run, "RA", op="resource", room="res", text=LONG)
    sent = run.wait_event("RA", lambda e: e["event"] == "resource_sent" and e["at"] >= since, 40)
    check(sent is not None and sent["complete"], "RA's Resource reached the bouncer whole")
    got = run.wait_event("RD", lambda e: e["event"] == "resource" and e["at"] >= since and e["sha256"] == long_sha, 40)
    announced = [e for e in envs(run, "RD", t=50, room="res") if e["at"] >= since]
    found["resource_RD_from_RA"] = announced
    check(got is not None and got["text"] == LONG, f"RD, on rrcd H, gets the {len(long)}-byte notice as a Resource, "
          "byte for byte")
    check(announced and announced[-1]["src"] == user and announced[-1]["body"]["1"] == "notice"
          and announced[-1]["body"]["3"] == long_sha, "its RESOURCE_ENVELOPE is the user's, in res, kind notice")
    check("Received large NOTICE via resource" in run.log("H"), "rrcd took it as a NOTICE to res")
    lines = LONG.splitlines()
    run.wait_event("RN", lambda e: len([n for n in envs(run, "RN", t=21, room="res") if n["at"] >= since])
                   >= len(lines), 20)
    notices = [e for e in envs(run, "RN", t=21, room="res") if e["at"] >= since]
    check(sorted(e["body"] for e in notices) == sorted(lines) and all(e["src"] == user for e in notices),
          f"RN, the user's other client, gets it as {len(lines)} NOTICEs from the user")
    check(not [e for e in resources(run, "RA", since) if e["sha256"] == long_sha], "RA, which sent it, gets no copy")
    hub = run.wait_status(upstream, lambda h: h["hub_resources_out"] > before["hub_resources_out"]
                          and h["client_resources_in"] > before["client_resources_in"], STATUS_TICK)
    check(hub["client_resources_in"] == before["client_resources_in"] + 1
          and hub["hub_resources_out"] == before["hub_resources_out"] + 1,
          "status.json: client_resources_in and hub_resources_out each one more")

    print("resource: a long notice from a client of rrcd H to res", flush=True)
    driven(run, "RA2", port_r, "user", local, "ra2", resource_cap=True)
    check(wait_env(run, "RA2", 40, t=2) is not None, "RA2 (with the cap) links to the local hub")
    got_greeting("RA2", hub_id, 1)
    command(run, "RA2", op="join", room="res")
    check(wait_env(run, "RA2", 40, t=11, room="res") is not None, "RA2 joins res")
    text = LONG.replace("long notice to res", "long notice from RD")
    data_sha = hashlib.sha256(text.encode()).hexdigest()
    before = run.status(upstream)
    since = time.time()
    command(run, "RD", op="resource", room="res", text=text)
    for name in ("RA", "RA2"):
        got = run.wait_event(name, lambda e: e["event"] == "resource" and e["sha256"] == data_sha, 40)
        announced = [e for e in envs(run, name, t=50, room="res") if e["at"] >= since]
        check(got is not None and got["text"] == text and announced and announced[-1]["src"] == direct
              and announced[-1]["body"]["3"] == data_sha,
              f"{name} gets it as RESOURCE_ENVELOPE (from RD's identity, in res) + Resource, byte for byte")
    lines = text.splitlines()
    run.wait_event("RN", lambda e: len([n for n in envs(run, "RN", t=21, room="res") if n["at"] >= since])
                   >= len(lines), 20)
    notices = [e for e in envs(run, "RN", t=21, room="res") if e["at"] >= since]
    check(sorted(e["body"] for e in notices) == sorted(lines) and all(e["src"] == direct for e in notices),
          f"RN gets it as {len(lines)} NOTICEs from RD's identity")
    hub = run.wait_status(upstream, lambda h: h["hub_resources_in"] > before["hub_resources_in"], STATUS_TICK)
    check(hub["hub_resources_in"] == before["hub_resources_in"] + 1, "status.json: hub_resources_in one more")
    path = history_file(run, upstream, "res")
    end = time.time() + 10
    while time.time() < end and not (os.path.exists(path) and
                                     sum(r["env"].get(6) == lines[-1] for r in read_history(path)[0])):
        time.sleep(0.3)
    records = [r["env"] for r in read_history(path)[0]] if os.path.exists(path) else []
    check([e.get(6) for e in records if e.get(1) == 21 and e.get(4) == bytes.fromhex(direct)] == lines,
          "history keeps it in res as the same NOTICEs")
    mine = [e.get(6) for e in records if e.get(1) == 21 and e.get(4) == bytes.fromhex(user)]
    check(mine == LONG.splitlines(), "and RA's long notice too")

    print("resource: Resources from a client that do not hold up", flush=True)
    before = run.status(upstream)
    since = time.time()
    big = "x" * (256 * 1024 + 1)
    command(run, "RA", op="resource", room="res", text=big)
    error = run.wait_event("RA", lambda e: e["event"] == "env" and e["t"] == 40 and e["at"] >= since, 20)
    found["resource_too_large"] = error
    check(error is not None and error["src"] == hub_id, f"a RESOURCE_ENVELOPE past 256 KiB gets ERROR \"{error and error['body']}\"")
    sent = run.wait_event("RA", lambda e: e["event"] == "resource_sent" and e["at"] >= since, 60)
    check(sent is not None and not sent["complete"], "and its Resource is refused")
    since = time.time()
    bad = "a notice whose envelope lies about its sha256 " * 20
    command(run, "RA", op="resource", room="res", text=bad, sha256="00" * 32)
    sent = run.wait_event("RA", lambda e: e["event"] == "resource_sent" and e["at"] >= since, 40)
    found["resource_bad_hash_sent"] = sent
    hub = run.wait_status(upstream, lambda h: h["client_resource_drops"] >= before["client_resource_drops"] + 3,
                          STATUS_TICK)
    found["resource_status_drops"] = hub
    check(hub["client_resource_drops"] >= before["client_resource_drops"] + 3,
          f"status.json: client_resource_drops {before['client_resource_drops']} -> {hub['client_resource_drops']} "
          "(the large envelope, its Resource, the bad sha256)")
    check(hub["client_resources_in"] == before["client_resources_in"] and hub["hub_resources_out"] == before["hub_resources_out"],
          "neither went to the hub")
    time.sleep(2)
    check(not [e for e in run.events("RD") if e["at"] >= since and e["event"] in ("resource", "env")
               and e.get("t") in (21, 50)], "RD saw nothing of them")
    ident = os.urandom(8).hex()
    command(run, "RA", op="send", id=ident, t=20, room="res", body="still here after the bad Resources")
    check(wait_env(run, "RD", 20, t=20, room="res", body="still here after the bad Resources") is not None,
          "the relay works on: RA's next message reaches RD")
    out = run.bouncer("--config", config, "status")
    found["resource_status_out"] = out.stdout
    print("  " + "\n  ".join(l for l in out.stdout.splitlines() if "resources" in l), flush=True)
    check("hub resources    " in out.stdout and "client resources " in out.stdout, "status prints both Resource lines")
    check(not [e for name in ("RA", "RA2", "RN", "RD") for e in run.events(name) if e["event"] == "error"],
          "the driven clients raised nothing")
    for name in ("RA", "RA2", "RN", "RD"):
        run.stop(name)
    check(run.stop("BR") == 0, "the bouncer stops cleanly")


def only(run, args, ports, found, phases):
    port_r = free_port(ports)
    run.rnsd("R", port_r)
    upstream = RNS.Destination.hash(RNS.Identity.from_file(run.identity("rrcd-hub")), "rrc", "hub").hex()
    user = RNS.Identity.from_file(run.path("ids/user")).hash.hex()
    config = run.config("rrcd", f'tcp = "127.0.0.1:{port_r}"', upstream=upstream, nick="bnc")
    local = {k.strip(): v for k, v in (line.rsplit(None, 1) for line in
                                       run.bouncer("--config", config, "hashes").stdout.splitlines())}["local hub"]
    env = {"RRC_BOUNCER_PING_SECS": str(BOUNCER_PING), "RRC_BOUNCER_BACKOFF_SECS": str(BOUNCER_BACKOFF)}
    phases(run, args, port_r, upstream, user, local, env, found)
    run.stop("H")
    run.stop("R")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--work", required=True)
    parser.add_argument("--python", required=True)
    parser.add_argument("--rnsd", required=True)
    parser.add_argument("--rrcd", required=True)
    parser.add_argument("--bouncer", required=True)
    parser.add_argument("--only", choices=["rrcd", "history", "resource"])
    args = parser.parse_args()
    shutil.rmtree(args.work, ignore_errors=True)
    os.makedirs(args.work)
    run = Run(args.work, args.python, args.rnsd, args.bouncer)
    run.identity("user")
    found = {}
    try:
        port_t = free_port()
        port_s = free_port({port_t})
        port_d = free_port({port_t, port_s})
        port_l = free_port({port_t, port_s, port_d})
        if args.only == "rrcd":
            rrcd_phases(run, args, {port_t, port_s, port_d, port_l}, found)
            raise Done()
        if args.only in ("history", "resource"):
            only(run, args, {port_t, port_s, port_d, port_l}, found,
                 history_phases if args.only == "history" else resource_phases)
            raise Done()
        tcp = f'tcp = "127.0.0.1:{port_t}"'
        config = run.config("tcp", tcp)
        out = run.bouncer("--config", config, "hashes")
        check(out.returncode == 0, f"hashes runs ({out.stderr.strip()})")
        lines = {k.strip(): v for k, v in (line.rsplit(None, 1) for line in out.stdout.splitlines())}
        hub = lines["local hub"]
        check(lines["upstream hub"] == UPSTREAM, "hashes prints the upstream hub")
        check(lines["identity"] == RNS.Identity.from_file(run.path("ids/user")).hash.hex(),
              "hashes prints the user's identity hash")
        check(oct(os.stat(run.path("state/hubs", f"{UPSTREAM}.identity")).st_mode & 0o777) == "0o600",
              "the hub identity file is mode 0600")

        print("direct: a client connects to the bouncer's own interface, announce = false", flush=True)
        rns_config(run.path("rns", "D"), port_d, server=True, transport=False)
        config_d = run.config("direct", 'config_dir = "rns/D"')
        run.start("BD", [args.bouncer, "--config", config_d, "run"])
        check(run.wait_log("BD", f"local hub {hub}", 20), "the bouncer registers the hub")
        wait_port(port_d)
        done, pong, events = ping(run, "C0", port_d, "user", hub)
        found["direct_client_events"] = events
        check(any(e["event"] == "path" for e in events),
              "rsReticulum answers a path request for its never-announced hub")
        check(hub_announces(events, hub, True) and not hub_announces(events, hub, False),
              "the answer is a path response, not an announce")
        check(pong is not None, "the hub answers PING with PONG on an identified link")
        check(pong["body_matches"] and pong["src_is_hub"], "PONG echoes the body and carries the hub's identity")
        # Each client sends its one PING in the same TCP segment as its
        # identify; a hub that handled a packet before the link's identity
        # dropped about one in six.
        pongs = sum(ping(run, f"C0{i}", port_d, "user", hub, timeout=10)[1] is not None for i in range(5))
        check(pongs == 5, f"{pongs}/5 more clients get PONG for a PING sent right behind the identify")
        check("ignored a packet on a link not identified" not in run.log("BD"),
              "the hub dropped no packet of an identified link")
        check(run.stop("BD") == 0, "the bouncer stops cleanly on SIGTERM")

        print("tcp: the bouncer connects to rnsd T, announce = false", flush=True)
        run.rnsd("T", port_t)
        run.peer("W", port_t, "watcher")
        run.start("B", [args.bouncer, "--config", config, "run"])
        check(run.wait_log("B", f"local hub {hub} for upstream hub {UPSTREAM}", 20), "the bouncer registers the hub")
        check(run.wait_log("B", "sent the uplink a path response", 20), "the bouncer tells rnsd T where the hub is")
        time.sleep(QUIET)
        done, pong, events = ping(run, "C", port_t, "user", hub)
        found["tcp_client_events"] = events
        check(any(e["event"] == "path" for e in events), "rnsd T answers a path request for the hub")
        check(hub_announces(events, hub, True) and not hub_announces(events, hub, False),
              "the answer is a path response, not an announce")
        check(pong is not None, "the hub answers PING with PONG on an identified link")
        check(pong["body_matches"] and pong["src_is_hub"], "PONG echoes the body and carries the hub's identity")
        check(run.wait_log("B", "identified as", 5), "the bouncer saw the link identify")
        done, pong, events = ping(run, "X", port_t, "stranger", hub, timeout=12)
        check(pong is None and any(e["event"] == "link" for e in events),
              "a link identified as another identity gets no PONG")
        linked = next(e["at"] for e in events if e["event"] == "link")
        check(any(e["event"] == "packet" and e["t"] == 40 for e in events)
              and any(e["event"] == "closed" and e["at"] < linked + 6 for e in events),
              "it gets an ERROR and the hub closes its link")
        check(not hub_announces(run.events("W"), hub, False) and not hub_announces(run.events("W"), hub, True),
              "the watcher saw no announce of the hub")
        out = run.bouncer("--config", config, "status")
        check(out.returncode == 0 and hub in out.stdout, "status shows the running hub")
        check(run.stop("B") == 0, "the bouncer stops cleanly on SIGTERM")
        out = run.bouncer("--config", config, "hashes")
        check(hub in out.stdout, "the hub address is the same on the next run")

        print("tcp: announce = true", flush=True)
        config = run.config("tcp-announce", tcp, announce=True)
        run.start("BA", [args.bouncer, "--config", config, "run"])
        app_data = cbor2.dumps({"proto": "rrc", "v": 1, "hub": f"rrc-bouncer for {UPSTREAM}"}).hex()
        event = run.wait_event("W", lambda e: e["event"] == "announce" and e["dest"] == hub
                               and not e["path_response"], 20)
        check(event is not None, "the watcher sees the hub announce")
        check(event["app_data"] == app_data, "the announce carries rrcd's app data")
        run.stop("BA")
        run.stop("W")
        run.stop("T")

        print(f"tcp: rnsd TL drops never-used paths after {LINGER} s", flush=True)
        removed = f"Removing never used path to <{hub}>"
        run.rnsd("TL", port_l, linger=LINGER)
        check(run.wait_log("TL", f"Unused path linger time is {float(LINGER)}s", 10), f"rnsd TL lingers {LINGER} s")
        config = run.config("tcp-linger", f'tcp = "127.0.0.1:{port_l}"')
        run.start("BL0", [args.bouncer, "--config", config, "run"],
                  env={"RRC_BOUNCER_UPLINK_REFRESH_SECS": "3600"})
        check(run.wait_log("BL0", "sent the uplink a path response", 20), "the bouncer tells rnsd TL where the hub is")
        check(run.wait_log("TL", removed, LINGER + 15),
              "without a refresh, rnsd TL drops the path once the linger is over")
        run.stop("BL0")
        print(f"tcp: the bouncer refreshes the path every {REFRESH} s", flush=True)
        run.start("BL", [args.bouncer, "--config", config, "run"],
                  env={"RRC_BOUNCER_UPLINK_REFRESH_SECS": str(REFRESH)})
        check(run.wait_log("BL", "sent the uplink a path response", 20), "the bouncer tells rnsd TL where the hub is")
        removals = run.log("TL").count(removed)
        updates = run.log("TL").count(f"Destination <{hub}>")
        time.sleep(3 * LINGER)
        refreshes = run.log("BL").count("(refresh)")
        found["tl_refreshes"] = refreshes
        found["tl_path_updates"] = run.log("TL").count(f"Destination <{hub}>") - updates
        check(refreshes >= 3 * LINGER // REFRESH - 1, f"the bouncer sent {refreshes} refreshes in {3 * LINGER} s")
        check(found["tl_path_updates"] >= refreshes,
              f"rnsd TL took every refresh as a newer path ({found['tl_path_updates']} path table updates)")
        check(run.log("TL").count(removed) == removals,
              f"rnsd TL kept the path for {3 * LINGER} s, {3 * LINGER // LINGER} linger times")
        done, pong, events = ping(run, "CL", port_l, "user", hub)
        check(any(e["event"] == "path" for e in events) and pong is not None,
              "a client still gets a path through rnsd TL, links and gets PONG")
        check(run.stop("BL") == 0, "the bouncer stops cleanly")
        run.stop("TL")

        print("shared: the bouncer attaches to rnsd S's shared instance, announce = false", flush=True)
        instance = f"rrcm1-e2e-{os.getpid()}-S"
        run.rnsd("S", port_s, shared=instance)
        run.peer("W2", port_s, "watcher")
        config = run.config("shared", f'shared_instance = true\ninstance_name = "{instance}"')
        run.start("BS", [args.bouncer, "--config", config, "run"])
        check(run.wait_log("BS", f"local hub {hub}", 20), "the bouncer attaches and registers the hub")
        time.sleep(QUIET)
        done, pong, events = ping(run, "C2", port_s, "user", hub)
        found["shared_client_events"] = events
        check(pong is not None, "a client on rnsd S reaches the hub (path request, link, PING/PONG)")
        check(not hub_announces(run.events("W2"), hub, False), "the watcher on rnsd S saw no announce of the hub")

        print("shared: the bouncer restarts, rnsd S stays up", flush=True)
        # C2's path request was answered by rnsd S from its path table,
        # which left the hub in S's pending_local_path_requests
        # (Transport.py:3788-3797); S clears that only when a local client
        # sends a path response for the hub, as the bouncer does when it
        # registers again (Transport.py:2627-2647).
        check(run.stop("BS") == 0, "the bouncer stops cleanly")
        log_from = len(run.log("S"))
        restarted = time.time()
        run.start("BS", [args.bouncer, "--config", config, "run"])
        check(run.wait_log("BS", f"local hub {hub}", 20), "the bouncer attaches again")
        time.sleep(20)
        after = [e for e in run.events("W2") if e["at"] >= restarted]
        leaked = hub_announces(after, hub, False)
        found["announces_after_bouncer_restart"] = after
        found["s_log_after_bouncer_restart"] = [line for line in run.log("S")[log_from:].splitlines() if hub in line]
        if leaked:
            print("  known  after the bouncer attached again, rnsd S announced the hub to the network", flush=True)
        else:
            print("  ok  no announce of the hub after the bouncer attached again", flush=True)
        done, pong, events = ping(run, "C4", port_s, "user", hub)
        check(pong is not None, "the hub is reachable after the bouncer restarted")

        print("shared: rnsd S restarts", flush=True)
        run.stop("S")
        restarted = time.time()
        run.rnsd("S", port_s, shared=instance)
        time.sleep(20)
        after = [e for e in run.events("W2") if e["at"] >= restarted]
        leaked = hub_announces(after, hub, False)
        found["announces_after_restart"] = after
        if leaked:
            print("  known  after the shared instance came back, the hub was announced to the network", flush=True)
        else:
            print("  ok  no announce of the hub after the shared instance came back", flush=True)
        done, pong, events = ping(run, "C3", port_s, "user", hub)
        check(pong is not None, "the hub is reachable again after rnsd S restarted")
        check(run.stop("BS") == 0, "the bouncer stops cleanly")
        run.stop("S")

        rrcd_phases(run, args, {port_t, port_s, port_d, port_l}, found)
    except Done:
        pass
    except Failure as e:
        print(f"FAIL  {e}", flush=True)
        print(f"logs: {args.work}", flush=True)
        return 1
    finally:
        run.stop_all()
        with open(run.path("found.json"), "w") as f:
            json.dump(found, f, indent=1, default=lambda v: v.hex() if isinstance(v, bytes) else repr(v))
    print(f"passed (logs: {args.work})", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
