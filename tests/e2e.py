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
"""

import argparse
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
                return [json.loads(line) for line in f if line.strip()]
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
        if not self.wait_event(name, lambda e: e["event"] == "ready", 60):
            raise Failure(f"{name} did not start")

    def config(self, name, reticulum, announce=False, upstream=UPSTREAM, nick=None):
        path = self.path(f"{name}.toml")
        with open(path, "w") as f:
            f.write(f'identity = "ids/user"\nstate_dir = "state"\nlog_level = "debug"\n'
                    f"announce = {'true' if announce else 'false'}\n"
                    f'allowed_identities = []\n\n[reticulum]\n{reticulum}\n\n'
                    f'[[hub]]\naddress = "{upstream}"\nrooms = ["lobby"]\n')
            if nick:
                f.write(f'nick = "{nick}"\n')
        return path

    def rrcd(self, binary, port):
        """Start rrcd H on rnsd R; every file it uses is under the work directory."""
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
                    f'hub_name = "{HUB_NAME}"\ngreeting = "e2e"\ninclude_joined_member_list = true\n'
                    f"max_rooms_per_session = 2\nrate_limit_msgs_per_minute = 120\n"
                    f"ping_interval_s = {RRCD_PING}\nping_timeout_s = {RRCD_TIMEOUT}\n\n"
                    f'[logging]\nlevel = "DEBUG"\nconsole = true\nfile = ""\n')
        self.start("H", [binary, "--config", config, "--identity", self.identity("rrcd-hub"),
                         "--room-registry", registry, "--configdir", rns, "--log-level", "DEBUG"])
        if not self.wait_log("H", "Hub running at", 30):
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
    check(run.stop("BR") == 0, "the bouncer stops cleanly")
    run.stop("RC")
    run.stop("H")
    run.stop("R")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--work", required=True)
    parser.add_argument("--python", required=True)
    parser.add_argument("--rnsd", required=True)
    parser.add_argument("--rrcd", required=True)
    parser.add_argument("--bouncer", required=True)
    parser.add_argument("--only", choices=["rrcd"])
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
            json.dump(found, f, indent=1)
    print(f"passed (logs: {args.work})", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
