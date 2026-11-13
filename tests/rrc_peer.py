# SPDX-License-Identifier: LicenseRef-Reticulum
"""A Python Reticulum peer for tests/e2e.py.

It watches `rrc.hub` announces (path responses included) and, with --ping,
asks for a path to a hub, links to it, identifies, sends one PING right
after the identify (no pause, no retry) and waits for the PONG. Everything
it sees goes to --events as JSON lines.
"""

import argparse
import json
import os
import socket
import sys
import threading
import time

import cbor2
import RNS

K_V, K_T, K_ID, K_TS, K_SRC, K_BODY = 0, 1, 2, 3, 4, 6
T_PING, T_PONG = 30, 31

lock = threading.Lock()


def emit(path, **event):
    event["at"] = time.time()
    with lock, open(path, "a") as f:
        f.write(json.dumps(event) + "\n")


class Watcher:
    aspect_filter = "rrc.hub"
    receive_path_responses = True

    def __init__(self, events):
        self.events = events

    def received_announce(self, destination_hash, announced_identity, app_data, announce_packet_hash, is_path_response):
        emit(self.events, event="announce", dest=destination_hash.hex(), path_response=bool(is_path_response),
             app_data=app_data.hex() if app_data else None)


def ping(args, identity):
    hub = bytes.fromhex(args.ping)
    start = time.time()
    if not RNS.Transport.has_path(hub):
        RNS.Transport.request_path(hub)
    while not RNS.Transport.has_path(hub):
        if time.time() - start > args.timeout:
            emit(args.events, event="no_path")
            return 1
        time.sleep(0.2)
    emit(args.events, event="path", hops=RNS.Transport.hops_to(hub))
    hub_identity = RNS.Identity.recall(hub)
    destination = RNS.Destination(hub_identity, RNS.Destination.OUT, RNS.Destination.SINGLE, "rrc", "hub")
    established = threading.Event()
    pongs = []

    def on_packet(data, packet):
        try:
            env = cbor2.loads(data)
        except Exception as e:
            emit(args.events, event="bad_packet", error=str(e))
            return
        emit(args.events, event="packet", t=env.get(K_T), src=env.get(K_SRC, b"").hex(), body=env.get(K_BODY))
        if env.get(K_T) == T_PONG:
            pongs.append(env)

    link = RNS.Link(destination, established_callback=lambda _l: established.set())
    link.set_packet_callback(on_packet)
    if not established.wait(args.timeout):
        emit(args.events, event="no_link")
        return 1
    emit(args.events, event="link")
    # The PING goes out right behind the identify, as an RRC client's HELLO
    # does, in the same TCP segment (TCP_CORK), so the hub receives both at
    # once; it must not handle the PING before it knows the link's identity.
    body = 12345.678901
    env = {K_V: 1, K_T: T_PING, K_ID: os.urandom(8), K_TS: int(time.time() * 1000),
           K_SRC: identity.hash, K_BODY: body}
    sock = getattr(link.attached_interface, "socket", None)
    cork = sock is not None and hasattr(socket, "TCP_CORK")
    if cork:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)
    link.identify(identity)
    RNS.Packet(link, cbor2.dumps(env)).send()
    if cork:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 0)
    while time.time() - start < args.timeout and not pongs:
        time.sleep(0.1)
    if pongs:
        pong = pongs[0]
        emit(args.events, event="pong", body_matches=pong.get(K_BODY) == body,
             src_is_hub=pong.get(K_SRC) == hub_identity.hash)
    else:
        emit(args.events, event="no_pong")
    link.teardown()
    return 0 if pongs else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rnsconfig", required=True)
    parser.add_argument("--identity", required=True)
    parser.add_argument("--events", required=True)
    parser.add_argument("--ping")
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()

    RNS.Reticulum(args.rnsconfig)
    identity = RNS.Identity.from_file(args.identity)
    RNS.Transport.register_announce_handler(Watcher(args.events))
    emit(args.events, event="ready")
    if args.ping:
        code = ping(args, identity)
        emit(args.events, event="done", code=code)
    while True:
        time.sleep(1)


if __name__ == "__main__":
    sys.exit(main())
