# SPDX-License-Identifier: LicenseRef-Reticulum
"""A Python Reticulum peer for tests/e2e.py.

It watches `rrc.hub` announces (path responses included) and, with --ping,
asks for a path to a hub, links to it, identifies, sends one PING right
after the identify (no pause, no retry) and waits for the PONG.

With --room-client it is an RRC client of a hub: it links, identifies,
says HELLO, joins --room, answers PING, and links again whenever the link
closes. Everything it sees goes to --events as JSON lines.
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

K_V, K_T, K_ID, K_TS, K_SRC, K_ROOM, K_BODY, K_NICK = 0, 1, 2, 3, 4, 5, 6, 7
T_HELLO, T_WELCOME, T_JOIN = 1, 2, 10
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


def plain(value):
    """An envelope value as JSON can hold it: bytes as hex."""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, list):
        return [plain(v) for v in value]
    if isinstance(value, dict):
        return {str(k): plain(v) for k, v in value.items()}
    return value


def envelope(identity, t, room=None, body=None, nick=None):
    env = {K_V: 1, K_T: t, K_ID: os.urandom(8), K_TS: int(time.time() * 1000), K_SRC: identity.hash}
    if room is not None:
        env[K_ROOM] = room
    if body is not None:
        env[K_BODY] = body
    if nick is not None:
        env[K_NICK] = nick
    return env


def room_session(args, identity, hub, session):
    """One link to the hub, until it closes."""
    if not RNS.Transport.has_path(hub):
        RNS.Transport.request_path(hub)
        end = time.time() + 10
        while not RNS.Transport.has_path(hub) and time.time() < end:
            time.sleep(0.2)
        if not RNS.Transport.has_path(hub):
            return False
    hub_identity = RNS.Identity.recall(hub)
    destination = RNS.Destination(hub_identity, RNS.Destination.OUT, RNS.Destination.SINGLE, "rrc", "hub")
    established, closed = threading.Event(), threading.Event()
    heard = [time.time()]

    def send(env):
        RNS.Packet(link, cbor2.dumps(env)).send()

    def on_packet(data, packet):
        try:
            env = cbor2.loads(data)
        except Exception as e:
            emit(args.events, event="bad_packet", session=session, error=str(e))
            return
        heard[0] = time.time()
        t = env.get(K_T)
        emit(args.events, event="env", session=session, t=t, room=env.get(K_ROOM), nick=env.get(K_NICK),
             src=plain(env.get(K_SRC)), body=plain(env.get(K_BODY)))
        if t == T_WELCOME:
            send(envelope(identity, T_JOIN, room=args.room, nick=args.nick))
        elif t == T_PING:
            send(envelope(identity, T_PONG, body=env.get(K_BODY)))

    link = RNS.Link(destination, established_callback=lambda _l: established.set(),
                    closed_callback=lambda _l: closed.set())
    link.set_packet_callback(on_packet)
    if not established.wait(10):
        link.teardown()
        return False
    emit(args.events, event="link", session=session)
    link.identify(identity)
    time.sleep(0.5)
    send(envelope(identity, T_HELLO, body={0: "e2e-client", 1: 1, 2: {}}, nick=args.nick))
    heard[0] = time.time()
    # A hub that was killed closes nothing: give up on a link the hub's own
    # PINGs stopped coming over.
    while not closed.wait(0.5):
        if time.time() - heard[0] > args.idle:
            emit(args.events, event="idle", session=session)
            link.teardown()
            closed.wait(5)
            break
    emit(args.events, event="closed", session=session)
    return True


def room_client(args, identity):
    hub = bytes.fromhex(args.room_client)
    session = 0
    while True:
        if room_session(args, identity, hub, session + 1):
            session += 1
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rnsconfig", required=True)
    parser.add_argument("--identity", required=True)
    parser.add_argument("--events", required=True)
    parser.add_argument("--ping")
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--room-client")
    parser.add_argument("--room", default="lobby")
    parser.add_argument("--nick", default="client")
    parser.add_argument("--idle", type=float, default=15)
    args = parser.parse_args()

    RNS.Reticulum(args.rnsconfig)
    identity = RNS.Identity.from_file(args.identity)
    RNS.Transport.register_announce_handler(Watcher(args.events))
    emit(args.events, event="ready")
    if args.ping:
        code = ping(args, identity)
        emit(args.events, event="done", code=code)
    if args.room_client:
        room_client(args, identity)
    while True:
        time.sleep(1)


if __name__ == "__main__":
    sys.exit(main())
