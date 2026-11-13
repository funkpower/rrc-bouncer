#!/usr/bin/env python3
# SPDX-License-Identifier: LicenseRef-Reticulum
"""Write the envelope fixtures in envelopes/ with rrcd's own envelope code.

Run it with a Python that has rrcd installed (pip install from
https://github.com/kc1awv/rrcd); the tests use the committed bytes, so
`cargo test` needs no Python:

    python3 tests/fixtures/make_envelopes.py

Every value is fixed, so running it again gives the same bytes.
"""

from pathlib import Path

import rrcd
from rrcd.codec import encode
from rrcd.constants import (
    B_RES_ENCODING,
    B_RES_ID,
    B_RES_KIND,
    B_RES_SHA256,
    B_RES_SIZE,
    T_ACTION,
    T_ERROR,
    T_HELLO,
    T_JOIN,
    T_JOINED,
    T_MSG,
    T_NOTICE,
    T_PART,
    T_PARTED,
    T_PING,
    T_PONG,
    T_RESOURCE_ENVELOPE,
    T_WELCOME,
)
from rrcd.envelope import make_envelope, validate_envelope

K_FMT = 64

USER = bytes.fromhex("00112233445566778899aabbccddeeff")
HUB = bytes.fromhex("ffeeddccbbaa99887766554433221100")
OTHER = bytes.fromhex("0123456789abcdef0123456789abcdef")
TS = 1_763_000_000_000


def mid(n: int) -> bytes:
    return bytes([n]) * 8


def envelopes() -> dict[str, dict]:
    welcome_body = {
        0: "test hub",
        1: str(rrcd.__version__),
        2: {1: True, 2: True, 0: True},
        3: {0: 32, 1: 64, 2: 350, 3: 32, 4: 240},
    }
    resource_body = {
        B_RES_ID: bytes(range(16)),
        B_RES_KIND: "motd",
        B_RES_SIZE: 1200,
        B_RES_SHA256: bytes(range(32)),
        B_RES_ENCODING: "utf-8",
    }
    fmt = make_envelope(T_MSG, src=USER, room="lobby", body="red text", nick="alice", mid=mid(9), ts=TS + 9)
    fmt[K_FMT] = [{0: 0, 1: 3, 2: "#ff0000"}, {0: 4, 1: 8, 2: "#00ff00", 3: True}]
    unknown_type = make_envelope(99, src=USER, body={0: "future"}, mid=mid(20), ts=TS + 20)
    unknown_type[70] = b"\x01\x02"
    unknown_type[65] = -3
    return {
        "hello": make_envelope(
            T_HELLO,
            src=USER,
            body={0: "weechat-rrc", 1: "0.1.0", 2: {0: True, 1: True, 2: True}},
            nick="alice",
            mid=mid(1),
            ts=TS + 1,
        ),
        "hello_legacy_nick": make_envelope(
            T_HELLO, src=USER, body={0: "old-client", 1: 1, 64: "bob"}, mid=mid(2), ts=TS + 2
        ),
        "welcome": make_envelope(T_WELCOME, src=HUB, body=welcome_body, mid=mid(3), ts=TS + 3),
        "join": make_envelope(T_JOIN, src=USER, room="lobby", nick="alice", mid=mid(4), ts=TS + 4),
        "joined": make_envelope(T_JOINED, src=HUB, room="lobby", body=[USER, OTHER], mid=mid(5), ts=TS + 5),
        "joined_no_body": make_envelope(T_JOINED, src=HUB, room="lobby", nick="carol", mid=mid(6), ts=TS + 6),
        "part": make_envelope(T_PART, src=USER, room="lobby", mid=mid(7), ts=TS + 7),
        "parted": make_envelope(T_PARTED, src=HUB, room="lobby", body=[OTHER], mid=mid(8), ts=TS + 8),
        "msg": make_envelope(T_MSG, src=USER, room="lobby", body="hello, world ✓", nick="alice", mid=mid(10), ts=TS + 10),
        "msg_fmt": fmt,
        "notice": make_envelope(T_NOTICE, src=HUB, room="lobby", body="welcome to lobby", mid=mid(11), ts=TS + 11),
        "notice_direct": make_envelope(
            T_NOTICE, src=OTHER, dst=USER, body="psst", nick="carol", mid=mid(12), ts=TS + 12
        ),
        "action": make_envelope(T_ACTION, src=USER, room="lobby", body="waves", nick="alice", mid=mid(13), ts=TS + 13),
        "ping": make_envelope(T_PING, src=USER, body=12345.678901, mid=mid(14), ts=TS + 14),
        "ping_short_float": make_envelope(T_PING, src=USER, body=1.5, mid=mid(15), ts=TS + 15),
        "pong": make_envelope(T_PONG, src=HUB, body=12345.678901, mid=mid(16), ts=TS + 16),
        "error": make_envelope(T_ERROR, src=HUB, room="lobby", body="not in room", mid=mid(17), ts=TS + 17),
        "resource_envelope": make_envelope(
            T_RESOURCE_ENVELOPE, src=HUB, body=resource_body, mid=mid(18), ts=TS + 18
        ),
        "unknown_type": unknown_type,
    }


def main() -> None:
    out = Path(__file__).resolve().parent / "envelopes"
    out.mkdir(exist_ok=True)
    for name, env in envelopes().items():
        validate_envelope(env)
        (out / f"{name}.cbor").write_bytes(encode(env))


if __name__ == "__main__":
    main()
