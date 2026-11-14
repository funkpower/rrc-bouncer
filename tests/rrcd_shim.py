# SPDX-License-Identifier: LicenseRef-Reticulum
"""rrcd for tests/e2e.py, with its taking of a client's long NOTICE
mended. Takes rrcd's arguments.

rrcd 0.3.2 fails a client's Resource twice over:

- It decides on the advertisement by `resource.total_size` or
  `resource.size` (resources.py:253), but RNS 1.6 hands it a
  ResourceAdvertisement, which has neither: the callback raises, RNS neither
  accepts nor rejects, and the sender's advertisement times out. Here the
  advertisement gets `total_size`, its data size.
- It forwards the NOTICE it received to the room with `link.packet(payload)`
  (resources.py:420), and an RNS.Link's `packet` is the packet that opened
  it, or None: every forward fails and the room sees nothing. Here the
  NOTICE goes to the room's other members as rrcd meant it to. One that fits
  a packet goes as one; a longer one goes as rrcd sends its own long
  greeting (resources.py:500-540): a RESOURCE_ENVELOPE, from the sender and
  in the room, then the Resource.
"""

import hashlib
import os
import sys
import time

import cbor2
import RNS
from rrcd.cli import main
from rrcd.resources import ResourceManager

K_V, K_T, K_ID, K_TS, K_SRC, K_ROOM, K_BODY = 0, 1, 2, 3, 4, 5, 6
T_NOTICE, T_RESOURCE_ENVELOPE = 21, 50


def forward(link, payload):
    if len(payload) <= RNS.Link.MDU:
        RNS.Packet(link, payload).send()
        return
    notice = cbor2.loads(payload)
    data = notice[K_BODY].encode("utf-8")
    envelope = dict(notice)
    envelope[1] = T_RESOURCE_ENVELOPE
    envelope[2] = os.urandom(8)
    envelope[K_BODY] = {0: os.urandom(8), 1: "notice", 2: len(data), 3: hashlib.sha256(data).digest(), 4: "utf-8"}
    RNS.Packet(link, cbor2.dumps(envelope)).send()
    RNS.Resource(data, link, advertise=True, auto_compress=False)


dispatch = ResourceManager._dispatch_received_resource


def dispatch_notice(self, link, exp, payload):
    if exp.kind != "notice" or not exp.room:
        return dispatch(self, link, exp, payload)
    text = payload.decode(exp.encoding or "utf-8")
    self.log.info("Received large NOTICE via resource room=%r chars=%s", exp.room, len(text))
    with self.hub._state_lock:
        sess = self.hub.session_manager.sessions.get(link)
        peer = sess.get("peer") if sess else None
        members = set(self.hub.room_manager.get_room_members(exp.room))
    if peer is None:
        return
    notice = cbor2.dumps({K_V: 1, K_T: T_NOTICE, K_ID: os.urandom(8), K_TS: int(time.time() * 1000), K_SRC: peer,
                          K_ROOM: exp.room, K_BODY: text})
    for other in members - {link}:
        forward(other, notice)
    self.log.info("Forwarded NOTICE resource to %d members room=%s", len(members - {link}), exp.room)


ResourceManager._dispatch_received_resource = dispatch_notice
RNS.ResourceAdvertisement.total_size = property(lambda advertisement: advertisement.d)

if __name__ == "__main__":
    sys.argv[0] = "rrcd"
    sys.exit(main())
