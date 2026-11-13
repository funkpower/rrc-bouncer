# SPDX-License-Identifier: LicenseRef-Reticulum
"""rnsd with the linger time of never-used paths set to RRC_TEST_PATH_LINGER
seconds (normally 1.75 hours with transport, 12 minutes without), for
tests/e2e.py. Takes rnsd's arguments."""

import os
import sys

import RNS
from RNS.Utilities.rnsd import main

linger = float(os.environ["RRC_TEST_PATH_LINGER"])
# Read once when Transport starts (Transport.py:451-455).
RNS.Transport.UNUSED_PATH_LINGER = linger
RNS.Transport.UNUSED_PATH_LINGER_LEAF = linger

if __name__ == "__main__":
    sys.argv[0] = "rnsd"
    sys.exit(main())
