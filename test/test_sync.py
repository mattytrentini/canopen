import asyncio
import unittest

import canopen

from .util import FakeBus


PERIOD = 0.01
TIMEOUT = PERIOD * 10


def run(coro):
    return asyncio.run(coro)


class TestSync(unittest.TestCase):
    def setUp(self):
        self.net = canopen.Network()
        self.bus = FakeBus()
        self.sync = canopen.sync.SyncProducer(self.net)

    def test_sync_producer_transmit(self):
        run(self._test_sync_producer_transmit())

    async def _test_sync_producer_transmit(self):
        self.net.connect(self.bus)
        try:
            self.sync.transmit()
            await asyncio.sleep(TIMEOUT)
        finally:
            await self.net.disconnect()
        self.assertEqual(self.bus.sent, [(0x80, b"")])

    def test_sync_producer_transmit_count(self):
        run(self._test_sync_producer_transmit_count())

    async def _test_sync_producer_transmit_count(self):
        self.net.connect(self.bus)
        try:
            self.sync.transmit(2)
            await asyncio.sleep(TIMEOUT)
        finally:
            await self.net.disconnect()
        self.assertEqual(self.bus.sent, [(0x80, b"\x02")])

    def test_sync_producer_start_invalid_period(self):
        with self.assertRaises(ValueError):
            self.sync.start(0)

    def test_sync_producer_start(self):
        run(self._test_sync_producer_start())

    async def _test_sync_producer_start(self):
        # Periodic frames are delivered via the subscriber-queue path (as
        # real aiocan does), not the send() log, so subscribe to observe them.
        received = []
        self.net.subscribe(0x80, lambda can_id, data, ts: received.append(data))
        self.net.connect(self.bus)
        try:
            self.sync.start(PERIOD)
            # Sample messages for a handful of periods.
            await asyncio.sleep(PERIOD * 5)
            self.sync.stop()
        finally:
            await self.net.disconnect()

        self.assertGreaterEqual(len(received), 2)
        for data in received:
            self.assertEqual(data, b"")

    def test_sync_producer_restart(self):
        run(self._test_sync_producer_restart())

    async def _test_sync_producer_restart(self):
        self.net.connect(self.bus)
        try:
            self.sync.start(PERIOD)
            # Cannot start again while running
            with self.assertRaises(RuntimeError):
                self.sync.start(PERIOD)
            # Can restart after stopping
            self.sync.stop()
            self.sync.start(PERIOD)
            self.sync.stop()
        finally:
            await self.net.disconnect()


if __name__ == "__main__":
    unittest.main()
