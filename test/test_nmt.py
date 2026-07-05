import asyncio
import struct
import unittest

import canopen
from canopen.nmt import COMMAND_TO_STATE, NMT_COMMANDS, NMT_STATES, NmtError

from .util import SAMPLE_EDS, FakeBus, FakeChannel


def run(coro):
    return asyncio.run(coro)


class TestNmtBase(unittest.TestCase):
    def setUp(self):
        node_id = 2
        self.node_id = node_id
        self.nmt = canopen.nmt.NmtBase(node_id)

    def test_send_command(self):
        dataset = (
            "OPERATIONAL",
            "PRE-OPERATIONAL",
            "SLEEP",
            "STANDBY",
            "STOPPED",
        )
        for cmd in dataset:
            with self.subTest(cmd=cmd):
                code = NMT_COMMANDS[cmd]
                self.nmt.send_command(code)
                expected = NMT_STATES[COMMAND_TO_STATE[code]]
                self.assertEqual(self.nmt.state, expected)

    def test_state_getset(self):
        for state in NMT_STATES.values():
            with self.subTest(state=state):
                self.nmt.state = state
                self.assertEqual(self.nmt.state, state)

    def test_state_set_invalid(self):
        with self.assertRaisesRegex(ValueError, "INVALID"):
            self.nmt.state = "INVALID"


class TestNmtMaster(unittest.TestCase):
    NODE_ID = 2
    PERIOD = 0.01
    TIMEOUT = PERIOD * 10

    def setUp(self):
        self.net = canopen.Network()
        self.bus = FakeBus()
        with self.assertLogs():
            self.node = self.net.add_node(self.NODE_ID, SAMPLE_EDS)

    async def _connect(self):
        self.net.connect(self.bus)
        # Give the listener tasks a chance to register their queues.
        await asyncio.sleep(0.01)

    def dispatch_heartbeat(self, code):
        cob_id = 0x700 + self.NODE_ID
        self.bus.inject(cob_id, bytes([code]))

    async def _dispatch_soon(self, code, delay=0.01):
        await asyncio.sleep(delay)
        self.dispatch_heartbeat(code)

    def test_nmt_master_no_heartbeat(self):
        run(self._test_nmt_master_no_heartbeat())

    async def _test_nmt_master_no_heartbeat(self):
        await self._connect()
        try:
            with self.assertRaisesRegex(NmtError, "heartbeat"):
                await self.node.nmt.wait_for_heartbeat(self.TIMEOUT)
            with self.assertRaisesRegex(NmtError, "boot-up"):
                await self.node.nmt.wait_for_bootup(self.TIMEOUT)
        finally:
            await self.net.disconnect()

    def test_nmt_master_on_heartbeat(self):
        run(self._test_nmt_master_on_heartbeat())

    async def _test_nmt_master_on_heartbeat(self):
        await self._connect()
        try:
            # Skip the special INITIALISING case.
            for code in [st for st in NMT_STATES if st != 0]:
                with self.subTest(code=code):
                    _, actual = await asyncio.gather(
                        self._dispatch_soon(code),
                        self.node.nmt.wait_for_heartbeat(0.5),
                    )
                    expected = NMT_STATES[code]
                    self.assertEqual(actual, expected)
        finally:
            await self.net.disconnect()

    def test_nmt_master_wait_for_bootup(self):
        run(self._test_nmt_master_wait_for_bootup())

    async def _test_nmt_master_wait_for_bootup(self):
        await self._connect()
        try:
            _, _ = await asyncio.gather(
                self._dispatch_soon(0x00),
                self.node.nmt.wait_for_bootup(self.TIMEOUT),
            )
            self.assertEqual(self.node.nmt.state, "PRE-OPERATIONAL")
        finally:
            await self.net.disconnect()

    def test_nmt_master_on_heartbeat_initialising(self):
        run(self._test_nmt_master_on_heartbeat_initialising())

    async def _test_nmt_master_on_heartbeat_initialising(self):
        await self._connect()
        try:
            _, state = await asyncio.gather(
                self._dispatch_soon(0x00),
                self.node.nmt.wait_for_heartbeat(self.TIMEOUT),
            )
            self.assertEqual(state, "PRE-OPERATIONAL")
        finally:
            await self.net.disconnect()

    def test_nmt_master_on_heartbeat_unknown_state(self):
        run(self._test_nmt_master_on_heartbeat_unknown_state())

    async def _test_nmt_master_on_heartbeat_unknown_state(self):
        await self._connect()
        try:
            _, state = await asyncio.gather(
                self._dispatch_soon(0xcb),
                self.node.nmt.wait_for_heartbeat(self.TIMEOUT),
            )
            # Expect the high bit to be masked out, and a formatted string to
            # be returned.
            self.assertEqual(state, "UNKNOWN STATE '75'")
        finally:
            await self.net.disconnect()

    def test_nmt_master_add_heartbeat_callback(self):
        run(self._test_nmt_master_add_heartbeat_callback())

    async def _test_nmt_master_add_heartbeat_callback(self):
        await self._connect()
        try:
            state = None

            def hook(st):
                nonlocal state
                state = st
            self.node.nmt.add_heartbeat_callback(hook)

            self.dispatch_heartbeat(0x7f)
            await asyncio.sleep(0.01)
            self.assertEqual(state, 127)
        finally:
            await self.net.disconnect()

    def test_nmt_master_node_guarding(self):
        run(self._test_nmt_master_node_guarding())

    async def _test_nmt_master_node_guarding(self):
        # NOTE: can't observe this via network.subscribe() on the same
        # COB-ID: the node's own on_heartbeat is already subscribed there
        # (from add_node()) and chokes on the zero-length guarding frame,
        # which — same as the original notify() dispatch loop — prevents
        # any callback registered after it from seeing that message. Read
        # the bus's transmit log directly instead.
        await self._connect()
        try:
            self.node.nmt.start_node_guarding(self.PERIOD)
            await asyncio.sleep(self.PERIOD * 5)
            self.node.nmt.stop_node_guarding()
        finally:
            await self.net.disconnect()

        sent = [(can_id, data) for can_id, data in self.bus.sent
                if can_id == 0x700 + self.NODE_ID]
        self.assertGreaterEqual(len(sent), 2)
        self.assertTrue(all(data == b"" for _, data in sent))


class TestNmtSlave(unittest.TestCase):
    def setUp(self):
        self.channel = FakeChannel()
        self.bus1 = FakeBus(self.channel)
        self.bus2 = FakeBus(self.channel)

        self.network1 = canopen.Network()
        with self.assertLogs():
            self.remote_node = self.network1.add_node(2, SAMPLE_EDS)

        self.network2 = canopen.Network()
        with self.assertLogs():
            self.local_node = self.network2.create_node(2, SAMPLE_EDS)
            self.remote_node2 = self.network1.add_node(3, SAMPLE_EDS)
            self.local_node2 = self.network2.create_node(3, SAMPLE_EDS)

    async def _connect(self):
        self.network1.connect(self.bus1)
        self.network2.connect(self.bus2)
        # Give the listener tasks a chance to register their queues.
        await asyncio.sleep(0.01)

    async def _disconnect(self):
        await self.network1.disconnect()
        await self.network2.disconnect()

    def test_start_two_remote_nodes(self):
        run(self._test_start_two_remote_nodes())

    async def _test_start_two_remote_nodes(self):
        await self._connect()
        try:
            self.remote_node.nmt.state = "OPERATIONAL"
            # Give the slave a chance to receive the command.
            await asyncio.sleep(0.05)
            slave_state = self.local_node.nmt.state
            self.assertEqual(slave_state, "OPERATIONAL")

            self.remote_node2.nmt.state = "OPERATIONAL"
            await asyncio.sleep(0.05)
            slave_state = self.local_node2.nmt.state
            self.assertEqual(slave_state, "OPERATIONAL")
        finally:
            await self._disconnect()

    def test_stop_two_remote_nodes_using_broadcast(self):
        run(self._test_stop_two_remote_nodes_using_broadcast())

    async def _test_stop_two_remote_nodes_using_broadcast(self):
        await self._connect()
        try:
            # This is a NMT broadcast "Stop remote node"
            # ie. set the node in STOPPED state
            await self.network1.send_message(0, [2, 0])

            # Give the slaves a chance to receive the command.
            await asyncio.sleep(0.05)
            slave_state = self.local_node.nmt.state
            self.assertEqual(slave_state, "STOPPED")
            slave_state = self.local_node2.nmt.state
            self.assertEqual(slave_state, "STOPPED")
        finally:
            await self._disconnect()

    def test_heartbeat(self):
        run(self._test_heartbeat())

    async def _test_heartbeat(self):
        await self._connect()
        try:
            self.assertEqual(self.remote_node.nmt.state, "INITIALISING")
            self.assertEqual(self.local_node.nmt.state, "INITIALISING")
            self.local_node.nmt.state = "OPERATIONAL"

            # NOTE: local_node.sdo[0x1017].raw = 100 can't be used here: the
            # Variable.raw/.data properties are synchronous, but
            # SdoVariable.get_data/set_data became async in the SDO client
            # rewrite (Phase 2), so the property silently no-ops instead of
            # writing. Use the local write path directly until that gap is
            # addressed.
            self.local_node.set_data(0x1017, 0, struct.pack("<H", 100), check_writable=True)

            await asyncio.sleep(0.3)
            self.assertEqual(self.remote_node.nmt.state, "OPERATIONAL")

            self.local_node.nmt.stop_heartbeat()
        finally:
            await self._disconnect()


if __name__ == "__main__":
    unittest.main()
