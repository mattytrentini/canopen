import asyncio
import unittest

import canopen

from .util import SAMPLE_EDS, FakeBus


def run(coro):
    return asyncio.run(coro)


class TestNetwork(unittest.TestCase):

    def setUp(self):
        self.network = canopen.Network()

    def test_network_add_node(self):
        # Add using str.
        with self.assertLogs():
            node = self.network.add_node(2, SAMPLE_EDS)
        self.assertEqual(self.network[2], node)
        self.assertEqual(node.id, 2)
        self.assertIsInstance(node, canopen.RemoteNode)

        # Add using OD.
        node = self.network.add_node(3, self.network[2].object_dictionary)
        self.assertEqual(self.network[3], node)
        self.assertEqual(node.id, 3)
        self.assertIsInstance(node, canopen.RemoteNode)

        # Add using RemoteNode.
        with self.assertLogs():
            node = canopen.RemoteNode(4, SAMPLE_EDS)
        self.network.add_node(node)
        self.assertEqual(self.network[4], node)
        self.assertEqual(node.id, 4)
        self.assertIsInstance(node, canopen.RemoteNode)

        # Add using LocalNode.
        with self.assertLogs():
            node = canopen.LocalNode(5, SAMPLE_EDS)
        self.network.add_node(node)
        self.assertEqual(self.network[5], node)
        self.assertEqual(node.id, 5)
        self.assertIsInstance(node, canopen.LocalNode)

        # Verify that we've got the correct number of nodes.
        self.assertEqual(len(self.network), 4)

    def test_network_create_node(self):
        with self.assertLogs():
            self.network.create_node(2, SAMPLE_EDS)
            self.network.create_node(3, SAMPLE_EDS)
            node = canopen.RemoteNode(4, SAMPLE_EDS)
            self.network.create_node(node)
        self.assertIsInstance(self.network[2], canopen.LocalNode)
        self.assertIsInstance(self.network[3], canopen.LocalNode)
        self.assertIsInstance(self.network[4], canopen.RemoteNode)

    def test_network_notify(self):
        with self.assertLogs():
            self.network.add_node(2, SAMPLE_EDS)
        node = self.network[2]
        self.network.notify(0x82, b'\x01\x20\x02\x00\x01\x02\x03\x04', 1473418396.0)
        self.assertEqual(len(node.emcy.active), 1)
        self.network.notify(0x702, b'\x05', 1473418396.0)
        self.assertEqual(node.nmt.state, 'OPERATIONAL')
        self.assertListEqual(self.network.scanner.nodes, [2])

    def test_network_subscribe_unsubscribe(self):
        N_HOOKS = 3
        accumulators = [] * N_HOOKS

        for i in range(N_HOOKS):
            accumulators.append([])
            def hook(*args, i=i):
                accumulators[i].append(args)
            self.network.subscribe(i, hook)

        self.network.notify(0, bytes([1, 2, 3]), 1000)
        self.network.notify(1, bytes([2, 3, 4]), 1001)
        self.network.notify(1, bytes([3, 4, 5]), 1002)
        self.network.notify(2, bytes([4, 5, 6]), 1003)

        self.assertEqual(accumulators[0], [(0, bytes([1, 2, 3]), 1000)])
        self.assertEqual(accumulators[1], [
            (1, bytes([2, 3, 4]), 1001),
            (1, bytes([3, 4, 5]), 1002),
        ])
        self.assertEqual(accumulators[2], [(2, bytes([4, 5, 6]), 1003)])

        self.network.unsubscribe(0)
        self.network.notify(0, bytes([7, 7, 7]), 1004)
        # Verify that no new data was added to the accumulator.
        self.assertEqual(accumulators[0], [(0, bytes([1, 2, 3]), 1000)])

    def test_network_subscribe_multiple(self):
        N_HOOKS = 3

        accumulators = []
        hooks = []
        for i in range(N_HOOKS):
            accumulators.append([])
            def hook(*args, i=i):
                accumulators[i].append(args)
            hooks.append(hook)
            self.network.subscribe(0x20, hook)

        self.network.notify(0xaa, bytes([1, 1, 1]), 2000)
        self.network.notify(0x20, bytes([2, 3, 4]), 2001)
        self.network.notify(0xbb, bytes([2, 2, 2]), 2002)
        self.network.notify(0x20, bytes([3, 4, 5]), 2003)
        self.network.notify(0xcc, bytes([3, 3, 3]), 2004)

        BATCH1 = [
            (0x20, bytes([2, 3, 4]), 2001),
            (0x20, bytes([3, 4, 5]), 2003),
        ]
        for n, acc in enumerate(accumulators):
            with self.subTest(hook=n):
                self.assertEqual(acc, BATCH1)

        # Unsubscribe the second hook; dispatch a new message.
        self.network.unsubscribe(0x20, hooks[1])

        BATCH2 = 0x20, bytes([4, 5, 6]), 2005
        self.network.notify(*BATCH2)
        self.assertEqual(accumulators[0], BATCH1 + [BATCH2])
        self.assertEqual(accumulators[1], BATCH1)
        self.assertEqual(accumulators[2], BATCH1 + [BATCH2])

        # Unsubscribe the first hook; dispatch yet another message.
        self.network.unsubscribe(0x20, hooks[0])

        BATCH3 = 0x20, bytes([5, 6, 7]), 2006
        self.network.notify(*BATCH3)
        self.assertEqual(accumulators[0], BATCH1 + [BATCH2])
        self.assertEqual(accumulators[1], BATCH1)
        self.assertEqual(accumulators[2], BATCH1 + [BATCH2] + [BATCH3])

        # Unsubscribe the rest (only one remaining); dispatch a new message.
        self.network.unsubscribe(0x20)
        self.network.notify(0x20, bytes([7, 7, 7]), 2007)
        self.assertEqual(accumulators[0], BATCH1 + [BATCH2])
        self.assertEqual(accumulators[1], BATCH1)
        self.assertEqual(accumulators[2], BATCH1 + [BATCH2] + [BATCH3])

    def test_network_item_access(self):
        with self.assertLogs():
            self.network.add_node(2, SAMPLE_EDS)
            self.network.add_node(3, SAMPLE_EDS)
        self.assertEqual([2, 3], [node for node in self.network])

        # Check __delitem__.
        del self.network[2]
        self.assertEqual([3], [node for node in self.network])
        with self.assertRaises(KeyError):
            del self.network[2]

        # Check __setitem__.
        old = self.network[3]
        with self.assertLogs():
            new = canopen.Node(3, SAMPLE_EDS)
        self.network[3] = new

        # Check __getitem__.
        self.assertNotEqual(self.network[3], old)
        self.assertEqual([3], [node for node in self.network])


class TestNetworkFakeBus(unittest.TestCase):
    """Integration tests exercising Network's asyncio listener tasks against
    a FakeBus test double, standing in for aiocan.Bus."""

    async def _settle(self):
        # Give newly created listener tasks a chance to register their
        # subscription queues before we inject/send anything.
        await asyncio.sleep(0.01)

    def test_send_message_reaches_bus(self):
        run(self._send_message_reaches_bus())

    async def _send_message_reaches_bus(self):
        network = canopen.Network()
        bus = FakeBus()
        network.connect(bus)
        try:
            await network.send_message(0x123, [1, 2, 3, 4, 5, 6, 7, 8])
            await network.send_message(0x12345, [])
        finally:
            await network.disconnect()

        self.assertEqual(bus.sent, [
            (0x123, bytes([1, 2, 3, 4, 5, 6, 7, 8])),
            (0x12345, b""),
        ])

    def test_send_message_without_bus_raises(self):
        run(self._send_message_without_bus_raises())

    async def _send_message_without_bus_raises(self):
        network = canopen.Network()
        with self.assertRaisesRegex(RuntimeError, "Not connected"):
            await network.send_message(0, [])

    def test_incoming_frame_dispatched_via_listener_task(self):
        run(self._incoming_frame_dispatched_via_listener_task())

    async def _incoming_frame_dispatched_via_listener_task(self):
        network = canopen.Network()
        bus = FakeBus()
        received = []
        network.subscribe(0x321, lambda can_id, data, ts: received.append(data))
        network.connect(bus)
        try:
            await self._settle()
            bus.inject(0x321, b'\xAA')
            await self._settle()
        finally:
            await network.disconnect()

        self.assertEqual(received, [b'\xAA'])

    def test_subscribe_after_connect_starts_listener_immediately(self):
        run(self._subscribe_after_connect_starts_listener_immediately())

    async def _subscribe_after_connect_starts_listener_immediately(self):
        network = canopen.Network()
        bus = FakeBus()
        network.connect(bus)
        await self._settle()

        received = []
        network.subscribe(0x654, lambda can_id, data, ts: received.append(data))
        try:
            await self._settle()
            bus.inject(0x654, b'\xBB')
            await self._settle()
        finally:
            await network.disconnect()

        self.assertEqual(received, [b'\xBB'])

    def test_disconnect_cancels_listener_tasks(self):
        run(self._disconnect_cancels_listener_tasks())

    async def _disconnect_cancels_listener_tasks(self):
        network = canopen.Network()
        bus = FakeBus()
        # The LSS master subscribes during Network.__init__, so connect()
        # should start at least one listener task.
        network.connect(bus)
        self.assertTrue(network._listener_tasks)

        await network.disconnect()
        self.assertEqual(network._listener_tasks, {})
        self.assertTrue(bus.deinit_called)

    def test_context_manager(self):
        run(self._context_manager())

    async def _context_manager(self):
        network = canopen.Network()
        bus = FakeBus()
        async with network.connect(bus):
            pass
        with self.assertRaisesRegex(RuntimeError, "Not connected"):
            await network.send_message(0, [])
        self.assertTrue(bus.deinit_called)

    def test_send_periodic(self):
        run(self._send_periodic())

    async def _send_periodic(self):
        DATA1 = b'\x01\x02\x03'
        DATA2 = b'\x04\x05\x06'
        COB_ID = 0x123
        PERIOD = 0.01

        network = canopen.Network()
        bus = FakeBus()
        received = []
        network.subscribe(COB_ID, lambda can_id, data, ts: received.append(data))
        network.connect(bus)
        await self._settle()

        task = network.send_periodic(COB_ID, DATA1, PERIOD)
        try:
            await asyncio.sleep(PERIOD * 5)
            self.assertGreaterEqual(len(received), 2)
            self.assertTrue(all(d == DATA1 for d in received))

            received.clear()
            task.update(DATA2)
            await asyncio.sleep(PERIOD * 5)
            self.assertTrue(all(d == DATA2 for d in received))
        finally:
            task.stop()
            await network.disconnect()

    def test_node_boot_up_via_heartbeat(self):
        """End-to-end: a boot-up heartbeat frame injected on FakeBus is
        picked up by Network's listener task and resolves
        NmtMaster.wait_for_bootup()."""
        run(self._node_boot_up_via_heartbeat())

    async def _node_boot_up_via_heartbeat(self):
        network = canopen.Network()
        with self.assertLogs():
            node = network.add_node(2, SAMPLE_EDS)
        bus = FakeBus()
        network.connect(bus)
        try:
            await self._settle()
            # Boot-up heartbeat: state byte 0x00.
            bus.inject(0x700 + node.id, bytes([0]))
            await node.nmt.wait_for_bootup(timeout=1)
            self.assertEqual(node.nmt.state, "PRE-OPERATIONAL")
        finally:
            await network.disconnect()

    def test_sdo_request_reaches_bus(self):
        """A node's SDO client should transmit through the real send path."""
        run(self._sdo_request_reaches_bus())

    async def _sdo_request_reaches_bus(self):
        network = canopen.Network()
        with self.assertLogs():
            node = network.add_node(2, SAMPLE_EDS)
        bus = FakeBus()
        network.connect(bus)
        try:
            await self._settle()
            with self.assertRaises(Exception):
                # No SDO server will ever respond, so this must time out;
                # we only care that the request frame actually hit the bus.
                node.sdo.RESPONSE_TIMEOUT = 0.05
                await node.sdo.upload(0x1018, 0x01)
        finally:
            await network.disconnect()

        # The client retries once, then sends an abort frame on final timeout.
        self.assertGreaterEqual(len(bus.sent), 1)
        can_id, data = bus.sent[0]
        self.assertEqual(can_id, 0x600 + node.id)
        self.assertEqual(data[1:3], bytes([0x18, 0x10]))
        self.assertEqual(data[3], 0x01)


class TestScanner(unittest.TestCase):
    TIMEOUT = 0.1

    def setUp(self):
        self.scanner = canopen.network.NodeScanner()

    def test_scanner_on_message_received(self):
        # Emergency frames should be recognized.
        self.scanner.on_message_received(0x081)
        # Heartbeats should be recognized.
        self.scanner.on_message_received(0x703)
        # Tx PDOs should be recognized, but not Rx PDOs.
        self.scanner.on_message_received(0x185)
        self.scanner.on_message_received(0x206)
        self.scanner.on_message_received(0x287)
        self.scanner.on_message_received(0x308)
        self.scanner.on_message_received(0x389)
        self.scanner.on_message_received(0x40a)
        self.scanner.on_message_received(0x48b)
        self.scanner.on_message_received(0x50c)
        # SDO responses from .search() should be recognized,
        # but not SDO requests.
        self.scanner.on_message_received(0x58d)
        self.scanner.on_message_received(0x50e)
        self.assertListEqual(self.scanner.nodes, [1, 3, 5, 7, 9, 11, 13])

    def test_scanner_reset(self):
        self.scanner.nodes = [1, 2, 3]  # Mock scan.
        self.scanner.reset()
        self.assertListEqual(self.scanner.nodes, [])

    def test_scanner_search_no_network(self):
        with self.assertRaisesRegex(RuntimeError, "No actual Network object was assigned"):
            run(self.scanner.search())

    def test_scanner_search(self):
        run(self._scanner_search())

    async def _scanner_search(self):
        bus = FakeBus()
        net = canopen.Network()
        net.connect(bus)
        try:
            self.scanner.network = net
            await self.scanner.search()
        finally:
            await net.disconnect()

        payload = bytes([64, 0, 16, 0, 0, 0, 0, 0])
        self.assertEqual(len(bus.sent), 127)
        for node_id, (can_id, data) in enumerate(bus.sent, start=1):
            with self.subTest(node_id=node_id):
                self.assertEqual(can_id, 0x600 + node_id)
                self.assertEqual(data, payload)

    def test_scanner_search_limit(self):
        run(self._scanner_search_limit())

    async def _scanner_search_limit(self):
        bus = FakeBus()
        net = canopen.Network()
        net.connect(bus)
        try:
            self.scanner.network = net
            await self.scanner.search(limit=1)
        finally:
            await net.disconnect()

        self.assertEqual(bus.sent, [
            (0x601, bytes([64, 0, 16, 0, 0, 0, 0, 0])),
        ])


if __name__ == "__main__":
    unittest.main()
