import asyncio
import unittest

import canopen

from .util import SAMPLE_EDS, FakeBus, FakeChannel


def run(coro):
    return asyncio.run(coro)


class TestSDO(unittest.TestCase):
    """
    Test SDO client and server against each other.

    Structured as a single test method (with subTests) rather than one
    asyncio.run() per test, since the shared connected network pair uses
    asyncio.create_task()-based listener tasks tied to one event loop —
    unittest's setUpClass/tearDownClass can't straddle a per-test loop.
    """

    def test_all(self):
        run(self._test_all())

    async def _test_all(self):
        channel = FakeChannel()
        bus1 = FakeBus(channel)
        bus2 = FakeBus(channel)

        network1 = canopen.Network()
        remote_node = network1.add_node(2, SAMPLE_EDS)
        network2 = canopen.Network()
        local_node = network2.create_node(2, SAMPLE_EDS)
        remote_node2 = network1.add_node(3, SAMPLE_EDS)
        local_node2 = network2.create_node(3, SAMPLE_EDS)

        network1.connect(bus1)
        network2.connect(bus2)
        # Give the listener tasks a chance to register their queues.
        await asyncio.sleep(0.01)

        try:
            with self.subTest("expedited_upload"):
                await local_node.sdo[0x1400][1].write(0x99)
                vendor_id = await remote_node.sdo[0x1400][1].read()
                self.assertEqual(vendor_id, 0x99)

            with self.subTest("expedited_upload_default_value_visible_string"):
                device_name = await remote_node.sdo["Manufacturer device name"].read()
                self.assertEqual(device_name, "TEST DEVICE")

            with self.subTest("expedited_upload_default_value_real"):
                sampling_rate = await remote_node.sdo["Sensor Sampling Rate (Hz)"].read()
                self.assertAlmostEqual(sampling_rate, 5.2, places=2)

            with self.subTest("upload_zero_length"):
                await local_node.sdo["Manufacturer device name"].write(b"")
                with self.assertRaises(canopen.SdoAbortedError) as error:
                    await remote_node.sdo["Manufacturer device name"].get_data()
                # Should be No data available
                self.assertEqual(error.exception.code, 0x0800_0024)

            with self.subTest("segmented_upload"):
                await local_node.sdo["Manufacturer device name"].write("Some cool device")
                device_name = await remote_node.sdo["Manufacturer device name"].get_data()
                self.assertEqual(device_name, b"Some cool device")

            with self.subTest("expedited_download"):
                await remote_node.sdo[0x2004].write(0xfeff)
                value = await local_node.sdo[0x2004].read()
                self.assertEqual(value, 0xfeff)

            with self.subTest("expedited_download_wrong_datatype"):
                # Try to write 32 bit in integer16 type
                with self.assertRaises(canopen.SdoAbortedError) as error:
                    await remote_node.sdo.download(0x2001, 0x0, bytes([10, 10, 10, 10]))
                self.assertEqual(error.exception.code, 0x06070010)
                # Try to write normal 16 bit word, should be ok
                await remote_node.sdo.download(0x2001, 0x0, bytes([10, 10]))
                value = await remote_node.sdo.upload(0x2001, 0x0)
                self.assertEqual(value, bytes([10, 10]))

            with self.subTest("segmented_download"):
                await remote_node.sdo[0x2000].write("Another cool device")
                value = await local_node.sdo[0x2000].get_data()
                self.assertEqual(value, b"Another cool device")

            with self.subTest("slave_send_heartbeat"):
                # Setting the heartbeat time should trigger heartbeating
                # to start
                await remote_node.sdo["Producer heartbeat time"].write(100)
                state = await remote_node.nmt.wait_for_heartbeat()
                local_node.nmt.stop_heartbeat()
                # The NMT master will change the state INITIALISING (0)
                # to PRE-OPERATIONAL (127)
                self.assertEqual(state, 'PRE-OPERATIONAL')

            with self.subTest("nmt_state_initializing_to_preoper"):
                # Initialize the heartbeat timer
                await local_node.sdo["Producer heartbeat time"].write(100)
                local_node.nmt.stop_heartbeat()
                # This transition shall start the heartbeating
                local_node.nmt.state = 'INITIALISING'
                local_node.nmt.state = 'PRE-OPERATIONAL'
                state = await remote_node.nmt.wait_for_heartbeat()
                local_node.nmt.stop_heartbeat()
                self.assertEqual(state, 'PRE-OPERATIONAL')

            with self.subTest("receive_abort_request"):
                await remote_node.sdo.abort(0x0504_0003)  # Invalid sequence number
                # Give the local node a chance to receive the abort.
                await asyncio.sleep(0.05)
                self.assertEqual(local_node.sdo.last_received_error, 0x0504_0003)

            with self.subTest("start_remote_node"):
                remote_node.nmt.state = 'OPERATIONAL'
                # Give the slave a chance to receive the command.
                await asyncio.sleep(0.05)
                slave_state = local_node.nmt.state
                self.assertEqual(slave_state, 'OPERATIONAL')

            with self.subTest("two_nodes_on_the_bus"):
                await local_node.sdo["Manufacturer device name"].write("Some cool device")
                device_name = await remote_node.sdo["Manufacturer device name"].get_data()
                self.assertEqual(device_name, b"Some cool device")

                await local_node2.sdo["Manufacturer device name"].write("Some cool device2")
                device_name = await remote_node2.sdo["Manufacturer device name"].get_data()
                self.assertEqual(device_name, b"Some cool device2")

            with self.subTest("abort"):
                with self.assertRaises(canopen.SdoAbortedError) as cm:
                    _ = await remote_node.sdo.upload(0x1234, 0)
                # Should be Object does not exist
                self.assertEqual(cm.exception.code, 0x06020000)

                with self.assertRaises(canopen.SdoAbortedError) as cm:
                    _ = await remote_node.sdo.upload(0x1018, 100)
                # Should be Subindex does not exist
                self.assertEqual(cm.exception.code, 0x06090011)

                with self.assertRaises(canopen.SdoAbortedError) as cm:
                    _ = await remote_node.sdo[0x1001].get_data()
                # Should be Resource not available
                self.assertEqual(cm.exception.code, 0x060A0023)

            with self.subTest("callbacks"):
                calls = {}

                def some_read_callback(**kwargs):
                    calls.update(kwargs)
                    if kwargs["index"] == 0x1003:
                        return 0x0201

                def some_write_callback(**kwargs):
                    calls.update(kwargs)

                local_node.add_read_callback(some_read_callback)
                local_node.add_write_callback(some_write_callback)

                data = await remote_node.sdo.upload(0x1003, 5)
                self.assertEqual(data, b"\x01\x02\x00\x00")
                self.assertEqual(calls["index"], 0x1003)
                self.assertEqual(calls["subindex"], 5)

                await remote_node.sdo.download(0x1017, 0, b"\x03\x04")
                self.assertEqual(calls["index"], 0x1017)
                self.assertEqual(calls["subindex"], 0)
                self.assertEqual(calls["data"], b"\x03\x04")
        finally:
            await network1.disconnect()
            await network2.disconnect()


class TestPDO(unittest.TestCase):
    """
    Test PDO slave.
    """

    def test_all(self):
        run(self._test_all())

    async def _test_all(self):
        channel = FakeChannel()
        bus1 = FakeBus(channel)
        bus2 = FakeBus(channel)

        network1 = canopen.Network()
        remote_node = network1.add_node(2, SAMPLE_EDS)
        network2 = canopen.Network()
        local_node = network2.create_node(2, SAMPLE_EDS)

        network1.connect(bus1)
        network2.connect(bus2)
        await asyncio.sleep(0.01)

        try:
            with self.subTest("read"):
                # TODO: Do some more checks here. Currently it only tests that
                # they can be called without raising an error.
                await remote_node.pdo.read()
                await local_node.pdo.read()

            with self.subTest("save"):
                # TODO: Do some more checks here. Currently it only tests that
                # they can be called without raising an error.
                await remote_node.pdo.save()
                await local_node.pdo.save()
        finally:
            await network1.disconnect()
            await network2.disconnect()


if __name__ == "__main__":
    unittest.main()
