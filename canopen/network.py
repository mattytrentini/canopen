import asyncio
import time

try:
    import logging
    logger = logging.getLogger(__name__)
except ImportError:
    class _Logger:
        def debug(self, *a, **k): pass
        def info(self, *a, **k): pass
        def warning(self, *a, **k): pass
        def error(self, *a, **k): pass
    logger = _Logger()

try:
    from collections.abc import MutableMapping, Callable
except ImportError:
    MutableMapping = object

from canopen.lss import LssMaster
from canopen.nmt import NmtMaster
from canopen.node import LocalNode, RemoteNode
from canopen.objectdictionary import ObjectDictionary
from canopen.objectdictionary.eds import import_from_node
from canopen.sync import SyncProducer
from canopen.timestamp import TimeProducer


class Network(MutableMapping):
    """Representation of one CAN bus containing one or more nodes."""

    def __init__(self, bus: object = None):
        """
        :param bus:
            A CAN bus instance to re-use (aiocan.Bus).
        """
        #: CAN bus instance, set after :meth:`canopen.Network.connect` is called
        self.bus = bus
        #: A :class:`~canopen.network.NodeScanner` for detecting nodes
        self.scanner = NodeScanner(self)
        self.nodes: dict[int, RemoteNode | LocalNode] = {}
        self.subscribers: dict[int, list] = {}
        #: One asyncio.Task per subscribed COB-ID, created when connect() is called
        self._listener_tasks: dict[int, asyncio.Task] = {}
        self.sync = SyncProducer(self)
        self.time = TimeProducer(self)
        self.nmt = NmtMaster(0)
        self.nmt.network = self

        self.lss = LssMaster()
        self.lss.network = self
        self.subscribe(self.lss.LSS_RX_COBID, self.lss.on_message_received)

        if bus is not None:
            for can_id in self.subscribers:
                self._start_listener(can_id)

    def subscribe(self, can_id: int, callback) -> None:
        """Listen for messages with a specific CAN ID.

        :param can_id:
            The CAN ID to listen for.
        :param callback:
            Function to call when message is received.
        """
        self.subscribers.setdefault(can_id, [])
        if callback not in self.subscribers[can_id]:
            self.subscribers[can_id].append(callback)
        if self.bus is not None:
            self._start_listener(can_id)

    def unsubscribe(self, can_id: int, callback=None) -> None:
        """Stop listening for message.

        :param int can_id:
            The CAN ID from which to unsubscribe.
        :param callback:
            If given, remove only this callback.  Otherwise all callbacks for
            the CAN ID.
        """
        if callback is not None:
            self.subscribers[can_id].remove(callback)
        if not self.subscribers.get(can_id) or callback is None:
            self.subscribers.pop(can_id, None)
            task = self._listener_tasks.pop(can_id, None)
            if task is not None:
                task.cancel()

    def connect(self, bus) -> "Network":
        """Connect to CAN bus.

        :param bus:
            An :class:`aiocan.Bus` instance wrapping a configured
            ``machine.CAN`` object.
        """
        self.bus = bus
        logger.info("Connected to CAN bus")
        for can_id in self.subscribers:
            self._start_listener(can_id)
        return self

    def _start_listener(self, can_id: int) -> None:
        """Create a listener task for *can_id* if one doesn't already exist."""
        if can_id not in self._listener_tasks:
            self._listener_tasks[can_id] = asyncio.create_task(self._listen(can_id))

    async def _listen(self, can_id: int) -> None:
        """Receive loop: dispatch every frame on *can_id* through notify()."""
        async with self.bus.subscribe(can_id) as q:
            while True:
                msg = await q.get()
                self.notify(can_id, msg.data, time.time())

    async def disconnect(self) -> None:
        """Disconnect from the CAN bus."""
        for node in self.nodes.values():
            if hasattr(node, "pdo"):
                node.pdo.stop()
        for task in self._listener_tasks.values():
            task.cancel()
        self._listener_tasks.clear()
        if self.bus is not None:
            await self.bus.deinit()
            self.bus = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.disconnect()

    def add_node(
        self,
        node: int | RemoteNode | LocalNode,
        object_dictionary: str | ObjectDictionary | None = None,
        upload_eds: bool = False,
    ) -> RemoteNode | LocalNode:
        """Add a remote node to the network.

        :param node:
            Can be either an integer representing the node ID, a
            :class:`canopen.RemoteNode` or :class:`canopen.LocalNode` object.
        :param object_dictionary:
            Can be either a string for specifying the path to an
            Object Dictionary file or a
            :class:`canopen.ObjectDictionary` object.
        :param upload_eds:
            Set ``True`` if EDS file should be uploaded from 0x1021.

        :return:
            The Node object that was added.
        """
        if isinstance(node, int):
            if upload_eds:
                logger.info("Trying to read EDS from node %d", node)
                object_dictionary = import_from_node(node, self)
            node = RemoteNode(node, object_dictionary)
        self[node.id] = node
        return node

    def create_node(
        self,
        node: int | LocalNode,
        object_dictionary: str | ObjectDictionary | None = None,
    ) -> LocalNode:
        """Create a local node in the network.

        :param node:
            An integer representing the node ID.
        :param object_dictionary:
            Can be either a string for specifying the path to an
            Object Dictionary file or a
            :class:`canopen.ObjectDictionary` object.

        :return:
            The Node object that was added.
        """
        if isinstance(node, int):
            node = LocalNode(node, object_dictionary)
        self[node.id] = node
        return node

    async def send_message(self, can_id: int, data: bytes, remote: bool = False) -> None:
        """Send a raw CAN message to the network.

        :param int can_id:
            CAN-ID of the message
        :param data:
            Data to be transmitted (anything that can be converted to bytes)
        :param bool remote:
            Set to True to send remote frame (deferred — rarely needed in CANopen)
        """
        if not self.bus:
            raise RuntimeError("Not connected to CAN bus")
        await self.bus.send(can_id, bytes(data))

    def send_periodic(
        self, can_id: int, data: bytes, period: float, remote: bool = False
    ) -> "PeriodicMessageTask":
        """Start sending a message periodically.

        :param can_id:
            CAN-ID of the message
        :param data:
            Data to be transmitted (anything that can be converted to bytes)
        :param period:
            Seconds between each message
        :param remote:
            Ignored (RTR periodic not supported by aiocan)

        :return:
            A task object with ``.stop()`` and ``.update()`` methods.
        """
        pt = self.bus.send_periodic(can_id, data, int(period * 1000))
        return PeriodicMessageTask(pt)

    def notify(self, can_id: int, data: bytearray, timestamp: float) -> None:
        """Feed incoming message to this library.

        If a custom interface is used, this function must be called for each
        message read from the CAN bus.

        :param can_id:
            CAN-ID of the message
        :param data:
            Data part of the message (0 - 8 bytes)
        :param timestamp:
            Timestamp of the message, preferably as a Unix timestamp
        """
        if can_id in self.subscribers:
            callbacks = self.subscribers[can_id]
            for callback in callbacks:
                callback(can_id, data, timestamp)
        self.scanner.on_message_received(can_id)

    def check(self) -> None:
        """No-op — no background thread to check."""

    def __getitem__(self, node_id: int) -> RemoteNode | LocalNode:
        return self.nodes[node_id]

    def __setitem__(self, node_id: int, node: RemoteNode | LocalNode):
        assert node_id == node.id
        if node_id in self.nodes:
            self.nodes[node_id].remove_network()
        self.nodes[node_id] = node
        node.associate_network(self)

    def __delitem__(self, node_id: int):
        self.nodes[node_id].remove_network()
        del self.nodes[node_id]

    def __iter__(self):
        return iter(self.nodes)

    def __len__(self) -> int:
        return len(self.nodes)


class _UninitializedNetwork(Network):
    """Empty network implementation as a placeholder before actual initialization."""

    def __init__(self, bus: object = None):
        """Do not initialize attributes, by skipping the parent constructor."""

    def __getattribute__(self, name):
        raise RuntimeError("No actual Network object was assigned, "
                           "try associating to a real network first.")


#: Singleton instance
_UNINITIALIZED_NETWORK: Network = _UninitializedNetwork()


class PeriodicMessageTask:
    """Wraps an aiocan.PeriodicTask to match the canopen API."""

    def __init__(self, task):
        self._task = task

    def stop(self) -> None:
        """Stop transmission."""
        self._task.cancel()

    def update(self, data: bytes) -> None:
        """Update the payload for subsequent transmissions."""
        self._task.update(data)


class NodeScanner:
    """Observes which nodes are present on the bus.

    Listens for the following messages:
     - Heartbeat (0x700)
     - SDO response (0x580)
     - TxPDO (0x180, 0x280, 0x380, 0x480)
     - EMCY (0x80)

    :param canopen.Network network:
        The network to use when doing active searching.
    """

    SERVICES = (0x700, 0x580, 0x180, 0x280, 0x380, 0x480, 0x80)

    def __init__(self, network: "Network | None" = None):
        if network is None:
            network = _UNINITIALIZED_NETWORK
        self.network: Network = network
        #: A :class:`list` of nodes discovered
        self.nodes: list[int] = []

    def on_message_received(self, can_id: int):
        service = can_id & 0x780
        node_id = can_id & 0x7F
        if node_id not in self.nodes and node_id != 0 and service in self.SERVICES:
            self.nodes.append(node_id)

    def reset(self):
        """Clear list of found nodes."""
        self.nodes = []

    async def search(self, limit: int = 127) -> None:
        """Search for nodes by sending SDO requests to all node IDs."""
        sdo_req = b"\x40\x00\x10\x00\x00\x00\x00\x00"
        for node_id in range(1, limit + 1):
            await self.network.send_message(0x600 + node_id, sdo_req)
