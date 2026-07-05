import asyncio
import contextlib
import os
import tempfile


DATATYPES_EDS = os.path.join(os.path.dirname(__file__), "datatypes.eds")
SAMPLE_EDS = os.path.join(os.path.dirname(__file__), "sample.eds")


@contextlib.contextmanager
def tmp_file(*args, **kwds):
    with tempfile.NamedTemporaryFile(*args, **kwds) as tmp:
        tmp.close()
        yield tmp


class _FakeMessage:
    def __init__(self, data: bytes):
        self.data = data


class _FakeSubscription:
    def __init__(self, bus: "FakeBus", can_id: int, maxsize: int = 4):
        self._bus = bus
        self._can_id = can_id
        self._queue: asyncio.Queue = asyncio.Queue(maxsize)

    async def __aenter__(self) -> asyncio.Queue:
        self._bus._queues.setdefault(self._can_id, []).append(self._queue)
        return self._queue

    async def __aexit__(self, *exc):
        subs = self._bus._queues.get(self._can_id, [])
        if self._queue in subs:
            subs.remove(self._queue)


class FakePeriodicTask:
    """Test double for aiocan.PeriodicTask."""

    def __init__(self, bus: "FakeBus", can_id: int, data: bytes, period_ms: int):
        self._bus = bus
        self.can_id = can_id
        self.data = bytearray(data)
        self.period_ms = period_ms
        self.cancelled = False
        self._task = asyncio.create_task(self._run())

    async def _run(self):
        while True:
            await asyncio.sleep(self.period_ms / 1000)
            self._bus.sent.append((self.can_id, bytes(self.data)))
            self._bus._deliver(self.can_id, bytes(self.data))
            if self._bus._channel is not None:
                self._bus._channel._deliver(self._bus, self.can_id, bytes(self.data))

    def update(self, data: bytes) -> None:
        self.data = bytearray(data)

    def cancel(self) -> None:
        self.cancelled = True
        self._task.cancel()


class FakeChannel:
    """Shared medium connecting multiple FakeBus 'ports'.

    Mimics a python-can virtual bus channel: a frame sent on one port is
    delivered to every *other* port's subscribers, not its own (i.e. no
    self-loopback), so two independent Network instances can be wired
    together to exercise real master/slave message exchange.
    """

    def __init__(self):
        self._ports: list["FakeBus"] = []

    def _register(self, port: "FakeBus") -> None:
        self._ports.append(port)

    def _deliver(self, sender: "FakeBus", can_id: int, data: bytes) -> None:
        for port in self._ports:
            if port is not sender:
                port._deliver(can_id, data)


class FakeBus:
    """Minimal async test double for aiocan.Bus.

    Implements just the surface that :class:`canopen.network.Network` relies
    on (``send``, ``subscribe``, ``send_periodic``, ``deinit``), so
    Network's asyncio task management can be exercised without needing the
    real aiocan package or MicroPython.

    :param channel: Optional :class:`FakeChannel` to share with other
        FakeBus instances, so frames sent on one are received by the others.
    """

    def __init__(self, channel: "FakeChannel | None" = None):
        #: Log of every frame passed to send(), as (can_id, data).
        self.sent: list[tuple[int, bytes]] = []
        self._queues: dict[int, list[asyncio.Queue]] = {}
        self.deinit_called = False
        self._channel = channel
        if channel is not None:
            channel._register(self)

    async def send(self, can_id: int, data: bytes) -> None:
        self.sent.append((can_id, bytes(data)))
        if self._channel is not None:
            self._channel._deliver(self, can_id, bytes(data))

    def subscribe(self, can_id: int, maxsize: int = 4) -> _FakeSubscription:
        return _FakeSubscription(self, can_id, maxsize)

    def send_periodic(self, can_id: int, data: bytes, period_ms: int) -> FakePeriodicTask:
        return FakePeriodicTask(self, can_id, data, period_ms)

    async def deinit(self) -> None:
        self.deinit_called = True

    def inject(self, can_id: int, data: bytes) -> None:
        """Simulate a frame arriving on the bus, independent of send()."""
        self._deliver(can_id, bytes(data))

    def _deliver(self, can_id: int, data: bytes) -> None:
        for q in self._queues.get(can_id, []):
            q.put_nowait(_FakeMessage(data))
