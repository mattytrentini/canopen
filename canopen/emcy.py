import asyncio
import struct
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

import canopen.network


# Error code, error register, vendor specific data
EMCY_STRUCT = struct.Struct("<HB5s")


class EmcyConsumer:

    def __init__(self):
        #: Log of all received EMCYs for this node
        self.log: list[EmcyError] = []
        #: Only active EMCYs. Will be cleared on Error Reset
        self.active: list[EmcyError] = []
        self.callbacks = []
        self._emcy_event = asyncio.Event()

    def on_emcy(self, can_id, data, timestamp):
        code, register, data = EMCY_STRUCT.unpack(data)
        entry = EmcyError(code, register, data, timestamp)

        if code & 0xFF00 == 0:
            # Error reset
            self.active = []
        else:
            self.active.append(entry)
        self.log.append(entry)
        self._emcy_event.set()

        for callback in self.callbacks:
            callback(entry)

    def add_callback(self, callback: object):
        """Get notified on EMCY messages from this node.

        :param callback:
            Callable which must take one argument of an
            :class:`~canopen.emcy.EmcyError` instance.
        """
        self.callbacks.append(callback)

    def reset(self):
        """Reset log and active lists."""
        self.log = []
        self.active = []

    async def wait(
        self, emcy_code: int | None = None, timeout: float = 10
    ) -> "EmcyError | None":
        """Wait for a new EMCY to arrive.

        :param emcy_code: EMCY code to wait for
        :param timeout: Max time in seconds to wait

        :return: The EMCY exception object or None if timeout
        """
        end_time = time.time() + timeout
        while True:
            remaining = end_time - time.time()
            if remaining <= 0:
                return None
            self._emcy_event.clear()
            try:
                await asyncio.wait_for(self._emcy_event.wait(), remaining)
            except asyncio.TimeoutError:
                return None
            emcy = self.log[-1]
            logger.info("Got %s", emcy)
            if emcy_code is None or emcy.code == emcy_code:
                return emcy
            # Not the code we wanted — loop and wait for the next one


class EmcyProducer:

    def __init__(self, cob_id: int):
        self.network: canopen.network.Network = canopen.network._UNINITIALIZED_NETWORK
        self.cob_id = cob_id

    def send(self, code: int, register: int = 0, data: bytes = b""):
        payload = EMCY_STRUCT.pack(code, register, data)
        self.network.send_message(self.cob_id, payload)

    def reset(self, register: int = 0, data: bytes = b""):
        payload = EMCY_STRUCT.pack(0, register, data)
        self.network.send_message(self.cob_id, payload)


class EmcyError(Exception):
    """EMCY exception."""

    DESCRIPTIONS = [
        # Code   Mask    Description
        (0x0000, 0xFF00, "Error Reset / No Error"),
        (0x1000, 0xFF00, "Generic Error"),
        (0x2000, 0xF000, "Current"),
        (0x3000, 0xF000, "Voltage"),
        (0x4000, 0xF000, "Temperature"),
        (0x5000, 0xFF00, "Device Hardware"),
        (0x6000, 0xF000, "Device Software"),
        (0x7000, 0xFF00, "Additional Modules"),
        (0x8000, 0xF000, "Monitoring"),
        (0x9000, 0xFF00, "External Error"),
        (0xF000, 0xFF00, "Additional Functions"),
        (0xFF00, 0xFF00, "Device Specific")
    ]

    def __init__(self, code: int, register: int, data: bytes, timestamp: float):
        #: EMCY code
        self.code = code
        #: Error register
        self.register = register
        #: Vendor specific data
        self.data = data
        #: Timestamp of message
        self.timestamp = timestamp

    def get_desc(self) -> str:
        for code, mask, description in self.DESCRIPTIONS:
            if self.code & mask == code:
                return description
        return ""

    def __str__(self):
        text = f"Code 0x{self.code:04X}"
        description = self.get_desc()
        if description:
            text = text + ", " + description
        return text
