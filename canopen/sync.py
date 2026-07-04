import asyncio


class SyncProducer:
    """Transmits a SYNC message periodically."""

    #: COB-ID of the SYNC message
    cob_id = 0x80

    def __init__(self, network: object):
        self.network = network
        self.period: float | None = None
        self._task: object | None = None

    def transmit(self, count: int | None = None):
        """Send out a SYNC message once.

        :param count:
            Counter to add in message.
        :raises ValueError:
            If the counter value does not fit in one byte.
        """
        data = bytes([count]) if count is not None else b""
        asyncio.create_task(self.network.send_message(self.cob_id, data))

    def start(self, period: float | None = None):
        """Start periodic transmission of SYNC message in a background thread.

        :param period:
            Period of SYNC message in seconds.
        :raises RuntimeError:
            If a periodic transmission is already started.
        :raises ValueError:
            If no period is set via argument nor the instance attribute.
        """
        if self._task is not None:
            raise RuntimeError("Periodic SYNC transmission task already running")

        if period is not None:
            self.period = period

        if not self.period:
            raise ValueError("A valid transmission period has not been given")

        self._task = self.network.send_periodic(self.cob_id, b"", self.period)

    def stop(self):
        """Stop periodic transmission of SYNC message."""
        if self._task is not None:
            self._task.stop()
        self._task = None
