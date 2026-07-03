import asyncio
import struct

try:
    from aiocan import CanError
except ImportError:
    try:
        from can import CanError
    except ImportError:
        CanError = Exception

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

from canopen.sdo.base import SdoBase
from canopen.sdo.constants import *
from canopen.sdo.exceptions import *
from canopen.utils import pretty_index


class SdoClient(SdoBase):
    """Handles communication with an SDO server."""

    #: Max time in seconds to wait for response from server
    RESPONSE_TIMEOUT = 0.3

    #: Max number of request retries before raising error
    MAX_RETRIES = 1

    #: Seconds to wait before sending a request, for rate limiting
    PAUSE_BEFORE_SEND = 0.0

    #: Seconds to wait before retrying a request after a send error
    RETRY_DELAY = 0.1

    def __init__(self, rx_cobid, tx_cobid, od):
        SdoBase.__init__(self, rx_cobid, tx_cobid, od)
        self.responses = asyncio.Queue()

    def on_response(self, can_id, data, timestamp):
        self.responses.put_nowait(bytes(data))

    async def send_request(self, request):
        if self.PAUSE_BEFORE_SEND:
            await asyncio.sleep(self.PAUSE_BEFORE_SEND)
        retries_left = self.MAX_RETRIES
        while True:
            try:
                await self.network.send_message(self.rx_cobid, request)
            except CanError as e:
                retries_left -= 1
                if not retries_left:
                    raise
                logger.info(str(e))
                if self.RETRY_DELAY:
                    await asyncio.sleep(self.RETRY_DELAY)
            else:
                break

    async def read_response(self):
        """Wait for an SDO response and handle timeout or remote abort.

        :raises SdoCommunicationError: After timeout with no response received.
        :raises SdoAbortedError: When receiving an SDO abort response.
        """
        try:
            response = await asyncio.wait_for(
                self.responses.get(), self.RESPONSE_TIMEOUT)
        except asyncio.TimeoutError:
            raise SdoCommunicationError("No SDO response received")
        res_command, = struct.unpack_from("B", response)
        if res_command == RESPONSE_ABORTED:
            abort_code, = struct.unpack_from("<L", response, 4)
            raise SdoAbortedError(abort_code)
        return response

    async def request_response(self, sdo_request):
        retries_left = self.MAX_RETRIES
        # Discard any stale responses
        while not self.responses.empty():
            self.responses.get_nowait()
        while True:
            await self.send_request(sdo_request)
            try:
                return await self.read_response()
            except SdoCommunicationError as e:
                retries_left -= 1
                if not retries_left:
                    await self.abort(ABORT_TIMED_OUT)
                    raise
                logger.warning(str(e))

    async def abort(self, abort_code=ABORT_GENERAL_ERROR):
        """Abort current transfer."""
        request = bytearray(8)
        request[0] = REQUEST_ABORTED
        struct.pack_into("<L", request, 4, abort_code)
        await self.send_request(request)
        logger.error("Transfer aborted by client with code 0x%08X", abort_code)

    async def upload(self, index: int, subindex: int) -> bytes:
        """Read a value from the node using expedited or segmented transfer.

        :param index: Object dictionary index.
        :param subindex: Object dictionary sub-index.
        :return: Raw data as bytes.
        :raises SdoCommunicationError: On unexpected response or timeout.
        :raises SdoAbortedError: When node responds with an abort.
        """
        logger.debug("Reading 0x%04X:%02X from node %d", index, subindex,
                     self.rx_cobid - 0x600)
        request = bytearray(8)
        SDO_STRUCT.pack_into(request, 0, REQUEST_UPLOAD, index, subindex)
        response = await self.request_response(request)
        res_command, res_index, res_subindex = SDO_STRUCT.unpack_from(response)
        res_data = response[4:8]

        if res_command & 0xE0 != RESPONSE_UPLOAD:
            raise SdoCommunicationError(
                f"Unexpected response 0x{res_command:02X}")
        if res_index != index or res_subindex != subindex:
            raise SdoCommunicationError(
                f"Node returned a value for {pretty_index(res_index, res_subindex)} "
                "instead, maybe there is another SDO client communicating "
                "on the same SDO channel?")

        if res_command & EXPEDITED:
            if res_command & SIZE_SPECIFIED:
                size = 4 - ((res_command >> 2) & 0x3)
                return bytes(res_data[:size])
            return bytes(res_data)

        # Segmented upload
        size = None
        if res_command & SIZE_SPECIFIED:
            size, = struct.unpack("<L", res_data)
            logger.debug("Using segmented transfer of %d bytes", size)
        else:
            logger.debug("Using segmented transfer")

        data = bytearray()
        toggle = 0
        while True:
            request = bytearray(8)
            request[0] = REQUEST_SEGMENT_UPLOAD | toggle
            response = await self.request_response(request)
            res_command, = struct.unpack_from("B", response)
            if res_command & 0xE0 != RESPONSE_SEGMENT_UPLOAD:
                await self.abort(ABORT_INVALID_COMMAND_SPECIFIER)
                raise SdoCommunicationError(
                    f"Unexpected response 0x{res_command:02X}")
            if res_command & TOGGLE_BIT != toggle:
                await self.abort(ABORT_TOGGLE_NOT_ALTERNATED)
                raise SdoCommunicationError("Toggle bit mismatch")
            length = 7 - ((res_command >> 1) & 0x7)
            data.extend(response[1:length + 1])
            if res_command & NO_MORE_DATA:
                break
            toggle ^= TOGGLE_BIT

        if size and size < len(data):
            return bytes(data[:size])
        return bytes(data)

    async def download(
        self,
        index: int,
        subindex: int,
        data: bytes,
        force_segment: bool = False,
    ) -> None:
        """Write a value to the node using expedited or segmented transfer.

        :param index: Object dictionary index.
        :param subindex: Object dictionary sub-index.
        :param data: Data to write.
        :param force_segment: Force segmented transfer even for small data.
        :raises SdoCommunicationError: On unexpected response or timeout.
        :raises SdoAbortedError: When node responds with an abort.
        """
        size = len(data)
        if size <= 4 and not force_segment:
            # Expedited download
            command = REQUEST_DOWNLOAD | EXPEDITED | SIZE_SPECIFIED
            command |= (4 - size) << 2
            request = SDO_STRUCT.pack(command, index, subindex) + bytes(data).ljust(4, b"\x00")
            response = await self.request_response(request)
            res_command, = struct.unpack_from("B", response)
            if res_command & 0xE0 != RESPONSE_DOWNLOAD:
                await self.abort(ABORT_INVALID_COMMAND_SPECIFIER)
                raise SdoCommunicationError(
                    f"Unexpected response 0x{res_command:02X}")
        else:
            # Segmented download
            request = bytearray(8)
            command = REQUEST_DOWNLOAD | SIZE_SPECIFIED
            struct.pack_into("<L", request, 4, size)
            SDO_STRUCT.pack_into(request, 0, command, index, subindex)
            response = await self.request_response(request)
            res_command, = struct.unpack_from("B", response)
            if res_command != RESPONSE_DOWNLOAD:
                await self.abort(ABORT_INVALID_COMMAND_SPECIFIER)
                raise SdoCommunicationError(
                    f"Unexpected response 0x{res_command:02X}")

            toggle = 0
            pos = 0
            while pos < size:
                request = bytearray(8)
                command = REQUEST_SEGMENT_DOWNLOAD | toggle
                bytes_sent = min(size - pos, 7)
                if pos + bytes_sent >= size:
                    command |= NO_MORE_DATA
                command |= (7 - bytes_sent) << 1
                request[0] = command
                request[1:bytes_sent + 1] = data[pos:pos + bytes_sent]
                response = await self.request_response(request)
                res_command, = struct.unpack_from("B", response)
                if res_command & 0xE0 != RESPONSE_SEGMENT_DOWNLOAD:
                    await self.abort(ABORT_INVALID_COMMAND_SPECIFIER)
                    raise SdoCommunicationError(
                        f"Unexpected response 0x{res_command:02X}")
                toggle ^= TOGGLE_BIT
                pos += bytes_sent
