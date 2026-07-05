import binascii

try:
    from collections.abc import Mapping
except ImportError:
    Mapping = object

import canopen.network
from canopen import objectdictionary
from canopen import variable
from canopen.utils import pretty_index


class CrcXmodem:
    """Mimics CrcXmodem from crccheck."""

    def __init__(self):
        self._value = 0

    def process(self, data):
        self._value = binascii.crc_hqx(data, self._value)

    def final(self):
        return self._value


class SdoBase(Mapping):

    #: The CRC algorithm used for block transfers
    crc_cls = CrcXmodem

    def __init__(
        self,
        rx_cobid: int,
        tx_cobid: int,
        od: objectdictionary.ObjectDictionary,
    ):
        """
        :param rx_cobid:
            COB-ID that the server receives on (usually 0x600 + node ID)
        :param tx_cobid:
            COB-ID that the server responds with (usually 0x580 + node ID)
        :param od:
            Object Dictionary to use for communication
        """
        self.rx_cobid = rx_cobid
        self.tx_cobid = tx_cobid
        self.network: canopen.network.Network = canopen.network._UNINITIALIZED_NETWORK
        self.od = od

    def __getitem__(self, index: str | int) -> "SdoVariable | SdoArray | SdoRecord":
        entry = self.od[index]
        if isinstance(entry, objectdictionary.ODVariable):
            return SdoVariable(self, entry)
        elif isinstance(entry, objectdictionary.ODArray):
            return SdoArray(self, entry)
        elif isinstance(entry, objectdictionary.ODRecord):
            return SdoRecord(self, entry)

    def __iter__(self):
        return iter(self.od)

    def __len__(self) -> int:
        return len(self.od)

    def __contains__(self, key: object) -> bool:
        return key in self.od

    def get_variable(
        self, index: int | str, subindex: int = 0
    ) -> "SdoVariable | None":
        """Get the variable object at specified index (and subindex if applicable).

        :return: SdoVariable if found, else `None`
        """
        obj = self.get(index)
        if isinstance(obj, SdoVariable):
            return obj
        elif isinstance(obj, (SdoRecord, SdoArray)):
            return obj.get(subindex)
        return None

    async def upload(self, index: int, subindex: int) -> bytes:
        raise NotImplementedError()

    async def download(
        self,
        index: int,
        subindex: int,
        data: bytes,
        force_segment: bool = False,
    ) -> None:
        raise NotImplementedError()


class SdoRecord(Mapping):

    def __init__(self, sdo_node: SdoBase, od: objectdictionary.ODRecord):
        self.sdo_node = sdo_node
        self.od = od

    def __repr__(self) -> str:
        return f"<{type(self).__qualname__} {self.od.name!r} at {pretty_index(self.od.index)}>"

    def __getitem__(self, subindex: int | str) -> "SdoVariable":
        return SdoVariable(self.sdo_node, self.od[subindex])

    def __iter__(self):
        # Skip the "highest subindex" entry, which is not part of the data
        return filter(None, iter(self.od))

    def __len__(self) -> int:
        # Skip the "highest subindex" entry, which is not part of the data
        return len(self.od) - int(0 in self.od)

    def __contains__(self, subindex: object) -> bool:
        return subindex in self.od


class SdoArray:
    """Access a CANopen ARRAY object using the SDO protocol.

    Unlike :class:`SdoRecord`, an array's actual length can only be known
    by reading subindex 0 from the node — network I/O that requires
    awaiting. This does not implement the synchronous ``Mapping`` protocol
    (``len()``, ``in``, ``for``); use :meth:`get_length`, :meth:`contains`,
    and ``async for`` instead.
    """

    def __init__(self, sdo_node: SdoBase, od: objectdictionary.ODArray):
        self.sdo_node = sdo_node
        self.od = od

    def __repr__(self) -> str:
        return f"<{type(self).__qualname__} {self.od.name!r} at {pretty_index(self.od.index)}>"

    def __getitem__(self, subindex: int | str) -> "SdoVariable":
        return SdoVariable(self.sdo_node, self.od[subindex])

    async def __aiter__(self):
        # Skip the "highest subindex" entry, which is not part of the data
        for subindex in range(1, await self.get_length() + 1):
            yield subindex

    async def get_length(self) -> int:
        """Read the actual array length (subindex 0) from the node."""
        return await self[0].read()

    async def contains(self, subindex: object) -> bool:
        if not isinstance(subindex, int):
            return False
        return 0 <= subindex <= await self.get_length()


class SdoVariable(variable.Variable):
    """Access object dictionary variable values using SDO protocol.

    SDO transfers require network I/O, so unlike the base
    :class:`~canopen.variable.Variable`, the synchronous ``.raw`` /
    ``.phys`` / ``.desc`` / ``.data`` properties are not available here.
    Use the async :meth:`read` / :meth:`write` methods instead.
    """

    def __init__(self, sdo_node: SdoBase, od: objectdictionary.ODVariable):
        self.sdo_node = sdo_node
        variable.Variable.__init__(self, od)

    async def get_data(self) -> bytes:
        data = await self.sdo_node.upload(self.od.index, self.od.subindex)
        if self.od.fixed_size:
            var_size = len(self.od) // 8
            if var_size < len(data):
                data = data[:var_size]
        return data

    async def set_data(self, data: bytes):
        force_segment = self.od.data_type == objectdictionary.DOMAIN
        await self.sdo_node.download(self.od.index, self.od.subindex, data, force_segment)

    @property
    def data(self):
        raise TypeError(
            "SDO variable access requires network I/O and is asynchronous; "
            "use 'await variable.read()' / 'await variable.write(value)' "
            "instead of '.data' / '.raw' / '.phys' / '.desc'"
        )

    @data.setter
    def data(self, value):
        raise TypeError(
            "SDO variable access requires network I/O and is asynchronous; "
            "use 'await variable.read()' / 'await variable.write(value)' "
            "instead of '.data' / '.raw' / '.phys' / '.desc'"
        )

    async def read(self, fmt: str = "raw"):
        """Async equivalent of Variable.raw / .phys / .desc.

        :param str fmt: How to return the value — 'raw', 'phys', or 'desc'.
        """
        raw = self.od.decode_raw(await self.get_data())
        if fmt == "raw":
            return raw
        elif fmt == "phys":
            return self.od.decode_phys(raw)
        elif fmt == "desc":
            if not isinstance(raw, int):
                raise TypeError("Description of values only supported for integer objects")
            return self.od.decode_desc(raw)
        raise ValueError(f"Invalid format '{fmt}'")

    async def write(self, value, fmt: str = "raw") -> None:
        """Async equivalent of Variable.raw / .phys / .desc setters.

        :param str fmt: How to interpret *value* — 'raw', 'phys', or 'desc'.
        """
        if fmt == "phys":
            value = self.od.encode_phys(value)
        elif fmt == "desc":
            if not isinstance(value, str):
                raise TypeError("fmt=desc requires a string value")
            value = self.od.encode_desc(value)
        elif fmt != "raw":
            raise ValueError(f"Invalid format '{fmt}'")
        await self.set_data(self.od.encode_raw(value))

    @property
    def writable(self) -> bool:
        return self.od.writable

    @property
    def readable(self) -> bool:
        return self.od.readable


# For compatibility
Record = SdoRecord
Array = SdoArray
Variable = SdoVariable
