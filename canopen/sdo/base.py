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


class SdoArray(Mapping):

    def __init__(self, sdo_node: SdoBase, od: objectdictionary.ODArray):
        self.sdo_node = sdo_node
        self.od = od

    def __repr__(self) -> str:
        return f"<{type(self).__qualname__} {self.od.name!r} at {pretty_index(self.od.index)}>"

    def __getitem__(self, subindex: int | str) -> "SdoVariable":
        return SdoVariable(self.sdo_node, self.od[subindex])

    def __iter__(self):
        # Skip the "highest subindex" entry, which is not part of the data
        return iter(range(1, len(self) + 1))

    def __len__(self) -> int:
        return self[0].raw

    def __contains__(self, subindex: object) -> bool:
        if not isinstance(subindex, int):
            return False
        return 0 <= subindex <= len(self)


class SdoVariable(variable.Variable):
    """Access object dictionary variable values using SDO protocol."""

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
    def writable(self) -> bool:
        return self.od.writable

    @property
    def readable(self) -> bool:
        return self.od.readable


# For compatibility
Record = SdoRecord
Array = SdoArray
Variable = SdoVariable
