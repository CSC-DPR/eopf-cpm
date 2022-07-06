import errno
import glob
import os
from typing import TYPE_CHECKING, Any, Iterator, MutableMapping

import numpy as np

from eopf.product.store.abstract import EOProductStore, StorageStatus

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject


class PoolMemMap:

    _buffer: Any
    _n_packets: int
    _packet_length: Any
    _packet_offset: Any
    _items: Any

    _n_packets = 0
    _loaded = False
    _items = {}

    def __new__(cls: type["PoolMemMap"], url: str) -> "PoolMemMap":

        if url not in cls._items:
            cls._items[url] = super().__new__(cls)
        return cls._items[url]

    def open(self, url: str) -> None:

        if url == "":
            return

        if not self._loaded:
            self._loaded = True

    def close(self) -> None:

        self._loaded = False
        self._n_packets = 0

        if hasattr(self, "_buffer"):
            del self._buffer
        if hasattr(self, "_packet_length"):
            del self._packet_length
        if hasattr(self, "_packet_offset"):
            del self._packet_offset


class MemMapAccessor(EOProductStore):

    incr_step = 10000
    primary_header = 6

    def __init__(self, url: str, **kwargs: Any) -> None:
        if hasattr(self, "url"):
            return
        url = next(glob.iglob(url), "")

        self._poolmemmap = PoolMemMap(url)
        self.url = url
        self._target_type = None
        super().__init__(url)

    def open(self, mode: str = "r", **kwargs: Any) -> None:

        try:
            self._target_type = kwargs["target_type"]
        except KeyError as e:
            raise TypeError(f"Missing configuration parameter: {e}")

        if self._status == StorageStatus.OPEN:
            return

        if self.url == "":
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.url)

        super().open(mode=mode)

        if not self._poolmemmap._loaded:
            self.loadbuffer(self._poolmemmap)
            self._poolmemmap._loaded = True

    def close(self) -> None:

        if self._status == StorageStatus.CLOSE:
            return

        super().close()

        if hasattr(self, "_poolmemmap"):
            self._poolmemmap.close()

    def loadbuffer(self, pool: PoolMemMap) -> None:

        try:
            with open(self.url, "rb") as f:
                pool._buffer = np.fromfile(f, np.dtype("B"))
        except IOError:
            raise IOError(f"Error While Opening {self.url}!")

        pool._packet_length = np.zeros((self.incr_step), dtype="uint")
        pool._packet_offset = np.zeros((self.incr_step), dtype="uint")

        k = 0
        while k < len(pool._buffer):
            if pool._buffer[k] != 12:
                print("error!", k)
                break

            if pool._n_packets == pool._packet_length.shape[0]:
                (pool._packet_length).resize(pool._n_packets + self.incr_step, refcheck=False)
                (pool._packet_offset).resize(pool._n_packets + self.incr_step, refcheck=False)

            pool._packet_offset[pool._n_packets] = k
            pool._packet_length[pool._n_packets] = (
                int.from_bytes(pool._buffer[k + 4 : k + self.primary_header], "big") + self.primary_header + 1  # noqa
            )

            k += int(pool._packet_length[pool._n_packets])
            pool._n_packets += 1

    def parsekey(self, offset_in_bits: int, length_in_bits: int, output_type: Any) -> Any:

        if output_type == "var_bytearray":
            start_byte = offset_in_bits // 8
            parameter = np.zeros(
                (self._poolmemmap._n_packets, np.max(self._poolmemmap._packet_length) - start_byte),
                dtype="uint8",
            )
            for k in range(self._poolmemmap._n_packets):
                end_byte = self._poolmemmap._packet_length[k]
                parameter[k, 0 : end_byte - start_byte] = self._poolmemmap._buffer[  # noqa
                    self._poolmemmap._packet_offset[k]
                    + start_byte : self._poolmemmap._packet_offset[k]  # noqa
                    + end_byte  # noqa
                ]

            return parameter

        elif output_type == "bytearray":
            start_byte = offset_in_bits // 8
            parameter = np.zeros((self._poolmemmap._n_packets, length_in_bits // 8), dtype="uint8")
            for k in range(self._poolmemmap._n_packets):
                end_byte = length_in_bits // 8 + start_byte
                parameter[k,] = self._poolmemmap._buffer[  # noqa
                    self._poolmemmap._packet_offset[k]
                    + start_byte : self._poolmemmap._packet_offset[k]  # noqa
                    + end_byte  # noqa
                ]

            return parameter

        else:
            output_packets = self._poolmemmap._n_packets
            if output_type[:2] == "s_":
                output_packets = 1
                output_type = output_type[2:]
            parameter = np.zeros(output_packets, dtype=output_type)
            start_byte = offset_in_bits // 8
            end_byte = (offset_in_bits + length_in_bits - 1) // 8 + 1
            shift = end_byte * 8 - (offset_in_bits + length_in_bits)
            mask = np.sum(2 ** np.arange(length_in_bits))

            for k in range(output_packets):
                data = (
                    self._poolmemmap._buffer[
                        int(self._poolmemmap._packet_offset[k] + start_byte) : int(  # noqa
                            self._poolmemmap._packet_offset[k] + end_byte,
                        )  # noqa
                    ]
                    >> shift
                )
                parameter[k] = (int.from_bytes(data, "big")) & mask

            return parameter

    def __getitem__(self, key: slice) -> "EOObject": # type: ignore
        """
        This method is used to return eo_variables if parameters value match

        Parameters
        ----------
        key: str
            xpath

        Raise
        ----------
        AttributeError, it the given key doesn't match

        Return
        ----------
        EOVariable
        """
        from eopf.product.core import EOVariable

        offset_in_bits = key.start
        length_in_bits = key.step

        ndarray = self.parsekey(offset_in_bits, length_in_bits, self._target_type)
        if len(ndarray.shape) == 0:
            raise KeyError
        return EOVariable(data=ndarray)

    def __iter__(self) -> Iterator[str]:
        """Has no functionality within this store"""
        yield from ()

    def __len__(self) -> int:
        """Has no functionality within this accessor"""
        return 0

    def __setitem__(self, key: str, value: Any) -> None:
        """Has no functionality within this store"""
        raise NotImplementedError()

    def is_group(self, path: str) -> bool:
        return False

    def is_variable(self, path: str) -> bool:
        return True

    def iter(self, path: str) -> Iterator[str]:
        """Has no functionality within this store"""
        yield from ()

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        raise NotImplementedError()


class FixedMemMapAccessor(EOProductStore):
    def __init__(self, url: str, **kwargs: Any) -> None:

        if hasattr(self, "url"):
            return
        url = next(glob.iglob(url), "")

        self._poolmemmap = PoolMemMap(url)
        self.url = url
        self._target_type = None
        super().__init__(url)

    def open(self, mode: str = "r", **kwargs: Any) -> None:

        try:
            self._target_type = kwargs["target_type"]
        except KeyError as e:
            raise TypeError(f"Missing configuration parameter: {e}")

        if self._status == StorageStatus.OPEN:
            return

        if self.url == "":
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.url)

        super().open(mode=mode)

        if not self._poolmemmap._loaded:
            self.loadbuffer(self._poolmemmap)
            self._poolmemmap._loaded = True

    def close(self) -> None:
        if self._status == StorageStatus.CLOSE:
            return

        super().close()

        if hasattr(self, "_poolmemmap"):
            self._poolmemmap.close()

    def loadbuffer(self, pool: PoolMemMap) -> None:

        try:
            with open(self.url, "rb") as f:
                pool._buffer = np.fromfile(f, np.dtype("B"))
        except IOError:
            raise IOError(f"Error While Opening {self.url}")

    def parsekey(self, offset_in_bits: int, length_in_bits: int, packet_len: int, output_type: Any) -> Any:

        if output_type == "bytearray":
            start_byte = offset_in_bits // 8
            parameter = np.zeros((self._poolmemmap._n_packets, length_in_bits // 8), dtype="uint8")
            for k in range(self._poolmemmap._n_packets):
                end_byte = length_in_bits // 8 + start_byte
                parameter[k,] = self._poolmemmap._buffer[  # noqa
                    packet_len * k + start_byte : packet_len * k + end_byte  # noqa
                ]

            return parameter

        else:
            output_packets = self._poolmemmap._n_packets
            if output_type[:2] == "s_":
                output_packets = 1
                output_type = output_type[2:]
            parameter = np.zeros(output_packets, dtype=output_type)
            start_byte = offset_in_bits // 8
            end_byte = (offset_in_bits + length_in_bits - 1) // 8 + 1
            shift = end_byte * 8 - (offset_in_bits + length_in_bits)
            mask = np.sum(2 ** np.arange(length_in_bits))

            for k in range(output_packets):
                data = (
                    self._poolmemmap._buffer[packet_len * k + start_byte : packet_len * k + end_byte] >> shift  # noqa
                )  # noqa
                parameter[k] = (int.from_bytes(data, "big")) & mask

            return parameter

    def __getitem__(self, key: slice) -> "EOObject": # type: ignore
        """
        This method is used to return eo_variables if parameters value match

        Parameters
        ----------
        key: str
            xpath

        Raise
        ----------
        AttributeError, it the given key doesn't match

        Return
        ----------
        EOVariable
        """
        from eopf.product.core import EOVariable

        offset_in_bits = key.start
        length_in_bits = key.stop - offset_in_bits
        packet_length = key.step
        self._poolmemmap._n_packets = self._poolmemmap._buffer.size // packet_length

        ndarray = self.parsekey(offset_in_bits, length_in_bits, packet_length, self._target_type)
        if len(ndarray.shape) == 0:
            raise KeyError
        return EOVariable(data=ndarray)

    def __iter__(self) -> Iterator[str]:
        """Has no functionality within this store"""
        yield from ()

    def __len__(self) -> int:
        """Has no functionality within this accessor"""
        return 0

    def __setitem__(self, key: str, value: Any) -> None:
        """Has no functionality within this store"""
        raise NotImplementedError()

    def is_group(self, path: str) -> bool:
        return False

    def is_variable(self, path: str) -> bool:
        return True

    def iter(self, path: str) -> Iterator[str]:
        """Has no functionality within this store"""
        yield from ()

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        raise NotImplementedError()
