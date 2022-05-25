from typing import TYPE_CHECKING, Any, Iterator, MutableMapping

import numpy as np

from eopf.product.store import EOProductStore


if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject

class MemMapAccessor(EOProductStore):

    incr_step      = 10000
    primary_header = 6

    def __init__(self, url: str, **kwargs: Any) -> None:
        super().__init__(url)
        self._root = None
        self._url = url
        self._target_type = None
        self._n_packets = 0

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open()
        if mode != "r":
            raise NotImplementedError()
        try:
            self._target_type = kwargs["target_type"]
        except KeyError as e:
            raise TypeError(f"Missing configuration parameter: {e}")

        self._root = self._url

    def loadbuffer(self):

        # read data

        try:
            with open(self._url, "rb") as f:
                self._buffer = np.fromfile(f, np.dtype('B'))
        except IOError:
            print('Error While Opening the file!')

        self._packet_length = np.zeros((self.incr_step), dtype='uint')
        self._packet_offset = np.zeros((self.incr_step), dtype='uint')

        k = 0
        while k < len(self._buffer):
            if (self._buffer[k] != 12):
                print("error!", k)
                break

            if (self._n_packets == self._packet_length.shape[0]):
                (self._packet_length).resize(self._n_packets + self.incr_step, refcheck=False)
                (self._packet_offset).resize(self._n_packets + self.incr_step, refcheck=False)

            self._packet_offset[self._n_packets] = k
            self._packet_length[self._n_packets] = int.from_bytes(self._buffer[k + 4:k + primary_header], "big") + primary_header + 1

            k += self._packet_length[self._n_packets]
            self._n_packets += 1

    def parsekey(self, offset_in_bits, length_in_bits, outputType):

        if (outputType == 'var_bytearray'):
            start_byte = offset_in_bits // 8
            parameter = np.zeros((self._n_packets, np.max(self._packet_length) - start_byte), dtype='uint8')
            for k in range(self._n_packets):
                end_byte = (self._packet_length[k])
                parameter[k,end_byte-start_byte] = self._buffer[self._packet_offset[k] + start_byte:self._packet_offset[k] + end_byte]  # noqa

            return parameter

        else:
            output_packets = self._n_packets
            if (outputType[:2] == "s_"):
                outputpackets = 1
                outputType = outputType[2:]
            parameter = np.zeros(output_packets, dtype=outputType)
            start_byte = offset_in_bits // 8
            end_byte = (offset_in_bits + length_in_bits - 1) // 8 + 1
            shift = end_byte * 8 - (offset_in_bits + length_in_bits)
            mask = np.sum(2 ** np.arange(length_in_bits))

            for k in range(output_packets):
                data = self._buffer[self._packet_offset[k] + start_byte:self._packet_offset[k] + end_byte] >> shift
                parameter[k] = ((int.from_bytes(data, 'big')) & mask)

            return parameter

    def __getitem__(self, key: slice) -> "EOObject":
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

        self.loadbuffer()

        ndarray = parsekey(offset_in_bits, length_in_bits, self._target_type)
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
        super().__init__(url)
        self._root = None
        self._url = url
        self._target_type = None

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open()
        if mode != "r":
            raise NotImplementedError()
        try:
            self._target_type = kwargs["target_type"]

        except KeyError as e:
            raise TypeError(f"Missing configuration parameter: {e}")

        self._root = self._url

    def loadbuffer(self):

        # read data

        try:
            with open(self._url, "rb") as f:
                self._buffer = np.fromfile(f, np.dtype('B'))
        except IOError:
            print('Error While Opening the file!')

    def parsekey(self, offset_in_bits, length_in_bits, packet_len, outputType):

        if (outputType == 'bytearray'):
            start_byte = offset_in_bits // 8
            parameter = np.zeros((self._n_packets, length_in_bits//8), dtype='uint8')
            for k in range(self._n_packets):
                end_byte = (packet_len*k - start_byte)
                parameter[k,] = self._buffer[packet_len*k + start_byte:packet_len*k + end_byte]

            return parameter

        else:
            output_packets = self._n_packets
            if (outputType[:2] == "s_"):
                outputpackets = 1
                outputType=outputType[2:]
            parameter = np.zeros(output_packets, dtype=outputType)
            start_byte = offset_in_bits // 8
            end_byte = (offset_in_bits + length_in_bits - 1) // 8 + 1
            shift = end_byte * 8 - (offset_in_bits + length_in_bits)
            mask = np.sum(2 ** np.arange(length_in_bits))

            for k in range(output_packets):
                data = self._buffer[packet_len*k + start_byte:packet_len*k + end_byte] >> shift
                parameter[k] = ((int.from_bytes(data, 'big')) & mask)

            return parameter

    def __getitem__(self, key: slice) -> "EOObject":
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
        length_in_bits = key.stop
        packet_length = key.step

        self.loadbuffer()
        self._n_packets = self._buffer.size // self._packet_length['value']

        ndarray = parsekey(self._offset_in_bits['value'], self._length_in_bits['value'], self._packet_length['value'], self._target_type['name'])
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

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        return False

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        return True

    def iter(self, path: str) -> Iterator[str]:
        """Has no functionality within this store"""
        yield from ()

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        raise NotImplementedError()
