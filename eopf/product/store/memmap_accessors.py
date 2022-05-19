from typing import TYPE_CHECKING, Any, Iterator, MutableMapping, Optional, TextIO, Union, List

import lxml
import xarray as xr
import glob
import numpy as np
from datetime import datetime

from eopf.exceptions import StoreNotOpenError, XmlParsingError
from eopf.product.store import EOProductStore
from eopf.product.utils import (  # to be reviewed
    apply_xpath,
    parse_xml,
    translate_structure,
)

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject

class MemMapAccessor(EOProductStore):
    def __init__(self, url: str, **kwargs: Any) -> None:
        super().__init__(url)
        self._root = None
        self._url = [p for p in glob.iglob(url)]
        self._offset_in_bits = None
        self._length_in_bits = None
        self._target_type = None

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open()
        if mode != "r":
            raise NotImplementedError()
        try:
            self._offset_in_bits  = kwargs["offset_in_bits"]
            self._length_in_bits = kwargs["length_in_bits"]
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
            self._packet_length[self._n_packets] = int.from_bytes(self._buffer[k + 4:k + 6], "big") + 1

            k += self._packet_length[self._n_packets] + 6
            self._n_packets += 1

    def parsekey(self, offset_in_bits, length_in_bits, outputType):

        if (outputType == 'userData'):
            start_byte = offset_in_bits // 8
            parameter = np.zeros((self._n_packets, np.max(self._packet_length) - start_byte), dtype='uint8')
            for k in range(self._n_packets):
                end_byte = (self._packet_length[k] + 6 - start_byte)
                parameter[k,] = self._buffer[self._packet_offset[k] + start_byte:self._packet_offset[k] + end_byte]

            return parameter

        else:
            parameter = np.zeros(self._n_packets, dtype=outputType)
            start_byte = offset_in_bits // 8
            end_byte = (offset_in_bits + length_in_bits - 1) // 8 + 1
            shift = end_byte * 8 - (offset_in_bits + length_in_bits)
            mask = np.sum(2 ** np.arange(length_in_bits))

            for k in range(self._n_packets):
                data = self._buffer[self._packet_offset[k] + start_byte:self._packet_offset[k] + end_byte] >> shift
                parameter[k] = ((int.from_bytes(data, 'big')) & mask)

            return parameter

    def __getitem__(self, key: str) -> "EOObject":
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

        self.loadbuffer()

        ndarray = parsekey(self._offset_in_bits['value'], self._length_in_bits['value'], self._target_type['name'])
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
        self._url = [p for p in glob.iglob(url)]
        self._offset_in_bits = None
        self._length_in_bits = None
        self._target_type = None
        self._packet_length = None

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open()
        if mode != "r":
            raise NotImplementedError()
        try:
            self._offset_in_bits  = kwargs["offset_in_bits"]
            self._length_in_bits = kwargs["length_in_bits"]
            self._target_type = kwargs["target_type"]
            self._packet_length = kwargs["target_type"]

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

        if (outputType == 'userData'):
            start_byte = offset_in_bits // 8
            parameter = np.zeros((self._n_packets, np.max(self._packet_length) - start_byte), dtype='uint8')
            for k in range(self._n_packets):
                end_byte = (packet_len*k + 6 - start_byte)
                parameter[k,] = self._buffer[self._packet_offset[k] + start_byte:self._packet_offset[k] + end_byte]

            return parameter

        else:
            parameter = np.zeros(self._n_packets, dtype=outputType)
            start_byte = offset_in_bits // 8
            end_byte = (offset_in_bits + length_in_bits - 1) // 8 + 1
            shift = end_byte * 8 - (offset_in_bits + length_in_bits)
            mask = np.sum(2 ** np.arange(length_in_bits))

            for k in range(self._n_packets):
                data = self._buffer[packet_len*k + start_byte:packet_len*k + end_byte] >> shift
                parameter[k] = ((int.from_bytes(data, 'big')) & mask)

            return parameter

    def __getitem__(self, key: str) -> "EOObject":
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

        self.loadbuffer()
        self._n_packets = self._buffer // self._packet_length['value']

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
