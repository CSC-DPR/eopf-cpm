import datetime
import re
from typing import TYPE_CHECKING, Any, Iterator, NamedTuple, Optional

import fsspec

from eopf.exceptions import StoreNotOpenError
from eopf.product.core.eo_variable import EOVariable
from eopf.product.store.abstract import EOReadOnlyStore

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject


class FilePart(NamedTuple):
    """Data class to aggregate file information for :obj:`FilenameToVariableAccessor`"""

    value: int
    start_time: datetime.datetime
    end_time: datetime.datetime

    @classmethod
    def from_string(cls, string: str) -> "FilePart":
        """Create instance of FilePart from the filename as a string"""
        time_format = "%Y%m%dt%H%M%S"
        file_type_str, start_time_str, end_time_str = re.findall(
            r".{3}-(.{2,3})-ocn-vv-(.*)-(.*)-\d{6}-\w{6}-\d{3}\.nc",
            string,
        )[0]
        return cls(
            int(file_type_str[-1]) if len(file_type_str) == 3 else 0,
            datetime.datetime.strptime(start_time_str, time_format),
            datetime.datetime.strptime(end_time_str, time_format),
        )


# MB: filenames of a wave mode product have several files in the measurements directory.
# (I though that we have that with image mode already, but it seems this is not true.)
# The information on the sub-swath is the second element of the file name.
# Some are wv1, others are wv2. For an imaging mode the file name contains either iw or ew in this position.
# The filename_to_subswath accessor shall translate the file name into a variable with a
# dimension that is the number of files and with values 0, 1 or 2 depending on the file name of the respective file.
# The accessor shall sort the files by time.


class FilenameToVariableAccessor(EOReadOnlyStore):
    """Convert Filename in measurement of legacy product to a specific variable"""

    _fsmap: Optional[fsspec.FSMap] = None

    def open(self, mode: str = "r", storage_options: dict[str, Any] = {}, **kwargs: Any) -> None:
        super().open(mode, **kwargs)
        self._fsmap = fsspec.get_mapper(self.url, **storage_options)

    def close(self) -> None:
        super().close()
        self._fsmap = None

    def __getitem__(self, key: str) -> "EOObject":
        self.check_node(key)
        files = (f.rpartition("/")[-1] for f in self._fsmap.keys())  # type: ignore[union-attr]
        data = [d.value for d in sorted(map(FilePart.from_string, files), key=lambda x: x.start_time)]
        dim = len(data)
        return EOVariable(data=data, dims=(str(dim),))

    def __len__(self) -> int:
        return 0

    def iter(self, path: str) -> Iterator[str]:
        self.check_node(path)
        return iter([])

    def is_group(self, path: str) -> bool:
        self.check_node(path)
        return False

    def is_variable(self, path: str) -> bool:
        self.check_node(path)
        return True

    def check_node(self, path: str) -> None:
        if self._fsmap is None:
            raise StoreNotOpenError()
        if path not in ["", self._fsmap.fs.sep]:
            raise KeyError
