from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterable, Iterator, Optional, Dict, Union

import contextlib
import h5py
import xarray as xr

from eopf.exceptions import StoreNotOpenError
from eopf.product.store import EOProductStore
from eopf.exceptions import StoreNotOpenError
from eopf.product.utils import join_path, weak_cache
from ..core.eo_product import EOProduct
from ..core.eo_variable import EOVariable
from ..core.eo_group import EOGroup

if TYPE_CHECKING:
    from eopf.product.core.eo_object import EOObject


class EOHDF5Store(EOProductStore):

    _fs: Optional[h5py.File] = None
    _root: Optional[h5py.Group] = None
    sep = "/"

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.url = url

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open(mode=mode, **kwargs)
        self._fs = h5py.File(self.url, mode=mode)
        if mode == "r":
            self._root: h5py.Group = self._fs.get('/')

    def close(self) -> None:
        super().close()
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before close")
        self._fs.close()
        self._fs = None
        self._root = None

    def is_group(self, path: str) -> bool:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return isinstance(self._fs[path], h5py.Group)

    def is_variable(self, path: str) -> bool:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return isinstance(self._fs[path], h5py.Dataset)

    def write_attrs(self, obj_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if obj_path == "":
            obj_path = "/"
        self._fs[obj_path].attrs.update(attrs)

    def iter(self, path: str) -> Iterator[str]:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return iter(self._fs[path])

    def __getitem__(self, key: str) -> "EOObject":
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")

        obj = self._fs[key]
        if self.is_group(key):
            return EOGroup(attrs=obj.attrs)
        return EOVariable(data=obj[()], attrs=obj.attrs)

    def __setitem__(self, key: str, value: "EOObject") -> None:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")

        if isinstance(value, EOGroup):
            self._fs.create_group(key)
        elif isinstance(value, EOVariable):
            da = xr.DataArray(data=value._data)
            ds = self._fs.create_dataset(key, data=da)
        else:
            raise TypeError("Only EOGroup and EOVariable can be set")
        self.write_attrs(key, value.attrs)

    def __iter__(self) -> Iterator[str]:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return iter(self._fs)

    def __len__(self) -> int:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return len(self._fs)

    def _h5_group(self, eogroup: EOGroup, relative_path: Iterable[str] = []) -> None:
        """
        Iterate through groups in EOProduct and creates hdf5 groups, variables, datasets and attributes
        Parameters
        ----------
        eogroup: the source group in EOProduct
        relative_path: path of the new group
        ----------
        """

        self.__setitem__(join_path(*relative_path, eogroup.name, sep=self.sep), eogroup)

        for key, eovar in eogroup.variables:
            self.__setitem__(join_path(*relative_path, eogroup.name+'/'+eovar.name, sep=self.sep), eovar)
           
        path: Iterable = (relative_path + [eogroup.name])
        if eogroup.groups is not None:
            for key, g in eogroup.groups:
                self._h5_group(g, path)

    def write(self, product: EOProduct) -> None:
        """
        Creates HDF5 file from groups in EOProduct by iteration for each group in rout of EOProduct
        Parameters
        ----------
        product: the EOProduct
        ----------
        """
        self.open("w")
        path:Iterable = (["/"])

        for name, group in product._groups.items():
            self._h5_group(group, path)
    
        self.close()

    def _descend_obj_all(self, obj: h5py.Group, sep: str = "\t") -> None:
        """
        Iterate through groups in a HDF5 file and prints the groups and datasets names and datasets attributes
        Parameters:
        ----------
        obj: the group to be dumped
        sep: the separator to print
        ----------
        """
        if type(obj) in [h5py._hl.group.Group, h5py._hl.files.File]:
            for key in obj.keys():
                print(sep, "-", key, ":", obj[key])
                self._descend_obj_all(obj[key], sep=sep + "\t")
        elif type(obj) == h5py._hl.dataset.Dataset:
            for key in obj.attrs.keys():
                print(sep + "\t", "-", key, ":", obj.attrs[key])

            
    def _descend_obj_gr(self, obj: h5py.Group, sep: str = "\t") -> None:
        """
        Iterate through groups in a HDF5 file and prints the name of groups
        Parameters:
        ----------
        obj: the group to be dumped
        sep: the separator to print
        ----------
        """
        if type(obj) in [h5py._hl.group.Group, h5py._hl.files.File]:
            print(sep, "-", obj.name)
            for key in obj.keys():
                self._descend_obj_gr(obj[key], sep=sep + "\t")

    def _descend_obj_var(self, obj: h5py.Group, sep: str = "\t") -> None:
        """
        Iterate through groups in a HDF5 file and prints the variables and datasets attributes
        Parameters:
        ----------
        obj: the group to be dumped
        sep: the separator to print
        ----------
        """
        if type(obj) in [h5py._hl.group.Group, h5py._hl.files.File]:
            for key in obj.keys():
                if type(obj[key]) == h5py._hl.dataset.Dataset:
                    print(sep + obj.name)
                self._descend_obj_var(obj[key], sep=sep + "\t")
        elif type(obj) == h5py._hl.dataset.Dataset:
            print(sep, "-", str(obj))
            for key in obj.attrs.keys():
                print(sep + "\t", "-", key, ":", obj.attrs[key])

    def h5dump(self, output_file_path: str = "", dump_type: str = "all", group: str = "/") -> None:
        """
        Print HDF5 file metadata
        Parameters:
        ----------
        group: the group to be dumped; you can give a specific group, defaults to the root group
        dump_type: the type of dump:
                   - all(default) - print name of group(s) and datasets names and datasets attributes
                   - gr - print the name of group(s)
                   - var - print all variables and datasets attributes
        output_file_path: the dump file
                            - "" - dump to console
                            - specific path to a text file
        ---------
        """
        self.open()
        if dump_type == "all":
            if output_file_path != "":
                with open(output_file_path, "w") as f:
                    with contextlib.redirect_stdout(f):
                        self._descend_obj_all(self._fs.get(group))
            else:
                self._descend_obj_all(self._fs.get(group))
        elif dump_type == "gr":
            if output_file_path != "":
                with open(output_file_path, "w") as f:
                    with contextlib.redirect_stdout(f):
                        self._descend_obj_gr(self._fs.get(group))
            else:
                self._descend_obj_gr(self._fs.get(group))
        elif dump_type == "var":
            if output_file_path != "":
                with open(output_file_path, "w") as f:
                    with contextlib.redirect_stdout(f):
                        self._descend_obj_var(self._fs.get(group))
            else:
                self._descend_obj_var(self._fs.get(group))
        self.close()

    def _get_dict_group(self, obj: h5py.Group, gr_dict: Dict[str, str] = {}) -> Dict[str, str]:
        """
        Iterate through groups in a HDF5 file and creates a dictionary of groups
        Parameters
        ----------
        obj: the group
        sep: the separator to print
        ----------
        Return: dictionary
        ----------
        """

        if type(obj) in [h5py._hl.group.Group, h5py._hl.files.File]:
            name = obj.name
            gr_dict[name] = name
            for key in obj.keys():
                gr_dict = self._get_dict_group(obj[key], gr_dict)

        return gr_dict

    def _get_dict_vars(self, obj: h5py.Group, vars: Dict[str, str] = {}) -> Dict[str, str]:
        """
        Iterate through group(s) in a HDF5 file and creates a dictionary of variables
        Parameters:
        ----------
        obj: the group
        sep: the separator to print
        ----------
        Return: dictionary
        ----------
        """
        if type(obj) in [h5py._hl.group.Group, h5py._hl.files.File]:
            for key in obj.keys():
                if type(obj[key]) == h5py._hl.dataset.Dataset:
                    str_dict = ""
                    for key_ in obj[key].attrs.keys():
                        str_dict = str_dict + key_ + "=" +str(obj[key].attrs[key_]) + ", "
                    vars[key] = str_dict
                vars = self._get_dict_vars(obj[key], vars)
        return vars

    def h5dict(self, dump_type: str = "var", group: str = "/") -> Dict[str, str]:
        """
        Creates a dictionary of items from HDF5 file
        group: the group where become the dictionary; you can give a specific group, defaults to the root group
        dump_type: the type of dump:
                   - gr - for group(s)
                   - var - for variables (default)
        ---------
        Return: dictionary
        ----------
        """
        self.open()
        dict = EOHDF5Store.json_empty_dict()
        if dump_type == "var":
                dict = self._get_dict_vars(self._fs.get(group), dict)
        elif dump_type == "gr":
                dict = self._get_dict_group(self._fs.get(group), dict)
        self.close()
        return dict

    @staticmethod
    def json_empty_dict() -> Any:
        """
        Creates an empty JSON dict
        """
        result = dict()
        result[""] = ""
        return result