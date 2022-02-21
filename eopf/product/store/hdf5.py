from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterable, Iterator, Optional, Dict, Union
import fsspec
import itertools
import contextlib
import h5py
import numpy as np
import xarray as xr
import os

from eopf.exceptions import StoreNotOpenError
from eopf.product.store import EOProductStore
from eopf.exceptions import StoreNotOpenError
from eopf.product.utils import join_path, weak_cache
from ..core import EOGroup, EOProduct, EOVariable

if TYPE_CHECKING:
    from eopf.product.core.eo_object import EOObject


class EOHDF5Store(EOProductStore):

    _fs: Optional[h5py.File] = None
    _root: Optional[h5py.Group] = None
    _root_name: str = None
    sep = "/"

    def __init__(self, url: str, root_name: str = '') -> None:
        super().__init__(url)
        self.url = url
        self._root_name = root_name
        self.current_variable = None

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
        from eopf.product.core import EOGroup, EOVariable

        obj = self._fs[key]
        if self.is_group(key):
            return EOGroup(attrs=obj.attrs)
        return EOVariable(data=obj[()], attrs=obj.attrs)

    def __setitem__(self, key: str, value: "EOObject") -> None:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        from eopf.product.core import EOGroup, EOVariable

        if isinstance(value, EOGroup):
            self._fs.create_group(key)
        elif isinstance(value, EOVariable):
            self._fs.create_dataset(key, value._data)
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

    def listdir(self, path: Optional[str] = None) -> Any:
        """list the given path on the store, or the root if no path.

        Parameters
        ----------
        path: str, optional
            path to list on the store
        """
        raise NotImplementedError()

    def rmdir(self, path: Optional[str] = None) -> None:
        """remove the given path on the store, or the root if no path.

        Parameters
        ----------
        path: str, optional
            path to remove on the store
        """
        raise NotImplementedError()

    def clear(self) -> None:
        """clear all the store from root path"""
        raise NotImplementedError()

    def getsize(self, path: Optional[str] = None) -> Any:
        """return size under the path or root if no path given

        Parameters
        ----------
        path: str, optional
            path to get size on the store
        """
        raise NotImplementedError()

    def dir_path(self, path: Optional[str] = None) -> Any:
        """return directory path of the given path or root

        Parameters
        ----------
        path: str, optional
            path to get directory on the store
        """
        raise NotImplementedError()
    
    def add_group(self, name: str, relative_path: Iterable[str] = []) -> None:
        """write a group over the store

        Parameters
        ----------
        name: str
            name of the group
        relative_path: Iterable[str], optional
            list of all parents from root
        """
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        self._fs.create_group(join_path(*relative_path, name, sep=self.sep))

    def add_variables(self, name: str, dataset: xr.Dataset, relative_path: Iterable[str] = []) -> None:
        """write variables over the store

        Parameters
        ----------
        name: str
            name of the dataset
        relative_path: Iterable[str], optional
            list of all parents from root
        """
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        self.current_variable = self._fs.create_dataset(join_path(*relative_path, name, sep=self.sep), data=dataset)

    def _h5_group(self, eogroup: EOGroup, relative_path: Iterable[str] = []) -> None:
        """
        Iterate through groups in EOProduct and creates hdf5 groups, variables, datasets and attributes
        Parameters
        ----------
        eogroup: the source group in EOProduct
        relative_path: path of the new group
        ----------
        """
       
        self.add_group(eogroup.name, relative_path)

        json_dict = EOHDF5Store.json_serializable_dict_of(eogroup.attrs)
        #self._set_attr(current_node, json_dict)
        
        for key, eovar in eogroup.variables:
            json_dict_ = EOHDF5Store.json_serializable_dict_of(eovar.attrs)

            #if eovar.dims:
            da = xr.DataArray(data=eovar._data.values)

            if json_dict_ is not None:
                if "_FillValue" in json_dict_:
                    da.fillna(json_dict_["_FillValue"])
            
            if eovar._data.dtype !=  np.dtype('O'):
                
                self.add_variables(eogroup.name+'/'+eovar.name, da, relative_path)
                self._set_attr(self.current_variable, json_dict_)
                #self.write_attrs(eogroup.name+'/'+eovar.name, json_dict_)

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
        self._fs.create_group(self._root_name)
        path:Iterable = ([self._root_name])

        # eogroup: Union[EOVariable, "EOGroup"] = product._get_group("attributes")
        # if isinstance(eogroup, EOGroup) and eogroup is not None:
        #    self._h5_group(f, root, eogroup)
        eogroup1: Union[EOVariable, "EOGroup"] = product._get_item("coordinates")
        if isinstance(eogroup1, EOGroup):
            self._h5_group(eogroup1, path)
        eogroup2: Union[EOVariable, "EOGroup"] = product._get_item("measurements")
        if isinstance(eogroup2, EOGroup):
            self._h5_group(eogroup2, path)
        eogroup3: Union[EOVariable, "EOGroup"] = product._get_item("quality")
        if isinstance(eogroup3, EOGroup) and eogroup3 is not None:
            self._h5_group(eogroup3, path)
        eogroup4: Union[EOVariable, "EOGroup"] = product._get_item("conditions")
        if isinstance(eogroup4, EOGroup) and eogroup4 is not None:
            self._h5_group(eogroup4, path)
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
        dict = EOHDF5Store.json_dict()
        if dump_type == "var":
                vrs = EOHDF5Store.json_dict()
                dict = self._get_dict_vars(self._fs.get(group), vrs)
        elif dump_type == "gr":
                gr = EOHDF5Store.json_dict()
                dict = self._get_dict_group(self._fs.get(group), gr)
        self.close()
        return dict

    def _set_attr(self, h5_node: h5py.Group, attrs: Any) -> None:
        """
        Set attributes to a node
        Parameters:
        ----------
        h5_node: the HDF5 node to which it is added attributes
        attrs: the attributes to be added
        ----------
        """
        for attr in attrs:
            h5_node.attrs[attr] = attrs[attr]

    @staticmethod
    def json_serializable_dict_of(attrs: Any) -> Any:
        """
        Creates a JSON dict from attributes
        Parameters
        ----------
        attrs: attributes
        ----------
        """
        result = dict()
        for attr in attrs:
            value = attrs[attr]
            if isinstance(value, np.integer):
                value = int(value)
            elif isinstance(value, np.floating):
                value = float(value)
            elif isinstance(value, np.ndarray):
                value = value.tolist()
            result[attr] = value
        return result

    @staticmethod
    def json_dict() -> Any:
        """
        Creates an empty JSON dict
        """
        result = dict()
        result[""] = ""
        return result