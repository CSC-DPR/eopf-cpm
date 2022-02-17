from typing import Any, Iterable, Iterator, Optional, Dict, Union

import fsspec
import itertools
import contextlib
import h5py
import numpy as np
import xarray as xr
import os

from .abstract import EOProductStore
from eopf.exceptions import StoreNotOpenError
from eopf.product.utils import join_path, weak_cache
from ..core import EOGroup, EOProduct, EOVariable

class EOHDF5Store(EOProductStore):


    _root: Optional[h5py.Group] = None
    _root_name: str = None
    _fs: h5py = None
    sep = "/"

    def __init__(self, url: str, root_name: str = '') -> None:
        super().__init__(url)
        self.url = url
        self._root_name = root_name
        self.current_node: h5py.Group = None
        self.current_variable = None

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        """open the store in the given mode

        Parameters
        ----------
        mode: str, optional
            mode to open the store
        **kwargs: Any
            extra kwargs of open on zarr librairy
        """

        super().open()
        self._fs = h5py.File(self.url, mode)
        if mode == "r":
            self._root: h5py.Group = self._fs.get('/')
        
    def close(self) -> None:
        """close the store"""
        super().close()
        self._root = None
        if self._fs is not None:
            self._fs.close()
            self._fs = None

    def iter(self, path: str) -> Iterator[str]:
        """iter over the given path"""
        raise NotImplementedError()

    def map(self) -> fsspec.FSMap:
        """FSMap accessor"""
        raise NotImplementedError()

    def __getitem__(self, key: str) -> tuple[str, list[str], Optional[Any], Any]:
        raise NotImplementedError()

    def __setitem__(self, key: Any, value: Any) -> None:
        """"""
        raise NotImplementedError()
        

    def __delitem__(self, key: str) -> None:
        """"""
        raise NotImplementedError()
        
    def __len__(self) -> int:
        """"""
        raise NotImplementedError()

    def __iter__(self) -> Iterator[str]:
        """"""
        raise NotImplementedError()

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

    def is_group(self, path: str) -> bool:
        """check if the given path under root corresponding to a group representation

        Parameters
        ----------
        path: str
            path to check
        """
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        obj = self._fs.get(path)
        if type(obj) in [h5py._hl.group.Group, h5py._hl.files.File]:
            return True
        return False

    def is_variable(self, path: str) -> bool:
        """check if the given path under root corresponding to a group representation

        Parameters
        ----------
        path: str
            path to check
        """
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        obj = self._fs.get(path)
        if type(obj) in [h5py._hl.dataset.Dataset, h5py._hl.files.File]:
            return True
        return False

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
        #self.current_node = self._fs.create_group(join_path(*relative_path, name))
        self.current_node = self._fs.create_group(join_path(self._root_name, *relative_path, name))
        #self.current_node = self._fs.create_group(os.path.join(self._root_name, *relative_path, name))


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
        #self.current_variable = self.current_node.create_dataset(join_path(*relative_path, name), data=dataset)
        self.current_variable = self.current_node.create_dataset(join_path(self._root_name, *relative_path, name), data=dataset)
        #self.current_variable = self.current_node.create_dataset(os.path.join(self._root_name, *relative_path, name), data=dataset)
        

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

        #json_dict = EOHDF5Store.json_serializable_dict_of(eogroup.attrs)
        #self._set_attr(current_node, json_dict)

        path = itertools.chain(relative_path, [eogroup.name])
        for key, eovar in eogroup.variables:
            json_dict_ = EOHDF5Store.json_serializable_dict_of(eovar.attrs)

            #if eovar.dims:
            da = xr.DataArray(data=eovar._data.values)

            if json_dict_ is not None:
                if "_FillValue" in json_dict_:
                    da.fillna(json_dict_["_FillValue"])
            
            if eovar._data.dtype !=  np.dtype('O'):
                
                self.add_variables(eovar.name, da, path)
                self._set_attr(self.current_variable, json_dict_)

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
        path:Iterable = ([''])

        # eogroup: Union[EOVariable, "EOGroup"] = product._get_group("attributes")
        # if isinstance(eogroup, EOGroup) and eogroup is not None:
        #    self._h5_group(f, root, eogroup)
        eogroup1: Union[EOVariable, "EOGroup"] = product._get_group("coordinates")
        if isinstance(eogroup1, EOGroup):
            self._h5_group(eogroup1, path)
        eogroup2: Union[EOVariable, "EOGroup"] = product._get_group("measurements")
        if isinstance(eogroup2, EOGroup):
            self._h5_group(eogroup2, path)
        eogroup3: Union[EOVariable, "EOGroup"] = product._get_group("quality")
        if isinstance(eogroup3, EOGroup) and eogroup3 is not None:
            self._h5_group(eogroup3, path)
        eogroup4: Union[EOVariable, "EOGroup"] = product._get_group("conditions")
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
                    #for attr in obj[key].attrs:
                    #    str_dict = str_dict + str(attr)+ " "
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
