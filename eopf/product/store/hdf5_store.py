""""
Define HDF5 Store Object for EOProduct.
"""

import contextlib
from abc import abstractclassmethod, abstractmethod
from typing import (
    Any,
    Callable,
    Dict,
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Type,
    Union,
)

import h5py
import numpy as np
import xarray as xr

from ..core import EOGroup, EOProduct, EOVariable

# import dask
# from dask.distributed import LocalCluster, Client


class EOHDF5Store:
    def __init__(self, store: str = "") -> None:
        """
        Init of the EOHDF5Store object
        Parameters:
        ----------
        store: the path to the HDF5 file
        ----------
        """
        self._store = store
        # self.cluster = LocalCluster( scheduler_port = 8786 , n_workers = 2 )

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

    # @dask.delayed
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

    # @dask.delayed
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

    # @dask.delayed
    def _descend_obj_var(self, obj: h5py.Group, sep: str = "\t") -> None:
        """
        Iterate through groups in a HDF5 file and prints the variables and datasets attributes
        Parameters:
        ----------
        obj: the group to be dumped
        sep: the separator to print
        ----------
        """
        print(sep + obj.name)
        if type(obj) in [h5py._hl.group.Group, h5py._hl.files.File]:
            for key in obj.keys():
                if type(obj[key]) == h5py._hl.dataset.Dataset:
                    print(sep + "\t", "-", key)
                self._descend_obj_var(obj[key], sep=sep + "\t")
        elif type(obj) == h5py._hl.dataset.Dataset:
            for key in obj.attrs.keys():
                print(sep + "\t", "-", key, ":", obj.attrs[key])

    # @dask.delayed
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
        if dump_type == "all":
            if output_file_path != "":
                with open(output_file_path, "w") as f:
                    with contextlib.redirect_stdout(f):
                        ff = h5py.File(self._store, "r")
                        self._descend_obj_all(ff.get(group))
            else:
                with h5py.File(self._store, "r") as ff:
                    self._descend_obj_all(ff.get(group))
        elif dump_type == "gr":
            if output_file_path != "":
                with open(output_file_path, "w") as f:
                    with contextlib.redirect_stdout(f):
                        with h5py.File(self._store, "r") as ff:
                            self._descend_obj_gr(ff.get(group))
            else:
                with h5py.File(self._store, "r") as ff:
                    self._descend_obj_gr(ff.get(group))
        elif dump_type == "var":
            if output_file_path != "":
                with open(output_file_path, "w") as f:
                    with contextlib.redirect_stdout(f):
                        with h5py.File(self._store, "r") as ff:
                            self._descend_obj_var(ff.get(group))
            else:
                with h5py.File(self._store, "r") as ff:
                    self._descend_obj_var(ff.get(group))

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
                    vars[key] = key
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
        dict = EOHDF5Store.json_dict()
        if dump_type == "var":
            with h5py.File(self._store, "r") as f:
                vrs = EOHDF5Store.json_dict()
                dict = self._get_dict_vars(f.get(group), vrs)
        elif dump_type == "gr":
            with h5py.File(self._store, "r") as f:
                gr = EOHDF5Store.json_dict()
                dict = self._get_dict_group(f.get(group), gr)
        return dict

    def _h5_group(self, f: h5py.File, parent_node: h5py.Group, eogroup: EOGroup) -> None:
        """
        Iterate through groups in EOProduct and creates hdf5 groups, variables, datasets and attributes
        Parameters
        ----------
        eogroup: the source group in EOProduct
        parent_node: the parent group of the HDF5 group that will be stored
        ----------
        """
        if eogroup is None:
            return
        current_node = parent_node.create_group(eogroup.name)

        json_dict = EOHDF5Store.json_serializable_dict_of(eogroup.attrs)
        self._set_attr(current_node, json_dict)

        for key, eovar in eogroup.variables:
            json_dict_ = EOHDF5Store.json_serializable_dict_of(eovar.attrs)
            self._set_attr(current_node, json_dict_)

            if eovar.dims:
                da = xr.DataArray(data=eovar._data.values)
                if json_dict_ != None:
                    if "_FillValue" in json_dict_:
                        da.fillna(json_dict_["_FillValue"])
                    h5_var = current_node.create_dataset(eovar.name, data=da)
                    self._set_attr(h5_var, json_dict_)

        if eogroup.groups != None:
            for key, g in eogroup.groups:
                self._h5_group(f, current_node, g)

    def write(self, product: EOProduct) -> None:
        """
        Creates HDF5 file from groups in EOProduct by iteration for each group in rout of EOProduct
        Parameters
        ----------
        product: the EOProduct
        ----------
        """
        f = h5py.File(self._store, "w")
        root = f.create_group(self._store)

        # eogroup: Union[EOVariable, "EOGroup"] = product._get_group("attributes")
        # if isinstance(eogroup, EOGroup) and eogroup is not None:
        #    self._h5_group(f, root, eogroup)
        eogroup1: Union[EOVariable, "EOGroup"] = product._get_group("coordinates")
        if isinstance(eogroup1, EOGroup):
            self._h5_group(f, root, eogroup1)
        eogroup2: Union[EOVariable, "EOGroup"] = product._get_group("measurements")
        if isinstance(eogroup2, EOGroup):
            self._h5_group(f, root, eogroup2)
        eogroup3: Union[EOVariable, "EOGroup"] = product._get_group("quality")
        if isinstance(eogroup3, EOGroup) and eogroup3 is not None:
            self._h5_group(f, root, eogroup3)
        eogroup4: Union[EOVariable, "EOGroup"] = product._get_group("conditions")
        if isinstance(eogroup4, EOGroup) and eogroup4 is not None:
            self._h5_group(f, root, eogroup4)
        f.close()

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
        # result = {}
        result[""] = ""
        return result

        #    @staticmethod
        #    def create_cluster_client()->None:
        """
        Creates a cluster and client dask and return them
        """
        #        print("Create cluster")
        #        cluster = LocalCluster( scheduler_port = 33211 , n_workers = 2 )
        #        print("Create client")
        #         client = Client(cluster)
        #         return cluster, client

        #    @staticmethod
        #    def close_cluster_client(cluster: LocalCluster, client: Client)->None:
        """
        Closes a cluster and client dask
        Parameters
        ----------
        cluster: the cluster to be closed
        """


#        cluster.close()
#        client.close()
