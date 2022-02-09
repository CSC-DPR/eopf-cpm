from xmlrpc.client import boolean
import pytest
import os
import os.path
import contextlib
#from pathlib import Path
from pathlib import Path
import glob
import abc
import h5py
from eopf.product.store.hdf5_store import EOHDF5Store
from eopf.product.store.convert import S3L1EOPConverter, OLCIL1EOPConverter
from eopf.product.core import EOProduct,EOVariable,EOGroup

from typing import (
    Any,
    Callable,
    ItemsView,
    Hashable,
    Iterable,
    Iterator,
    MutableMapping,
    Mapping,
    Optional,
    Type,
    Union,
)

def _get_path(path):
    if path == "path_to_product":
        return glob.glob("../data/S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3")[0]
    if path == "path_to_hdf5_file":
        return glob.glob("../data/prod.hdf5")[0]
    if path == "path_to_output_file":
        return glob.glob("../data/out.txt")[0]
    if path == "path_to_group":    
        return "data/prod.hdf5/coordinates"

def _file_exists(path_to_file):
    #path = Path(path_to_file)
    #return path.is_file()
    return os.path.isfile(path_to_file)

def _check_item(item):
    f = open(_get_path(path_to_output_file), "r")
    for line in f:
        if item in line:
             return True
    return False

def _hdf5_write():
    file_path = _get_path(path_to_hdf5_file)
    if _file_exists(file_path):
        os.remove(file_path)
    s3_olci_l0 = OLCIL1EOPConverter(_get_path(path_to_product))
    s3_olci_l0.build_eop()
    h5 = EOHDF5Store(file_path)
    h5.write(s3_olci_l0.eop)

@pytest.mark.hdf5_test
def test_hdf5_write():
    _hdf5_write()
    file_path = _get_path(path_to_hdf5_file)
    file_exists = _file_exists(file_path)
    assert not file_exists, "HDF5 file " + file_path + " is not created"

@pytest.mark.hdf5_test
def test_hdf5_h5dump_all():
    file_path = _get_path(path_to_hdf5_file)
    if not os.path.isfile(file_path):
        _hdf5_write()
    h5 = EOHDF5Store(file_path)
    output_path = _get_path(path_to_output_file)
    h5.h5dump(output_path, group='/')
    file_exists = _file_exists(output_path)
    assert not file_exists, "Output file "+output_path + " is not created"

@pytest.mark.hdf5_test
def test_hdf5_h5dump_all_variables():
    file_path = _get_path(path_to_hdf5_file)
    if not os.path.isfile(_get_path(file_path)):
        _hdf5_write()
    h5 = EOHDF5Store(file_path)
    output_path = _get_path(path_to_output_file)
    h5.h5dump(output_path, dump_type='var', group='/')
    file_exists = _file_exists(output_path)
    assert not file_exists, "Output file "+output_path + " is not created"

@pytest.mark.hdf5_test
def test_hdf5_h5dump_group_all():
    file_path = _get_path(path_to_hdf5_file)
    if not os.path.isfile(file_path):
        _hdf5_write()
    h5 = EOHDF5Store(file_path)
    output_path = _get_path(path_to_output_file)
    h5.h5dump(path_to_output_file, group=path_to_group)
    file_exists = _file_exists(path_to_output_file)
    assert not file_exists, "Output file "+path_to_output_file + " is not created"

@pytest.mark.hdf5_test
def test_hdf5_h5dump_group_variables():
    file_path = _get_path(path_to_hdf5_file)
    if not os.path.isfile(path_to_hdf5_file):
        _hdf5_write()
    h5 = EOHDF5Store(path_to_hdf5_file)
    output_path = _get_path(path_to_output_file)
    h5.h5dump(path_to_output_file, dump_type='var', group=path_to_group)
    file_exists = _file_exists(path_to_output_file)
    assert not file_exists, "Output file "+path_to_output_file + " is not created"

@pytest.mark.hdf5_test
def test_hdf5_descend_obj_all():
    f = h5py.File(_get_path(path_to_hdf5_file), "r")
    with open(_get_path(path_to_output_file), "w") as ff:
        with contextlib.redirect_stdout(ff):
            EOHDF5Store._descend_obj_all(f.get('/'))
    check_result = _check_item("coordinates")
    assert not check_result, "coordinates group is not in HDF5 file"

@pytest.mark.hdf5_test
def test_hdf5_descend_obj_gr():
    f = h5py.File(_get_path(path_to_hdf5_file), "r")
    with open(_get_path(path_to_output_file), "w") as ff:
        with contextlib.redirect_stdout(ff):
            EOHDF5Store._descend_obj_gr(f.get('/'))
    check_result = _check_item("coordinates")
    assert not check_result, "coordinates group is not in HDF5 file"

@pytest.mark.hdf5_test
def test_hdf5_descend_obj_var():
    f = h5py.File(_get_path(path_to_hdf5_file), "r")
    with open(_get_path(path_to_output_file), "w") as ff:
        with contextlib.redirect_stdout(ff):
            EOHDF5Store._descend_obj_var(f.get('/'))
    check_result = _check_item("altitude")
    assert not check_result, "altitude variable is not in HDF5 file"

@pytest.mark.hdf5_test
def test_hdf5_descend_obj_var():
    f = h5py.File(_get_path(path_to_hdf5_file), "r")
    dict = EOHDF5Store._get_dict_vars()
    
    assert not "altitude" in dict, "altitude variable is not in dictionary"

