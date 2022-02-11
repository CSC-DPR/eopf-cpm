import contextlib
import glob
import os
import os.path

import h5py
import pytest

from eopf.product.convert import OLCIL1EOPConverter
from eopf.product.store.hdf5_store import EOHDF5Store


def _get_path(path):
    if path == "path_to_product":
        return glob.glob(
            "data/S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3",
        )[0]
    if path == "path_to_hdf5_file":
        return "data/prod.hdf5"
    if path == "path_to_output_file":
        return "data/out.txt"
    if path == "path_to_group":
        return "data/prod.hdf5/coordinates"


def _file_exists(path_to_file):
    return os.path.exists(path_to_file)


def _check_item(item):
    f = open(_get_path("path_to_output_file"), "r")
    for line in f:
        if item in line:
            return True
    return False


def _hdf5_write():
    file_path = _get_path("path_to_hdf5_file")
    if _file_exists(file_path):
        os.remove(file_path)
    s3_olci_l0 = OLCIL1EOPConverter(_get_path("path_to_product"))
    s3_olci_l0.read()
    h5 = EOHDF5Store(file_path)
    h5.write(s3_olci_l0.eop)


@pytest.mark.hdf5_test
def test_hdf5_write():
    """Test write function"""
    _hdf5_write()
    file_path = _get_path("path_to_hdf5_file")
    file_exists = _file_exists(file_path)
    assert file_exists, "HDF5 file " + file_path + " is not created"


@pytest.mark.hdf5_test
def test_hdf5_h5dump_all():
    """Test h5dump create output file for all items in hdf5 file"""
    file_path = _get_path("path_to_hdf5_file")
    if not os.path.isfile(file_path):
        _hdf5_write()
    h5 = EOHDF5Store(file_path)
    output_path = _get_path("path_to_output_file")
    h5.h5dump(output_path, group="/")
    file_exists = _file_exists(output_path)
    assert file_exists, "Output file " + output_path + " is not created"


@pytest.mark.hdf5_test
def test_hdf5_h5dump_all_variables():
    """Test h5dump ceate output file for variables in hdf5 file"""
    file_path = _get_path("path_to_hdf5_file")
    if not os.path.isfile(file_path):
        _hdf5_write()
    h5 = EOHDF5Store(file_path)
    output_path = _get_path("path_to_output_file")
    h5.h5dump(output_path, dump_type="var", group="/")
    file_exists = _file_exists(output_path)
    assert file_exists, "Output file " + output_path + " is not created"


@pytest.mark.hdf5_test
def test_hdf5_h5dump_group_all():
    """Test h5dump create output file for a group in hdf5 file"""
    file_path = _get_path("path_to_hdf5_file")
    if not os.path.isfile(file_path):
        _hdf5_write()
    h5 = EOHDF5Store(file_path)
    output_path = _get_path("path_to_output_file")
    h5.h5dump(output_path, group=_get_path("path_to_group"))
    file_exists = _file_exists(output_path)
    assert file_exists, "Output file " + output_path + " is not created"


@pytest.mark.hdf5_test
def test_hdf5_h5dump_group_variables():
    """Test h5dump create output file for variables of specifyed group in hdf5 file"""
    file_path = _get_path("path_to_hdf5_file")
    if not os.path.isfile(file_path):
        _hdf5_write()
    h5 = EOHDF5Store(file_path)
    output_path = _get_path("path_to_output_file")
    h5.h5dump(output_path, dump_type="var", group=_get_path("path_to_group"))
    file_exists = _file_exists(output_path)
    assert file_exists, "Output file " + output_path + " is not created"


@pytest.mark.hdf5_test
def test_hdf5_descend_obj_all():
    """Test _descend_obj_all for all items in hdf5 file"""
    path_file = _get_path("path_to_hdf5_file")
    with open(_get_path("path_to_output_file"), "w") as ff:
        with contextlib.redirect_stdout(ff):
            with h5py.File(path_file, "r") as f:
                h5 = EOHDF5Store(path_file)
                obj: h5py.Group = f.get("/")
                h5._descend_obj_all(obj, sep="\t")
    check_result = _check_item("coordinates")
    assert check_result, "coordinates group is not in HDF5 file"


@pytest.mark.hdf5_test
def test_hdf5_descend_obj_gr():
    """Test _descend_obj_gr for all groups in hdf5 file"""
    path_file = _get_path("path_to_hdf5_file")
    with open(_get_path("path_to_output_file"), "w") as ff:
        with contextlib.redirect_stdout(ff):
            with h5py.File(path_file, "r") as f:
                h5 = EOHDF5Store(path_file)
                obj: h5py.Group = f.get("/")
                h5._descend_obj_gr(obj, sep="\t")
    check_result = _check_item("coordinates")
    assert check_result, "coordinates group is not in HDF5 file"


@pytest.mark.hdf5_test
def test_hdf5_descend_obj_var():
    """Test _descend_obj_var for all variables in hdf5 file"""
    path_file = _get_path("path_to_hdf5_file")
    with open(_get_path("path_to_output_file"), "w") as ff:
        with contextlib.redirect_stdout(ff):
            with h5py.File(path_file, "r") as f:
                h5 = EOHDF5Store(path_file)
                obj: h5py.Group = f.get("/")
                h5._descend_obj_var(obj, sep="\t")
    check_result = _check_item("altitude")
    assert check_result, "altitude variable is not in HDF5 file"


@pytest.mark.hdf5_test
def test_hdf5_get_dict_vars():
    """Test _get_dict_vars for dictionary of all variables in hdf5 file"""
    f = h5py.File(_get_path("path_to_hdf5_file"), "r")
    vars = {}
    obj: h5py.Group = f.get("/")
    h5 = EOHDF5Store(_get_path("path_to_hdf5_file"))
    dict = h5._get_dict_vars(obj, vars)
    assert "altitude" in dict, "altitude variable is not in dictionary"


@pytest.fixture(scope="session", autouse=True)
def cleanup(request):
    """Cleanup testing files."""

    def remove_test_files():
        os.remove(_get_path("path_to_hdf5_file"))
        os.remove(_get_path("path_to_output_file"))

    request.addfinalizer(remove_test_files)
