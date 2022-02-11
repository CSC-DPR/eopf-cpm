import glob
import os

import h5py
import pytest

from eopf.product.convert import OLCIL1EOPConverter, SLSTRL1EOPConverter
from eopf.product.store.hdf5_store import EOHDF5Store

from .utils import diff_s3_hdf5, get_duplicate_s3_vars, get_s3_vars


@pytest.mark.unit
def test_verify_non_duplicate_variables_persistence_olci_l1_in_hdf5_format():
    """Given a legacy OLCI L1 product, considering only variables that are unique by name,
    the information from these variables must persist in the built hdf5: name, attributes, coordinates and data"""

    # Tested on the product:
    # S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3
    # On 10th February 2022
    # Only verifies variable names
    olci_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    olci_vars = get_s3_vars(olci_path)
    olci_duplicates = get_duplicate_s3_vars(olci_path)
    olci_eop = OLCIL1EOPConverter(olci_path)
    read_ok = olci_eop.read()
    assert read_ok, "The product should be read"

    hdf5_path = "data/prod.hdf5"
    if os.path.exists(hdf5_path):
        os.remove(hdf5_path)
    h5 = EOHDF5Store(hdf5_path)
    h5.write(olci_eop.eop)
    hdf5_vars = h5.h5dict()

    _, vars_not_found = diff_s3_hdf5(olci_vars, hdf5_vars, olci_duplicates)
    assert len(vars_not_found) == 0


@pytest.mark.unit
def test_verify_duplicate_variables_persistence_olci_l1_in_hdf5_format():
    """Given a legacy OLCI L1 product, considering only variables that are unique by name,
    the information from these variables must persist in the built hdf5: name, attributes, coordinates and data"""

    # Tested on the product:
    # S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3
    # On 10th February 2022
    # Only verifies variable names

    olci_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    olci_eop = OLCIL1EOPConverter(olci_path)
    read_ok = olci_eop.read()
    assert read_ok, "The product should be read"
    hdf5_path = "data/prod.hdf5"

    if os.path.exists(hdf5_path):
        os.remove(hdf5_path)
    h5 = EOHDF5Store(hdf5_path)
    h5.write(olci_eop.eop)
    h5f = h5py.File(hdf5_path, "r")

    var_name = "Oa01_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa02_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa03_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa04_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa05_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa06_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa07_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa08_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa09_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa10_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa11_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa12_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa13_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa14_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa15_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa16_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa17_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa18_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa19_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa20_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "Oa21_radiance"
    assert h5f.get(f"data/prod.hdf5/measurements/radiances/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "SZA"
    assert h5f.get(f"data/prod.hdf5/conditions/geometry/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "altitude"
    assert h5f.get(f"data/prod.hdf5/coordinates/image_grid/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "longitude"
    assert h5f.get(f"data/prod.hdf5/coordinates/image_grid/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/coordinates/tie_point_grid/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "latitude"
    assert h5f.get(f"data/prod.hdf5/coordinates/image_grid/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/coordinates/tie_point_grid/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "detector_index"
    assert h5f.get(f"data/prod.hdf5/conditions/instrument_data/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None

    var_name = "quality_flags"
    assert h5f.get(f"data/prod.hdf5/quality/{var_name}") is not None
    assert h5f.get(f"data/prod.hdf5/measurements/orphans/{var_name}") is not None


@pytest.mark.unit
def test_verify_non_duplicate_variables_persistence_slstr_l1_in_hdf5_format():
    """Given a legacy OLCI L1 product, considering only variables that are unique by name,
    the information from these variables must persist in the built hdf5: name, attributes, coordinates and data"""

    # Tested on the product:
    # S3A_SL_1_RBT____20220118T083600_20220118T083900_20220118T110259_0179_081_064_2160_LN2_O_NR_004.SEN3
    # On 10th February 2022
    # Only verifies variable names
    slstr_path = glob.glob("data/S3A_SL_1_RBT*.SEN3")[0]
    slstr_vars = get_s3_vars(slstr_path)
    slstr_duplicates = get_duplicate_s3_vars(slstr_path)
    slstr_eop = SLSTRL1EOPConverter(slstr_path)
    read_ok = slstr_eop.read()
    assert read_ok, "The product should be read"

    hdf5_path = "data/prod.hdf5"
    if os.path.exists(hdf5_path):
        os.remove(hdf5_path)
    h5 = EOHDF5Store(hdf5_path)
    h5.write(slstr_eop.eop)
    hdf5_vars = h5.h5dict()

    _, vars_not_found = diff_s3_hdf5(slstr_vars, hdf5_vars, slstr_duplicates)
    assert len(vars_not_found) == 0
