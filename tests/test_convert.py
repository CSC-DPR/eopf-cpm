import glob
import os

import pytest
import xarray as xr

from eopf.product.constants import (
    CF_MAP_OLCI_L1,
    CF_MAP_SLSTR_L1,
    EOP_MAP_OLCI_L1,
    EOP_MAP_SLSTR_L1,
    NAMESPACES_OLCI_L1,
    NAMESPACES_SLSTR_L1,
)
from eopf.product.conveniences import (
    apply_xpath,
    etree_to_dict,
    parse_xml,
    translate_structure,
)
from eopf.product.convert import OLCIL1EOPConverter, SLSTRL1EOPConverter

from .utils import (  # cmp_s3_zarr_var,; diff_s3_zarr,; get_zarr_vars,
    diff_s3_eop,
    get_duplicate_s3_vars,
    get_eop_vars,
    get_s3_vars,
)


@pytest.fixture
def olci_manifest():
    olci_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    metadata_path = os.path.join(olci_path, "xfdumanifest.xml")
    metadata_file = open(metadata_path)
    tree = parse_xml(metadata_file)
    return tree


@pytest.fixture
def slstr_manifest():
    slstr_path = glob.glob("data/S3A_SL_1_RBT*.SEN3")[0]
    metadata_path = os.path.join(slstr_path, "xfdumanifest.xml")
    metadata_file = open(metadata_path)
    tree = parse_xml(metadata_file)
    return tree


@pytest.mark.usecase
def test_harmozied_structure_olci_l1_in_eop_format():
    """Given a legacy OLCI L1 product,
    the eop must respect the harmozied data structure"""

    olci_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    olci_eop = OLCIL1EOPConverter(olci_path)
    read_ok = olci_eop.read()
    assert read_ok, "The product should be read"
    olci_eop_top_level_groups = olci_eop.eop._groups.keys()
    assert "measurements" in olci_eop_top_level_groups
    assert "coordinates" in olci_eop_top_level_groups
    allowed_top_level_groups = ["measurements", "coordinates", "quality", "conditions"]
    for group in olci_eop_top_level_groups:
        assert group in allowed_top_level_groups


@pytest.mark.usecase
def test_verify_non_duplicate_variables_persistence_olci_l1_in_eop_format():
    """Given a legacy OLCI L1 product, considering only variables that are unique by name,
    the information from these variables must persist in the built eop: name, attributes, coordinates and data"""

    # Tested on the product:
    # S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3
    # On 7th February 2022
    olci_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    olci_vars = get_s3_vars(olci_path)
    olci_duplicates = get_duplicate_s3_vars(olci_path)
    olci_eop = OLCIL1EOPConverter(olci_path)
    read_ok = olci_eop.read()
    assert read_ok, "The product should be read"
    olci_eop_vars = get_eop_vars(olci_eop.eop)
    _, vars_not_found = diff_s3_eop(olci_vars, olci_eop_vars, olci_duplicates)
    assert len(vars_not_found) == 0, "There should be no elements in list vars_not_found"


@pytest.mark.usecase
def test_verify_duplicate_variables_persistence_olci_l1_in_eop_format():
    """Given a legacy OLCI L1 product, considering only variables that are not unique by name,
    the information from these variables must persist in the built eop: name, attributes, coordinates and data"""

    # Tested on the product:
    # S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3
    # On 7th February 2022
    olci_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    olci_duplicates = get_duplicate_s3_vars(olci_path)
    olci_eop = OLCIL1EOPConverter(olci_path)
    read_ok = olci_eop.read()
    assert read_ok, "The product should be read"

    s3_var_file = "Oa01_radiance.nc"
    s3_var_name = "Oa01_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa01_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa01_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa01_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa02_radiance.nc"
    s3_var_name = "Oa02_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa02_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa02_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa02_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa03_radiance.nc"
    s3_var_name = "Oa03_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa03_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa03_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa03_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa04_radiance.nc"
    s3_var_name = "Oa04_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa04_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa04_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa04_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa05_radiance.nc"
    s3_var_name = "Oa05_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa05_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa05_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa05_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa06_radiance.nc"
    s3_var_name = "Oa06_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa06_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa06_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa06_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa07_radiance.nc"
    s3_var_name = "Oa07_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa07_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa07_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa07_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa08_radiance.nc"
    s3_var_name = "Oa08_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa08_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa08_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa08_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa09_radiance.nc"
    s3_var_name = "Oa09_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa09_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa09_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa09_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa10_radiance.nc"
    s3_var_name = "Oa10_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa10_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa10_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa10_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa11_radiance.nc"
    s3_var_name = "Oa11_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa11_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa11_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa11_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa12_radiance.nc"
    s3_var_name = "Oa12_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa12_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa12_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa12_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa13_radiance.nc"
    s3_var_name = "Oa13_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa13_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa13_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa13_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa14_radiance.nc"
    s3_var_name = "Oa14_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa14_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa14_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa14_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa15_radiance.nc"
    s3_var_name = "Oa15_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa15_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa15_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa15_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa16_radiance.nc"
    s3_var_name = "Oa16_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa16_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa16_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa16_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa17_radiance.nc"
    s3_var_name = "Oa17_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa17_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa17_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa17_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa18_radiance.nc"
    s3_var_name = "Oa18_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa18_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa18_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa18_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa19_radiance.nc"
    s3_var_name = "Oa19_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa19_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa19_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa19_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa20_radiance.nc"
    s3_var_name = "Oa20_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa20_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa20_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa20_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "Oa21_radiance.nc"
    s3_var_name = "Oa21_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.radiances.Oa21_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "Oa21_radiance"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.Oa21_radiance._data
    assert s3_var.equals(eop_var)

    s3_var_file = "tie_geometries.nc"
    s3_var_name = "SZA"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.conditions.geometry.SZA._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "SZA"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.SZA._data
    assert s3_var.equals(eop_var)

    s3_var_file = "geo_coordinates.nc"
    s3_var_name = "altitude"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.coordinates.image_grid.altitude._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "altitude"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.altitude._data
    assert s3_var.equals(eop_var)

    s3_var_file = "geo_coordinates.nc"
    s3_var_name = "longitude"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.coordinates.image_grid.longitude._data
    assert s3_var.equals(eop_var)

    s3_var_file = "tie_geo_coordinates.nc"
    s3_var_name = "longitude"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.coordinates.tie_point_grid.longitude._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "longitude"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.longitude._data
    assert s3_var.equals(eop_var)

    s3_var_file = "geo_coordinates.nc"
    s3_var_name = "latitude"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.coordinates.image_grid.latitude._data
    assert s3_var.equals(eop_var)

    s3_var_file = "tie_geo_coordinates.nc"
    s3_var_name = "latitude"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.coordinates.tie_point_grid.latitude._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "latitude"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.latitude._data
    assert s3_var.equals(eop_var)

    s3_var_file = "instrument_data.nc"
    s3_var_name = "detector_index"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.conditions.instrument_data.detector_index._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "detector_index"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.detector_index._data
    assert s3_var.equals(eop_var)

    s3_var_file = "qualityFlags.nc"
    s3_var_name = "quality_flags"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.quality.quality_flags._data
    assert s3_var.equals(eop_var)

    s3_var_file = "removed_pixels.nc"
    s3_var_name = "quality_flags"
    assert s3_var_name in olci_duplicates
    olci_file = os.path.join(olci_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = olci_eop.eop.measurements.orphans.quality_flags._data
    assert s3_var.equals(eop_var)


@pytest.mark.usecase
def test_harmozied_structure_slstr_l1_in_eop_format():
    """Given a legacy SLSTR L1 product,
    the eop must respect the harmozied data structure"""

    slstr_path = glob.glob("data/S3A_SL_1_RBT*.SEN3")[0]
    slstr_eop = SLSTRL1EOPConverter(slstr_path)
    read_ok = slstr_eop.read()
    assert read_ok, "The product should be read"
    slstr_eop_top_level_groups = slstr_eop.eop._groups.keys()
    assert "measurements" in slstr_eop_top_level_groups
    assert "coordinates" in slstr_eop_top_level_groups
    allowed_top_level_groups = ["measurements", "coordinates", "quality", "conditions"]
    for group in slstr_eop_top_level_groups:
        assert group in allowed_top_level_groups


# @pytest.mark.usecase
# def test_verify_non_duplicate_variables_persistence_slstr_l1_in_eop_format():
#     """Given a legacy SLSTR L1 product, considering only variables that are unique by name,
#     the information from these variables must persist in the built eop: name, attributes, coordinates and data"""

#     # Tested on the product:
#     # S3A_SL_1_RBT____20220118T083600_20220118T083900_20220118T110259_0179_081_064_2160_LN2_O_NR_004.SEN3
#     # On 7th February 2022
#     slstr_path = glob.glob("data/S3A_SL_1_RBT*.SEN3")[0]
#     slstr_vars = get_s3_vars(slstr_path)
#     slstr_duplicates = get_duplicate_s3_vars(slstr_path)
#     slstr_eop = SLSTRL1EOPConverter(slstr_path)
#     read_ok = slstr_eop.read()
#     assert read_ok, "The product should be read"
#     slstr_eop_vars = get_eop_vars(slstr_eop.eop)
#     _, vars_not_found = diff_s3_eop(slstr_vars, slstr_eop_vars, slstr_duplicates)
#     assert len(vars_not_found) == 0, "There should be no elements in list vars_not_found"


@pytest.mark.usecase
def test_verify_duplicate_variables_persistence_slstr_l1_in_eop_format():
    """Given a legacy SLSTR L1 product, considering only variables that are not unique by name,
    the information from these variables must persist in the built eop: name, attributes, coordinates and data"""

    # Tested on the product:
    # S3A_SL_1_RBT____20220118T083600_20220118T083900_20220118T110259_0179_081_064_2160_LN2_O_NR_004.SEN3
    # On 7th February 2022
    slstr_path = glob.glob("data/S3A_SL_1_RBT*.SEN3")[0]
    slstr_duplicates = get_duplicate_s3_vars(slstr_path)
    slstr_eop = SLSTRL1EOPConverter(slstr_path)
    read_ok = slstr_eop.read()
    assert read_ok, "The product should be read"

    s3_var_file = "time_an.nc"
    s3_var_name = "SCANSYNC"
    assert s3_var_name in slstr_duplicates
    olci_file = os.path.join(slstr_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = slstr_eop.eop.conditions.time_a.SCANSYNC._data
    assert s3_var.equals(eop_var)

    s3_var_file = "time_bn.nc"
    s3_var_name = "SCANSYNC"
    assert s3_var_name in slstr_duplicates
    olci_file = os.path.join(slstr_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = slstr_eop.eop.conditions.time_b.SCANSYNC._data
    assert s3_var.equals(eop_var)

    s3_var_file = "time_in.nc"
    s3_var_name = "SCANSYNC"
    assert s3_var_name in slstr_duplicates
    olci_file = os.path.join(slstr_path, s3_var_file)
    file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
    s3_var = file_ds.get(s3_var_name)
    eop_var = slstr_eop.eop.conditions.time_i.SCANSYNC._data
    assert s3_var.equals(eop_var)


# @pytest.mark.usecase
# def test_harmozied_structure_olci_l1_in_zarr_format():
#     """Given a legacy OLCI L1 product,
#     the zarr must respect the harmozied data structure"""

#     olci_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
#     olci_eop = OLCIL1EOPConverter(olci_path)
#     read_ok = olci_eop.read()
#     assert read_ok, "The product should be read"
#     zarr_path = "data/olci_zarr"
#     olci_eop.write(zarr_path)

#     top_level_zarr_groups_pattern = os.path.join(zarr_path, "*")
#     top_level_zarr_groups = glob.glob(top_level_zarr_groups_pattern)
#     group_names = []
#     for group_path in top_level_zarr_groups:
#         assert os.path.isdir(group_path), "Must be a directory"
#         if group_path[-1] == os.path.sep:
#             group_name = os.path.basename(group_path[:-1])
#         else:
#             group_name = os.path.basename(group_path)
#         group_names.append(group_name)

#     assert "measurements" in group_names
#     assert "coordinates" in group_names
#     allowed_top_level_groups = ["measurements", "coordinates", "quality", "conditions"]
#     for group in group_names:
#         assert group in allowed_top_level_groups


# @pytest.mark.usecase
# def test_verify_non_duplicate_variables_persistence_olci_l1_in_zarr_format():
#     """Given a legacy OLCI L1 product, considering only variables that are unique by name,
#     the information from these variables must persist in zarr format: name, attributes, coordinates and data"""

#     # Tested on the product:
#     # S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3
#     # The persistence of variables coordinates is not checked since they are not yet populated in zarr
#     # On 8th February 2022
#     olci_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
#     olci_vars = get_s3_vars(olci_path)
#     olci_duplicates = get_duplicate_s3_vars(olci_path)
#     olci_eop = OLCIL1EOPConverter(olci_path)
#     read_ok = olci_eop.read()
#     assert read_ok, "The product should be read"
#     zarr_path = "data/olci_zarr"
#     olci_eop.write(zarr_path)
#     olci_zarr_vars = get_zarr_vars(zarr_path)
#     _, vars_not_found = diff_s3_zarr(olci_vars, olci_zarr_vars, olci_duplicates)
#     assert len(vars_not_found) == 0, "There should be no elements in list vars_not_found"


# @pytest.mark.usecase
# def test_verify_duplicate_variables_persistence_olci_l1_in_zarr_format():
#     """Given a legacy OLCI L1 product, considering only variables that are not unique by name,
#     the information from these variables must persist in zarr format: name, attributes, coordinates and data"""

#     # Tested on the product:
#     # S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3
#     # The persistence of variables coordinates is not checked since they are not yet populated in zarr
#     # On 8th February 2022
#     olci_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
#     olci_duplicates = get_duplicate_s3_vars(olci_path)
#     olci_eop = OLCIL1EOPConverter(olci_path)
#     read_ok = olci_eop.read()
#     assert read_ok, "The product should be read"
#     zarr_path = "data/slstr_zarr"
#     olci_eop.write(zarr_path)

#     s3_var_file = "Oa01_radiance.nc"
#     s3_var_name = "Oa01_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa01_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa02_radiance.nc"
#     s3_var_name = "Oa02_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa02_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa03_radiance.nc"
#     s3_var_name = "Oa03_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa03_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa04_radiance.nc"
#     s3_var_name = "Oa04_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa04_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa05_radiance.nc"
#     s3_var_name = "Oa05_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa05_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa06_radiance.nc"
#     s3_var_name = "Oa06_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa06_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa07_radiance.nc"
#     s3_var_name = "Oa07_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa07_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa08_radiance.nc"
#     s3_var_name = "Oa08_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa08_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa09_radiance.nc"
#     s3_var_name = "Oa09_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa09_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa10_radiance.nc"
#     s3_var_name = "Oa10_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa10_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa11_radiance.nc"
#     s3_var_name = "Oa11_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa11_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa12_radiance.nc"
#     s3_var_name = "Oa12_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa12_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa13_radiance.nc"
#     s3_var_name = "Oa13_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa13_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa14_radiance.nc"
#     s3_var_name = "Oa14_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa14_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa15_radiance.nc"
#     s3_var_name = "Oa15_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa15_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa16_radiance.nc"
#     s3_var_name = "Oa16_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa16_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa17_radiance.nc"
#     s3_var_name = "Oa17_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa17_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa18_radiance.nc"
#     s3_var_name = "Oa18_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa18_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa19_radiance.nc"
#     s3_var_name = "Oa19_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa19_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa20_radiance.nc"
#     s3_var_name = "Oa20_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa20_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "Oa21_radiance.nc"
#     s3_var_name = "Oa21_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/radiances")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "Oa21_radiance"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "tie_geometries.nc"
#     s3_var_name = "SZA"
#     s3_zarr_path = os.path.join(zarr_path, "conditions/geometry")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "SZA"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "geo_coordinates.nc"
#     s3_var_name = "altitude"
#     s3_zarr_path = os.path.join(zarr_path, "coordinates/image_grid")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "altitude"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "geo_coordinates.nc"
#     s3_var_name = "longitude"
#     s3_zarr_path = os.path.join(zarr_path, "coordinates/image_grid")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "tie_geo_coordinates.nc"
#     s3_var_name = "longitude"
#     s3_zarr_path = os.path.join(zarr_path, "coordinates/tie_point_grid")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "longitude"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "geo_coordinates.nc"
#     s3_var_name = "latitude"
#     s3_zarr_path = os.path.join(zarr_path, "coordinates/image_grid")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "tie_geo_coordinates.nc"
#     s3_var_name = "latitude"
#     s3_zarr_path = os.path.join(zarr_path, "coordinates/tie_point_grid")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "latitude"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "instrument_data.nc"
#     s3_var_name = "detector_index"
#     s3_zarr_path = os.path.join(zarr_path, "conditions/instrument_data")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "detector_index"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "qualityFlags.nc"
#     s3_var_name = "quality_flags"
#     s3_zarr_path = os.path.join(zarr_path, "quality")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "removed_pixels.nc"
#     s3_var_name = "quality_flags"
#     s3_zarr_path = os.path.join(zarr_path, "measurements/orphans")
#     assert s3_var_name in olci_duplicates
#     olci_file = os.path.join(olci_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)


# @pytest.mark.usecase
# def test_harmozied_structure_slstr_l1_in_zarr_format():
#     """Given a legacy OLCI L1 product,
#     the zarr must respect the harmozied data structure"""

#     slstr_path = glob.glob("data/S3A_SL_1_RBT*.SEN3")[0]
#     slstr_eop = SLSTRL1EOPConverter(slstr_path)
#     read_ok = slstr_eop.read()
#     assert read_ok, "The product should be read"
#     zarr_path = "data/olci_zarr"
#     slstr_eop.write(zarr_path)

#     top_level_zarr_groups_pattern = os.path.join(zarr_path, "*")
#     top_level_zarr_groups = glob.glob(top_level_zarr_groups_pattern)
#     group_names = []
#     for group_path in top_level_zarr_groups:
#         assert os.path.isdir(group_path), "Must be a directory"
#         if group_path[-1] == os.path.sep:
#             group_name = os.path.basename(group_path[:-1])
#         else:
#             group_name = os.path.basename(group_path)
#         group_names.append(group_name)

#     assert "measurements" in group_names
#     assert "coordinates" in group_names
#     allowed_top_level_groups = ["measurements", "coordinates", "quality", "conditions"]
#     for group in group_names:
#         assert group in allowed_top_level_groups


# @pytest.mark.usecase
# def test_verify_non_duplicate_variables_persistence_slstr_l1_in_zarr_format():
#     """Given a legacy SLSTR L1 product, considering only variables that are unique by name,
#     the information from these variables must persist in zarr format: name, attributes, coordinates and data"""

#     # Tested on the product:
#     # S3A_SL_1_RBT____20220118T083600_20220118T083900_20220118T110259_0179_081_064_2160_LN2_O_NR_004.SEN3
#     # The persistence of variables coordinates is not checked since they are not yet populated in zarr
#     # On 8th February 2022
#     slstr_path = glob.glob("data/S3A_SL_1_RBT*.SEN3")[0]
#     slstr_vars = get_s3_vars(slstr_path)
#     slstr_duplicates = get_duplicate_s3_vars(slstr_path)
#     slstr_eop = SLSTRL1EOPConverter(slstr_path)
#     read_ok = slstr_eop.read()
#     assert read_ok, "The product should be read"
#     zarr_path = "data/slstr_zarr"
#     slstr_eop.write(zarr_path)
#     slstr_zarr_vars = get_zarr_vars(zarr_path)
#     _, vars_not_found = diff_s3_zarr(slstr_vars, slstr_zarr_vars, slstr_duplicates)
#     assert len(vars_not_found) == 0, "There should be no elements in list vars_not_found"


# @pytest.mark.usecase
# def test_verify_duplicate_variables_persistence_slstr_l1_in_zarr_format():
#     """Given a legacy SLSTR L1 product, considering only variables that are not unique by name,
#     the information from these variables must persist in zarr format: name, attributes, coordinates and data"""

#     # Tested on the product:
#     # S3A_SL_1_RBT____20220118T083600_20220118T083900_20220118T110259_0179_081_064_2160_LN2_O_NR_004.SEN3
#     # The persistence of variables coordinates is not checked since they are not yet populated in zarr
#     # On 8th February 2022
#     slstr_path = glob.glob("data/S3A_SL_1_RBT*.SEN3")[0]
#     slstr_duplicates = get_duplicate_s3_vars(slstr_path)
#     slstr_eop = SLSTRL1EOPConverter(slstr_path)
#     read_ok = slstr_eop.read()
#     assert read_ok, "The product should be read"
#     zarr_path = "data/slstr_zarr"
#     slstr_eop.write(zarr_path)

#     s3_var_file = "time_an.nc"
#     s3_var_name = "SCANSYNC"
#     s3_zarr_path = os.path.join(zarr_path, "conditions/time_a")
#     assert s3_var_name in slstr_duplicates
#     olci_file = os.path.join(slstr_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "time_bn.nc"
#     s3_var_name = "SCANSYNC"
#     s3_zarr_path = os.path.join(zarr_path, "conditions/time_b")
#     assert s3_var_name in slstr_duplicates
#     olci_file = os.path.join(slstr_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)

#     s3_var_file = "time_in.nc"
#     s3_var_name = "SCANSYNC"
#     s3_zarr_path = os.path.join(zarr_path, "conditions/time_i")
#     assert s3_var_name in slstr_duplicates
#     olci_file = os.path.join(slstr_path, s3_var_file)
#     file_ds = xr.open_dataset(olci_file, decode_times=False, mask_and_scale=False)
#     s3_var = file_ds.get(s3_var_name)
#     zarr_ds = xr.open_zarr(s3_zarr_path, decode_times=False, mask_and_scale=False)
#     zarr_var = zarr_ds.get(s3_var_name)
#     assert cmp_s3_zarr_var(s3_var, zarr_var)


@pytest.mark.usecase
def test_xml_persistance_olci(olci_manifest):
    """Gigen a legacy OLCI L1 product,
    the legacy information from the xfdumanifest must persist in the built eop"""

    olci_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    olci_eop = OLCIL1EOPConverter(olci_path)
    olci_eop.read()

    # Legacy attributes
    root = olci_manifest.getroot()
    xfdu_dict = etree_to_dict(root[1])
    cf_dict = {attr: apply_xpath(olci_manifest, CF_MAP_OLCI_L1[attr], NAMESPACES_OLCI_L1) for attr in CF_MAP_OLCI_L1}
    eop_dict = {
        attr: translate_structure(EOP_MAP_OLCI_L1[attr], olci_manifest, NAMESPACES_OLCI_L1) for attr in EOP_MAP_OLCI_L1
    }

    # EOProduct attributes
    cf_eop_dict = olci_eop.eop.attrs["CF"]
    om_eop_dict = olci_eop.eop.attrs["OM-EOP"]
    xfdu_eop_dict = olci_eop.eop.attrs["XFDU"]

    assert xfdu_dict == xfdu_eop_dict
    assert cf_dict == cf_eop_dict
    assert eop_dict == om_eop_dict


@pytest.mark.usecase
def test_xml_persistance_slstr(slstr_manifest):
    """Gigen a legacy SLSTR L1 product,
    the legacy information from the xfdumanifest must persist in the built eop"""

    slstr_path = glob.glob("data/S3A_SL_1_RBT*.SEN3")[0]
    slstr_eop = SLSTRL1EOPConverter(slstr_path)
    slstr_eop.read()

    # Legacy attributes
    root = slstr_manifest.getroot()
    xfdu_dict = etree_to_dict(root[1])
    cf_dict = {
        attr: apply_xpath(slstr_manifest, CF_MAP_SLSTR_L1[attr], NAMESPACES_SLSTR_L1) for attr in CF_MAP_SLSTR_L1
    }
    eop_dict = {
        attr: translate_structure(EOP_MAP_SLSTR_L1[attr], slstr_manifest, NAMESPACES_SLSTR_L1)
        for attr in EOP_MAP_SLSTR_L1
    }  # noqa

    # EOProduct attributes
    cf_eop_dict = slstr_eop.eop.attrs["CF"]
    om_eop_dict = slstr_eop.eop.attrs["OM-EOP"]
    xfdu_eop_dict = slstr_eop.eop.attrs["XFDU"]

    assert xfdu_dict == xfdu_eop_dict
    assert cf_dict == cf_eop_dict
    assert eop_dict == om_eop_dict
