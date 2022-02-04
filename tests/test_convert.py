import glob
import os

import pytest
import xarray as xr
from utils import diff_vars, get_duplicate_s3_vars, get_eop_vars, get_s3_vars

from eopf.product.convert import OLCIL1EOPConverter, SLSTRL1EOPConverter

# python3 -m pytest -v test_convert.py


@pytest.mark.unit
def test_verify_non_duplicate_variables_persistence_olci_l1():
    olci_path = glob.glob("../data/S3A_OL_1*.SEN3")[0]
    olci_vars = get_s3_vars(olci_path)
    olci_duplicates = get_duplicate_s3_vars(olci_path)
    olci_eop = OLCIL1EOPConverter(olci_path)
    _ = olci_eop.read()
    # assert read_ok, "The product should be read"
    olci_eop_vars = get_eop_vars(olci_eop.eop)
    _, vars_not_found = diff_vars(olci_vars, olci_eop_vars, olci_duplicates)
    assert len(vars_not_found) == 0, "There should be no elements in list vars_not_found"


def test_verify_duplicate_variables_persistence_olci_l1():
    olci_path = glob.glob("../data/S3A_OL_1*.SEN3")[0]
    olci_duplicates = get_duplicate_s3_vars(olci_path)
    olci_eop = OLCIL1EOPConverter(olci_path)
    _ = olci_eop.read()
    # assert read_ok == True, "The product should be read"

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
    eop_var = olci_eop.eop.measurements.radiances.Oa01_radiance._data
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


@pytest.mark.unit
def test_verify_non_duplicate_variables_persistence_slstr_l1():
    slstr_path = glob.glob("data/S3A_SL_1_RBT*.SEN3")[0]
    slstr_vars = get_s3_vars(slstr_path)
    slstr_duplicates = get_duplicate_s3_vars(slstr_path)
    slstr_eop = OLCIL1EOPConverter(slstr_path)
    _ = slstr_eop.read()
    # assert read_ok, "The product should be read"
    slstr_eop_vars = get_eop_vars(slstr_eop.eop)
    _, vars_not_found = diff_vars(slstr_vars, slstr_eop_vars, slstr_duplicates)
    assert len(vars_not_found) == 0, "There should be no elements in list vars_not_found"


def test_verify_duplicate_variables_persistence_slstr_l1():
    slstr_path = glob.glob("../data/S3A_SL_1_RBT*.SEN3")[0]
    slstr_duplicates = get_duplicate_s3_vars(slstr_path)
    slstr_eop = SLSTRL1EOPConverter(slstr_path)
    _ = slstr_eop.read()
    # assert read_ok == True, "The product should be read"

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
