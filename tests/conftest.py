import json
import os
import shutil
from datetime import timedelta

import pytest

# import required dask fixtures :
# dask_solomulti require client.
# client require loop and cluster_fixture.
from distributed.utils_test import (  # noqa # pylint: disable=unused-import
    cleanup,
    client,
    cluster_fixture,
    loop,
)
from hypothesis import HealthCheck, settings

from .utils import EMBEDED_TEST_DATA_PATH, MAPPING_PATH, TEST_DATA_PATH, glob_fixture

# ----------------------------------#
# --- pytest command line options --#
# ----------------------------------#


def pytest_addoption(parser):
    parser.addoption("--s3", action="store_true", default=False, help="run real s3 tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "real_s3: mark test as requiring real s3")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--s3"):
        # option given: do not skip  tests
        return
    skip_s3 = pytest.mark.skip(reason="need --s3 option to run")
    for item in items:
        if "real_s3" in item.keywords:
            item.add_marker(skip_s3)


# ----------------------------------#
# ---------- DATA FOLDER -----------#
# ----------------------------------#
@pytest.fixture
def INPUT_DIR():
    """Path to te folder where the data should be readed"""
    folder = TEST_DATA_PATH
    if os.path.isdir(folder):
        return folder
    raise FileNotFoundError(
        f"{folder=} does not exist or is not accessible, "
        "please refer to the online documentation to setup test data: "
        "https://cpm.pages.csc-eopf.csgroup.space/eopf-cpm/main/contributing.html#testing",
    )


@pytest.fixture
def OUTPUT_DIR(tmp_path):
    """Path to te folder where the data should be writed"""
    if output_folder := os.environ.get("TEST_OUTPUT_FOLDER"):
        yield output_folder
        shutil.rmtree(output_folder)
    else:
        yield tmp_path


@pytest.fixture
def MAPPING_FOLDER():
    """Path to the folder that contain all the mappings"""
    return MAPPING_PATH


@pytest.fixture
def EMBEDED_TEST_DATA_FOLDER():
    """Path to test data folder"""
    return EMBEDED_TEST_DATA_PATH


# ----------------------------------#
# ------------ MAPPING -------------#
# ----------------------------------#


@pytest.fixture
def S1_IW_OCN_MAPPING(MAPPING_FOLDER: str):
    """Path to a S1 OCN IW 1 mapping"""
    return os.path.join(MAPPING_FOLDER, "S1_OCN_IW_mapping.json")


@pytest.fixture
def S1_SM_OCN_MAPPING(MAPPING_FOLDER: str):
    """Path to a S1 OCN SM 1 mapping"""
    return os.path.join(MAPPING_FOLDER, "S1_OCN_SM_mapping.json")


@pytest.fixture
def S1_WV_OCN_MAPPING(MAPPING_FOLDER: str):
    """Path to a S1 OCN WV 1 mapping"""
    return os.path.join(MAPPING_FOLDER, "S1_OCN_WV_mapping.json")


@pytest.fixture
def S2_MSIL1C_MAPPING(MAPPING_FOLDER: str):
    """Path to a S2 MSIL1C mapping"""
    return os.path.join(MAPPING_FOLDER, "S2_MSIL1C_mapping.json")


@pytest.fixture
def S2_MSIL2A_MAPPING(MAPPING_FOLDER: str):
    """Path to a S2 MSIL1C mapping"""
    return os.path.join(MAPPING_FOLDER, "S2_MSIL2A_mapping.json")


@pytest.fixture
def S3_OL_1_EFR_MAPPING(MAPPING_FOLDER: str):
    """Path to a S3 OL LEVEL 1 mapping"""
    return os.path.join(MAPPING_FOLDER, "S3_OL_1_EFR_mapping.json")


@pytest.fixture
def S3_OL_2_LFR_MAPPING(MAPPING_FOLDER: str):
    """Path to a S3 OL LEVEL 1 mapping"""
    return os.path.join(MAPPING_FOLDER, "S3_OL_2_LFR_mapping.json")


@pytest.fixture
def S3_SL_1_RBT_MAPPING(MAPPING_FOLDER: str):
    """Path to a S3 SL 1 RBT mapping"""
    return os.path.join(MAPPING_FOLDER, "S3_SL_1_RBT_mapping.json")


@pytest.fixture
def S3_SL_2_LST_MAPPING(MAPPING_FOLDER: str):
    """Path to a S3 SL 2 LST mapping"""
    return os.path.join(MAPPING_FOLDER, "S3_SL_2_LST_mapping.json")


@pytest.fixture
def S3_SY_2_SYN_MAPPING(MAPPING_FOLDER: str):
    """Path to a S3 SY 2 SYN mapping"""
    return os.path.join(MAPPING_FOLDER, "S3_SY_2_SYN_mapping.json")


@pytest.fixture
def S3_SR_2_LAN_MAPPING(MAPPING_FOLDER: str):
    """Path to a S3 SR 2 LAN mapping"""
    return os.path.join(MAPPING_FOLDER, "S3_SR_2_LAN_mapping.json")


# ----------------------------------#
# ------------ PRODUCT -------------#
# ----------------------------------#


@glob_fixture("test*test")
def TEST_PRODUCT(request):
    """Path to a S3 SY 2 SYN product"""
    return request.param


@glob_fixture("test*zip", protocols=["zip"])
def TEST_PRODUCT_ZIP(request):
    """Path to a S3 SY 2 SYN product"""
    return request.param


# ############# S1 ##############
@glob_fixture("S1*_IW_OCN*[!.zarr]")
def S1_IW_OCN(request):
    """Path to a S1 OCN LEVEL 1 product"""
    return request.param


@glob_fixture("S1*_IW_OCN*[!.zarr][!.SAFE].zip", protocols=["zip"])
def S1_IW_OCN_ZIP(request):
    """Path to a S1 OCN LEVEL 1 product"""
    return request.param


@glob_fixture("S1*_S[1-6]_OCN*[!.zarr]")
def S1_SM_OCN(request):
    """Path to a S1 OCN LEVEL 1 product"""
    return request.param


@glob_fixture("S1*_S[1-6]_OCN*[!.zarr][!.SAFE].zip", protocols=["zip"])
def S1_SM_OCN_ZIP(request):
    """Path to a S1 OCN LEVEL 1 product"""
    return request.param


@glob_fixture("S1*_WV_OCN*[!.zarr]")
def S1_WV_OCN(request):
    """Path to a S1 OCN LEVEL 1 product"""
    return request.param


@glob_fixture("S1*_WV_OCN*[!.zarr][!.SAFE].zip", protocols=["zip"])
def S1_WV_OCN_ZIP(request):
    """Path to a S1 OCN LEVEL 1 product"""
    return request.param


# ############# S2 ##############
@glob_fixture("S2*_MSIL1C*.SAFE")
def S2_MSIL1C(request):
    """Path to a S2 MSIL1C LEVEL 1 product"""
    return request.param


@glob_fixture("S2*_MSIL1C*[!.zarr][!.SAFE].zip", protocols=["zip"])
def S2_MSIL1C_ZIP(request):
    """Path to a S2 MSIL1C LEVEL 1 product in zip format"""
    return request.param


@glob_fixture("S2*_MSIL2A*.SAFE")
def S2_MSIL2A(request):
    """Path to a S2 MSIL2A LEVEL 2 product"""
    return request.param


@glob_fixture("S2*_MSIL2A*[!.zarr][!.SAFE].zip", protocols=["zip"])
def S2_MSIL2A_ZIP(request):
    """Path to a S2 MSIL2A LEVEL 2 product in zip format"""
    return request.param


# ############# S3 ##############
@glob_fixture("S3*_OL_1_E*R*[!.zarr][!.SAFE].zip", protocols=["zip"])
def S3_OL_1_EFR_ZIP(request):
    """Path to a S3 OL LEVEL 1 product"""
    return request.param


@glob_fixture("S3*_OL_1_E*R*.SEN3")
def S3_OL_1_EFR(request):
    """Path to a S3 OL LEVEL 1 product"""
    return request.param


@glob_fixture("S3*_OL_1_E*R*.zarr")
def S3_OL_1_EFR_ZARR(request):
    """Path to a S3 OL LEVEL 1 product"""
    return request.param


@glob_fixture("S3*_OL_2_L*R*[!.zarr][!.SAFE].zip", protocols=["zip"])
def S3_OL_2_LFR_ZIP(request):
    """Path to a S3 OL LEVEL 2 product"""
    return request.param


@glob_fixture("S3*_OL_2_LFR*.SEN3")
def S3_OL_2_LFR(request):
    """Path to a S3 OL LEVEL 2 product"""
    return request.param


@glob_fixture("S3*_SL_1_RBT*.SEN3")
def S3_SL_1_RBT(request):
    """Path to a S3 SL 1 RBT product"""
    return request.param


@glob_fixture("S3*_SL_1_RBT*[!.zarr][!.SAFE].zip", protocols=["zip"])
def S3_SL_1_RBT_ZIP(request):
    """"""
    return request.param


@glob_fixture("S3*_SL_2_LST*.SEN3")
def S3_SL_2_LST(request):
    """Path to a S3 SL 2 LST product"""
    return request.param


@glob_fixture("S3*_SL_2_LST*[!.zarr][!.SAFE].zip", protocols=["zip"])
def S3_SL_2_LST_ZIP(request):
    """Path to a S3 SL 2 LST product"""
    return request.param


@glob_fixture("S3*_SY_2_SYN*.SEN3")
def S3_SY_2_SYN(request):
    """Path to a S3 SY 2 SYN product"""
    return request.param


@glob_fixture("S3*_SY_2_SYN*[!.zarr][!.SAFE].zip", protocols=["zip"])
def S3_SY_2_SYN_ZIP(request):
    """Path to a S3 SY 2 SYN product"""
    return request.param


@glob_fixture("S3*_SR_2_LAN*.SEN3")
def S3_SR_2_LAN(request):
    """Path to a S3 SY 2 SYN product"""
    return request.param


@glob_fixture("S3*_SR_2_LAN*[!.zarr][!.SAFE].zip", protocols=["zip"])
def S3_SR_2_LAN_ZIP(request):
    """Path to a S3 SY 2 SYN product"""
    return request.param


# ----------------------------------#
# --------- Dask Cluster  ----------#
# ----------------------------------#


@pytest.fixture(params=[True, False])
def dask_client_all(request):
    """Run the test once with and without dask distributed."""
    if request.param:
        return request.getfixturevalue("client")
    return None


# Compatibility between function level fixture (like dask_client_all) and hypothesis.
# Note that function level fixture are estimated one for all hypothesis.
# If the function level fixture is parametized, it loop over the fixture, each sub fixture being estimated once
# for all the hypothesis. The sub_fixture get deleted before the next sub fixture is estimated.
# TLDR : it work and is necessary for my dask client. There is a test in convenience that check it.
settings.register_profile(
    "function_fixture_fast",
    deadline=timedelta(milliseconds=20000),
    max_examples=10,
    suppress_health_check=(HealthCheck.function_scoped_fixture,),
)
settings.register_profile(
    "function_fixture_slow",
    deadline=timedelta(milliseconds=5000),
    suppress_health_check=(HealthCheck.function_scoped_fixture,),
)
# The small hypothesis tests are far slower with dask distributed.
settings.load_profile("function_fixture_fast")


# ----------------------------------#
# ---------   TRIGGERING  ----------#
# ----------------------------------#


@pytest.fixture
def TRIGGER_JSON_FILE(dask_client_all, EMBEDED_TEST_DATA_FOLDER, OUTPUT_DIR, S3_OL_1_EFR):
    trigger_filename = "trigger.json"
    filepath = os.path.join(EMBEDED_TEST_DATA_FOLDER, trigger_filename)
    with open(filepath) as f:
        data = json.load(f)
    data["I/O"]["inputs_products"][0]["path"] = S3_OL_1_EFR
    data["I/O"]["output_product"]["path"] = os.path.join(OUTPUT_DIR, data["I/O"]["output_product"]["path"])
    if dask_client_all:
        data["dask_context"] = {"cluster_type": "local", "cluster_config": {"processes": True}, "client_config": {}}
    else:
        data["dask_context"] = {}
    output_name = os.path.join(OUTPUT_DIR, trigger_filename)
    with open(os.path.join(OUTPUT_DIR, trigger_filename), mode="w") as f:
        json.dump(data, f)

    return output_name
