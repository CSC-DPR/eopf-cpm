import glob
import json
import os
import shutil
from datetime import timedelta
from typing import Optional

import pytest

# import required dask fixtures :
# dask_solomulti require client.
# client require loop and cluster_fixture.
from distributed.utils_test import (  # noqa # pylint: disable=unused-import
    client,
    cluster_fixture,
    loop,
)
from hypothesis import HealthCheck, settings

from .utils import PARENT_DATA_PATH

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
    folder = os.environ.get("TEST_DATA_FOLDER", os.path.join(PARENT_DATA_PATH, "data"))
    if os.path.isdir(folder):
        return folder
    raise FileNotFoundError(f"{folder} does not exist or is not accessible")


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
    return os.path.join(PARENT_DATA_PATH, "eopf", "product", "store", "mapping")


@pytest.fixture
def EMBEDED_TEST_DATA_FOLDER():
    """Path to test data folder"""
    return os.path.join(PARENT_DATA_PATH, "tests", "data")


# ----------------------------------#
# ------------ MAPPING -------------#
# ----------------------------------#


@pytest.fixture
def S1_IM_OCN_MAPPING(MAPPING_FOLDER: str):
    """Path to a S1 OCN IW 1 mapping"""
    return os.path.join(MAPPING_FOLDER, "S1_OCN_IW_mapping.json")


@pytest.fixture
def S2A_MSIL1C_MAPPING(MAPPING_FOLDER: str):
    """Path to a S2A MSIL1C mapping"""
    return os.path.join(MAPPING_FOLDER, "S2_MSIL1C_mapping.json")


@pytest.fixture
def S3_OL_1_EFR_MAPPING(MAPPING_FOLDER: str):
    """Path to a S3 OLCI LEVEL 1 mapping"""
    return os.path.join(MAPPING_FOLDER, "S3_OL_1_EFR_mapping.json")


@pytest.fixture
def S3_OL_2_LFR_MAPPING(MAPPING_FOLDER: str):
    """Path to a S3 OLCI LEVEL 1 mapping"""
    return os.path.join(MAPPING_FOLDER, "S3_OL_2_LFR_mapping.json")


@pytest.fixture
def S3_SL_1_RBT_MAPPING(MAPPING_FOLDER: str):
    """Path to a S3 SL 1 RBT mapping"""
    return os.path.join(MAPPING_FOLDER, "S3_SL_1_RBT_mapping.json")


@pytest.fixture
def S3_SL_2_LST_MAPPING(MAPPING_FOLDER: str):
    """Path to a S3 SL 1 RBT mapping"""
    return os.path.join(MAPPING_FOLDER, "S3_SL_2_LST_mapping.json")


@pytest.fixture
def S3_SY_2_SYN_MAPPING(MAPPING_FOLDER: str):
    """Path to a S3 SY 2 SYN mapping"""
    return os.path.join(MAPPING_FOLDER, "S3_SY_2_SYN_mapping.json")


# ----------------------------------#
# ------------ PRODUCT -------------#
# ----------------------------------#


def _glob_to_url(input_dir: str, file_name_pattern: str, protocols: Optional[list[str]] = None):
    if protocols is None:
        protocols = []
    protocols.append("file")

    glob_path = os.path.join(input_dir, file_name_pattern)
    matched_files = glob.glob(glob_path)
    if len(matched_files) != 1:
        raise IOError(f"{len(matched_files)} files matched {glob_path} instead of 1.")
    protocols_string = "::".join(protocols)
    return f"{protocols_string}://{matched_files[0]}"


@pytest.fixture
def S1_IM_OCN(INPUT_DIR):
    """Path to a S2 MSIL1C LEVEL 1 product"""
    return _glob_to_url(INPUT_DIR, "S1A_IW_OCN*.zip", protocols=["zip"])


@pytest.fixture
def S2A_MSIL1C(INPUT_DIR):
    """Path to a S2 MSIL1C LEVEL 1 product"""
    return _glob_to_url(
        INPUT_DIR,
        "S2A_MSIL1C*.SAFE",
    )


@pytest.fixture
def S2A_MSIL1C_ZIP(INPUT_DIR):
    """Path to a S2 MSIL1C LEVEL 1 product in zip format"""
    return _glob_to_url(INPUT_DIR, "S2A_MSIL1C*.zip", protocols=["zip"])


@pytest.fixture
def S3_OL_1_EFR(INPUT_DIR):
    """Path to a S3 OLCI LEVEL 1 product"""
    return _glob_to_url(INPUT_DIR, "S3*_OL_1_E*R*.zip", protocols=["zip"])


@pytest.fixture
def S3_OL_2_LFR(INPUT_DIR):
    """Path to a S3 OLCI LEVEL 2 product"""
    return _glob_to_url(INPUT_DIR, "S3*_OL_2_LFR*.SEN3")


@pytest.fixture
def S3_SL_1_RBT(INPUT_DIR):
    """Path to a S3 SL 1 RBT product"""
    return _glob_to_url(INPUT_DIR, "S3*_SL_1_RBT*.SEN3")


@pytest.fixture
def S3_SL_2_LST(INPUT_DIR):
    """Path to a S3 SL 2 LST product"""
    return _glob_to_url(INPUT_DIR, "S3*_SL_2_LST*.SEN3")


@pytest.fixture
def S3_SY_2_SYN(INPUT_DIR):
    """Path to a S3 SY 2 SYN product"""
    return _glob_to_url(INPUT_DIR, "S3*_SY_2_SYN*.SEN3")


@pytest.fixture
def TEST_PRODUCT(INPUT_DIR):
    """Path to a S3 SY 2 SYN product"""
    return _glob_to_url(INPUT_DIR, "test*test")


@pytest.fixture
def TEST_PRODUCT_ZIP(INPUT_DIR):
    """Path to a S3 SY 2 SYN product"""
    return _glob_to_url(INPUT_DIR, "test*zip", protocols=["zip"])


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
    data["I/O"]["input_product"]["path"] = S3_OL_1_EFR
    data["I/O"]["output_product"]["path"] = os.path.join(OUTPUT_DIR, data["I/O"]["output_product"]["path"])
    if dask_client_all:
        data["dask_context"] = {"distributed": "processes"}
    output_name = os.path.join(OUTPUT_DIR, trigger_filename)
    with open(os.path.join(OUTPUT_DIR, trigger_filename), mode="w") as f:
        json.dump(data, f)

    return output_name
