import glob
import json
import os
import shutil
from datetime import timedelta

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
def S3_OLCI_1_MAPPING(MAPPING_FOLDER: str):
    """Path to a S3 OLCI LEVEL 1 mapping"""
    return os.path.join(MAPPING_FOLDER, "S3_OL_1_EFR_mapping.json")


def S3_OLCI_2_MAPPING(MAPPING_FOLDER: str):
    """Path to a S3 OLCI LEVEL 2 mapping"""
    return os.path.join(MAPPING_FOLDER, "S3_OL_2_EFR_mapping.json")


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
def S2_MSIL1C_MAPPING(MAPPING_FOLDER: str):
    """Path to a S2A MSIL1C mapping"""
    return os.path.join(MAPPING_FOLDER, "S2_MSIL1C_mapping.json")


@pytest.fixture
def S1_IM_OCN_MAPPING(MAPPING_FOLDER: str):
    """Path to a S1 OCN IW 1 mapping"""
    return os.path.join(MAPPING_FOLDER, "S1_OCN_IW_mapping.json")


# ----------------------------------#
# ------------ PRODUCT -------------#
# ----------------------------------#
@pytest.fixture
def S3_OLCI_2_LFR_ZIP(INPUT_DIR: str):
    """Path to a S3 OLCI LEVEL 2 product"""
    file_name = "S3*_OL_2_L*R*.zip"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"


@pytest.fixture
def S3_OLCI_1_EFR_ZIP(INPUT_DIR: str):
    """Path to a S3 OLCI LEVEL 1 product"""
    file_name = "S3*_OL_1_E*R*.zip"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"


@pytest.fixture
def S3_SL_2_LST_ZIP(INPUT_DIR: str):
    """Path to a S3 SL 2 LST product"""
    file_name = "S3*_SL_2_LST*.zip"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"


@pytest.fixture
def S3_SL_1_RBT_ZIP(INPUT_DIR: str):
    """Path to a S3 SL 1 RBT product"""
    file_name = "S3*_SL_1_RBT*.zip"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"


@pytest.fixture
def S3_SY_2_SYN_ZIP(INPUT_DIR: str):
    """Path to a S3 SY 2 SYN product"""
    file_name = "S3*_SY_2_SYN*.zip"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"


@pytest.fixture
def S2_MSIL1C_ZIP(INPUT_DIR: str):
    """Path to a S2 MSIL1C LEVEL 1 product in zip format"""
    file_name = "S2*_MSIL1C*.zip"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"


@pytest.fixture
def S2_MSIL1C(INPUT_DIR: str):
    """Path to a S2 MSIL1C LEVEL 1 product"""
    file_name = "S2*_MSIL1C*.SAFE"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"file://{glob.glob(glob_path)[0]}"


@pytest.fixture
def S1_IM_OCN_ZIP(INPUT_DIR: str):
    """Path to a S2 MSIL1C LEVEL 1 product"""
    file_name = "S1*_IW_OCN*.zip"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"


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
def TRIGGER_JSON_FILE(dask_client_all, EMBEDED_TEST_DATA_FOLDER, OUTPUT_DIR, S3_OLCI_L1_EFR):
    trigger_filename = "trigger.json"
    filepath = os.path.join(EMBEDED_TEST_DATA_FOLDER, trigger_filename)
    with open(filepath) as f:
        data = json.load(f)
    data["input_product"]["path"] = S3_OLCI_L1_EFR
    data["output_product"]["path"] = os.path.join(OUTPUT_DIR, data["output_product"]["path"])
    if dask_client_all:
        data["dask_context"] = {"distributed": "processes"}
    output_name = os.path.join(OUTPUT_DIR, trigger_filename)
    with open(os.path.join(OUTPUT_DIR, trigger_filename), mode="w") as f:
        json.dump(data, f)

    return output_name
