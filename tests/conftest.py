import glob
import os
import shutil
from datetime import timedelta

import pytest
from hypothesis import HealthCheck, settings

from .utils import PARENT_DATA_PATH


# ----------------------------------#
# ---------- DATA FOLDER -----------#
# ----------------------------------#
@pytest.fixture
def INPUT_DIR():
    """Path to te folder where the data should be readed"""
    return os.environ.get("TEST_DATA_FOLDER", os.path.join(PARENT_DATA_PATH, "data"))


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
def S3_OLCI_L1_MAPPING(MAPPING_FOLDER: str):
    """Path to a S3 OLCI LEVEL 1 mapping"""
    return os.path.join(MAPPING_FOLDER, "S3_OL_1_EFR_mapping.json")


@pytest.fixture
def S3_SL_1_RBT_MAPPING(MAPPING_FOLDER: str):
    """Path to a S3 SL 1 RBT mapping"""
    return os.path.join(MAPPING_FOLDER, "S3_SL_1_RBT_mapping.json")


@pytest.fixture
def S3_SY_2_SYN_MAPPING(MAPPING_FOLDER: str):
    """Path to a S3 SY 2 SYN mapping"""
    return os.path.join(MAPPING_FOLDER, "S3_SY_2_SYN_mapping.json")


@pytest.fixture
def S1_IM_OCN_MAPPING(MAPPING_FOLDER: str):
    """Path to a S1 OCN IW 1 mapping"""
    return os.path.join(MAPPING_FOLDER, "S1_OCN_IW_mapping.json")


@pytest.fixture
def S2A_MSIL1C_MAPPING(MAPPING_FOLDER: str):
    """Path to a S2A MSIL1C mapping"""
    return os.path.join(MAPPING_FOLDER, "S2_MSIL1C_mapping.json")


# ----------------------------------#
# ------------ PRODUCT -------------#
# ----------------------------------#
@pytest.fixture
def S3_OLCI_L1_EFR(INPUT_DIR: str):
    """Path to a S3 OLCI LEVEL 1 product"""
    file_name = "S3*_OL_1_E*R*.zip"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"


@pytest.fixture
def S2A_MSIL1C_ZIP(INPUT_DIR: str):
    """Path to a S3 OLCI LEVEL 1 mapping"""
    file_name = "S2A_MSIL1C*.zip"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"


@pytest.fixture
def S2A_MSIL1C(INPUT_DIR: str):
    """Path to a S3 OLCI LEVEL 1 mapping"""
    file_name = "S2A_MSIL1C*.SAFE"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"file://{glob.glob(glob_path)[0]}"


@pytest.fixture
def S1_IM_OCN(INPUT_DIR: str):
    """Path to a S2 MSIL1C LEVEL 1 product"""
    file_name = "S1A_IW_OCN*.zip"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"


# ----------------------------------#
# --------- Dask Cluster  ----------#
# ----------------------------------#

# import required dask fixtures :
# dask_solomulti require client.
# client require loop and cluster_fixture.
from distributed.utils_test import (  # noqa # pylint: disable=unused-import
    client,
    cluster_fixture,
    loop,
)


@pytest.fixture(params=[True, False])
def dask_client_all(request):
    """Run the test once with and without dask distributed."""
    if request.param:
        # The small hypothesis tests are far slower with dask distributed.
        # We use a lower hypothesis max_examples with them
        settings.load_profile("function_fixture_fast")
        return request.getfixturevalue("client")
    settings.load_profile("function_fixture_slow")
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
settings.load_profile("function_fixture_fast")
