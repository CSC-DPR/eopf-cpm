import glob
import os
import shutil

import pytest

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
def S2_MSIL1C_MAPPING(MAPPING_FOLDER: str):
    """Path to a S1 OCN IW 1 mapping"""
    return os.path.join(MAPPING_FOLDER, "S2_MSIL1C_mapping.json")


@pytest.fixture
def S1_IM_OCN_MAPPING(MAPPING_FOLDER: str):
    """Path to a S1 OCN IW 1 mapping"""
    return os.path.join(MAPPING_FOLDER, "S1_OCN_IW_mapping.json")


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
def S3_SL_1_RBT(MAPPING_FOLDER: str):
    """Path to a S3 SL 1 RBT product"""
    file_name = "S3*_SL_1_RBT*.zip"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"


@pytest.fixture
def S3_SY_2_SYN(MAPPING_FOLDER: str):
    """Path to a S3 SY 2 SYN product"""
    file_name = "S3*_SY_2_SYN*.zip"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"


@pytest.fixture
def S2A_MSIL1C_ZIP(INPUT_DIR: str):
    """Path to a S2 MSIL1C LEVEL 1 product in zip format"""
    file_name = "S2A_MSIL1C*.zip"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"


@pytest.fixture
def S2A_MSIL1C(INPUT_DIR: str):
    """Path to a S2 MSIL1C LEVEL 1 product"""
    file_name = "S2A_MSIL1C*.SAFE"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"file://{glob.glob(glob_path)[0]}"


@pytest.fixture
def S1_IM_OCN(INPUT_DIR: str):
    """Path to a S2 MSIL1C LEVEL 1 product"""
    file_name = "S1A_IW_OCN*.zip"
    glob_path = os.path.join(INPUT_DIR, file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"
