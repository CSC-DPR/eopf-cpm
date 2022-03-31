import glob
import os

import pytest

from .utils import PARENT_DATA_PATH

TEST_DATA_FOLDER = os.environ.get("TEST_DATA_FOLDER", os.path.join(PARENT_DATA_PATH, "data"))


@pytest.fixture
def S3_OLCI_L1_EFR():
    file_name = "S3*_OL_1_E*R*.zip"
    glob_path = os.path.join(TEST_DATA_FOLDER, file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"
