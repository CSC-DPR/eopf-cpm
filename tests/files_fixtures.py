import datetime
import glob
import os
import random

import pytest

from .utils import PARENT_DATA_PATH

ENS_PATH_ENV_VAlUE = "ENS_PATH"


@pytest.fixture
def S3_OLCI_L1_EFR():
    file_name = "S3*_OL_1_E*R*zip"
    if ens_path := os.environ.get(ENS_PATH_ENV_VAlUE):
        now = datetime.datetime.now()
        year = random.randint(2016, now.year)
        glob_path = os.path.join(ens_path, "S3", "OLCI", "LEVEL-1", "OL_1_E*R___", str(year), "*", "*", file_name)
    else:
        glob_path = os.path.join(PARENT_DATA_PATH, "data", file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"
