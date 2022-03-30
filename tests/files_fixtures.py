import datetime
import glob
import os
import random

import pytest

from .utils import PARENT_DATA_PATH

ENS_PATH_ENV_VAlUE = "ENS_PATH"


@pytest.fixture
def S3_OLCI_L1_EFR():
    file_name = "S3A_OL_1_EFR____*_LN1_O_NT_002.zip"
    if ens_path := os.environ.get(ENS_PATH_ENV_VAlUE):
        now = datetime.datetime.now()
        year = random.randint(2016, now.year)
        glob_path = os.path.join(ens_path, "S3", "OLCI", "LEVEL-1", "OL_1_EFR___", str(year), "*", "*", file_name)
    else:
        glob_path = os.path.join(PARENT_DATA_PATH, "data", file_name)
    return f"zip::file://{glob.glob(glob_path)[0]}"
