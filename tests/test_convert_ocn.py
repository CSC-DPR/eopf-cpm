import os

import pytest

from eopf.product.store.conveniences import convert
from eopf.product.store.safe import EOSafeStore
from eopf.product.store.zarr import EOZarrStore


@pytest.fixture
def INPUT_ID():
    return "S1A_IW_OCN__2SDV_20220106T040500_20220106T040525_041335_04E9F6_CB53"


@pytest.fixture
def RESULT_DIR(INPUT_DIR):
    # os.makedirs(OUTPUT_DIR, exist_ok=True)
    # return OUTPUT_DIR
    # temporarily preserve the output in order to allow inspecting it
    dir = INPUT_DIR + "-intermediate"
    os.makedirs(dir, exist_ok=True)
    return dir


@pytest.fixture
def S1A_IW_OCN__2SDV_20220106T040500(INPUT_DIR, INPUT_ID):
    return os.path.join(INPUT_DIR, f"{INPUT_ID}.SAFE")


@pytest.fixture
def S1A_IW_OCN__2SDV_20220106T040500_ZARR(RESULT_DIR, INPUT_ID):
    return os.path.join(RESULT_DIR, f"{INPUT_ID}.zarr")


def test_convert(S1A_IW_OCN__2SDV_20220106T040500, S1A_IW_OCN__2SDV_20220106T040500_ZARR):
    input_store = EOSafeStore(S1A_IW_OCN__2SDV_20220106T040500)
    output_store = EOZarrStore(S1A_IW_OCN__2SDV_20220106T040500_ZARR)
    convert(input_store, output_store)
