import os
from unittest import mock

import pytest
from pytest_lazyfixture import lazy_fixture

from eopf.exceptions import InvalidProductError
from eopf.product.conveniences import open_store
from eopf.product.core.eo_product import EOProduct
from eopf.product.store.safe import EOSafeStore
from eopf.qualitycontrol.eo_qc import EOQCFormula, EOQCProcessingUnit, EOQCValidRange
from eopf.qualitycontrol.eo_qc_config import EOPCConfigFactory, EOQCConfig
from eopf.qualitycontrol.eo_qc_processor import EOQCProcessor

quality_report_path = os.path.dirname(os.path.realpath(__file__))
sample_config_path = f"{os.path.dirname(quality_report_path)}/eopf/qualitycontrol/configs/test_qc.json"


def check_data(id):
    switcher = {
        "valid_formula": {
            "check_id": "check_1",
            "check_version": "0.0.1",
            "thresholds": [{"name": "K", "value": 10000}],
            "variables_or_attributes": [
                {"name": "var1", "short_name": "oa01_radiance"},
                {"name": "var2", "short_name": "oa02_radiance"},
            ],
            "formula": "(var1._data.max() * 2.03 + var2._data.max() ) < K",
            "message_if_passed": "Message if the check is passed",
            "message_if_failed": "Message if the check is failed",
        },
        "unvalid_formula": {
            "check_id": "check_1",
            "check_version": "0.0.1",
            "thresholds": [{"name": "K", "value": 0}],
            "variables_or_attributes": [
                {"name": "var1", "short_name": "oa01_radiance"},
                {"name": "var2", "short_name": "oa02_radiance"},
            ],
            "formula": "(var1._data.max() * 2.03 + var2._data.max() ) < K",
            "message_if_passed": "Message if the check is passed",
            "message_if_failed": "Message if the check is failed",
        },
        "security_issue_formula": {
            "check_id": "check_1",
            "check_version": "0.0.1",
            "thresholds": [{"name": "K", "value": 0}],
            "variables_or_attributes": [{"name": "var1", "path": "rm"}, {"name": "var2", "path": "rm"}],
            "formula": "(var1._data.max() * 2.03 + var2._data.max() ) < K",
            "message_if_passed": "Message if the check is passed",
            "message_if_failed": "Message if the check is failed",
        },
        "valid_valid_range": {
            "check_id": "valid_range",
            "check_version": "0.0.1",
            "short_name": "oa01_radiance",
            "valid_min": 0,
            "valid_max": 1000,
            "message_if_passed": "PASSED",
            "message_if_failed": "FAILED",
        },
        "unvalid_valid_range": {
            "check_id": "valid_range",
            "check_version": "0.0.1",
            "short_name": "oa01_radiance",
            "valid_min": 0,
            "valid_max": 0,
            "message_if_passed": "PASSED",
            "message_if_failed": "FAILED",
        },
        "qc_unit": {
            "check_id": "check_3",
            "check_version": "0.0.1",
            "module": "eopf.qualitycontrol.configs.olci",
            "processing_unit": "QC01Unit",
            "parameters": {"threshold": 23, "param_2": 65},
            "aux_data": [
                {"path": "path_to_the_needed_aux_data", "format": "json"},
                {"path": "path_to_the_needed_aux_data", "format": "json"},
            ],
            "message_if_passed": "PASSED",
            "message_if_failed": "FAILED",
        },
    }
    return switcher.get(id, None)


@pytest.fixture
def eoqcProcessor():
    qc_processor = EOQCProcessor()
    return qc_processor


@pytest.fixture
def eoqcConfig():
    qc_config = EOQCConfig(sample_config_path)
    return qc_config


@pytest.fixture
def eoqcConfigFactory():
    qc_configFactory = EOPCConfigFactory()
    return qc_configFactory


@pytest.mark.unit
@pytest.mark.parametrize(
    "qc",
    [
        EOQCFormula(check_data("valid_formula")),
        EOQCValidRange(check_data("valid_valid_range")),
        EOQCProcessingUnit(check_data("qc_unit")),
    ],
)
def test_eoqcstatus(qc):
    assert qc.status is False


@pytest.mark.unit
def test_eoqcConfig_qclist(eoqcConfig):
    assert eoqcConfig.qclist() == eoqcConfig._qclist


@pytest.mark.unit
def test_eoqcConfig__set_getitem__(eoqcConfig):
    test_check = EOQCValidRange(check_data("valid_valid_range"))
    eoqcConfig.__setitem__(check_id="TestCheck", qc=test_check)
    assert eoqcConfig.__getitem__(check_id="TestCheck") == test_check


@pytest.mark.unit
def test_eoqcConfig_rm_qc(eoqcConfig):
    config = eoqcConfig
    config.__setitem__(check_id="TestCheck", qc=EOQCValidRange(check_data("valid_valid_range")))
    config.rm_qc(check_id="TestCheck")
    ret = "TestCheck" not in config.qclist()
    assert ret is True


@pytest.mark.unit
def test_eoqcConfig__len(eoqcConfig):
    assert eoqcConfig.__len__() == len(eoqcConfig.qclist())


@pytest.mark.unit
@pytest.mark.parametrize("store_type", [(lazy_fixture("S3_OL_1_EFR"))])
def test_eoqcConfigFactory_get_default(eoqcConfigFactory, store_type):
    store = EOSafeStore(store_type)
    product = EOProduct("my_product", storage=store)
    with open_store(product):
        default_config = eoqcConfigFactory.get_default(product.type)
        assert default_config.default is True


@pytest.mark.unit
@pytest.mark.parametrize("store_type", [(lazy_fixture("S3_OL_1_EFR"))])
def test_eoqcConfigFactory_get_qc_configs(eoqcConfigFactory, store_type):
    store = EOSafeStore(store_type)
    product = EOProduct("my_product", storage=store)
    with open_store(product):
        configs = eoqcConfigFactory.get_qc_configs(product.type)
        assert all([True if config.product_type == product.type else False for config in configs]) is True


@pytest.mark.unit
def test_eoqcConfigFactory_add_get_config(eoqcConfigFactory, eoqcConfig):
    configFactory = eoqcConfigFactory
    configFactory.add_qc_config(id="Config_test", config=eoqcConfig)
    assert configFactory.get_config_by_id("Config_test") == eoqcConfig


@pytest.mark.unit
def test_EOQCProcessor_init():
    eoqc = EOQCProcessor(config_path=sample_config_path)
    assert eoqc.qc_config is not None


@pytest.mark.need_files
@pytest.mark.unit
@pytest.mark.parametrize("store_type", [(lazy_fixture("S3_OL_1_EFR"))])
def test_EOQCProcessor_productType(store_type, eoqcProcessor):
    store = EOSafeStore(store_type)
    product = EOProduct("my_product", storage=store)
    with open_store(product):
        product.set_type("")
        with pytest.raises(InvalidProductError):
            eoqcProcessor.run(eoproduct=product, update_attrs=False, write_report=False)


@pytest.mark.need_files
@pytest.mark.unit
@pytest.mark.parametrize(
    "store_type",
    [(lazy_fixture("S3_OL_1_EFR"))],
)
def test_create_quality_group(store_type):
    eoqcProcessor = EOQCProcessor(config_path=sample_config_path)
    store = EOSafeStore(store_type)
    product = EOProduct("my_product", storage=store)
    empty_product = EOProduct("empty_product")
    with open_store(product):
        eoqcProcessor.run(eoproduct=product, update_attrs=True, write_report=False)
        eoqcProcessor.update_attributs(eoproduct=empty_product, qc_config=eoqcProcessor.qc_config)
        assert "quality" in empty_product


@pytest.mark.need_files
@pytest.mark.unit
@pytest.mark.parametrize(
    "store_type, update_attrs",
    [(lazy_fixture("S3_OL_1_EFR"), True), (lazy_fixture("S3_OL_1_EFR"), False)],
)
def test_update_attribute(store_type, eoqcProcessor, update_attrs):
    store = EOSafeStore(store_type)
    product = EOProduct("my_product", storage=store)
    with open_store(product):
        eoqcProcessor.run(eoproduct=product, update_attrs=update_attrs, write_report=False)
        if update_attrs:
            assert product.quality.attrs != {}
        else:
            assert product.quality.attrs == {}


@pytest.mark.need_files
@pytest.mark.unit
@pytest.mark.parametrize(
    "store_type",
    [(lazy_fixture("S3_OL_1_EFR"))],
)
def test_qc_exception(store_type, eoqcProcessor):
    store = EOSafeStore(store_type)
    product = EOProduct("my_product", storage=store)
    with open_store(product):
        with mock.patch("eopf.qualitycontrol.eo_qc.EOQCValidRange.check", side_effect=Exception()):
            with pytest.raises(Exception):
                eoqcProcessor.run(eoproduct=product, update_attrs=False, write_report=False)


@pytest.mark.need_files
@pytest.mark.unit
@pytest.mark.parametrize(
    "store_type, write_report, report_path",
    [(lazy_fixture("S3_OL_1_EFR"), True, quality_report_path), (lazy_fixture("S3_OL_1_EFR"), True, None)],
)
def test_write_report(store_type, eoqcProcessor, write_report, report_path):
    store = EOSafeStore(store_type)
    product = EOProduct("my_product", storage=store)
    with open_store(product):
        if write_report and report_path is not None:
            eoqcProcessor.run(eoproduct=product, update_attrs=False, write_report=write_report, report_path=report_path)
            assert os.path.isfile(os.path.join(report_path, "QC_report_my_product.json")) is True
            os.remove(os.path.join(report_path, "QC_report_my_product.json"))
        else:
            with pytest.raises(ValueError):
                eoqcProcessor.run(
                    eoproduct=product,
                    update_attrs=False,
                    write_report=write_report,
                    report_path=report_path,
                )


@pytest.mark.need_files
@pytest.mark.unit
@pytest.mark.parametrize(
    "store_type, report_path",
    [(lazy_fixture("S3_OL_1_EFR"), quality_report_path)],
)
def test_error_writing_report(store_type, report_path):
    # Initialisation of a qcprocessor with a configuration
    broken_eoqcProcessor = EOQCProcessor(config_path=sample_config_path)
    # Creation of a new qcCheck
    modified_check = EOQCValidRange(check_data("unvalid_valid_range"))
    # Make it unserializable
    modified_check.message_if_failed = EOQCValidRange(check_data("unvalid_valid_range"))
    # Change it in the config
    broken_eoqcProcessor.qc_config.__setitem__("crash_test", modified_check)
    store = EOSafeStore(store_type)
    product = EOProduct("my_product", storage=store)
    with open_store(product):
        with pytest.raises(Exception):
            broken_eoqcProcessor.run(eoproduct=product, update_attrs=False, write_report=True, report_path=report_path)
        os.remove(os.path.join(report_path, "QC_report_my_product.json"))


@pytest.mark.need_files
@pytest.mark.integration
@pytest.mark.parametrize(
    "store_type, test_check, result",
    [
        (lazy_fixture("S3_OL_1_EFR"), EOQCValidRange(check_data("valid_valid_range")), True),
        (lazy_fixture("S3_OL_1_EFR"), EOQCValidRange(check_data("unvalid_valid_range")), False),
        (lazy_fixture("S3_OL_1_EFR"), EOQCFormula(check_data("valid_formula")), True),
        (lazy_fixture("S3_OL_1_EFR"), EOQCFormula(check_data("unvalid_formula")), False),
        (lazy_fixture("S3_OL_1_EFR"), EOQCFormula(check_data("security_issue_formula")), False),
    ],
)
def test_checks(store_type, test_check, result):
    store = EOSafeStore(store_type)
    product = EOProduct("my_product", storage=store)
    with open_store(product):
        assert test_check.check(product) == result
