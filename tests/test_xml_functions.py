import glob
import os
import xarray as xr

import pytest

from eopf.product.constants import (
    CF_MAP_OLCI_L1,
    CF_MAP_SLSTR_L1,
    EOP_MAP_OLCI_L1,
    EOP_MAP_SLSTR_L1,
    NAMESPACES_OLCI_L1,
    NAMESPACES_SLSTR_L1,
)
from eopf.product.conveniences import (
    apply_xpath,
    etree_to_dict,
    filter_paths_by,
    get_dir_files,
    parse_xml,
    translate_structure,
    read_xrd,
)
from eopf.product.convert import OLCIL1EOPConverter, SLSTRL1EOPConverter


@pytest.mark.unit
def test_parse_xml():
    tree = parse_xml(os.path.abspath("tests/data"), "*.xml")
    result = ""
    display_namespaces = True
    for element in tree.iter():
        tag = element.tag
        result += f"{tag}\n"
        if display_namespaces:
            display_namespaces = False
            for key, value in element.nsmap.items():
                result += f"{key} : {value}\n"
        attributes = element.attrib
        for key, value in attributes.items():
            result += f"{key} : {value}\n"
        textual_content = element.text
        if textual_content and textual_content.strip():
            result += textual_content + "\n"
    file_path = os.path.join(os.path.abspath("tests/data"), "solutions.txt")
    with open(file_path, "r") as f:
        expected = f.read()
    assert result == expected


@pytest.mark.unit
def test_translate_structure():
    dom = parse_xml(os.path.abspath("tests/data"), "*.xml")
    MAP = {
        "title": "concat('',metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:productName/text())",  # noqa
        "Conventions": "'CF-1.9'",
    }
    NAMESPACES = {
        "xfdu": "urn:ccsds:schema:xfdu:1",
        "gml": "http://www.opengis.net/gml",
        "sentinel-safe": "http://www.esa.int/safe/sentinel/1.1",
        "sentinel3": "http://www.esa.int/safe/sentinel/sentinel-3/1.0",
        "olci": "http://www.esa.int/safe/sentinel/sentinel-3/olci/1.0",
    }
    result = {attr: translate_structure(MAP[attr], dom, NAMESPACES) for attr in MAP}
    assert result == {
        "title": "S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3",
        "Conventions": "CF-1.9",
    }


@pytest.mark.unit
def test_apply_xpath():
    dom = parse_xml(os.path.abspath("tests/data"), "*.xml")
    MAP = {
        "title": "concat('',metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:productName/text())",  # noqa
        "Conventions": "'CF-1.9'",
    }
    NAMESPACES = {
        "xfdu": "urn:ccsds:schema:xfdu:1",
        "gml": "http://www.opengis.net/gml",
        "sentinel-safe": "http://www.esa.int/safe/sentinel/1.1",
        "sentinel3": "http://www.esa.int/safe/sentinel/sentinel-3/1.0",
        "olci": "http://www.esa.int/safe/sentinel/sentinel-3/olci/1.0",
    }
    result = {attr: apply_xpath(dom, MAP[attr], NAMESPACES) for attr in MAP}
    assert result == {
        "title": "S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3",
        "Conventions": "CF-1.9",
    }


@pytest.mark.unit
def test_etree_to_dict():
    tree = parse_xml(os.path.abspath("tests/data"), "*.xml")
    root = tree.getroot()
    ddict = etree_to_dict(root[0])
    assert ddict == {
        "informationPackageMap": {
            "{urn:ccsds:schema:xfdu:1}contentUnit": {
                "{urn:ccsds:schema:xfdu:1}contentUnit": [
                    {
                        "dataObjectPointer": {"@dataObjectID": "Oa01_radianceData"},
                        "@ID": "Oa01_radianceUnit",
                        "@unitType": "Measurement Data Unit",
                        "@textInfo": "TOA radiance for OLCI acquisition band Oa01",
                        "@dmdID": "geoCoordinatesAnnotation timeCoordinatesAnnotation qualityFlagsAnnotation",
                    },
                    {
                        "dataObjectPointer": {"@dataObjectID": "Oa02_radianceData"},
                        "@ID": "Oa02_radianceUnit",
                        "@unitType": "Measurement Data Unit",
                        "@textInfo": "TOA radiance for OLCI acquisition band Oa02",
                        "@dmdID": "geoCoordinatesAnnotation timeCoordinatesAnnotation qualityFlagsAnnotation",
                    },
                ],
                "@ID": "packageUnit",
                "@unitType": "Information Package",
                "@textInfo": "SENTINEL-3 OLCI Level 1 Earth Observation Full Resolution Product",
                "@dmdID": "acquisitionPeriod platform measurementOrbitReference measurementQualityInformation processing measurementFrameSet generalProductInformation olciProductInformation",  # noqa
                "@pdiID": "processing",
            },
        },
    }


@pytest.mark.unit
def test_xml_persistance_olci():
    olci_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    olci_eop = OLCIL1EOPConverter(olci_path)
    olci_eop.read()

    # Legacy attributes
    tree = parse_xml(olci_path, "xfdumanifest.xml")
    root = tree.getroot()
    xfdu_dict = etree_to_dict(root[1])
    cf_dict = {attr: apply_xpath(tree, CF_MAP_OLCI_L1[attr], NAMESPACES_OLCI_L1) for attr in CF_MAP_OLCI_L1}
    eop_dict = {attr: translate_structure(EOP_MAP_OLCI_L1[attr], tree, NAMESPACES_OLCI_L1) for attr in EOP_MAP_OLCI_L1}

    # EOProduct attributes
    cf_eop_dict = olci_eop.eop.attributes.attrs["CF"]
    om_eop_dict = olci_eop.eop.attributes.attrs["OM-EOP"]
    xfdu_eop_dict = olci_eop.eop.attributes.attrs["XFDU"]

    assert xfdu_dict == xfdu_eop_dict
    assert cf_dict == cf_eop_dict
    assert eop_dict == om_eop_dict


@pytest.mark.unit
def test_xml_persistance_slstr():
    slstr_path = glob.glob("data/S3A_SL_1_RBT*.SEN3")[0]
    slstr_eop = SLSTRL1EOPConverter(slstr_path)
    slstr_eop.read()

    # Legacy attributes
    tree = parse_xml(slstr_path, "xfdumanifest.xml")
    root = tree.getroot()
    xfdu_dict = etree_to_dict(root[1])
    cf_dict = {attr: apply_xpath(tree, CF_MAP_SLSTR_L1[attr], NAMESPACES_SLSTR_L1) for attr in CF_MAP_SLSTR_L1}
    eop_dict = {
        attr: translate_structure(EOP_MAP_SLSTR_L1[attr], tree, NAMESPACES_SLSTR_L1) for attr in EOP_MAP_SLSTR_L1
    }  # noqa

    # EOProduct attributes
    cf_eop_dict = slstr_eop.eop.attributes.attrs["CF"]
    om_eop_dict = slstr_eop.eop.attributes.attrs["OM-EOP"]
    xfdu_eop_dict = slstr_eop.eop.attributes.attrs["XFDU"]

    assert xfdu_dict == xfdu_eop_dict
    assert cf_dict == cf_eop_dict
    assert eop_dict == om_eop_dict


@pytest.mark.unit
def test_filter_paths_by_1():
    assert filter_paths_by(paths=[], filters=[]) == []


@pytest.mark.unit
def test_filter_paths_by_2():
    assert filter_paths_by(paths=None, filters=None) == []


@pytest.mark.unit
def test_filter_paths_by_3():
    assert filter_paths_by(paths=[], filters=["xml"]) == []


@pytest.mark.unit
def test_filter_paths_by_4():
    paths = ["data/filters_files_by_tests/empty_dir"]
    assert filter_paths_by(paths=paths, filters=[]) == []


@pytest.mark.unit
def test_filter_paths_by_5():
    paths = ["data/filters_files_by_tests/empty_dir/file_a"]
    filters = ["file"]
    assert filter_paths_by(paths=paths, filters=filters) == ["data/filters_files_by_tests/empty_dir/file_a"]


@pytest.mark.unit
def test_filter_paths_by_6():
    paths = ["data/filters_files_by_tests/empty_dir/file_a"]
    filters = ["file_b"]
    assert filter_paths_by(paths=paths, filters=filters) == []


@pytest.mark.unit
def test_filter_paths_by_7():
    paths = ["data/filters_files_by_tests/empty_dir/file_a", "data/filters_files_by_tests/empty_dir/file_b"]
    filters = ["file"]
    assert filter_paths_by(paths=paths, filters=filters) == [
        "data/filters_files_by_tests/empty_dir/file_a",
        "data/filters_files_by_tests/empty_dir/file_b",
    ]


@pytest.mark.unit
def test_get_dir_files_1(tmpdir):
    assert get_dir_files(dir_path=tmpdir) == []


@pytest.mark.unit
def test_get_dir_files_2(tmpdir):
    file_path = os.path.join(tmpdir, "filename")
    open(file_path, "a").close()
    assert get_dir_files(dir_path=tmpdir) == [file_path]


@pytest.mark.unit
def test_get_dir_files_3(tmpdir):
    file_path = os.path.join(tmpdir, "filename")
    open(file_path, "a").close()
    assert get_dir_files(dir_path=tmpdir, glob_pattern="*.txt") == []


@pytest.mark.unit
def test_get_dir_files_4(tmpdir):
    file_path = os.path.join(tmpdir, "filename.txt")
    open(file_path, "a").close()
    assert get_dir_files(dir_path=tmpdir, glob_pattern="*.txt") == [file_path]


@pytest.mark.unit
def test_get_dir_files_5(tmpdir):
    file_path1 = os.path.join(tmpdir, "filename1.txt")
    open(file_path1, "a").close()
    file_path2 = os.path.join(tmpdir, "filename2.xml")
    open(file_path2, "a").close()
    assert get_dir_files(dir_path=tmpdir, glob_pattern="*.txt") == [file_path1]


@pytest.mark.unit
def test_get_dir_files_6(tmpdir):
    file_path1 = os.path.join(tmpdir, "filename1.txt")
    open(file_path1, "a").close()
    file_path2 = os.path.join(tmpdir, "filename2.txt")
    open(file_path2, "a").close()
    assert sorted(get_dir_files(dir_path=tmpdir, glob_pattern="*.txt")) == sorted([file_path1, file_path2])


@pytest.mark.unit
def test_read_xrd_1():
    s3_prod_path = "data/"
    s3_file_name = "empty_file"
    s3_file_path = os.path.join(s3_prod_path, s3_file_name)
    files = [s3_file_path]
    out_ds = read_xrd(files=files)
    assert out_ds is None, "The funtion must return None since the file is not a supported format"


@pytest.mark.unit
def test_read_xrd_2():
    s3_prod_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    s3_file_name = "Oa22_radiance.nc"
    s3_file_path = os.path.join(s3_prod_path, s3_file_name)
    files = [s3_file_path]
    out_ds = read_xrd(files=files)
    assert out_ds is None, "The funtion must return None since the file does not exist"


@pytest.mark.unit
def test_read_xrd_3():
    files = []
    out_ds = read_xrd(files=files)
    assert out_ds is None, "The funtion must return None since no files are given"


@pytest.mark.unit
def test_read_xrd_4():
    s3_prod_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    s3_file_name = "Oa11_radiance.nc"
    s3_file_path = os.path.join(s3_prod_path, s3_file_name)
    files = [s3_file_path]
    skip =["Oa11_radiance"]
    out_ds = read_xrd(files=files, skip=skip)
    assert out_ds is None, "The funtion must return None since the only variable available in s3 file is the one skipped"


@pytest.mark.unit
def test_read_xrd_5():
    s3_prod_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    s3_file_name = "Oa11_radiance.nc"
    s3_file_path = os.path.join(s3_prod_path, s3_file_name)
    s3_file_ds = xr.open_dataset(s3_file_path, decode_times=False, mask_and_scale=False)
    s3_vars = {}
    s3_vars["Oa11_radiance"] = s3_file_ds.get("Oa11_radiance")
    s3_ds = xr.Dataset(data_vars=s3_vars)
    files = [s3_file_path]
    out_ds = read_xrd(files=files)
    assert s3_ds.equals(out_ds), "The datasets must be equal"


@pytest.mark.unit
def test_read_xrd_6():
    s3_prod_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    s3_file_name = "tie_geo_coordinates.nc"
    s3_file_path = os.path.join(s3_prod_path, s3_file_name)
    s3_file_ds = xr.open_dataset(s3_file_path, decode_times=False, mask_and_scale=False)
    files = [s3_file_path]
    pick = ["latitude"]
    skip = ["latitude"]
    out_ds = read_xrd(files=files, pick=pick, skip=skip)
    assert out_ds is None, "The output should be None since skip has precedence over pick"


@pytest.mark.unit
def test_read_xrd_7():
    s3_prod_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    s3_file_name = "tie_geo_coordinates.nc"
    s3_file_path = os.path.join(s3_prod_path, s3_file_name)
    s3_file_ds = xr.open_dataset(s3_file_path, decode_times=False, mask_and_scale=False)
    s3_vars = {}
    s3_vars["latitude"] = s3_file_ds.get("latitude")
    s3_ds = xr.Dataset(data_vars=s3_vars)
    files = [s3_file_path]
    pick = ["latitude"]
    out_ds = read_xrd(files=files, pick=pick)
    assert s3_ds.equals(out_ds), "The datasets must be equal"


@pytest.mark.unit
def test_read_xrd_8():
    s3_prod_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    s3_vars = {}

    s3_file_name_1 = "Oa10_radiance.nc"
    s3_file_path_1 = os.path.join(s3_prod_path, s3_file_name_1)
    s3_file_ds_1 = xr.open_dataset(s3_file_path_1, decode_times=False, mask_and_scale=False)
    s3_vars["Oa10_radiance"] = s3_file_ds_1.get("Oa10_radiance")

    s3_file_name_2 = "Oa11_radiance.nc"
    s3_file_path_2 = os.path.join(s3_prod_path, s3_file_name_2)
    s3_file_ds_2 = xr.open_dataset(s3_file_path_2, decode_times=False, mask_and_scale=False)
    s3_vars["Oa11_radiance"] = s3_file_ds_2.get("Oa11_radiance")
    s3_ds = xr.Dataset(data_vars=s3_vars)

    files = [s3_file_path_1, s3_file_path_2]
    out_ds = read_xrd(files=files)
    assert s3_ds.equals(out_ds), "The datasets must be equal"


@pytest.mark.unit
def test_read_xrd_8():
    s3_prod_path = glob.glob("data/S3A_OL_1*.SEN3")[0]

    s3_file_name_1 = "Oa10_radiance.nc"
    s3_file_path_1 = os.path.join(s3_prod_path, s3_file_name_1)

    s3_file_name_2 = "Oa22_radiance.nc"
    s3_file_path_2 = os.path.join(s3_prod_path, s3_file_name_2)

    files = [s3_file_path_1, s3_file_path_2]
    out_ds = read_xrd(files=files)
    assert out_ds is None, "The datasets must be equal"