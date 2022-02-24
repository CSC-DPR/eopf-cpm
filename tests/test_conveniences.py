import glob
import os

import pytest
import xarray as xr

from eopf.product.conveniences import (
    apply_xpath,
    etree_to_dict,
    filter_paths_by,
    get_dir_files,
    parse_xml,
    read_xrd,
    translate_structure,
)


@pytest.mark.unit
def test_parse_xml():
    """Given an input xml,
    the output of the function must match the expected output"""
    snippet_path = "tests/data/snippet_xfdumanifest.xml"
    snippet_file = open(snippet_path)
    tree = parse_xml(snippet_file)
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
    """Given an input xml,
    the output of the function must match the expected output"""
    snippet_path = "tests/data/snippet_xfdumanifest.xml"
    snippet_file = open(snippet_path)
    dom = parse_xml(snippet_file)
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
    """Given an input xml,
    the output of the function must match the expected output"""
    snippet_path = "tests/data/snippet_xfdumanifest.xml"
    snippet_file = open(snippet_path)
    dom = parse_xml(snippet_file)
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
    """Given an input xml,
    the output of the function must match the expected output"""
    snippet_path = "tests/data/snippet_xfdumanifest.xml"
    snippet_file = open(snippet_path)
    tree = parse_xml(snippet_file)
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
def test_filter_paths_by_1():
    """Given no paths and no filters,
    the function must return an empty array"""

    assert filter_paths_by(paths=[], filters=[]) == []


@pytest.mark.unit
def test_filter_paths_by_2():
    """Given no paths and no filters (None),
    the function must return an empty array"""

    assert filter_paths_by(paths=None, filters=None) == []


@pytest.mark.unit
def test_filter_paths_by_3():
    """Given no paths,
    the function must return an empty array"""

    assert filter_paths_by(paths=[], filters=["xml"]) == []


@pytest.mark.unit
def test_filter_paths_by_4():
    """Given paths and no filters,
    the function must return an empty array"""

    paths = ["data/filters_files_by_tests/empty_dir"]
    assert filter_paths_by(paths=paths, filters=[]) == []


@pytest.mark.unit
def test_filter_paths_by_5():
    """Given a path and a filter, with the path basename matching the filter
    the function must return the file path in an array"""

    paths = ["data/filters_files_by_tests/empty_dir/file_a"]
    filters = ["file"]
    assert filter_paths_by(paths=paths, filters=filters) == [
        "data/filters_files_by_tests/empty_dir/file_a",
    ]


@pytest.mark.unit
def test_filter_paths_by_6():
    """Given a path and a filter, with the path basename not matching the filter
    the function must return an empty array"""

    paths = ["data/filters_files_by_tests/empty_dir/file_a"]
    filters = ["file_b"]
    assert filter_paths_by(paths=paths, filters=filters) == []


@pytest.mark.unit
def test_filter_paths_by_7():
    """Given two paths and a filter, with both paths basenames matching the filter
    the function must return both paths in an array"""

    paths = ["data/filters_files_by_tests/empty_dir/file_a", "data/filters_files_by_tests/empty_dir/file_b/"]
    filters = ["file"]
    assert filter_paths_by(paths=paths, filters=filters) == [
        "data/filters_files_by_tests/empty_dir/file_a",
        "data/filters_files_by_tests/empty_dir/file_b/",
    ]


@pytest.mark.unit
def test_filter_paths_by_8():
    """Given a path and a filter, with the path basename not maching the filter,
    altough the the filter appears in the extented path,
    the function must return an empty array"""

    paths = ["data/filters_files_by_tests/file_dir/abc"]
    filters = ["file"]
    assert filter_paths_by(paths=paths, filters=filters) == []


@pytest.mark.unit
def test_filter_paths_by_9():
    """Given two paths and two filters, with both paths basenames matching at least one filter
    the function must return both paths in an array"""

    paths = ["data/filters_files_by_tests/empty_dir/file_a", "data/filters_files_by_tests/empty_dir/file_b/"]
    filters = ["file_a", "file_b"]
    assert filter_paths_by(paths=paths, filters=filters) == [
        "data/filters_files_by_tests/empty_dir/file_a",
        "data/filters_files_by_tests/empty_dir/file_b/",
    ]


@pytest.mark.unit
def test_filter_paths_by_10():
    """Given a path and two filters, with both filters matching the path basename
    the function must return an array with one occurence of the path"""

    paths = ["data/filters_files_by_tests/empty_dir/file_a"]
    filters = ["file_a", "file"]
    assert filter_paths_by(paths=paths, filters=filters) == [
        "data/filters_files_by_tests/empty_dir/file_a",
    ]


@pytest.mark.unit
def test_get_dir_files_1(tmpdir):
    """Given a dir_path containing no files,
    the function must return an empty array"""

    sub_dir = os.path.join(tmpdir, "sub_dir")
    os.mkdir(sub_dir)
    assert get_dir_files(dir_path=tmpdir) == []


@pytest.mark.unit
def test_get_dir_files_2(tmpdir):
    """Given a dir_path cotaining one file, with the default glob pattern(include all files),
    the function should return the file path"""

    file_path = os.path.join(tmpdir, "filename")
    open(file_path, "a").close()
    assert get_dir_files(dir_path=tmpdir) == [file_path]


@pytest.mark.unit
def test_get_dir_files_3(tmpdir):
    """Given a dir_path cotaining one file, file not maching the glob patten,
    the function must return an empty array"""

    file_path = os.path.join(tmpdir, "filename")
    open(file_path, "a").close()
    assert get_dir_files(dir_path=tmpdir, glob_pattern="*.txt") == []


@pytest.mark.unit
def test_get_dir_files_4(tmpdir):
    """Given a dir_ath cotaining one file, file matching the glob pattern,
    the function must return the file path"""

    file_path = os.path.join(tmpdir, "filename.txt")
    open(file_path, "a").close()
    assert get_dir_files(dir_path=tmpdir, glob_pattern="*.txt") == [file_path]


@pytest.mark.unit
def test_get_dir_files_5(tmpdir):
    """Given a dir_path cotaining two files, one matching and one not matching the glob_pattern,
    the function must return an array containing only the path matching the glob_pattern"""

    file_path1 = os.path.join(tmpdir, "filename1.txt")
    open(file_path1, "a").close()
    file_path2 = os.path.join(tmpdir, "filename2.xml")
    open(file_path2, "a").close()
    assert get_dir_files(dir_path=tmpdir, glob_pattern="*.txt") == [file_path1]


@pytest.mark.unit
def test_get_dir_files_6(tmpdir):
    """ "Given a dir_path cotaining two files, both matching the glob_pattern,
    the function shold return an array containing both file paths"""

    file_path1 = os.path.join(tmpdir, "filename1.txt")
    open(file_path1, "a").close()
    file_path2 = os.path.join(tmpdir, "filename2.txt")
    open(file_path2, "a").close()
    assert sorted(get_dir_files(dir_path=tmpdir, glob_pattern="*.txt")) == sorted([file_path1, file_path2])


@pytest.mark.unit
def test_read_xrd_1(tmpdir):
    """Given a file in an unsupported format,
    the function must return None"""

    file_path1 = os.path.join(tmpdir, "filename1.txt")
    open(file_path1, "a").close()
    files = [file_path1]
    out_ds = read_xrd(files=files)
    assert out_ds is None


@pytest.mark.usecase
def test_read_xrd_2():
    """Given a non existing file,
    the funtion must return None"""

    s3_prod_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    s3_file_name = "Oa22_radiance.nc"
    s3_file_path = os.path.join(s3_prod_path, s3_file_name)
    files = [s3_file_path]
    out_ds = read_xrd(files=files)
    assert out_ds is None


@pytest.mark.unit
def test_read_xrd_3():
    """Given no files,
    the funtion must return None"""

    files = []
    out_ds = read_xrd(files=files)
    assert out_ds is None


@pytest.mark.usecase
def test_read_xrd_4():
    """Given a file in a supported format, containing one variable which must be skipped,
    the funtion must return None"""

    s3_prod_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    s3_file_name = "Oa11_radiance.nc"
    s3_file_path = os.path.join(s3_prod_path, s3_file_name)
    files = [s3_file_path]
    skip = ["Oa11_radiance"]
    out_ds = read_xrd(files=files, skip=skip)
    assert out_ds is None


@pytest.mark.usecase
def test_read_xrd_5():
    """Given a file in a supported format, containing a variable,
    the function must return a dataset containing the variable from s3_file_path"""

    s3_prod_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    s3_file_name = "Oa11_radiance.nc"
    s3_file_path = os.path.join(s3_prod_path, s3_file_name)
    s3_file_ds = xr.open_dataset(s3_file_path, decode_times=False, mask_and_scale=False)
    s3_vars = {}
    s3_vars["Oa11_radiance"] = s3_file_ds.get("Oa11_radiance")
    s3_ds = xr.Dataset(data_vars=s3_vars)
    files = [s3_file_path]
    out_ds = read_xrd(files=files)
    assert s3_ds.equals(out_ds)


@pytest.mark.usecase
def test_read_xrd_6():
    """Given a file in a supported format, with a variable both skipped and picked,
    the function must return None since skip has precedence over pick"""

    s3_prod_path = glob.glob("data/S3A_OL_1*.SEN3")[0]
    s3_file_name = "tie_geo_coordinates.nc"
    s3_file_path = os.path.join(s3_prod_path, s3_file_name)
    files = [s3_file_path]
    pick = ["latitude"]
    skip = ["latitude"]
    out_ds = read_xrd(files=files, pick=pick, skip=skip)
    assert out_ds is None


@pytest.mark.usecase
def test_read_xrd_7():
    """Given a file in a supported format, with a variable to be picked,
    the function must return a dataset containing the variable from s3_file_path picked"""

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
    assert s3_ds.equals(out_ds)


@pytest.mark.usecase
def test_read_xrd_8():
    """Given files in a supported format, with no pick or skip,
    the function must return a dataset containing all the variables from all files"""

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
    assert s3_ds.equals(out_ds)


@pytest.mark.usecase
def test_read_xrd_9():
    """Given files in a supported format, with one file not existing,
    the function must return None due to non-existing file"""

    s3_prod_path = glob.glob("data/S3A_OL_1*.SEN3")[0]

    s3_file_name_1 = "Oa10_radiance.nc"
    s3_file_path_1 = os.path.join(s3_prod_path, s3_file_name_1)

    s3_file_name_2 = "Oa22_radiance.nc"
    s3_file_path_2 = os.path.join(s3_prod_path, s3_file_name_2)

    files = [s3_file_path_1, s3_file_path_2]
    out_ds = read_xrd(files=files)
    assert out_ds is None
