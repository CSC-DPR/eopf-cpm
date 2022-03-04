import os

import xarray as xr

from eopf.product.conveniences import get_dir_files
from eopf.product.core.eo_container import EOContainer


def get_s3_vars(path):
    """retrieve all variables from an s3 product"""

    vars = {}

    nc_files = get_dir_files(path, "*.nc")
    for file in nc_files:
        file_ds = xr.open_dataset(file, decode_times=False, mask_and_scale=False)
        for var_name in file_ds.data_vars:
            vars[var_name] = file_ds.get(var_name)

    return vars


def get_eop_vars(eop):
    """retrieve all variables in eop"""

    groups = []
    variables = {}

    for sg in eop._groups.values():
        groups.append(sg)

    while groups:
        g_var_names = []
        g = groups.pop(0)
        for key, value in g.groups:
            groups.append(value)
        for eovar_name, eovar in g.variables:
            g_var_names.append(eovar_name)
            variables[eovar_name] = eovar

    return variables


def get_duplicate_s3_vars(path):
    vars = []
    duplicates = []
    nc_files = get_dir_files(path, "*.nc")
    for file in nc_files:
        file_ds = xr.open_dataset(file, decode_times=False, mask_and_scale=False)
        for var_name in file_ds.data_vars:
            if var_name in vars:
                if var_name not in duplicates:
                    duplicates.append(var_name)
            else:
                vars.append(var_name)

    return duplicates


def diff_s3_eop(dict_s3, dict_eop, duplicates):
    """verify if variables from dict_s3 are equal in name and data with those in dict_eop"""

    vars_found = []
    vars_not_found = []

    for s3_var in dict_s3.keys():
        if s3_var in duplicates:
            continue
        found = False
        for eop_var in dict_eop.keys():
            if s3_var == eop_var:
                if dict_s3[s3_var].equals(dict_eop[eop_var]._data):
                    found = True
                break

        if not found:
            vars_not_found.append(s3_var)
        else:
            vars_found.append(s3_var)

    return vars_found, vars_not_found


def group_details(section_detail: dict, section_structure: dict) -> None:
    subgroup_structure = {}
    item_structure = {}
    item_names = section_detail.xpath("label/text()")
    item_name = None
    if item_names:
        item_name = str(item_names[-1]).strip("', :").strip()
    subgroup_names = section_detail.xpath("div/label/text()")
    subgroup_name = None
    if subgroup_names:
        subgroup_name = str(subgroup_names[-1]).strip("', :").strip()
    if item_name == "Attributes":
        attributes = section_detail.xpath("div/dl")
        if attributes:
            attr_name = attributes[0].xpath("dt/span/text()")
            attr_value = attributes[0].xpath("dd/text()")
            if attr_name and attr_value:
                item_structure[attr_name[0]] = attr_value[0]
                section_structure[item_name] = item_structure
            else:
                section_structure[item_name] = {}
    elif item_name == "Dimensions":
        dimensions = section_detail.xpath("div/div/text()")
        if dimensions:
            dimension_value = dimensions[-1].strip("', :").strip()
            section_structure[item_name] = dimension_value
    elif item_name == "Coordinates":
        coordinates = section_detail.xpath("div/dl")
        coordinates_dict = {}
        if coordinates:
            coordinates = coordinates[0]
            coordinates_len = len(coordinates) // 2
            for i in range(0, coordinates_len):
                coord_key = coordinates.xpath("dt/span/text()")[i].strip()
                coord_value = coordinates.xpath("dd/text()")[i].strip().partition("->")[2]
                coordinates_dict[coord_key] = coord_value
        section_structure[item_name] = coordinates_dict

    subgroups = section_detail.xpath("div/div/ul/li")
    if subgroups:
        for var in subgroups:
            group_details(var, subgroup_structure)
            section_structure[subgroup_name] = subgroup_structure
    elif subgroup_name:
        section_structure[subgroup_name] = {}


def compute_tree_structure(tree) -> dict:
    product_name = str(tree.xpath("/html/body/div/label/text()")[-1]).strip("', :").strip()
    sections = tree.xpath("/html/body/div/div/ul/li")
    product_structure = {"name": product_name, "groups": {}}

    for section in sections:
        section_structure = {}
        section_name = str(section.xpath("div/label/text()")[-1]).strip("', :").strip()
        section_details = section.xpath("div/div/ul/li")
        if section_details:
            for section_detail in section_details:
                group_details(section_detail, section_structure)
                product_structure["groups"][section_name] = section_structure
        else:
            product_structure["groups"][section_name] = section_structure
    return product_structure


def cmp_s3_zarr_var_attrs(s3_var, zarr_var):
    """compares attributes between a s3 var and a zarr var"""

    return (xr.Dataset(s3_var.attrs)).equals(xr.Dataset(zarr_var.attrs))


def cmp_s3_zarr_var_data(s3_var, zarr_var):
    """compares data between a s3 var and a zarr var"""

    if (s3_var.data == zarr_var.data).all:
        return True
    else:
        False


def cmp_s3_zarr_var(s3_var, zarr_var):
    """compares data and attributes between a s3 var and a zarr var"""

    return cmp_s3_zarr_var_data(s3_var, zarr_var) and cmp_s3_zarr_var_attrs(s3_var, zarr_var)


def diff_s3_zarr(dict_s3, dict_zarr, duplicates):
    """verify if variables from dict_s3 are equal in name and data with those in dict_zarr"""

    vars_found = []
    vars_not_found = []

    for s3_var in dict_s3.keys():
        if s3_var in duplicates:
            continue
        found = False
        for zarr_var in dict_zarr.keys():
            if s3_var == zarr_var:
                if cmp_s3_zarr_var(dict_s3[s3_var], dict_zarr[zarr_var]):
                    found = True
                break

        if not found:
            vars_not_found.append(s3_var)
        else:
            vars_found.append(s3_var)

    return vars_found, vars_not_found


def get_dirs_with_zarr_vars(zarr_prod_path):
    """returns a list of directory where zarr variables can be found"""

    dirs_with_zarr_vars = []
    for root, dirs, files in os.walk(zarr_prod_path):
        for dir in dirs:
            dir_path = os.path.join(root, dir)

            zattrs_path = os.path.join(dir_path, ".zattrs")
            zgroup_path = os.path.join(dir_path, ".zgroup")
            zmetadata_path = os.path.join(dir_path, ".zmetadata")

            if os.path.isfile(zattrs_path) and os.path.isfile(zgroup_path) and os.path.isfile(zmetadata_path):
                dirs_with_zarr_vars.append(dir_path)

    return dirs_with_zarr_vars


def get_zarr_vars(path):
    """returns a dictionary of all variables in zarr format at path"""

    zarr_vars = {}
    dirs_with_zarr_vars = get_dirs_with_zarr_vars(path)
    for dir in dirs_with_zarr_vars:
        dir_vars = xr.open_zarr(dir, mask_and_scale=False, decode_times=False)
        for var_name in list(dir_vars.variables.keys()):
            zarr_vars[var_name] = xr.DataArray(dir_vars.variables[var_name])

    return zarr_vars


def assert_contain(container: EOContainer, path: str, expect_type, path_offset="/"):
    obj = container[path]
    assert obj.path == path_offset + path
    assert obj.name == path.rpartition("/")[2]
    assert isinstance(obj, expect_type)
