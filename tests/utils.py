import xarray as xr

from eopf.product.conveniences import get_dir_files


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
    """verify if variables from dict_se are equal in name and data with those in dict_eop"""

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
        item_name = str(item_names[-1]).strip()
    subgroup_names = section_detail.xpath("div/label/text()")
    subgroup_name = None
    if subgroup_names:
        subgroup_name = str(subgroup_names[-1]).strip()
    attributes = section_detail.xpath("div/dl")
    if attributes:
        attr_name = attributes[0].xpath("dt/span/text()")
        attr_value = attributes[0].xpath("dd/text()")
        if attr_name and attr_value:
            item_structure[attr_name[0]] = attr_value[0]
            section_structure[item_name] = item_structure
        else:
            section_structure[item_name] = {}
    subgroups = section_detail.xpath("div/div/ul/li")
    for var in subgroups:
        group_details(var, subgroup_structure)
        section_structure[subgroup_name] = subgroup_structure


def compute_tree_structure(tree) -> dict:
    product_name = str(tree.xpath("/html/body/div/label/text()")[-1]).strip()
    sections = tree.xpath("/html/body/div/div/ul/li")
    product_structure = {"name": product_name, "groups": {}}

    for section in sections:
        section_structure = {}
        section_name = str(section.xpath("div/label/text()")[-1]).strip()

        section_details = section.xpath("div/div/ul/li")
        for section_detail in section_details:
            group_details(section_detail, section_structure)
            product_structure["groups"][section_name] = section_structure
    return product_structure
