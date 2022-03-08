from eopf.product.core.eo_container import EOContainer


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
    elif item_name == "Dimensions" or item_name == "Coordinates":
        dimensions = section_detail.xpath("div/div/text()")
        if dimensions:
            dimension_value = dimensions[-1].strip("', :").strip()
            dimension_value = dimension_value.partition("->")[2]
            section_structure[item_name] = dimension_value

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


def assert_contain(container: EOContainer, path: str, expect_type, path_offset="/"):
    obj = container[path]
    assert obj.path == path_offset + path
    assert obj.name == path.rpartition("/")[2]
    assert isinstance(obj, expect_type)
