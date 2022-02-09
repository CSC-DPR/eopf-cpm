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
