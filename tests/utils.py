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


def _compute_rec(node):
    try:
        name = str(node.xpath("label/text()")[-1]).strip("', :").strip()
    except Exception:
        return {}
    sections = node.xpath("div/ul/li/div")
    structure = {}
    for section in sections:
        structure |= _compute_rec(section)
    import json

    node_attrs = node.xpath('div/ul/li/div/dl[@class="eopf-attrs"][1]/dd')
    attrs = {}
    coords = []
    if node_attrs:
        attrs = json.loads(node_attrs[0].text)
        if len(node_attrs) > 1:
            coords = [i.text.strip().split("coordinates -> ")[-1] for i in node_attrs[1:]]

    node_dims = node.xpath('div/ul/li/div/div[@class="eopf-section-inline-details"]')
    dims = []
    if node_dims:
        for d in node_dims[0].text.strip()[1:-1].split(","):
            d = d[1:-1]
            if d:
                dims.append(d)
    structure["dims"] = tuple(dims)

    structure["attrs"] = attrs
    structure["coords"] = coords
    return {name: structure}


def compute_tree_structure(tree) -> dict:
    root = tree.xpath("/html/body/div")[0]
    return _compute_rec(root)


def assert_contain(container: EOContainer, path: str, expect_type, path_offset="/"):
    obj = container[path]
    assert obj.path == path_offset + path
    assert obj.name == path.rpartition("/")[2]
    assert isinstance(obj, expect_type)
