import json
import os
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from eopf.product.utils import conv


def renderer(template_name: str, attrs: dict[str, Any] = {}, **parameters: Any) -> str:
    """Render a eopf template from the given name

    Parameters
    ----------
    template_name: str
        name of the template to render
    **parameters: Any
        context parameters for the template

    Returns
    -------
    str
    """

    def human_readable_attrs(value):
        return json.dumps(conv(value), indent=4)

    dir_path = Path(__file__).resolve().parent
    file_loader = FileSystemLoader(os.path.join(dir_path, "templates"))
    env = Environment(loader=file_loader, autoescape=True)
    env.filters["human_readable_attrs"] = human_readable_attrs
    template = env.get_template(template_name)
    html = template.render({**parameters, "is_top": True})
    return html
