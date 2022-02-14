import os
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader


def renderer(template_name: str, **parameters: Any) -> str:
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
    dir_path = Path(__file__).resolve().parent
    file_loader = FileSystemLoader(os.path.join(dir_path, "templates"))
    env = Environment(loader=file_loader, autoescape=True)
    template = env.get_template(template_name)
    html = template.render({**parameters, "is_top": True})
    return html
