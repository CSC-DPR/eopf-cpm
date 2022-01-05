import os
from pathlib import Path

from jinja2 import Environment, FileSystemLoader


def renderer(template_name, **parameters):
    dir_path = Path(__file__).resolve().parent
    file_loader = FileSystemLoader(os.path.join(dir_path, "templates"))
    env = Environment(loader=file_loader, autoescape=True)
    template = env.get_template(template_name)
    html = template.render({**parameters, "is_top": True})
    return html
