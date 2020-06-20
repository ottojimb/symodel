import jinja2
from typing import Optional


class Jinja2:
    templateLoader = jinja2.FileSystemLoader(searchpath="templates")
    templateEnv = jinja2.Environment(
        loader=templateLoader, trim_blocks=True, lstrip_blocks=True
    )
    template: Optional[jinja2.Template]

    @classmethod
    def set_template(cls, template_file="base.py.jinja2"):
        cls.template = cls.templateEnv.get_template(template_file)

    @classmethod
    def render(cls, **kargs):
        return cls.template.render(**kargs)
