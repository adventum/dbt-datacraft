import pathlib
import yaml
import json
from jinja2 import Environment, FileSystemLoader


def get_datacraft_defaults(
    config_name: str, format: str, suffix: str = "", template_variables: dict = {}
) -> dict:
    filename = f"{config_name}{suffix or ''}.{format}"
    directory_path = pathlib.Path(__file__).parent
    filepath = directory_path / filename
    if not (filepath.exists() and filepath.is_file()):
        filepath = pathlib.Path(__file__).parent / f"{filename}.j2"
        if not (filepath.exists() and filepath.is_file()):
            raise DatacraftDefaultsError(f"config not found ({str(filepath)})")
        template_loader = FileSystemLoader(searchpath=directory_path)
        jinja_env = Environment(loader=template_loader)
        # template = jinja_env.get_template(str(filepath))
        template = jinja_env.get_template(f"{filename}.j2")
        file_content = template.render(**template_variables)
    else:
        file_content = filepath.read_text()
    match format:
        case "yaml":
            return yaml.safe_load(file_content)
        case "json":
            return json.loads(file_content)
    raise DatacraftDefaultsError(f"unknown format ({format})")


class DatacraftDefaultsError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return f"DatacraftDefaultsError: {self.message}"
