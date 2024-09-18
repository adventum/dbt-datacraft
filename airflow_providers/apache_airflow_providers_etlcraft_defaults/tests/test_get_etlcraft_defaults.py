from apache_airflow_providers_etlcraft_defaults import get_etlcraft_defaults

def get_etlcraft_defaults(config_name: str, format: str, suffix: str="", template_variables: dict={}) -> dict:
    filename = f"{config_name}{suffix or ''}.{format}"
    directory_path = pathlib.Path(__file__).parent
    filepath =  directory_path / filename
    if not (filepath.exists() and filepath.is_file()):
        filepath = pathlib.Path(__file__).parent / f"{filename}.j2"
        if not (filepath.exists() and filepath.is_file()):
            raise EtlcraftDefaultsError(f"config not found ({str(filepath)})")
        template_loader = FileSystemLoader(searchpath=directory_path)
        jinja_env = Environment(loader=template_loader)
        template = jinja_env.get_template(str(filepath))
        file_content = template.render(**template_variables)
    else:
        file_content = filepath.read_text()
    match format:
        case 'yaml':
            return yaml.safe_load(file_content)
        case 'json':
            return json.loads(file_content)
    raise EtlcraftDefaultsError(f"unknown format ({format})")        

class EtlcraftDefaultsError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return f"EtlcraftDefaultsError: {self.message}"
