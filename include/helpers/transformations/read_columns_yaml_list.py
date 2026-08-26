import yaml


def read_columns_yaml_list(yaml_path: str) -> list[str]:
    """
    Return the ordered list of column names declared in a YAML contract.
    """
    with open(yaml_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    columns = data.get("columns", [])
    if not isinstance(columns, list):
        raise ValueError("The YAML contract must contain a list under 'columns'.")

    return [col["name"] for col in columns if "name" in col]
