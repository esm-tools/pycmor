"""
Utilities that can help with testing
"""

import pathlib
import sys

import yaml


def ls_to_yaml(directory: str or pathlib.Path, output=None) -> str:
    """
    List the contents of a directory and write it to a yaml file

    Parameters
    ----------
    directory : str or pathlib.Path
        The directory to list
    output : str or file-like object
        The file to write the yaml to, defaults to sys.stdout

    Returns
    -------
    str
        The yaml string
    """
    directory = pathlib.Path(directory)
    output = sys.stdout if output is None else output
    files = [str(file) for file in directory.iterdir()]
    yaml_str = yaml.dump(files, default_flow_style=False)
    if output is not sys.stdout:
        with open(output, "w") as f:
            f.write(yaml_str)
    else:
        print(yaml_str)
    return yaml_str
