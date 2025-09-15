"""
Various utility functions needed around the package
"""

import importlib
import inspect
import os
import tempfile
import time
from functools import partial

import pkg_resources
import requests

from .logging import logger


def get_callable(name):
    """Get a callable from a string
    First, tries standard import, then tries entry points, then from script
    """
    try:
        return get_callable_by_name(name)
    except (ImportError, AttributeError):
        pass

    try:
        return get_entrypoint_by_name(name)
    except ValueError:
        pass

    try:
        return get_callable_by_script(name)
    except ValueError:
        pass

    raise ValueError(f"Callable '{name}' not found")


def get_callable_by_name(name):
    """
    Get a callable by its name.

    This function takes a string that represents the fully qualified name of a callable object
    (i.e., a function or a method), and returns the actual callable object. The name should be in
    the format 'module.submodule.callable'. If the callable does not exist, this function will raise
    an AttributeError.

    Parameters
    ----------
    name : str
        The fully qualified name of the callable to be retrieved. It should be in the format
        'module.submodule.callable'.

    Returns
    -------
    callable
        The callable object that corresponds to the given name.

    Raises
    ------
    ImportError
        If the module or submodule specified in the name does not exist.
    AttributeError
        If the callable specified in the name does not exist in the given module or submodule.
    """
    if "." not in name:
        raise ValueError(
            f"Name '{name}' is not a fully qualified name. It should be in the format 'module.submodule.callable'."
        )
    module_name, callable_name = name.rsplit(".", 1)
    logger.debug(f"Importing module '{module_name}' to get callable '{callable_name}'")
    module = __import__(module_name, fromlist=[callable_name])
    return getattr(module, callable_name)


def get_entrypoint_by_name(name, group="pymor.steps"):
    """
    Get an entry point by its name.

    This function takes a string that represents the name of an entry point in a given group,
    and returns the actual entry point object. If the entry point does not exist, this function
    will raise a ValueError.

    Parameters
    ----------
    name : str
        The name of the entry point to be retrieved.

    group : str
        The group that the entry point belongs to.

    Returns
    -------
    EntryPoint
        The entry point object that corresponds to the given name.

    Raises
    ------
    ValueError
        If the entry point specified by the name does not exist in the given group.
    """
    logger.debug(f"Getting entry point '{name}' from group '{group}'")
    for entry_point in pkg_resources.iter_entry_points(group=group):
        if entry_point.name == name:
            return entry_point.load()

    raise ValueError(f"Entry point '{name}' not found in group '{group}'")


def generate_partial_function(func: callable, open_arg: str, *args, **kwargs):
    """
    Reduces func to a partial function by fixing all but the argument named by open_arg.

    Parameters
    ----------
    func : callable
        The function to be partially applied.
    open_arg : str
        The name of the argument that should remain open in the partial function.
    *args
        Positional arguments to be passed to the partial function.
    **kwargs
        Keyword arguments to be passed to the partial function.

    Returns
    -------
    callable
        The partial function with the specified arguments fixed.
    """
    if not can_be_partialized(func, open_arg, args, kwargs):
        raise ValueError(
            f"Function '{func.__name__}' cannot be partially applied with open "
            f"argument '{open_arg}' by using the provided arguments {args=} and "
            f"keyword arguments {kwargs=}."
        )
    logger.debug(
        f"Generating partial function for '{func.__name__}' with open argument '{open_arg}'"
    )
    # Get the signature of the function
    signature = inspect.signature(func)
    # Get the parameter names
    param_names = list(signature.parameters.keys())
    # Get the index of the open argument
    open_arg_index = param_names.index(open_arg)
    # Get the names of the arguments to be fixed
    fixed_args = (
        param_names[:open_arg_index] + param_names[open_arg_index + 1 :]  # noqa: E203
    )
    # Get the values of the arguments to be fixed
    fixed_values = [kwargs[arg] for arg in fixed_args if arg in kwargs]
    # Remove the fixed arguments from the keyword arguments
    for arg in fixed_args:
        kwargs.pop(arg, None)
    # Create the partial function
    return partial(func, *fixed_values, *args, **kwargs)


def can_be_partialized(
    func: callable, open_arg: str, arg_list: list, kwargs_dict: dict
) -> bool:
    """
    Checks if a function can be reasonably partialized with a single argument open.

    Parameters
    ----------
    func : callable
        The function to be partially applied.
    open_arg : str
        The name of the argument that should remain open in the partial function.
    arg_list : list
        The list of arguments that will be passed to the partial function.
    kwargs_dict : dict
        The dictionary of keyword arguments that will be passed to the partial function.

    Returns
    -------
    bool
        True if the function can be partially applied with a single argument open, False otherwise.
    """
    signature = inspect.signature(func)
    param_names = list(signature.parameters.keys())
    # Check that all arguments in arg_list are in the function signature
    for arg in arg_list:
        if arg in param_names:
            param_names.remove(arg)
    for kwarg in kwargs_dict:
        if kwarg in param_names:
            param_names.remove(kwarg)
    # Check that there is only one argument left and that it is open_arg
    return len(param_names) == 1 and param_names[0] == open_arg


def get_function_from_script(script_path: str, function_name: str):
    """
    Get a function from a Python script.

    This function takes the path to a Python script and the name of a function defined in that script,
    and returns the actual function object. If the script does not exist or the function is not defined
    in the script, this function will raise an ImportError.

    Parameters
    ----------
    script_path : str
        The path to the Python script where the function is defined.
    function_name : str
        The name of the function to be retrieved.

    Returns
    -------
    callable
        The function object that corresponds to the given name in the specified script.

    Raises
    ------
    ImportError
        If the script does not exist or the function is not defined in the script.
    """
    logger.debug(f"Importing function '{function_name}' from script '{script_path}'")
    spec = importlib.util.spec_from_file_location("script", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, function_name)


def get_callable_by_script(step_signature):
    if not step_signature.startswith("script://"):
        raise ValueError(f"Step signature '{step_signature}' is not a script step")
    script_spec = step_signature.split("script://")[1]
    script_path = script_spec.split(":")[0]
    function_name = script_spec.split(":")[1]
    return get_function_from_script(script_path, function_name)


def wait_for_workers(client, n_workers, timeout=600):
    """
    Wait for a specific number of workers to be available.

    Args:
    client (distributed.Client): The Dask client
    n_workers (int): The number of workers to wait for
    timeout (int): Maximum time to wait in seconds

    Returns:
    bool: True if the required number of workers are available, False if timeout occurred
    """
    start_time = time.time()
    while len(client.scheduler_info()["workers"]) < n_workers:
        if time.time() - start_time > timeout:
            logger.critical(
                f"Timeout reached. Only {len(client.scheduler_info()['workers'])} workers available."
            )
            return False
        time.sleep(1)  # Wait for 1 second before checking again
    logger.info(f"{n_workers} workers are now available.")
    return True


def git_url_to_api_url(git_url, path="", branch="main"):
    """
    Convert a GitHub URL to the GitHub API URL for accessing directory contents.

    Parameters
    ---------
    git_url : str
        the original GitHub repository URL.
    path : str
        the path to the directory within the repository (default: "").
    branch : str
        the branch or commit hash to target (default: main).

    Returns
    -------
    str :
        the API URL.
    """
    if not git_url.startswith("https://github.com/"):
        raise ValueError("Invalid GitHub URL. Must start with 'https://github.com/'.")

    # Extract repo owner and name
    parts = git_url.replace("https://github.com/", "").strip("/").split("/")
    if len(parts) < 2:
        raise ValueError(
            "Invalid GitHub URL. Must include both owner and repository name."
        )

    repo_owner, repo_name = parts[:2]

    # Build the API URL
    api_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{path}?ref={branch}"
    return api_url


def list_files_in_directory(git_url, directory_path, branch="main"):
    """
    Get a list of file names in a directory from a GitHub repository.

    Parameters:
    - git_url: str, the GitHub repository URL.
    - directory_path: str, the path to the directory in the repository.
    - branch: str, the branch or commit hash to target (default: main).

    Returns:
    - list of str, filenames in the directory.
    """
    api_url = git_url_to_api_url(git_url, path=directory_path, branch=branch)

    response = requests.get(api_url)
    if response.status_code == 200:
        contents = response.json()
        filenames = [item["name"] for item in contents if item["type"] == "file"]
        return filenames
    else:
        raise ValueError(
            f"Failed to fetch directory contents. Status code: {response.status_code}"
        )


def download_json_tables_from_url(url: str, filenames: list):
    """
    Downloads JSON tables from a raw git URL

    Parameters
    ----------
    url : str
        The URL to download the JSON tables from.

    Returns
    -------
    str :
        The directory where the JSON tables were downloaded.
    """
    directory = tempfile.mkdtemp()
    logger.debug(f"Downloading JSON tables from '{url}' to '{directory}'")
    for filename in filenames:
        response = requests.get(f"{url}/{filename}")
        response.raise_for_status()
        with open(os.path.join(directory, filename), "w") as file:
            file.write(response.text)
            logger.debug(f"Loaded file {filename}")
    return directory
