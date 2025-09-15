import io
import os
import re

from setuptools import find_packages, setup

import versioneer


def read(filename):
    filename = os.path.join(os.path.dirname(__file__), filename)
    text_type = type("")
    with io.open(filename, mode="r", encoding="utf-8") as fd:
        return re.sub(text_type(r":[a-z]+:`~?(.*?)`"), text_type(r"``\1``"), fd.read())


docs_require = read("doc/requirements.txt").splitlines()


setup(
    name="pycmor",
    python_requires=">=3.9, <4",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    url="https://github.com/esm-tools/pycmor",
    license="MIT",
    author="Paul Gierz",
    author_email="pgierz@awi.de",
    description="Makes CMOR Simple",
    long_description=read("README.rst"),
    long_description_content_type="text/x-rst",
    package_dir={"": "src"},
    packages=find_packages(where="src", exclude=("tests",)),
    # NOTE: Please keep this list sorted! In vim, you can use
    # visual-block mode (Ctrl-V) to select the lines and then `:sort`.
    # or use the vim-ism (starting anywhere in the list)::
    #
    #   vi[:sort<CR>
    #
    # meaning: [v]isual [i]nside square brackets, command mode, sort, enter.
    install_requires=[
        "bokeh",
        "cerberus",
        "cf_xarray",
        "cftime",
        "chemicals",
        "click-loguru",
        "dask",
        "dask_jobqueue",
        "deprecation",
        "distributed",
        "dpath",
        "everett[yaml]",
        "flexparser < 0.4",  # NOTE(PG): See https://tinyurl.com/ypf99xnh
        "flox",
        "h5netcdf",
        "imohash",
        "joblib",
        "netcdf4",
        "numbagg<0.9.0",  # Pin to avoid TypeAlias import issue in Python 3.9
        "numpy",
        "pendulum",
        "pint-xarray<0.6.0",
        "prefect[dask]",
        "pyyaml",
        "questionary",
        "randomname",
        "semver >= 3.0.4",
        "rich-click",
        "streamlit",
        "tqdm",
        "versioneer",
        "xarray",
    ],
    extras_require={
        "dev": [
            "black",
            "dill",
            "flake8",
            "isort",
            "pooch",
            "pre-commit",
            "pyfakefs",
            "pytest",
            "pytest-asyncio",
            "pytest-cov",
            "pytest-mock",
            "pytest-xdist",
            "sphinx",
            "sphinx_rtd_theme",
            "yamllint",
        ],
        "doc": docs_require,
        "fesom": [
            # NOTE(PG): pyfesom2 is now auto-publishing (GH pyfesom2 #215)
            #           See the relevant information in shell::
            #
            #             $ gh pr view 215 --repo fesom/pyfesom2
            "pyfesom2",
        ],
    },
    entry_points={
        "console_scripts": [
            # Canonical entry point
            "pycmor=pycmor.cli:main",
            # Backward-compatible alias for one transition release
            "pymor=pycmor.cli:main",
        ],
        # New canonical plugin entry point group
        "pycmor.cli_subcommands": [
            "plugins=pycmor.core.plugins:plugins",
            "externals=pycmor.core.externals:externals",
        ],
        # Backward-compatible plugin group for existing plugins
        "pymor.cli_subcommands": [
            "plugins=pycmor.core.plugins:plugins",
            "externals=pycmor.core.externals:externals",
        ],
    },
    include_package_data=True,
    package_data={
        "pycmor": ["data/*.yaml", "data/cmip7/all_var_info.json"],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Physics",
        "Topic :: Scientific/Engineering :: Atmospheric Science",
        "Topic :: Scientific/Engineering :: Oceanography",
    ],
)
