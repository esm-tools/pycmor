import pathlib
import tempfile

import yaml
from pyfakefs.fake_filesystem_unittest import Patcher

from pycmor.dev.utils import ls_to_yaml


def test_ls_to_yaml():
    with Patcher() as patcher:
        # Arrange
        test_dir = pathlib.Path("/test_dir")
        patcher.fs.create_dir(test_dir)
        patcher.fs.create_file(test_dir / "file1.txt")
        patcher.fs.create_file(test_dir / "file2.txt")
        output_file = tempfile.mktemp()

        # Act
        ls_to_yaml(test_dir, output_file)

        # Assert
        with open(output_file, "r") as f:
            result = yaml.safe_load(f)
        assert sorted(result) == sorted(["/test_dir/file1.txt", "/test_dir/file2.txt"])
