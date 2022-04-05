import os
import pathlib
import subprocess
import sys

import dask.dataframe as dd

SCRIPT_GROUPBY_DATA = (
    pathlib.Path(__file__).parent.parent / "scripts" / "create_groupby_data.py"
)


def test_simple(tmp_path):

    num_rows = 30
    num_groups = 7
    num_files = 2
    assert not os.listdir(tmp_path)
    subprocess.run(
        [
            sys.executable,
            SCRIPT_GROUPBY_DATA,
            "--num-rows",
            str(num_rows),
            "--num-groups",
            str(num_groups),
            "--num-files",
            str(num_files),
            "--output-dir",
            str(tmp_path),
        ]
    )
    # Ensure we have the expected number of files, data set size, and
    # files are in the specified output directory
    assert len(os.listdir(tmp_path)) == num_files
    ddf = dd.read_csv(tmp_path / "*.csv")
    assert len(ddf) == num_rows
    assert len(ddf.columns) == 9
