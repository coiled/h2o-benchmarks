import os
import pathlib
import subprocess
import sys

import dask.dataframe as dd
import pandas as pd

SCRIPT_GROUPBY_DATA = (
    pathlib.Path(__file__).parent.parent / "scripts" / "create_groupby_data.py"
)
SCRIPT_MERGE = pathlib.Path(__file__).parent.parent / "scripts" / "create_single_csv.py"


def test_simple(tmp_path):

    # Generate multiple groupby data files
    subprocess.run(
        [
            sys.executable,
            SCRIPT_GROUPBY_DATA,
            "--num-rows",
            "30",
            "--num-groups",
            "7",
            "--num-files",
            "2",
            "--output-dir",
            str(tmp_path),
        ]
    )
    filepath = str(tmp_path / os.listdir(tmp_path)[0])
    expected = dd.read_csv(tmp_path / "*.csv")

    # Test merge script generates a single file with the same dataset
    merged_file = str(tmp_path / "merged.csv")
    subprocess.run(
        [
            sys.executable,
            SCRIPT_MERGE,
            "--input-files-name",
            "_".join(filepath.split("_")[:-1]),
            "--output-file",
            merged_file,
            "--num-files",
            "2",
        ]
    )
    result = pd.read_csv(merged_file)
    dd.utils.assert_eq(expected, result, check_index=False)
