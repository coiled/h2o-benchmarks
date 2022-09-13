import os
import pathlib
import subprocess
import sys

import dask.dataframe as dd
from dask.dataframe.utils import assert_eq

SCRIPT_GROUPBY_DATA = (
    pathlib.Path(__file__).parent.parent / "scripts" / "create_groupby_data.py"
)

SCRIPT_CSV_TO_PARQUET_DATA = (
    pathlib.Path(__file__).parent.parent / "scripts" / "csv_to_parquet_data.py"
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
    ddf = dd.read_csv(os.path.join(tmp_path, "*.csv"))
    assert len(ddf) == num_rows
    assert len(ddf.columns) == 9


def test_simple_parquet(tmp_path):

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

    ddf_csv = dd.read_csv(
        os.path.join(tmp_path, "*.csv"),
        dtype={
            "id1": "category",
            "id2": "category",
            "id3": "string[python]",
            "id4": "Int32",
            "id5": "Int32",
            "id6": "Int32",
            "v1": "Int32",
            "v2": "Int32",
            "v3": "float64",
        },
    )

    subprocess.run(
        [
            sys.executable,
            SCRIPT_CSV_TO_PARQUET_DATA,
            "--csv-dir",
            str(tmp_path),
            "--output-dir",
            str(tmp_path),
        ]
    )
    ddf_pq = dd.read_parquet(os.path.join(tmp_path, "*.parquet"))

    assert_eq(ddf_csv, ddf_pq)
