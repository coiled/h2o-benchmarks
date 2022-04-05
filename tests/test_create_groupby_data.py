import os
import pathlib
import subprocess
import sys

import dask.dataframe as dd

SCRIPT = pathlib.Path(__file__).parent.parent / "scripts" / "create_groupby_data.py"


def test_create_groupby_data(tmp_path):

    N = 30
    K = 7
    nfiles = 2
    assert not os.listdir(tmp_path)
    subprocess.run(
        [
            sys.executable,
            SCRIPT,
            "--N",
            str(N),
            "--K",
            str(K),
            "--nfiles",
            str(nfiles),
            "--output_dir",
            str(tmp_path),
        ]
    )
    # Ensure we have the expected number of files, data set size, and
    # files are in the specified output directory
    assert len(os.listdir(tmp_path)) == nfiles
    ddf = dd.read_csv(tmp_path / "*.csv")
    assert len(ddf) == N
    assert len(ddf.columns) == 9
