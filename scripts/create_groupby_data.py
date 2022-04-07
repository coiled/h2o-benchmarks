"""
This script will generate the groupby data for the h2o benchmark.
Notes
- This script does not include NA, and the data is NOT sorted.
- This script generates multiple files in parallel using dask
- If you desired one join file you can join them using bash.

Example for 1e6 rows (num_rows) and 100 groups (num_groups)

# This will create 10 files on a directory. If the directory provided
# does not exists it will create it.

$ python create_groupby_data.py --num-rows 1e6 --num-groups 1e2 --num-files 10 --output-dir test

# If you want to join the files using please check instructions for
# create_single_csv.py
"""
import functools
import os
import pathlib
import timeit

import click
import numpy as np
import pandas as pd
from dask.distributed import Client, wait


def create_file(
    num_rows: int, num_groups: int, num_files: int, output_dir: pathlib.Path, i: int
) -> None:
    """Creates a single pandas dataframe that contains num_rows/num_files rows

    Parameters
    ----------
    num_rows: int
        Total number of rows
    num_groups: int
        Number of groups
    num_files: int
        Number of output files
    output_dir: str
        Output directory
    i: int
        Integer to assign to the multiple files e.g. ``range(num_files)``
    """

    sample_id12 = [f"id{str(x).zfill(3)}" for x in range(1, num_groups + 1)]
    sample_id3 = [
        f"id{str(x).zfill(10)}" for x in range(1, int(num_rows / num_groups) + 1)
    ]

    size = int(num_rows / num_files)
    data = {}
    data["id1"] = np.random.choice(sample_id12, size=size, replace=True)
    data["id2"] = np.random.choice(sample_id12, size=size, replace=True)
    data["id3"] = np.random.choice(sample_id3, size=size, replace=True)
    data["id4"] = np.random.choice(num_groups, size=size, replace=True)
    data["id5"] = np.random.choice(num_groups, size=size, replace=True)
    data["id6"] = np.random.choice(int(num_rows / num_groups), size=size, replace=True)
    data["v1"] = np.random.choice(5, size=size, replace=True)
    data["v2"] = np.random.choice(15, size=size, replace=True)
    data["v3"] = np.random.uniform(0, 100, size=size)
    df = pd.DataFrame(data)

    output_file = (
        output_dir / f"groupby-num_rows_{num_rows}-num_groups_{num_groups}-file_{i}.csv"
    )
    df.to_csv(
        output_file,
        index=False,
        float_format="{:.6f}".format,
    )


@click.command(context_settings={"ignore_unknown_options": True})
@click.option(
    "--num-rows",
    type=int,
    required=True,
    help="Number of rows of the DataFrame",
)
@click.option(
    "--num-groups",
    type=int,
    required=True,
    help="Number of groups",
)
@click.option(
    "--num-files",
    type=int,
    required=True,
    help="Number of output files",
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False, path_type=pathlib.Path),
    required=True,
    help="Output directory",
)
def main(num_rows, num_groups, num_files, output_dir):

    os.makedirs(output_dir, exist_ok=True)
    with Client() as client:
        t_start = timeit.default_timer()
        futures = client.map(
            functools.partial(create_file, num_rows, num_groups, num_files, output_dir),
            range(num_files),
        )
        wait(futures)
        total_time = timeit.default_timer() - t_start
        print(f"Creating {num_files} files in {output_dir} took {total_time:.2f} s")


if __name__ == "__main__":
    main()
