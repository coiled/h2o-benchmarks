"""
This script will generate multiple parquet files of 100MB based
on the csv data generated using `create_groupby_data.py`

the csv-dir provided can be a localk directory or an s3 uri. For example to create
the parquet data for the case of 1e7 rows using the s3 public data we provide, run:

$ python csv_to_parquet_data.py --csv-dir s3://coiled-datasets/h2o-benchmark/N_1e7_K_1e2/ --output-dir test_parquet

You will find teh parquet files under the output directory test_parquet.
"""

import os
import pathlib
import timeit

import click
import dask.dataframe as dd
from dask.distributed import Client


def get_parquet(csv_dir, output_dir):
    ddf = dd.read_csv(
        csv_dir.rstrip("/") + "/*.csv",
        dtype={
            "id1": "category",
            "id2": "category",
            "id3": "category",
            "id4": "Int32",
            "id5": "Int32",
            "id6": "Int32",
            "v1": "Int32",
            "v2": "Int32",
            "v3": "float64",
        },
    )
    ddf.repartition(partition_size="100MB").to_parquet(
        output_dir,
        engine="pyarrow",
        compression="snappy",
        write_metadata_file=False,
    )


@click.command(context_settings={"ignore_unknown_options": True})
@click.option(
    "--csv-dir",
    type=str,
    required=True,
    help="csv files s3 uri",
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False, path_type=pathlib.Path),
    required=True,
    help="Output directory",
)
def main(csv_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    with Client() as client:  # noqa: F841
        t_start = timeit.default_timer()
        get_parquet(csv_dir, output_dir)
        total_time = timeit.default_timer() - t_start
        print(f"Creating parquet files in {output_dir} took {total_time:.2f} s")


if __name__ == "__main__":
    main()
