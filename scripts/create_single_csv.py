"""
This script will join the multiple csv files keeping the
header only for the first one.

For example for a case with of num_rows=1e7, num_groups=1e2 and num_files=10 created with
create_groupby_data.py. Assuming the directory of the data is called test,
to join the data and save it in the test/ directory you will do :

python create_single_csv.py \
    --output-file test/N_1e7_K_1e2_single.csv \
    --input-files-name test/groupby-N_10000000_K_100_file_ \
    --num-files 10
"""

import pathlib
import timeit

import click


def join_files(output_file, input_files_name, num_files):
    """
    Joins num_files into a single csv file keeping the ehader of first file.
    This functions assumes the numbering starts from 0
    Parameters
    ----------
    output_file: str
        destination of single file, for example "test/single_py.csv"
    input_files_name: str
        name of the original num_files without the number
        For example if your files are named groupby-num_rows_10000000-num_groups_100-file_53.csv
        then input_file_name is groupby-num_rows_10000000-num_groups_100-file_
    num_files: int
        number of file to join
    """
    with open(output_file, "wb") as fout:
        for n in range(num_files):
            with open(f"{input_files_name}_{n}.csv", "rb") as f:
                # Only write header from first file
                if n != 0:
                    next(f)
                fout.write(f.read())


@click.command(context_settings={"ignore_unknown_options": True})
@click.option(
    "--output-file",
    type=click.Path(dir_okay=False, path_type=pathlib.Path),
    required=True,
    help="Output file",
)
@click.option(
    "--input-files-name",
    type=str,
    required=True,
    help="Location and name of the files without the number",
)
@click.option(
    "--num-files",
    type=int,
    required=True,
    help="Number of files to join",
)
def main(output_file, input_files_name, num_files):
    t_start = timeit.default_timer()
    join_files(output_file, input_files_name, num_files)
    total_time = timeit.default_timer() - t_start
    print(f"Merging {num_files} took {total_time:.2f} s")


if __name__ == "__main__":
    main()
