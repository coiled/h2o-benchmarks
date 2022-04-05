"""
This script will join the multiple csv files keeping the
header only for the first one.

For example for a case with of N=1e7, K=1e2 and nfiles=10 created with
create_groupby_data.py. Assuming the directory of the data is called test,
to join the data and save it in the test/ directory you will do :

python create_single_csv.py -of test/N_1e7_K_1e2_single.csv -ifn test/groupby-N_10000000_K_100_file_ -nf 10
"""

import timeit
from argparse import ArgumentParser


def read_inputs():
    """
    Parse command-line arguments to run create_groupby_data.
    User should provide:
    -output_file : str,
        Destination of single file, for example "test/single_py.csv"
    -input_files_name: str,
        Location and name of the original nfiles without the number
        For example if your files are named groupby-N_10000000_K_100_file_53.csv
        and are in a directory named test then input_files is
        test/groupby-N_10000000_K_100_file_
    -nfiles : int
        Number of files that will make the single joined file.

    """

    parser = ArgumentParser(
        description="Manage create_single_csv command line arguments"
    )

    parser.add_argument(
        "-of",
        "--output_file",
        dest="of",
        type=str,
        default="",
        help="Single file destination",
    )

    parser.add_argument(
        "-ifn",
        "--input_files_name",
        dest="ifn",
        type=str,
        default="",
        help="Location and name of the files without the number",
    )

    parser.add_argument(
        "-nf",
        "--nfiles",
        dest="nfiles",
        type=int,
        default=None,
        help="Number of files to join",
    )

    return parser.parse_args()


def join_files(output_file, input_files_name, nfiles):
    """
    Joins nfiles into a single csv file keeping the ehader of first file.
    This functions assumes the numbering starts from 0
    Parameters
    ----------
    output_file: str
        destination of single file, for example "test/single_py.csv"
    input_files_name: str
        name of the original nfiles without the number
        For example if your files are named groupby-N_10000000_K_100_file_53.csv
        then input_file_name is groupby-N_10000000_K_100_file_
    nfiles: int
        number of file to join
    """
    with open(f"{output_file}", "wb") as fout:
        # first file:
        with open(f"{input_files_name}0.csv", "rb") as f:
            fout.write(f.read())
        # now the rest:
        for num in range(1, nfiles):
            with open(f"{input_files_name}{num}.csv", "rb") as f:
                next(f)  # skip the header
                fout.write(f.read())


if __name__ == "__main__":

    args = read_inputs()

    output_file = args.of
    input_files_name = args.ifn
    nfiles = args.nfiles

    tic = timeit.default_timer()
    join_files(output_file, input_files_name, nfiles)
    toc = timeit.default_timer()
    total_time = toc - tic

    print(f"Creating the single file took: {total_time:.2f} s")
