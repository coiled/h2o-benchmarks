# H2O Benchmark Data

[![Tests](https://github.com/coiled/h2o_benchmark_data/actions/workflows/tests.yml/badge.svg)](https://github.com/coiled/h2o_benchmark_data/actions/workflows/tests.yml) [![Linting](https://github.com/coiled/h2o_benchmark_data/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/coiled/h2o_benchmark_data/actions/workflows/pre-commit.yml)

This repository contains the scripts necessary to create the data for the h2o benchmark.

## Setup to create the data locally

- Clone this repository
- Create and activate the conda environment
    
     ```bash
     $ conda env create -f environment.yml
     $ conda activate h2o_data
     ```

## Instructions to create a dataset 

You can find the script to generate the datasets under the `scripts` directory. The `create_groupby_data.py` script will generate the `groupby` data for the h2o 
benchmark according to the arguments provided. For information on the arguments, run:

```bash
$ python create_groupby_data.py --help
```

**Notes**

- This script does not include NA, and the data is NOT sorted.
- This script generates multiple files in parallel using dask, you will need to modify the number of files according to your RAM availability.
- If you desired one single file you can join the `num_files` generated using `scripts/create_single.csv`.

Use the following example to check everything has been set up correctly, before you attempt the bigger files.

**Example:** 1 million rows (num_rows) and 100 groups (num_groups)

The following code will create 10 files on a directory call `test` located at the same level from where the script is running. For a different location of the directory you can provide a full or relative path. Note that if the directory doesn't exists it will create it. 

```bash
$ python create_groupby_data_dask.py --num-rows 1e6 --num-groups 1e2 --num-files 10 --output-dir test
```
To obtain a single file, you can join them using `create_single_csv.py`, the commands below will take care of the headers, keeping only the first one.

```bash
$ python create_single_csv.py -of output_dir/N_1e6_K_1e2_single.csv -ifn output_dir/groupby-N_1000000_K_100_file_ --num-files 10 
```

## The H2O benchmark data cases

The H2O benchmark has three sizes of data they use, everyone on a single file. You can generate them in your machine, but note that you might need to increase the number of files depending on your RAM. (The following were tested on a M1 arm64 with a 16GB of RAM)

### `num_rows=1e7, num_groups=1e2` (~0.5 GB as single file)

This results in 10 files
```bash
$ python create_groupby_data_dask.py --num-rows 1e7 --num-groups 1e2 --num-files 10 --output-dir output_dir
```

### `num_rows=1e8, num_groups=1e2` (~5 GB as single file)

This results in 100 files
```bash
$ python create_groupby_data_dask.py --num-rows 1e8 --num-groups 1e2 --num-files 100 --output-dir output_dir
```

### `num_rows=1e9, num_groups=1e2` (~50 GB as single file)

This results in 1000 files
```bash
$ python create_groupby_data.py --num-rows 1e9 --num-groups 1e2 --num-files 1000 --output-dir output_dir
```

**How to get a single file:**

In any of the cases above if you desired to join the files you can do it using the `create_single_csv.py`
script. There are details on the script on how to use it, but for example for the case of `num_rows=1e7`, `num_groups=1e2`
and `num_files=10`:

```bash
$ python create_single_csv.py \
     --output-file output_dir/merged.csv \
     --input-files-name output_dir/groupby-num_rows_10000000-num_groups_100-file_ \
     --num-files 10
```

## Public data on S3:

The following [S3 bucket](https://s3.console.aws.amazon.com/s3/buckets/coiled-datasets?region=us-east-2&prefix=h2o-benchmark/) contains the data to perform the h2o benchmark.We provide the single files for 
every case (num_rows=1e7, 1e8, and 1e9) as well as a folder for each case that contains `num_files=10, 100, 1000` respectively.

**S3 URI:** `s3://coiled-datasets/h2o-benchmark/`