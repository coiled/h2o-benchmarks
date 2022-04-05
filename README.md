# H2O Benchmark Data

This repository contains the scripts necessary to create the data for the h2o benchmark.

## Setup to create the data locally

- Clone this repository
- Create and activate the conda environment
    
     ```
     conda env create -f env/environment.yml

     conda activate h2o_data
     ```

## Instructions to create a dataset 

You can find the script to generate the datasets under the `scripts` directory. The `create_groupby_data.py` script will generate the `groupby` data for the h2o 
benchmark according to the arguments provided. For information on the arguments, run:
```
python create_groupby_data.py --help
```

**Notes**

- This script does not include NA, and the data is NOT sorted.
- This script generates multiple files in parallel using dask, you will need to modify the number of files according to your RAM availability.
- If you desired one single file you can join the `nfiles` generated using scripts/create_single.csv`.

Use the following example to check everything has been set up correctly, before you attempt the bigger files.

**Example:** 1 million rows (N) and 100 groups (K)

The following code will create 10 files on a directory call `test` located at the same level from where the script is running. For a different location of the directory you can provide a full or relative path. Note that if the directory doesn't exists it will create it. 

```
$ python create_groupby_data_dask.py -n 1e6 -k 1e2 -nf 10 -dir test
```
To obtain a single file, you can join them using `create_single_csv.py`, the commands below will take care of the headers, keeping only the first one.

```
$ python create_single_csv.py -of output_dir/N_1e6_K_1e2_single.csv -ifn output_dir/groupby-N_1000000_K_100_file_ -nf 10 
```

## The H2O benchmark data cases

The H2O benchmark has three sizes of data they use, everyone on a single file. You can generate them in your machine, but note that you might need to increase the number of files depending on your RAM. (The following were tested on a M1 arm64 with a 16GB of RAM)

### `N=1e7, K=1e2` (~0.5 GB as single file)

This results in 10 files
```
$ python create_groupby_data_dask.py -n 1e7 -k 1e2 -nf 10 -dir output_dir
```

### `N=1e8, K=1e2` (~5 GB as single file)

This results in 100 files
```
$ python create_groupby_data_dask.py -n 1e8 -k 1e2 -nf 100 -dir output_dir
```

### `N=1e9, K=1e2` (~50 GB as single file)

This results in 1000 files
```
$ python create_groupby_data.py -n 1e9 -k 1e2 -nf 1000 -dir output_dir
```

**How to get a single file:**

In any of the cases above if you desired to join the files you can do it using the `create_single_csv.py`
script. There are details on the script on how to use it, but for example for the case of `N=1e7`, `K=1e2`
and `nfiles=10`:

```
$ python create_single_csv.py -of output_dir/N_1e7_K_1e2_single.csv -ifn output_dir/groupby-N_10000000_K_100_file_ -nf 10
```

## Public data on S3:

The following [S3 bucket](https://s3.console.aws.amazon.com/s3/buckets/coiled-datasets?region=us-east-2&prefix=h2o-benchmark/) contains the data to perform the h2o benchmark.We provide the single files for 
every case (N=1e7, 1e8, and 1e9) as well as a folder for each case that contains `nfiles=10, 100, 1000` respectively.

**S3 URI:** `s3://coiled-datasets/h2o-benchmark/`