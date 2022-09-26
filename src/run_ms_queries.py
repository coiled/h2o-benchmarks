from pathlib import Path
import pandas as pd

import dask.dataframe as dd
from dask import config
from dask.distributed import Client
from distributed.diagnostics import MemorySampler

SHUFFLE="p2p"



def run(client, ddf, outfname):

    cwd = Path.cwd()
    outfname = Path(cwd, "data", outfname)
    print("Starting to run queries...")

    ms = MemorySampler()
    
    try:
        client.restart()
        print("Running query 1")
        with ms.sample("query_1"):
            ddf_q1 = (
                ddf.groupby("id1", dropna=False, observed=True
                ).agg({"v1": "sum"}).compute()
            )
    except Exception as e:
        print(f"Failed q1 with {e}")


    try:
        client.restart()
        print("Running query 2")
        with ms.sample("query_2"):    
            ddf_q2 = (
                ddf.groupby(["id1", "id2"], dropna=False, observed=True
                ).agg({"v1": "sum"}).compute()
                )
    except Exception as e:
        print(f"Failed q2 with {e}")


    try:
        client.restart()
        print("Running query 3")
        with ms.sample("query_3"):
            ddf_q3 = (
                ddf.groupby("id3", dropna=False, observed=True)
                .agg({"v1": "sum", "v3": "mean"}, split_out=6, shuffle=SHUFFLE)
                .compute()
        )
    except Exception as e:
        print(f"Failed q3 with {e}")


    try:
        client.restart()
        print("Running query 4")
        with ms.sample("query_4"):
            ddf_q4 = (
                ddf.groupby("id4", dropna=False, observed=True)
                .agg({"v1": "mean", "v2": "mean", "v3": "mean"}, split_out=4, shuffle=SHUFFLE)
                .compute()
            )
    except Exception as e:
        print(f"Failed q4 with {e}")



    try:
        client.restart()
        print("Running query 5")
        with ms.sample("query_5"):
            ddf_q5 =(
                ddf.groupby("id6", dropna=False, observed=True)
                .agg({"v1": "sum", "v2": "sum", "v3": "sum"}, split_out=4, shuffle=SHUFFLE)
                .compute()
            )
    except Exception as e:
        print(f"Failed q5 with {e}")

    # # Unable to run Query 6


    try:
        client.restart()

        print("Running query 7")
        with ms.sample("query_7"):
            ddf_q7 = (
                ddf.groupby("id3", dropna=False, observed=True)
                .agg({"v1": "max", "v2": "min"}, split_out=6, shuffle=SHUFFLE)
                .assign(range_v1_v2=lambda x: x["v1"] - x["v2"])[["range_v1_v2"]]
                .compute()
            )
    except Exception as e:
        print(f"Failed q7 with {e}")


    try:
        client.restart()
        print("Running query 8")
        with ms.sample("query_8"):    
            ddf_q8 = ddf[["id6", "v1", "v2", "v3"]]
            (
                ddf_q8[~ddf_q8["v3"].isna()][["id6", "v3"]]
                .groupby("id6", dropna=False, observed=True)
                .apply(
                    lambda x: x.nlargest(2, columns="v3"),
                    meta={"id6": "Int64", "v3": "float64"},
                )[["v3"]]
                .compute()
            )
    except Exception as e:
        print(f"Failed q8 with {e}")


    try:
        client.restart()
        print("Running query 9")
        with ms.sample("query_9"):
            ddf_q9 = ddf[["id2", "id4", "v1", "v2"]]
            (
                ddf_q9[["id2", "id4", "v1", "v2"]]
                .groupby(["id2", "id4"], dropna=False, observed=True)
                .apply(
                    lambda x: pd.Series({"r2": x.corr()["v1"]["v2"] ** 2}),
                    meta={"r2": "float64"},
                )
                .compute()
            )
    except Exception as e:
        print(f"Failed q9 with {e}")

        print(f"Output data will be written to {outfname}")
        df = ms.to_pandas(align=True)
        df.to_csv(outfname)

    finally:
        print("Shutting down the cluster.")
        client.shutdown()
