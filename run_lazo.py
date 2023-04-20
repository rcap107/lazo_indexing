#%%
import lazo_index_service
import polars as pl
import pandas as pd
from pathlib import Path

SERVER_HOST="localhost"
SERVER_PORT=15449

lazo_client = lazo_index_service.LazoIndexClient(host=SERVER_HOST, port=SERVER_PORT)
# %%
pth = Path("data/yago3-dl/seltab/csv")
dsname = "yago_seltab_wordnet_organization"
tgt_dataset = Path(pth, f"{dsname}.csv")
# %%
df = pl.read_csv(tgt_dataset)
columns = df.columns
#%%
df = df.select(
    [
        pl.all().str.lstrip("<").str.rstrip(">"),
        
    ]
)

# %%
df_pd = df.to_pandas()
# %%
sketches = lazo_client.get_lazo_sketch_from_data(list("abcd"), dsname, "type")
# %%
for col in df.columns:
    (n_permutations, hash_values, cardinality) = lazo_client.index_data(
        df[col].to_list(), dsname, col
    )
# %%
for col in df.columns:
    (n_permutations, hash_values, cardinality) = lazo_client.get_lazo_sketch_from_data(
        df[col].to_list(), dsname, col
    )

# %%
lazo_sketches = lazo_client.index_data_path(
    str(tgt_dataset), dsname, columns
)

# %%
tgt_cols = ['type',
 'subject',
 'wasCreatedOnDate',
 'created',
 'hasExpenses',
 'hasRevenue',
 'hasLongitude',
 'hasMotto',
 'participatedIn',
 'hasBudget',
 'wasDestroyedOnDate',
 'hasNumberOfPeople',
 'hasWonPrize',
 'isLocatedIn']