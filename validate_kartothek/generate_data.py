import numpy as np
import pandas as pd
from functools import partial

from storefact import get_store_from_url
from kartothek.io.eager import store_dataframes_as_dataset

d_column = [(3, 12), (25, 42), (7, 8)]
e_column = [
    (pd.Categorical(["train"]), pd.Categorical(["train"])),
    (pd.Categorical(["test", "train", "test"]), pd.Categorical(["train", "test", "train"])),
    (pd.Categorical(["test", "train", "test", "prod"]), pd.Categorical(["prod", "train", "test", "train"])),
]

for index, values in enumerate(d_column):
    length = len(e_column[index][0])
    df = pd.DataFrame(
        {
            "A": 1.0,
            "B": pd.Timestamp("20130102"),
            "C": np.array([values[0]] * length, dtype="int32"),
            "D": e_column[index][0],
            "E": "foo",
        }
    )

    another_df = pd.DataFrame(
        {
            "A": 5.0,
            "B": pd.Timestamp("20110102"),
            "C": np.array([values[1]] * length, dtype="int32"),
            "D": e_column[index][1],
            "E": "bar",
        }
    )

    dataset_uuid = "dataset_ktk_" + str(index)

    store_factory = partial(get_store_from_url, "hfs://data")

    dm = store_dataframes_as_dataset(store_factory, dataset_uuid, [df, another_df], partition_on=["D", "E"])


dummy_df = pd.DataFrame(
    {
        "A": 3.0,
        "B": pd.Timestamp("20120102"),
        "C": np.array([0], dtype="int32"),
        "D": pd.Categorical(["bad"]),
        "E": "foobar",
    }
)
dataset_uuid = "dummy_dataset"
store_factory = partial(get_store_from_url, "hfs://data")
dm = store_dataframes_as_dataset(store_factory, dataset_uuid, dummy_df, partition_on=["D", "E"])
