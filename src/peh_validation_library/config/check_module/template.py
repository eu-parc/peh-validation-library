from collections.abc import Sequence

import pandera.polars as pa
import polars as pl


def check_fn(data: pa.PolarsData, args_: Sequence[int]) -> pl.LazyFrame:
    return data.lazyframe.select(...)
