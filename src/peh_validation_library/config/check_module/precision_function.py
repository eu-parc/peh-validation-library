from collections.abc import Sequence

import pandera.polars as pa
import polars as pl


def check_fn(data: pa.PolarsData, args_: Sequence[int]) -> pl.LazyFrame:
    return data.lazyframe.select(
        pl.col(data.key)
        .cast(pl.String)
        .str.split('.')
        .list.get(1)
        .str.len_chars()
        .le(args_[0])
    )
