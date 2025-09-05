import polars as pl
import sys


def main(file_path: str):
    df = pl.scan_parquet(
        file_path,
        cache=True,
    )

    df = df.select(
        pl.col("name"),
        pl.col("group"),
        pl.when(
            pl.col("market").is_not_null() & pl.col("price").is_not_null(),
        )
        .then(
            pl.struct(
                market=pl.col("market"),
                price=pl.col("price"),
            ),
        )
        .otherwise(None)
        .alias("market"),
    )
    df = (
        df.group_by("name", "group", maintain_order=True)
        .agg(
            pl.col("market").alias("markets"),
        )
        .select(
            pl.col("name"),
            pl.col("group"),
            pl.when(pl.col("markets").list.first().is_not_null())
            .then(pl.col("markets"))
            .otherwise(None),
        )
    )

    df.sink_parquet("output_polars.parquet", compression="zstd", compression_level=3)


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) > 0:
        file_path = args[0]
    else:
        file_path = "sample-10m.parquet"
    main(file_path)
