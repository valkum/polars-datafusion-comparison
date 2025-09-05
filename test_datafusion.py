import datafusion as dfn
from datafusion import col, lit
from datafusion.expr import SortExpr
from datafusion.functions import array_agg, named_struct, when
import pyarrow as pa
import sys


def main(file_path: str):
    ctx = dfn.SessionContext(
        config=dfn.SessionConfig(
            {"datafusion.execution.parquet.compression": "zstd(3)"}
        )
    )

    ctx.register_parquet(
        "df",
        file_path,
        file_sort_order=[
            [
                SortExpr(col("name"), True, False),
                SortExpr(col("group"), True, False),
            ]
        ],
    )

    df = ctx.table("df").select(
        "name",
        "group",
        when(
            col("market").is_not_null() & col("price").is_not_null(),
            named_struct(
                [
                    (
                        "market",
                        col("market").cast(pa.dictionary(pa.uint16(), pa.utf8())),
                    ),
                    ("price", col("price")),
                ]
            ),
        )
        .otherwise(lit(None))
        .alias("market"),
    )
    df = df.aggregate(
        [col("df.name"), col("df.group")],
        array_agg(col("market")).alias("markets"),
    )
    df = df.select(
        col("df.name"),
        col("df.group"),
        when(col("markets")[0].is_not_null(), col("markets")).otherwise(lit(None)),
    )

    df.write_parquet("output_datafusion.parquet")


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) > 0:
        file_path = args[0]
    else:
        file_path = "sample-10m.parquet"
    main(file_path)
