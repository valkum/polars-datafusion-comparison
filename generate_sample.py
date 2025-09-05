"""
A sample file contains TARGET_ROWS.
10% of there rows have multiple markets. In this case, the row will be duplicated with multiple markets.
"""

import sys
from math import nan
import polars as pl
import numpy as np
import string


NUM_GROUS = 1000
ALPHABET = np.array(list(string.ascii_lowercase))
GROUPS = np.array([f"group_{i}" for i in range(NUM_GROUS)])
MARKETS = np.array([f"market_{i}" for i in range(10)])


def parse_size(size_str):
    if size_str.endswith("m"):
        return int(size_str[:-1]) * 1_000_000
    elif size_str.endswith("k"):
        return int(size_str[:-1]) * 1_000
    else:
        return int(size_str)


def generate_random_col1(target_rows, is_market, numbers_of_markets):
    """
    Col 1 is a series of TARGET_ROWS length, each element is a random string of different lengths
    """
    word_lengths = np.clip(
        np.floor(np.random.standard_normal(target_rows) + 20).astype(int), 3, 200
    )

    words = []
    for length in word_lengths:
        words.append("".join(np.random.choice(ALPHABET, size=length)))

    market_indices = np.where(is_market)[0]
    if len(market_indices) > 0:
        additional_words = []
        for idx in market_indices:
            additional_words.extend([words[idx]] * numbers_of_markets[idx])
        words.extend(additional_words)

    return pl.Series(words)


def generate_random_col2(target_rows, is_market, numbers_of_markets):
    """
    Col 2 is a random group, selected randomly from GROUPS
    """
    rows = np.random.choice(GROUPS, size=target_rows)

    market_indices = np.where(is_market)[0]
    if len(market_indices) > 0:
        additional_rows = np.repeat(
            rows[market_indices], numbers_of_markets[market_indices]
        )
        rows = np.concatenate((rows, additional_rows))

    return pl.Series(rows)


def generate_random_col3_and_4(target_rows, is_market, numbers_of_markets):
    """
    If is_market is True at n, Col 3 is a random market and Col 4 is a random price
    """
    # calculate additional rows after target_rows
    additional_rows = np.sum(numbers_of_markets[is_market])

    # grow is_market to target_rows + additional_rows filling each additional row with True
    is_market = np.concatenate((is_market, np.full(additional_rows, True)))

    markets = np.full(target_rows + additional_rows, None, dtype=object)
    markets[is_market] = np.random.choice(MARKETS, size=np.sum(is_market))
    prices = np.full(target_rows + additional_rows, nan)
    prices[is_market] = np.random.rand(np.sum(is_market))

    markets = pl.Series("markets", markets.tolist()).cast(pl.Enum(MARKETS.tolist()))

    return (
        markets,
        pl.Series("price", prices, dtype=pl.Float64, nan_to_null=True),
    )


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) > 0:
        file_name = args[0]
        target_rows = parse_size(args[0])
    else:
        target_rows = 10_000_000
    output_file = f"sample-{file_name}.parquet"

    print(f"Generating sample {output_file} file with {target_rows} rows")

    # Whether a name,group should have multiple markets
    is_market = np.random.rand(target_rows) < 0.01
    # for rows in is_market, assign a random number of markets between 1 and 3
    numbers_of_markets = np.random.randint(1, 3, size=target_rows)

    col1 = generate_random_col1(target_rows, is_market, numbers_of_markets)
    col2 = generate_random_col2(target_rows, is_market, numbers_of_markets)
    (col3, col4) = generate_random_col3_and_4(
        target_rows, is_market, numbers_of_markets
    )

    df = pl.DataFrame(
        {
            "name": col1,
            "group": col2,
            "market": col3,
            "price": col4,
        },
        schema={
            "name": pl.Utf8,
            "group": pl.Utf8,
            "market": pl.Enum(MARKETS.tolist()),
            "price": pl.Float64,
        },
    )
    df = df.sort("name", "group")
    df.write_parquet(output_file)
