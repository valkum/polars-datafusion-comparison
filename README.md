# A repro case for DictionaryKeyOverflowError on DataFrame.write_parquet

Found this while looking into a perf issue.
First create a sample file with `uv run generate_sample 10m`
Running test with `uv run test_datafusion.py sample-10m.parquet`.
This only occurs with a 10m sample set not with 1m.

You will get a:

```
    ❯ uv run test_datafusion.py

thread 'tokio-runtime-worker' panicked at /Users/runner/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/arrow-data-55.2.0/src/transform/mod.rs:670:31:
MutableArrayData::new is infallible: DictionaryKeyOverflowError
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
Traceback (most recent call last):
  File "/Users/valkum/git/polars-datafusion-comparison/test_datafusion.py", line 64, in <module>
    main(file_path)
    ~~~~^^^^^^^^^^^
  File "/Users/valkum/git/polars-datafusion-comparison/test_datafusion.py", line 55, in main
    df.write_parquet("output_datafusion.parquet")
    ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/valkum/git/polars-datafusion-comparison/.venv/lib/python3.13/site-packages/datafusion/dataframe.py", line 951, in write_parquet
    self.df.write_parquet(str(path), compression.value, compression_level)
    ~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pyo3_runtime.PanicException: MutableArrayData::new is infallible: DictionaryKeyOverflowError
```
