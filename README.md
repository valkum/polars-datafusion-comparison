# A repro case for a perf issue with data fusion

polars seems to be more than 10 times faster than datafusion.

To repro:

Generate a sample file using `uv run generate_sample.py 1m` then call `./test.sh sample-1m.parquet` to use hyperfine to test a polars and datafusion transform step.

```
    ❯ ./test.sh sample-1m.parquet
Benchmark 1: uv run test_polars.py sample-1m.parquet
  Time (mean ± σ):     300.2 ms ± 104.0 ms    [User: 538.6 ms, System: 140.1 ms]
  Range (min … max):   249.2 ms … 585.3 ms    10 runs
 
  Warning: The first benchmarking run for this command was significantly slower than the rest (585.3 ms). This could be caused by (filesystem) caches that were not filled until after the first run. You should consider using the '--warmup' option to fill those caches before the actual benchmark. Alternatively, use the '--prepare' option to clear the caches before each timing run.
 
Benchmark 2: uv run test_datafusion.py sample-1m.parquet
  Time (mean ± σ):      1.320 s ±  0.062 s    [User: 4.919 s, System: 0.258 s]
  Range (min … max):    1.264 s …  1.444 s    10 runs
 
Summary
  uv run test_polars.py sample-1m.parquet ran
    4.40 ± 1.54 times faster than uv run test_datafusion.py sample-1m.parquet
```
