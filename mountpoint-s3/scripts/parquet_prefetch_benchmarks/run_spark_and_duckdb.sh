#!/bin/bash

# Script to run AQB queries on both Spark and DuckDB using the parquet prefetcher mode (takes a little while but outputs after every query execution to the txt files in results)

./run_spark_benchmark.sh
./run_duckdb_benchmark.sh
