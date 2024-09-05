# DuckDB and Spark Benchmark Parquet Prefetcher Scripts for TPC-DS AQB Queries

## Description
This repository contains a very simple set of scripts to run queries for the TPC-DS dataset on DuckDB and Apache Spark, specifically for the AQB (Analytical Query Benchmark) queries. The scripts are designed to measure the performance of these systems on a set of SQL queries using a parquet dataset.

## Prerequisites
Before running the scripts, make sure you have the following installed:
- Python 3.x
- Do any necessary authentication to access S3 bucket

The key is to run the following commands within the `execution_scripts` directory to set up the venv and install the necessary requirements:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Setup
1. Download the TPC-DS dataset in Parquet format and place it in a suitable location (S3 bucket).
2. Update the necessary configuration variables in the scripts:
   - For both scripts (`run_spark_benchmark.sh` and `run_duckdb_benchmark.sh`):
     - Fill in the bucket name: `bucket_name='YOUR_BUCKET_NAME'`
     - Fill in the bucket name: `dataset_directory_name='YOUR_DATASET_DIRECTORY_NAME'`

## Running the Benchmarks
### DuckDB
1. Navigate to the root directory of the repository.
2. Make the `run_spark_benchmark.sh` script executable: `chmod +x run_duckdb_benchmark.sh`
3. Run the Spark benchmark script: `./run_duckdb_benchmark.sh`

### Apache Spark
1. Navigate to the root directory of the repository.
2. Make the `run_spark_benchmark.sh` script executable: `chmod +x run_spark_benchmark.sh`
3. Run the Spark benchmark script: `./run_spark_benchmark.sh`

### To Run Both
1. Navigate to the root directory of the repository.
2. Make the `run_spark_and_duckdb.sh` script executable: `chmod +x run_spark_and_duckdb.sh`
3. Run the Spark benchmark script: `./run_spark_and_duckdb.sh`

These script will mount the dataset from the specified bucket, execute the SQL queries in the `Spark_Queries_AQB` or `DuckDB_Queries_AQB` directory, and write the results to a `results_spark.txt` file or `results_duckdb.txt` in the `results` directory.

**Note:** The scripts assumes that you have the necessary authentication and permissions to access the specified bucket.

## Monitoring Progress
- The script will output the progress and results to the `results_spark.txt` file in the `results` directory.
- This includes time to plan/load tables for the queries, query execution time, and number of rows returned (for spark, also the top x rows in the results)
