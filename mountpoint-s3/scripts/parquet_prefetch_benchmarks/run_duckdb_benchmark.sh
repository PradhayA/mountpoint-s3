#!/bin/bash

# Script to run AQB queries on DuckDB using the parquet prefetcher mode (takes a little while but outputs after every query execution to the txt file in results)


# PLEASE FILL THE BUCKET NAME e.g. onumshin-tpcds-dataset
bucket_name='YOUR_BUCKET_NAME'

# PLEASE FILL THE DIRECTORY OF THE DATASET WITHIN THE BUCKET e.g. 'dataset' where this is the name of the 'directory' within that bucket
dataset_directory_name='YOUR_DATASET_DIRECTORY_NAME'

location_in_bucket="dataset/$dataset_directory_name"
export LOCATION_IN_BUCKET="$location_in_bucket"

# List of SQL query file names
sql_files=("25.sql" "31.sql" "49.sql" "76.sql" "77.sql" "80.sql" "88.sql" "96.sql")


sql_dir="$PWD/execution_scripts/DuckDB_Queries_AQB"
mount_dir="$PWD/execution_scripts/dataset"
python_dir="$PWD/execution_scripts"
output_dir="$PWD/results"
output_file="$output_dir/results_duckdb.txt"

# Create the results directory if it doesn't exist
mkdir -p "$output_dir"

# Create the mount directory if it doesn't exist
mkdir -p "$mount_dir"

# Create the output file if it doesn't exist
if [ ! -f "$output_file" ]; then
    touch "$output_file"
fi

# Virtual environment activation command
source "$python_dir/venv/bin/activate"

> "$output_file"

for sql_file in "${sql_files[@]}"; do
    cargo run "$bucket_name" "$mount_dir" --metadata-ttl indefinite --parquet-prefetch >> /dev/null 2>&1

    # Set the SQL file path as an environment variable
    export SQL_FILE_PATH="$sql_dir/$sql_file"

    # Run the Python script and redirect output to the file
    python "$python_dir/duckdb_runner.py" >> "$output_file" 2>&1

    sudo umount -l "$mount_dir" >> /dev/null 2>&1
    unset SQL_FILE_PATH
done

unset LOCATION_IN_BUCKET

deactivate