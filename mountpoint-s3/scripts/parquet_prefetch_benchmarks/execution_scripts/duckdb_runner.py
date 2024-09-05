# One iteration of the duckdb benchmarks for the AQB Queries from TPC-DS, for the parquet prefetcher

import duckdb
import os
import time
from tabulate import tabulate
import glob

location_in_bucket = os.environ.get('LOCATION_IN_BUCKET')

# Create a DuckDB connection
conn = duckdb.connect(config={'threads': 32}) # Set the number of threads to use

# Define the table names
table_names = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer_address", "customer_demographics", "customer", "date_dim",
    "household_demographics", "income_band", "inventory", "item",
    "promotion", "reason", "ship_mode", "store_returns", "store_sales",
    "store", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site"
]

# Define the base input URI
dirname = os.path.dirname(__file__)
base_input_uri = os.path.join(dirname, location_in_bucket)
sql_folder_path = os.path.join(dirname, 'DuckDB_Queries_AQB')

def load_parquet_tables():
    start_time = time.time()
    for table in table_names:
        table_dir = os.path.join(base_input_uri, table)
        parquet_files = glob.glob(os.path.join(table_dir, "*.parquet"))
        if parquet_files:
            parquet_queries = [f"SELECT * FROM parquet_scan('{file_path}')" for file_path in parquet_files]
            parquet_query = " UNION ALL ".join(parquet_queries)
            conn.execute(f"CREATE TEMPORARY VIEW {table} AS ({parquet_query})")
        else:
            print(f"No Parquet files found for table '{table}'")
    end_time = time.time()

    print("Load Timing: ", end_time - start_time)

def execute_sql_file(file_path):
    with open(file_path, 'r') as file:
        query = file.read()

    start_time = time.time()
    print(f"Executing: {file_path}")

    try:
        result = conn.execute(query)
        result_df = result.fetch_df()
        end_time = time.time()
        execution_time = end_time - start_time
        num_rows = result_df.shape[0]

        print(f"Query Execution time: {execution_time} seconds")
        print(f"Number of rows in result: {num_rows}")
    except Exception as e:
        print(f"An error occurred: {e}")
        return 0, 0

    print(f"Completed Query: {file_path}\n\n")
    return execution_time, num_rows

def main():
    sql_file_path = os.environ.get('SQL_FILE_PATH')
    print(f"Starting Query: {sql_file_path}")
    print("Loading tables for query")

    # Load the Parquet tables
    load_parquet_tables()
    sql_file_path = os.environ.get('SQL_FILE_PATH')

    if sql_file_path:
        file_name = os.path.basename(sql_file_path)
        file_path = os.path.join(sql_folder_path, file_name)

        if os.path.isfile(file_path):
            execution_time, num_rows_result = execute_sql_file(file_path)
        else:
            print(f"File '{file_name}' not found in the SQL folder.")
    else:
        print("SQL file path not provided.")
        
if __name__ == "__main__":
    main()
