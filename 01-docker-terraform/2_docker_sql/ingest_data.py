#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
import gzip
import shutil

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    # Define the file name based on URL extension
    if url.endswith('.csv.gz'):
        csv_name = 'green_tripdata_2019-10.csv.gz'  # Adjust this based on your file
    else:
        csv_name = 'taxi_zone_lookup.csv'  # Adjust for other cases
    
    # Download file
    print(f"Downloading file from {url}...")
    os.system(f"wget {url} -O {csv_name}")
    
    # If the file is gzipped, we need to unzip it
    if csv_name.endswith('.gz'):
        print(f"Unzipping {csv_name}...")
        with gzip.open(csv_name, 'rb') as f_in:
            with open(csv_name[:-3], 'wb') as f_out:  # Remove .gz extension for output
                shutil.copyfileobj(f_in, f_out)
        os.remove(csv_name)  # Remove the gzipped file after extraction
        csv_name = csv_name[:-3]  # Update file name to the unzipped version
    
    # Create a connection to PostgreSQL
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    # Read the CSV file in chunks
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    
    # Insert the first chunk
    df = next(df_iter)
    # Handle datetime columns if they exist
    if 'tpep_pickup_datetime' in df.columns:
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    if 'tpep_dropoff_datetime' in df.columns:
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # Create table in PostgreSQL if not exists, then insert the first chunk
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

    # Ingest remaining chunks
    while True:
        try:
            t_start = time()
            df = next(df_iter)
            if 'tpep_pickup_datetime' in df.columns:
                df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            if 'tpep_dropoff_datetime' in df.columns:
                df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            t_end = time()
            print(f'Inserted another chunk, took {t_end - t_start:.3f} seconds')

        except StopIteration:
            print("Finished ingesting data into PostgreSQL")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # Parameters for PostgreSQL connection
    parser.add_argument('--user', required=True, help='Username for PostgreSQL')
    parser.add_argument('--password', required=True, help='Password for PostgreSQL')
    parser.add_argument('--host', required=True, help='Host for PostgreSQL')
    parser.add_argument('--port', required=True, help='Port for PostgreSQL')
    parser.add_argument('--db', required=True, help='Database name for PostgreSQL')
    parser.add_argument('--table_name', required=True, help='Name of the table where we will write the results')
    parser.add_argument('--url', required=True, help='URL of the CSV file')

    args = parser.parse_args()
    main(args)
