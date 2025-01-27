#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    # Mengunduh file CSV
    if url.endswith('.csv.gz'):
        csv_name = 'green_tripdata_2019-10.csv.gz'  # Ganti dengan nama file sesuai yang diunduh
    else:
        csv_name = 'taxi_zone_lookup.csv'  # Ganti dengan nama file sesuai yang diunduh

    # Mengunduh file CSV
    os.system(f"wget {url} -O {csv_name}")

    # Membuat koneksi ke PostgreSQL
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Membaca file CSV dalam chunk
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    # Memuat chunk pertama
    df = next(df_iter)

    # Konversi tanggal jika ada kolom tanggal
    if 'tpep_pickup_datetime' in df.columns:
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    if 'tpep_dropoff_datetime' in df.columns:
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Memuat data ke tabel PostgreSQL
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')  # Menyiapkan tabel jika belum ada
    df.to_sql(name=table_name, con=engine, if_exists='append')  # Menambah data ke tabel

    # Menyelesaikan proses memuat data jika ada data lebih banyak
    while True:
        try:
            t_start = time()

            df = next(df_iter)
            if 'tpep_pickup_datetime' in df.columns:
                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            if 'tpep_dropoff_datetime' in df.columns:
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('Inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into PostgreSQL")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # Parameter untuk koneksi PostgreSQL
    parser.add_argument('--user', required=True, help='user name for PostgreSQL')
    parser.add_argument('--password', required=True, help='password for PostgreSQL')
    parser.add_argument('--host', required=True, help='host for PostgreSQL')
    parser.add_argument('--port', required=True, help='port for PostgreSQL')
    parser.add_argument('--db', required=True, help='database name for PostgreSQL')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='URL of the CSV file')

    args = parser.parse_args()

    main(args)
