import psycopg2
import pandas as pd

# Connect to the PostgreSQL database
conn = psycopg2.connect("dbname=ny_taxi user=postgres password=postgres host=localhost")
cur = conn.cursor()

# Load green taxi data
green_data = pd.read_csv('green_tripdata_2019-10.csv.gz')
for row in green_data.itertuples():
    cur.execute("""
        INSERT INTO green_taxi (trip_id, pickup_datetime, dropoff_datetime, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude, passenger_count, trip_distance, fare_amount, total_amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (row.trip_id, row.pickup_datetime, row.dropoff_datetime, row.pickup_longitude, row.pickup_latitude, row.dropoff_longitude, row.dropoff_latitude, row.passenger_count, row.trip_distance, row.fare_amount, row.total_amount))

conn.commit()

# Load taxi zone lookup data
zone_data = pd.read_csv('taxi_zone_lookup.csv')
for row in zone_data.itertuples():
    cur.execute("""
        INSERT INTO taxi_zone_lookup (location_id, borough, zone)
        VALUES (%s, %s, %s)
    """, (row.location_id, row.borough, row.zone))

conn.commit()

cur.close()
conn.close()
