#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def run(
    pg_user: str,
    pg_pass: str,
    pg_host: str,
    pg_port: int,
    pg_db: str,
    year: int,
    month: int,
    chunk_size: int,
    target_table: str,
):
    # values come from CLI options

    prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    url = f'{prefix}/green_tripdata_{year}-{month:02d}.parquet'
    engine = create_engine(f'postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')



    # pandas "read_parquet" does not support iterator/chunksize or dtype mapping the
    # same way read_csv does. We read the whole parquet file and then cast types.
    df = pd.read_parquet(url)

    # Try to enforce expected dtypes where possible
    try:
        df = df.astype(dtype)
    except Exception:
        # Some columns may not cast cleanly; ignore failures.
        pass

    # Ensure datetime columns are parsed
    for col in parse_dates:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df.to_sql(
        name=target_table,
        con=engine,
        if_exists='replace',
        index=False,
    )

@click.command()
@click.option('--pg_user', default='postgres', help='Postgres user')
@click.option('--pg_pass', default='postgres', help='Postgres password')
@click.option('--pg_host', default='localhost', help='Postgres host')
@click.option('--pg_port', default=5433, type=int, help='Postgres port')
@click.option('--pg_db', default='ny_taxi', help='Postgres database name')
@click.option('--year', default=2025, type=int, help='Year for taxi data')
@click.option('--month', default=11, type=int, help='Month for taxi data')
@click.option('--chunk_size', 'chunk_size', default=100000, type=int, help='CSV chunk size')
@click.option('--target_table', default='green_taxi_data', help='Destination table name')

def main(
    pg_user,
    pg_pass,
    pg_host,
    pg_port,
    pg_db,
    year,
    month,
    chunk_size,
    target_table,
):
    run(
        pg_user,
        pg_pass,
        pg_host,
        pg_port,
        pg_db,
        year,
        month,
        chunk_size,
        target_table,
    )


if __name__ == '__main__':
    main()