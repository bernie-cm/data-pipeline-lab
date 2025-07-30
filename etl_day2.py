# This script will execute the transform and load parts of the pipeline
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load saved data from etl.py
logger.info("Loading extracted data")
df = pd.read_csv("extracted-data.csv")
logger.info(f"Loaded {len(df)} records")

# Data quality checks and transformations
def transform_data(df):
    logger.info("Starting transformations...")

    # Check what columns we have
    logger.info(f"Available columns: {df.columns.tolist()}")

    # First remove rows with null coordinates or zero passengers
    if "PULocationID" in df.columns and "DOLocationID" in df.columns:
        df = df.dropna(subset=["PULocationID", "DOLocationID"])
        df = df[(df["PULocationID"] > 0) & (df["DOLocationID"] > 0)]
    # Passengers more than 0    
    df = df[df["passenger_count"] > 0]

    # Second calculate trip duration in minutes
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df["trip_duration_minutes"] = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60

    # Third remove outliers where trips < 1 min or > 3 hours
    df = df[(df["trip_duration_minutes"] >= 1) & (df["trip_duration_minutes"] <= 180)]

    # Fourth add load timestamp
    df["load_timestamp"] = datetime.now()

    # Fifth select and rename columns for warehouse
    warehouse_columns = {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "fare_amount": "fare_amount",
        "tip_amount": "tip_amount",
        "total_amount": "total_amount",
        "trip_duration_minutes": "trip_duration_minutes",
        "load_timestamp": "load_timestamp"
    }

    # Set up a transformed dataframe
    df_transformed = df[list(warehouse_columns.keys())].rename(columns=warehouse_columns)
    logger.info(f"Transformed data: {len(df_transformed)} records remain")

    return df_transformed

df_transformed = transform_data(df)

# Database connection and load
def create_table_and_load(df, conn_params):
    conn = None
    try:
        # Convert numpy datetime64 to Python datetime objects
        datetime_columns = ["pickup_datetime", "dropoff_datetime", "load_timestamp"]
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.to_pydatetime()

        # Connect to warehouse DB running on port 5433
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        # Create table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS taxi_trips (
            id SERIAL PRIMARY KEY,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance DECIMAL(10,2),
            fare_amount DECIMAL(10,2),
            tip_amount DECIMAL(10,2),
            total_amount DECIMAL(10,2),
            trip_duration_minutes DECIMAL(10,2),
            load_timestamp TIMESTAMP
        );
        """
        cur.execute(create_table_sql)
        logger.info("Table created/verified")

        # Prepare data for bulk insert
        # PostgreSQL expects data in tupe format for execute_values
        records = [tuple(x) for x in df.to_numpy()]
        columns = df.columns.tolist()

        # Bulk insert
        insert_query = f"""
        INSERT INTO taxi_trips ({','.join(columns)})
        VALUES %s
        """

        execute_values(cur, insert_query, records)
        conn.commit()

        # Verify load operation
        cur.execute("SELECT COUNT(*) FROM taxi_trips")
        count = cur.fetchone()[0]
        logger.info(f"Successfully loaded data. Table now has {count} records")
    
    except Exception as e:
        logger.error(f"Error during load: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

# Load to warehouse
warehouse_params = {
    "host": "localhost",
    "port": 5433,
    "database": "warehouse",
    "user": "dataeng",
    "password": "dataeng123"
}
create_table_and_load(df_transformed, warehouse_params)


# Basic pipeline stats
def generate_pipeline_stats(df_original, df_transformed):
    stats = {
        "original_records": len(df_original),
        "transformed_records": len(df_transformed),
        "records_dropped": len(df_original) - len(df_transformed),
        "drop_percentage": ((len(df_original)) - len(df_transformed)) / len(df_original) * 100,
        "avg_trip_duration": df_transformed["trip_duration_minutes"].mean(),
        "total_revenue": df_transformed["total_amount"].sum()
    }

    logger.info("Pipeline statistics:")
    for key, value in stats.items():
        logger.info(f"  {key}: {value:.2f}")

    return stats

stats = generate_pipeline_stats(df, df_transformed)

# Save pipeline run metadata
run_metadata = {
    "run_date": datetime.now().isoformat(),
    "status": "success",
    "records_processed": stats["transformed_records"]
}

logger.info("Pipeline completed successfully.")