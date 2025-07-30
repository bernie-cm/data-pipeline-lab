import pandas as pd
import psycopg2
from datetime import datetime

# Download sample data from NYC Taxi January 2023
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
df = pd.read_parquet(url)
print(f"Loaded {len(df)} records")

# Take a sample of 10K records for testing
df_sample = df.sample(n=10000, random_state=42)

# Do some basic EDA
print(df_sample.info())
print(df_sample.head())

# Final checkpoint and save state for further analysis"
df_sample.to_csv()
