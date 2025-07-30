import pandas as pd
import psycopg2
from datetime import datetime

# Download sample data from NYC Taxi January 2023
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
df = pd.read_parquet(url)
print(f"Loaded {len(df)} records")