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