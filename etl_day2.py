# This script will execute the transform and load parts of the pipeline
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging
