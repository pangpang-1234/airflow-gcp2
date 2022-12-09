from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io
import os
from io import BytesIO

# Get key object from airflow variables
PROJECT_ID = os.environ.get("PROJECT_ID")
JSON_KEY = os.environ.get("KEY")

# Create connection
STORAGE_CLIENT = storage.Client(JSON_KEY)
BIGQUERY_CLIENT = bigquery.Client(JSON_KEY)

# Define bucket and dataset name
BUCKET_NAME = 'first_dataset_btctousd'
DATASET_NAME = 'dataset'
PARTITION_DATASET = 'partitioned_tables'

# Get data from GCS > transform > load to GBQ
def transform(BUCKET_NAME,STORAGE_CLIENT,BIGQUERY_CLIENT, DATASET_NAME): # 
    bucket = STORAGE_CLIENT.get_bucket(BUCKET_NAME) # Create connection to GCS
    filenames = list(bucket.list_blobs(prefix='')) # List all objects in blob
    filename  = filenames[0].name # Select first filename
    blop = bucket.blob(filename)
    data = blop.download_as_string() # Download data from blob
    df = pd.read_csv(io.BytesIO(data), encoding='utf-8',sep=',') # Convert to dataframe 
    df['Average'] = (df['Open'] + df['High'] + df['Low'])/3 # Add column Average that calculate average from 3 Open, High, Low columns
    df['VolumeDivClose'] = df['Volume']/df['Close'] # Volume divide by Close
    df['Min'] = df.min(axis=1) # Find min value each day
    df['Max'] = df.max(axis=1) # Find max value each day
    table = f'{DATASET_NAME}.BTCUSD' # Define table name
    df = df.rename(columns={'Adj Close':'AdjClose'}) # Rename column Adj Close to AdjClose
    df['Date'] = df['Date'].astype('datetime64[ns]') # Cast column Date to date type
    job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField('Date', 'DATE')]) # Create schema
    job = BIGQUERY_CLIENT.load_table_from_dataframe(df, table, job_config=job_config) # Load dataframe to GBQ
    
# Create partition tables from GBQ data
def create_partition_table(BIGQUERY_CLIENT, PARTITION_DATASET, DATASET_NAME):
    query_month = f"""SELECT DISTINCT EXTRACT(MONTH FROM Date) as M from {DATASET_NAME}.BTCUSD""" # SQL statement that query unique month
    months = BIGQUERY_CLIENT.query(query_month).to_dataframe()['M'].tolist() # Create unique month list
    for month in months: # loop in month list
        query_df = f"""SELECT * from {DATASET_NAME}.BTCUSD 
                        WHERE EXTRACT(MONTH FROM Date) = {month}""" # SQL statement taht query data by month
        partitioned_df = BIGQUERY_CLIENT.query(query_df).to_dataframe() # Query data using above sql statement and cast to dataframe
        table = f'{PARTITION_DATASET}.partitioned_month_{month}' # Define table name
        job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField('Date', 'DATE')]) # Create schema
        job = BIGQUERY_CLIENT.load_table_from_dataframe(partitioned_df, table, job_config=job_config) # Upload dataframe to GBQ