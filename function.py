from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io
import os
from io import BytesIO

# Get key object from airflow variables
# PROJECT_ID = os.environ.get("PROJECT_ID")
JSON_KEY = os.environ.get("KEY")
PROJECT_ID = "ageless-math-370705"

# Create connection
STORAGE_CLIENT = storage.Client(JSON_KEY)
BIGQUERY_CLIENT = bigquery.Client(JSON_KEY)
# STORAGE_CLIENT = '1'
# BIGQUERY_CLIENT = '2'

# Define bucket and dataset name
BUCKET_NAME = 'first_dataset_btctousd'
DATASET_NAME = 'dataset'
PARTITION_DATASET = 'partitioned_tables'

# Get data from GCS > transform > load to GBQ
def transform(BUCKET_NAME,STORAGE_CLIENT,BIGQUERY_CLIENT, DATASET_NAME): 
    # Create connection to GCS
    bucket = STORAGE_CLIENT.get_bucket(BUCKET_NAME)
    
    # List all objects in blob 
    filenames = list(bucket.list_blobs(prefix='')) 
    
    # Select first filename
    filename  = filenames[0].name 
    blop = bucket.blob(filename)
    
    # Download data from blob
    data = blop.download_as_string() 
    
    # Convert to dataframe
    df = pd.read_csv(io.BytesIO(data), encoding='utf-8',sep=',')
    
    # Add column Average that calculate average from 3 Open, High, Low columns  
    df['Average'] = (df['Open'] + df['High'] + df['Low'])/3 
    
    # Volume divide by Close
    df['VolumeDivClose'] = df['Volume']/df['Close'] 
    
    # Find min value each day
    df['Min'] = df.min(axis=1)
    
     # Find max value each day 
    df['Max'] = df.max(axis=1)
    
    # Define table name
    table = f'{DATASET_NAME}.BTCUSD' 
    
    # Rename column Adj Close to AdjClose
    df = df.rename(columns={'Adj Close':'AdjClose'}) 
    
    # Cast column Date to date type
    df['Date'] = df['Date'].astype('datetime64[ns]') 
    
    # Create schema
    job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField('Date', 'DATE')])
    
    # Load dataframe to GBQ 
    job = BIGQUERY_CLIENT.load_table_from_dataframe(df, table, job_config=job_config) 
    
# Create partition tables from GBQ data
def create_partition_table(BIGQUERY_CLIENT, PARTITION_DATASET, DATASET_NAME, PROJECT_ID):
    PROJECT_ID = str(PROJECT_ID)
    # SQL statement that query data by month
    query_month = f"""SELECT DISTINCT EXTRACT(MONTH FROM Date) as M from {DATASET_NAME}.BTCUSD""" 
    
    # Define source_id
    source_id = f"{PROJECT_ID}.{DATASET_NAME}.BTCUSD"
    
    # # Create unique month list
    months = BIGQUERY_CLIENT.query(query_month).to_dataframe()['M'].tolist() 
    
    # loop in month list
    for month in months:
        view_id = f"{PROJECT_ID}.{PARTITION_DATASET}.partitioned_month_{month}"
        view = bigquery.Table(view_id)
        # Query partition view
        view.view_query = f"SELECT * FROM `{source_id}` WHERE EXTRACT(MONTH FROM Date) = {month}"
        # Create view
        view = BIGQUERY_CLIENT.create_table(view)
    
# def create_partition_table(BIGQUERY_CLIENT, PARTITION_DATASET, DATASET_NAME):
#     # SQL statement that query unique month
#     query_month = f"""SELECT DISTINCT EXTRACT(MONTH FROM Date) as M from {DATASET_NAME}.BTCUSD""" 
    
#     # Create unique month list
#     months = BIGQUERY_CLIENT.query(query_month).to_dataframe()['M'].tolist() 
    
#     # loop in month list
#     for month in months: 
#         query_df = f"""SELECT * from {DATASET_NAME}.BTCUSD 
#                         WHERE EXTRACT(MONTH FROM Date) = {month}""" 
                        
#         # Query data using above sql statement and cast to dataframe
#         partitioned_df = BIGQUERY_CLIENT.query(query_df).to_dataframe() 
        
#         # Define table name
#         table = f'{PARTITION_DATASET}.partitioned_month_{month}' 
        
#         # Create schema
#         job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField('Date', 'DATE')]) 
        
#         # Upload dataframe to GBQ
#         job = BIGQUERY_CLIENT.load_table_from_dataframe(partitioned_df, table, job_config=job_config) 



# create_partition_table(BIGQUERY_CLIENT, PARTITION_DATASET, DATASET_NAME, PROJECT_ID)