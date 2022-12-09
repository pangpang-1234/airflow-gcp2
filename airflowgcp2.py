import sys
sys.path.insert(1, '//gcs//dags')
from function import *
from typing import Optional, Sequence, Union
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.decorators import apply_defaults
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import ( BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator )    



# Begin DAG
default_args={
    'owner':'Pang',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

with DAG(
    dag_id = 'first_dag',
    default_args=default_args,
    description = 'GCP',
    start_date=days_ago(1), # Daily run
    schedule_interval='@once'
    ) as dag:
        # Startup task
        start = DummyOperator(
            task_id='start',
            dag=dag
        )
        # Create bucket
        create_bucket = GCSCreateBucketOperator(
            task_id='create_bucket',
            bucket_name = BUCKET_NAME,
            storage_class = 'REGIONAL', # for data availability
            location='asia-southeast1'
        )
        
        # Create dataset
        create_dataset = BigQueryCreateEmptyDatasetOperator(task_id='create_dataset', dataset_id=DATASET_NAME)
        
        # Get data from GCS > transform > load to GBQ
        transform_file = PythonOperator(
            task_id='transform_data',
            python_callable=transform,
            op_kwargs={'BUCKET_NAME':BUCKET_NAME, 'STORAGE_CLIENT':STORAGE_CLIENT, 'BIGQUERY_CLIENT':BIGQUERY_CLIENT,'DATASET_NAME':DATASET_NAME}
        )
        # Create dataset for partitioned tables
        partition_dataset = BigQueryCreateEmptyDatasetOperator(task_id='create_partition_dataset', dataset_id=PARTITION_DATASET)
        
        # Create partition tables
        partition_table = PythonOperator(
            task_id='create_partition_tables',
            python_callable=create_partition_table,
            op_kwargs={'BIGQUERY_CLIENT':BIGQUERY_CLIENT, 'PARTITION_DATASET':PARTITION_DATASET, 'DATASET_NAME':DATASET_NAME, 'PROJECT_ID':PROJECT_ID}
        ) 
        
        start >> create_bucket >> create_dataset >> transform_file >> partition_dataset >> partition_table
        