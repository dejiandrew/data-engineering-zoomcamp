import os
from datetime import datetime, timedelta
import requests
from google.cloud import storage, bigquery
from google.api_core.exceptions import RetryError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Constants - update these to match your Terraform settings
BUCKET = "zoomcamp-module-4-terra-bucket-022625"  # Your GCS bucket name from Terraform
PROJECT_ID = "de-zoomcamp-module-4"  # Your GCP project ID
DATASET_ID = "module4_all_trips_dataset"  # Your BigQuery dataset name from Terraform

# Taxi data types and their configurations
TAXI_CONFIGS = [
    {
        'type': 'green',
        'github_url': "https://api.github.com/repos/DataTalksClub/nyc-tlc-data/releases/tags/green",
        'gcs_prefix': 'green',
        'table_id': 'green_tripdata',
        'schema_fields': [
            {'name': 'VendorID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'lpep_pickup_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'lpep_dropoff_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'store_and_fwd_flag', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'RatecodeID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'PULocationID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'DOLocationID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'passenger_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'trip_distance', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'fare_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'extra', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'mta_tax', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'tip_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'tolls_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'ehail_fee', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'improvement_surcharge', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'total_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'payment_type', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'trip_type', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'congestion_surcharge', 'type': 'FLOAT', 'mode': 'NULLABLE'}
        ]
    },
    {
        'type': 'yellow',
        'github_url': "https://api.github.com/repos/DataTalksClub/nyc-tlc-data/releases/tags/yellow",
        'gcs_prefix': 'yellow',
        'table_id': 'yellow_tripdata',
        'schema_fields': [
            {'name': 'VendorID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'tpep_pickup_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'tpep_dropoff_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'passenger_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'trip_distance', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'RatecodeID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'store_and_fwd_flag', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'PULocationID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'DOLocationID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'payment_type', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'fare_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'extra', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'mta_tax', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'tip_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'tolls_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'improvement_surcharge', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'total_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'congestion_surcharge', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'airport_fee', 'type': 'FLOAT', 'mode': 'NULLABLE'}
        ]
    },
    {
        'type': 'fhv',
        'github_url': "https://api.github.com/repos/DataTalksClub/nyc-tlc-data/releases/tags/fhv",
        'gcs_prefix': 'fhv',
        'table_id': 'fhv_tripdata',
        'schema_fields': [
            {'name': 'dispatching_base_num', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'pickup_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'dropoff_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'PULocationID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'DOLocationID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'SR_Flag', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Affiliated_base_number', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    }
]

# Function to get the list of CSV files from GitHub release
def get_github_release_assets(github_url, taxi_type, **kwargs):
    print(f"Fetching {taxi_type} taxi data from {github_url}")
    response = requests.get(github_url)
    if response.status_code == 200:
        assets = response.json()['assets']
        csv_files = [asset['browser_download_url'] for asset in assets if asset['name'].endswith('.csv.gz')]
        print(f"Found {len(csv_files)} {taxi_type} CSV files to download")
        return csv_files
    else:
        print(f"Failed to fetch {taxi_type} release assets. Status code: {response.status_code}")
        return []

# Function to download a file from GitHub
def download_file(url):
    file_name = url.split('/')[-1]
    local_path = f"/tmp/{file_name}"
    
    print(f"Downloading {url} to {local_path}")
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded {local_path}")
        return local_path
    else:
        print(f"Failed to download {url}")
        return None

# Function to upload a file to GCS
def upload_to_gcs(local_file, gcs_prefix):
    if not local_file:
        print("No file to upload")
        return
    
    file_name = os.path.basename(local_file)
    object_name = f"{gcs_prefix}/{file_name}"
    
    print(f"Uploading {local_file} to gs://{BUCKET}/{object_name}")
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(object_name)
    
    # Set a large chunk size for large files and increase the timeout for the upload
    blob.chunk_size = 10 * 1024 * 1024  # 10 MB chunks
    try:
        blob.upload_from_filename(local_file, timeout=600)  # 10 minutes timeout
        print(f"Uploaded {local_file} to GCS: {object_name}")
        
        # Delete the local file after upload
        os.remove(local_file)
        print(f"Removed local file: {local_file}")
        return f"gs://{BUCKET}/{object_name}"
    except RetryError as e:
        print(f"RetryError encountered for {file_name}: {e}")
        return None

# Function to process files for a specific taxi type
def process_taxi_data(taxi_config, **context):
    taxi_type = taxi_config['type']
    github_url = taxi_config['github_url']
    gcs_prefix = taxi_config['gcs_prefix']
    
    print(f"Processing {taxi_type} taxi data")
    # Get list of files
    urls = get_github_release_assets(github_url, taxi_type)
    
    if not urls:
        print(f"No {taxi_type} URLs found to process")
        return

    # Process each URL
    uploaded_files = []
    for url in urls:
        try:
            print(f"Processing {taxi_type} URL: {url}")
            local_file = download_file(url)
            if local_file:
                gcs_path = upload_to_gcs(local_file, gcs_prefix)
                if gcs_path:
                    uploaded_files.append(gcs_path)
        except Exception as e:
            print(f"Error processing {taxi_type} URL {url}: {str(e)}")
    
    print(f"Successfully uploaded {len(uploaded_files)} {taxi_type} files to GCS")
    return uploaded_files

# Create the DAG
with DAG(
    'nyc_taxi_data_pipeline',
    default_args=default_args,
    description='Pipeline to download NYC Taxi data (green, yellow, FHV) from GitHub, upload to GCS, and create BigQuery external tables',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=days_ago(1),
    catchup=False,
    tags=['github', 'gcs', 'bigquery', 'taxi-data', 'nyc'],
) as dag:
    
    # Create tasks for each taxi type
    for config in TAXI_CONFIGS:
        taxi_type = config['type']
        
        # Process taxi data
        process_task = PythonOperator(
            task_id=f'process_{taxi_type}_data',
            python_callable=process_taxi_data,
            op_kwargs={'taxi_config': config},
        )
        
        # Create BigQuery external table
        create_table_task = BigQueryCreateExternalTableOperator(
            task_id=f'create_{taxi_type}_table',
            destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{config['table_id']}",
            bucket=BUCKET,
            source_objects=[f"{config['gcs_prefix']}/*.csv.gz"],
            schema_fields=config['schema_fields'],
            skip_leading_rows=1,
            source_format='CSV',
            compression='GZIP',
            field_delimiter=',',
        )
        
        # Set task dependencies
        process_task >> create_table_task



# This code doesn't use the correct schema for yellow.
# Use the below query in BigQuery to automatically infer the correct schema for yellow_tripdata
# CREATE EXTERNAL TABLE `de-zoomcamp-module-4.module4_all_trips_dataset.yellow_tripdata`
# OPTIONS (
#   format = 'CSV',
#   uris = ['gs://zoomcamp-module-4-terra-bucket-022625/yellow/*.csv.gz'],
#   skip_leading_rows = 1,  -- Skip the header row if it exists
#   field_delimiter = ',',  -- Set the delimiter for CSV files
#   compression = 'GZIP'    -- Specify that the files are compressed using GZIP
# );
