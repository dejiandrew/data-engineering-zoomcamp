import os
import requests
from google.cloud import storage
from google.api_core.exceptions import RetryError

BUCKET = "zoomcamp-module-4-terra-bucket-022625"
GITHUB_RELEASE_URL = "https://api.github.com/repos/DataTalksClub/nyc-tlc-data/releases/tags/fhv"

# Function to upload files to GCS with increased timeout and retry logic
def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    
    # Set a large chunk size for large files and increase the timeout for the upload
    blob.chunk_size = 10 * 1024 * 1024  # 10 MB chunks
    blob.upload_from_filename(local_file, timeout=1800)  # 30 minutes timeout
    
    print(f"Uploaded {local_file} to GCS: {object_name}")

# Function to download files from GitHub release
def download_from_github(url, save_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(save_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded {save_path}")
    else:
        print(f"Failed to download {url}")

# Get the list of assets (files) from the GitHub release API
def get_github_release_assets():
    response = requests.get(GITHUB_RELEASE_URL)
    if response.status_code == 200:
        assets = response.json()['assets']
        csv_files = [asset['browser_download_url'] for asset in assets if asset['name'].endswith('.csv.gz')]
        return csv_files
    else:
        print(f"Failed to fetch release assets. Status code: {response.status_code}")
        return []

# Main function to download CSVs and upload to GCS
def download_and_upload():
    # Get all the CSV files from the GitHub release
    csv_urls = get_github_release_assets()

    # Download and upload each file
    for url in csv_urls:
        # Extract the file name from the URL
        file_name = url.split('/')[-1]
        
        try:
            # Download the file
            download_from_github(url, file_name)
            
            # Upload to GCS
            upload_to_gcs(BUCKET, f"fhv/{file_name}", file_name)
            
            # Optionally, delete the local file after upload
            os.remove(file_name)
        
        except RetryError as e:
            print(f"RetryError encountered for {file_name}: {e}")
            # Handle retries or log the error as needed.

# Run the process
download_and_upload()
