# ----------------------------------------
# Imports
# ----------------------------------------
from google.cloud import storage
from google.cloud.exceptions import NotFound
import os

# ----------------------------------------
# Configurable Parameters
# ----------------------------------------
local_file_path = r'C:\Users\vince\OneDrive\Documents\Conflixis\conflixis-ai\dojscrape\doj005b_pp_typology_with_supplier.csv'
bucket_name = 'conflixis'
destination_blob_name = 'riskdata/doj005b_pp_typology_with_supplier.csv'
project_id = '273246361154'  # Replace with your Google Cloud Project ID

# ----------------------------------------
# Main Processing Functions
# ----------------------------------------
def upload_blob(local_file_path, bucket_name, destination_blob_name, project_id):
    try:
        print("Initializing Google Cloud Storage client...")
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)

        # Check if the bucket exists
        if not storage_client.lookup_bucket(bucket_name):
            print(f"Bucket {bucket_name} does not exist. Creating now.")
            storage_client.create_bucket(bucket_name)

        # Upload the file
        print(f"Uploading {local_file_path} to {destination_blob_name}...")
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_file_path)
        print(f"File uploaded successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

# ----------------------------------------
# Script Execution
# ----------------------------------------
if __name__ == '__main__':
    upload_blob(local_file_path, bucket_name, destination_blob_name, project_id)
