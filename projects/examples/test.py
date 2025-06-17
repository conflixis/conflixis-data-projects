from google.cloud import bigquery
import os

# Set up project ID and service account key
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "data-analytics-389803")
SERVICE_ACCOUNT_KEY = "/home/incent/conflixis-analytics/common/data-analytics-389803-a6f8a077b407.json"

try:
    # Construct a BigQuery client object with service account credentials
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_KEY, project=PROJECT_ID)
    
    datasets = list(client.list_datasets())  # Make an API request.
    project = client.project

    if datasets:
        print(f"Datasets in project {project}:")
        for dataset in datasets:
            print(f"\t{dataset.dataset_id}")
    else:
        print(f"{project} project does not contain any datasets.")
        
except Exception as e:
    print(f"Error accessing BigQuery: {e}")
    print("Please ensure you have:")
    print("1. Set up Google Cloud authentication (gcloud auth application-default login)")
    print("2. Set the GOOGLE_CLOUD_PROJECT environment variable or update PROJECT_ID in the script")
