"""
Triggers a Dataflow job to apply a schema to the CSV data from GCS and load it into BigQuery.
Pre-work:
npm install
zip -r apply-schema.zip apply_schema.js package.json node_modules
gsutil cp apply-schema.zip gs://your-bucket-name/apply-schema.zip
"""

from googleapiclient.discovery import build
import google.auth
from google.cloud import storage
import zipfile
import os
import requests
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# def download_and_extract_zip(url, extract_to='.'):
#     response = requests.get(url)
#     zip_path = os.path.join(extract_to, 'apply-schema.zip')
#     with open(zip_path, 'wb') as f:
#         f.write(response.content)
#     with zipfile.ZipFile(zip_path, 'r') as zip_ref:
#         zip_ref.extractall(extract_to)
#     os.remove(zip_path)
def download_and_extract_zip(gcs_url, extract_to='.'):
    # Parse the GCS URL
    if not gcs_url.startswith('gs://'):
        raise ValueError("URL must start with 'gs://'")

    bucket_name, blob_name = gcs_url[5:].split('/', 1)

    # Initialize the GCS client
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Download the blob to a local file
    zip_path = os.path.join(extract_to, 'apply-schema.zip')
    blob.download_to_filename(zip_path)

    # Extract the zip file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    os.remove(zip_path)
    
def trigger_dataflow_job(environment,data=None):
    """
    Programatically trigger a Dataflow job to read data from a CSV file in a GCS bucket,
    apply a schema to the data, and write the data to a BigQuery table using a predefined
    Dataflow template for GCS to BigQuery loading.
    """
    try:
        _, project = google.auth.default()   #__,'project-bird-430511'
        service = build('dataflow', 'v1b3')

        zip_url = "gs://project-bird-metadata/apply-schema.zip"
        download_and_extract_zip(zip_url)
        
        # From Dataflow Console "Job info" get 
        template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

        # Again, get most of these from the Console but can also grep for some of these:
        # gcloud dataflow jobs describe 2024-07-28_04_41_14-12501669212819531181|grep "project"
        template_body = {
            "jobName": "bqload-test",
            "parameters": {
                "javascriptTextTransformFunctionName": "transform",
                "JSONPath": "gs://project-bird-metadata/bq_schema.json",
                "javascriptTextTransformGcsPath": "gs://project-bird-metadata/apply_schema.js",
                "outputTable":"project-bird-430511:project_bird_dataset.project_bird_table",
                "gcpTempLocation":"gs://project-bird-metadata/temp/",
                "inputFilePattern": "gs://project-bird-bucket/recent_observations.csv",
                "bigQueryLoadingTemporaryDirectory":"gs://project-bird-metadata/temp/"
            }
        }
        logger.info("Triggering Dataflow job with template: %s", template_path)
        logger.info("Template body: %s", template_body)
    
        request=service.projects().templates().launch(
            projectId=project,
            gcsPath=template_path,
            body=template_body)
        
        response = request.execute()
        logger.info("Dataflow job triggered successfully: %s", response)

        print(response)

    except Exception as e:
        logger.error("Error triggering Dataflow job: %s", e)
        raise
