"""
Triggers a Dataflow job to apply a schema to the CSV data from GCS and load it into BigQuery.
"""

from googleapiclient.discovery import build
import google.auth

def trigger_dataflow_job():
    """
    Programatically triggers a Dataflow job to read data from a CSV file n a GCS bucket,
    apply a schema to the data, and write the data to a BigQuery table using a predefined
    Dataflow template for GCS to BigQuery loading.
    """
    credentials, project = google.auth.default()   #__,'project-bird-430511'
    service = build('dataflow', 'v1b3',credentials=credentials)

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

    request=service.projects().templates().launch(projectId=project,gcsPath=template_path,body=template_body)
    response = request.execute()
    print(response)

if __name__ == '__main__':
    trigger_dataflow_job()
