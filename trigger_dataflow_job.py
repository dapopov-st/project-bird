from googleapiclient.discovery import build
import google.auth

def trigger_dataflow_job():
    
    credentials, project = google.auth.default()
    service = build('dataflow', 'v1b3',credentials=credentials)
    #'project-bird-430511'

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
