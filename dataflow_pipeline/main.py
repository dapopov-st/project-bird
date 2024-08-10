import subprocess
import logging
from beam_pipeline.pipeline import run_pipeline
import functions_framework
import subprocess
import logging
import os

@functions_framework.cloud_event
def trigger_dataflow(cloud_event):
    os.environ['GOOGLE_CLOUD_PROJECT'] = 'project-bird-430511'

    logging.info(f'Received event: {cloud_event}')
    data = cloud_event.data
    bucket = data.get('bucket')
    name = data.get('name')
    input_file = f'gs://{bucket}/{name}'
    print('input_file: ',input_file)
    #input_file = 'gs://project-bird-bucket/recent_observations.csv'
    output_table = 'project-bird-430511:project_bird_dataset.project_bird_table'
    temp_location = 'gs://project-bird-meta/temp'
    staging_location = 'gs://project-bird-meta/staging'
    job_name = 'project-bird-430511-dataflow-job'
    project = 'project-bird-430511'
    region = 'us-east5'

    argv = [
        '--input', input_file,
        '--output', output_table,
        '--temp_location', temp_location,
        '--runner', 'DataflowRunner',
        '--project', project,
        '--staging_location', staging_location,
        '--region', region,
        '--job_name', job_name,
        '--setup_file', './setup.py' 
    ]

    logging.info(f'Running pipeline with arguments: {" ".join(argv)}')
    run_pipeline(argv)
