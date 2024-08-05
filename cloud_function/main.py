import subprocess
import logging

def trigger_dataflow(request):
    input_file = 'gs://project-bird-bucket/recent_observations.csv'
    output_table = 'project-bird-430511:project_bird_dataset.project_bird_table'
    temp_location = 'gs://project-bird-bucket/temp'
    staging_location = 'gs://project-bird-bucket/staging'
    job_name = 'project-bird-430511-dataflow-job'
    project = 'project-bird-430511'
    region = 'us-central1'

    command = [
        'python3', 'get_data_and_push_beam.py',
        '--input', input_file,
        '--output', output_table,
        '--temp_location', temp_location,
        '--runner', 'DataflowRunner',
        '--project', project,
        '--staging_location', staging_location,
        '--region', region,
        '--job_name', job_name
    ]

    logging.info(f'Running command: {" ".join(command)}')
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    logging.info(f'Stdout: {result.stdout.decode("utf-8")}')
    logging.error(f'Stderr: {result.stderr.decode("utf-8")}')
    
    return {
        'stdout': result.stdout.decode('utf-8'),
        'stderr': result.stderr.decode('utf-8')
    }

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    trigger_dataflow('request')