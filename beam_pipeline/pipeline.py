"""
This script processes and transforms CSV data using Apache Beam.

Functions:
    parse_location_string(location_string):
        Parses a location string to extract the location name, latitude, and longitude.

    transform(row):
        Transforms a row by ensuring all fields in the schema are present and 
        attempts to parse the location string to extract latitude and longitude 
        if they are not already present.
    run(argv=None):
        Runs the Apache Beam pipeline to process CSV data and write the results to BigQuery.

Classes:
    ParseCSVRow(beam.DoFn):
        A DoFn class for parsing CSV rows in an Apache Beam pipeline.

    FormatCSVRow(beam.CombineFn):
        A CombineFn class for formatting rows into a CSV string in an Apache Beam pipeline.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import csv
import io
import argparse
import logging

# Define the schema
schema = [
    {"name": "speciesCode", "type": "STRING", "mode": "REQUIRED"},
    {"name": "comName", "type": "STRING", "mode": "REQUIRED"},
    {"name": "sciName", "type": "STRING", "mode": "REQUIRED"},
    {"name": "locId", "type": "STRING", "mode": "REQUIRED"},
    {"name": "locName", "type": "STRING", "mode": "REQUIRED"},
    {"name": "obsDt", "type": "STRING", "mode": "REQUIRED"},
    {"name": "howMany", "type": "STRING", "mode": "REQUIRED"},
    {"name": "lat", "type": "STRING", "mode": "REQUIRED"},
    {"name": "lng", "type": "STRING", "mode": "REQUIRED"},
    {"name": "obsValid", "type": "STRING", "mode": "REQUIRED"},
    {"name": "obsReviewed", "type": "STRING", "mode": "REQUIRED"},
    {"name": "locationPrivate", "type": "STRING", "mode": "REQUIRED"},
    {"name": "subId", "type": "STRING", "mode": "REQUIRED"}
]
HEADER = "speciesCode,comName,sciName,locId,locName,obsDt,howMany,lat,lng,obsValid,obsReviewed,locationPrivate,subId"

def parse_location_string(location_string):
    """
    Parses a location string to extract the location name, latitude, and longitude.

    Args:
        location_string (str): The location string to parse.

    Returns:
        dict: A dictionary with keys 'locationName', 'lat', and 'long'.
    """
    import re
    regex_with_lat_long = r'^(.*),\s*([+-]?\d+\.\d+),\s*([+-]?\d+\.\d+)$'
    match_with_lat_long = re.match(regex_with_lat_long, location_string)

    if match_with_lat_long:
        return {
            'locationName': match_with_lat_long.group(1).strip(),
            'lat': float(match_with_lat_long.group(2)),
            'long': float(match_with_lat_long.group(3))
        }
    else:
        return {
            'locationName': location_string.strip(),
            'lat': None,
            'long': None
        }

def transform(row):
    """
    Transforms a row by ensuring all fields in the schema are present and 
    attempts to parse the location string to extract latitude and longitude 
    if they are not already present.

    Args:
        row (dict): The input row to transform.

    Returns:
        dict: The transformed row with all schema fields and parsed location data.
    """
    #print('calling transform')
    obj = {}

    for field in schema:
        #print('inside transform 1', obj)
        obj[field['name']] = row.get(field['name'], '')

    # Check if lat and lng are already present
    if not obj['lat'] or not obj['lng']:
        # Parse the location string if lat and lng are not present
        location_string = obj['locName']
        try:
            parsed_location = parse_location_string(location_string)
            obj['locName'] = parsed_location['locationName']
            obj['lat'] = parsed_location['lat'] or obj['lat']
            obj['lng'] = parsed_location['long'] or obj['lng']
        except Exception as error:
            print(f"Error parsing location string: {location_string}")

    return obj


class ParseCSVRow(beam.DoFn):
    """
    A DoFn class for parsing CSV rows in an Apache Beam pipeline.

    Methods:
        process(element):
            Processes a CSV element by combining it with a header row, 
            parsing it into a dictionary, and yielding each row.
    """
    def __init__(self,header):
        print('calling ParseCSVRow')
        self.header = header

    def process(self, element):
        print('Processing element in ParseCSVRow:', element)
        try:
            # Combine the header and the element
            csv_data = f"{self.header}\n{element}"
            
            reader = csv.DictReader(io.StringIO(csv_data))
            rows = list(reader)  # Convert reader to a list to inspect its contents
            if not rows:
                print('No rows found in the CSV element.')
            else:
                for row in rows:
                    #print('Parsed row:', row)
                    yield row
        except Exception as e:
            print(f"Error reading CSV element: {element}")
            print(f"Exception: {e}")




# class FormatCSVRow(beam.CombineFn):
#     """
#     A CombineFn class for formatting rows into a CSV string in an Apache Beam pipeline.

#     Methods:
#         create_accumulator():
#             Initializes an empty accumulator list.

#         add_input(accumulator, element):
#             Adds an input element to the accumulator.

#         merge_accumulators(accumulators):
#             Merges multiple accumulators into a single list.

#         extract_output(accumulator):
#             Converts the accumulated elements into a CSV formatted string.
#     """
#     print('calling FormatCSVRow')
#     def create_accumulator(self):
#         return []

#     def add_input(self, accumulator, element):
#         accumulator.append(element)
#         return accumulator

#     def merge_accumulators(self, accumulators):
#         merged = []
#         for accumulator in accumulators:
#             merged.extend(accumulator)
#         return merged

#     def extract_output(self, accumulator):
#         output = io.StringIO()
#         writer = csv.DictWriter(output, fieldnames=[field['name'] for field in schema])
#         writer.writeheader()
#         for element in accumulator:
#             writer.writerow(element)
#         return [output.getvalue().strip()]
        #return output.getvalue().strip()
# class FormatCSVRow(beam.DoFn):
#     def process(self, element):
#         try:
#             formatted_row = ','.join([str(element[field]) for field in element])
#             yield formatted_row
#         except Exception as e:
#             logging.error(f"Error formatting row: {element}, error: {e}")

class FormatCSVRow(beam.DoFn):
    def process(self, element):
        #values = element.split(',')
        #row = dict(zip(HEADER, element))
        yield element
# class CustomOptions(PipelineOptions):
#     @classmethod
#     def _add_argparse_args(cls, parser):
#         parser.add_argument('--input', dest='input', required=True, help='Input file to process.')
#         parser.add_argument('--output', dest='output', required=True, help='Output BigQuery table to write results to.')
#         parser.add_argument('--temp_location', dest='temp_location', required=True, help='GCS location for temporary files.')
#         parser.add_argument('--project', dest='project', required=True, help='GCP project ID.')
#         parser.add_argument('--job_name', dest='job_name', required=True, help='Dataflow job name.')
#         parser.add_argument('--staging_location', dest='staging_location', required=True, help='GCS staging location.')
#         parser.add_argument('--region', dest='region', required=True, help='GCP region.')

def run_pipeline(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True, help='Input file to process.')
    parser.add_argument('--output', dest='output', required=True, help='Output BigQuery table to write results to.')
    parser.add_argument('--temp_location', dest='temp_location', required=True, help='GCS location for temporary files.')
    parser.add_argument('--project', dest='project', required=True, help='GCP project ID.')
    parser.add_argument('--job_name', dest='job_name', required=True, help='Dataflow job name.')
    parser.add_argument('--staging_location', dest='staging_location', required=True, help='GCS staging location.')
    parser.add_argument('--region', dest='region', required=True, help='GCP region.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    #custom_options = pipeline_options.view_as(CustomOptions)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = known_args.project
    google_cloud_options.job_name = known_args.job_name
    google_cloud_options.staging_location = known_args.staging_location
    google_cloud_options.temp_location = known_args.temp_location
    google_cloud_options.region = known_args.region
    pipeline_options.view_as(StandardOptions)._pipelinener = 'DataflowRunner'
    #pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'

   


    ##############################
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadInput' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
            | 'ParseCSV' >> beam.ParDo(ParseCSVRow(header=HEADER))
            | 'Transform' >> beam.Map(transform)
            | 'FormatCSV' >> beam.ParDo(FormatCSVRow())
            #| 'WriteOutput' >> beam.io.WriteToText('output_data', file_name_suffix='.csv')

             | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                'project_bird_dataset.project_bird_table',
                schema='speciesCode:STRING, comName:STRING, sciName:STRING, locId:STRING, locName:STRING, obsDt:TIMESTAMP, howMany:INTEGER, lat:FLOAT, lng:FLOAT, obsValid:BOOLEAN, obsReviewed:BOOLEAN, locationPrivate:BOOLEAN, subId:STRING',
                #schema=bq_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
                     #   | 'WriteOutput' >> beam.io.WriteToText('output_data', file_name_suffix='.csv')

            #            | 'FormatCSV' >> beam.CombineGlobally(FormatCSVRow()).without_defaults()

            # | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            #     known_args.output,
            #     schema=bq_schema,
            #     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            #     custom_gcs_temp_location=google_cloud_options.temp_location,
            #    # with_errors = True
            # )
        )
        #           # | 'FormatCSV' >> beam.ParDo(FormatCSVRow())

        ##############################

    # with beam.Pipeline(options=pipeline_options) as p:
    #     formatted_rows = (
    #         p
    #         | 'ReadInput' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
    #         | 'ParseCSV' >> beam.ParDo(ParseCSVRow(header=HEADER))
    #         | 'Transform' >> beam.Map(transform)
    #         | 'FormatCSV' >> beam.ParDo(FormatCSVRow())
    #     )

    #     # Write formatted rows to a CSV file in Google Cloud Storage
    #     formatted_rows | 'WriteToGCS' >> beam.io.WriteToText('gs://project-bird-bucket/formatted_output', file_name_suffix='.csv', header=','.join(HEADER))

    #     # Read the CSV file from Google Cloud Storage and write to BigQuery
    #     (
    #         p
    #         | 'ReadFormattedCSV' >> beam.io.ReadFromText('gs://project-bird-bucket/formatted_output-00000-of-00001.csv', skip_header_lines=1)
    #         | 'ParseFormattedCSV' >> beam.ParDo(ParseCSVRow(header=HEADER))
    #         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
    #             known_args.output,
    #             schema=bq_schema,
    #             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    #             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #             custom_gcs_temp_location=google_cloud_options.temp_location,
    #         )
    #     )
    ##############################

def run_local(argv=None):
    pipeline_options = PipelineOptions(argv)
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadInput' >> beam.io.ReadFromText('gs://project-bird-bucket/recent_observations.csv', skip_header_lines=1)
            | 'ParseCSV' >> beam.ParDo(ParseCSVRow(HEADER))
            | 'Transform' >> beam.Map(transform)
            | 'FormatCSV' >> beam.CombineGlobally(FormatCSVRow()).without_defaults()
            | 'WriteOutput' >> beam.io.WriteToText('output_data', file_name_suffix='.csv')
        )
# Careful!  Below is good for testing, will interfere with Dataflow job ran with Cloud Function
# if __name__ == '__main__':
#     print('running')
#run_local()
#     #run()
#     print('done')
#run_pipeline()