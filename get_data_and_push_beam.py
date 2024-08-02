import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import io

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
    print('calling transform')
    obj = {}

    for field in schema:
        print('inside transform 1', obj)
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


# class ParseCSVRow(beam.DoFn):
#     print('calling ParseCSVRow')
#     def process(self, element):
#         print('calling process')
#         #print('Processing element in ParseCSVRow:', element)
#         reader = csv.DictReader(io.StringIO(element))
#         for row in reader:
#             print('Parsed row:', row)
#             yield row



# class ParseCSVRow(beam.DoFn):
#     def __init__(self):
#         print('calling ParseCSVRow')

#     def process(self, element):
#         #print('Processing element in ParseCSVRow:', element)
#         try:
#             reader = csv.DictReader(io.StringIO(element))
#             print('rows:', list(reader))
#             for row in reader:
#                 print('Parsed row:', row)
#                 yield row
#         except Exception as e:
#             print(f"Error reading CSV element: {element}")
#             print(f"Exception: {e}")


class ParseCSVRow(beam.DoFn):
    def __init__(self):
        print('calling ParseCSVRow')

    def process(self, element):
        print('Processing element in ParseCSVRow:', element)
        try:
            # Define the header row
            header = HEADER
            # Combine the header and the element
            csv_data = f"{header}\n{element}"
            
            reader = csv.DictReader(io.StringIO(csv_data))
            rows = list(reader)  # Convert reader to a list to inspect its contents
            if not rows:
                print('No rows found in the CSV element.')
            else:
                for row in rows:
                    print('Parsed row:', row)
                    yield row
        except Exception as e:
            print(f"Error reading CSV element: {element}")
            print(f"Exception: {e}")



# class FormatCSVRow(beam.DoFn):
#     def process(self, element):
#         output = io.StringIO()
#         writer = csv.DictWriter(output, fieldnames=[field['name'] for field in schema])
#         writer.writerow(element)
#         yield output.getvalue().strip()

# class FormatCSVRow(beam.DoFn):
#     def __init__(self):
#         self.header_written = False

#     def process(self, element):
#         output = io.StringIO()
#         writer = csv.DictWriter(output, fieldnames=[field['name'] for field in schema])
#         if not self.header_written:
#             writer.writeheader()
#             self.header_written = True
#         writer.writerow(element)
#         yield output.getvalue().strip()
class FormatCSVRow(beam.CombineFn):
    print('calling FormatCSVRow')
    def create_accumulator(self):
        return []

    def add_input(self, accumulator, element):
        accumulator.append(element)
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = []
        for accumulator in accumulators:
            merged.extend(accumulator)
        return merged

    def extract_output(self, accumulator):
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=[field['name'] for field in schema])
        writer.writeheader()
        for element in accumulator:
            writer.writerow(element)
        return [output.getvalue().strip()]
    

# def run(argv=None):
#     pipeline_options = PipelineOptions(argv)
#     with beam.Pipeline(options=pipeline_options) as p:
#         (
#             p
#             | 'ReadInput' >> beam.io.ReadFromText('recent_observations.csv', skip_header_lines=1)
#             | 'ParseCSV' >> beam.ParDo(ParseCSVRow())
#             | 'Transform' >> beam.Map(transform)
#             | 'FormatCSV' >> beam.CombineGlobally(FormatCSVRow()).without_defaults()
#             | 'WriteOutput' >> beam.io.WriteToText('output_data', file_name_suffix='.csv', header='speciesCode,comName,sciName,locId,locName,obsDt,howMany,lat,lng,obsValid,obsReviewed,locationPrivate,subId')
#         )



# def run(argv=None):
#     pipeline_options = PipelineOptions(argv)
#     with beam.Pipeline(options=pipeline_options) as p:
#         (
#             p
#             | 'ReadInput' >> beam.io.ReadFromText('recent_observations.csv', skip_header_lines=1)
#             | 'ParseCSV' >> beam.ParDo(ParseCSVRow())
#             | 'Transform' >> beam.Map(transform)
#             | 'FormatCSV' >> beam.CombineGlobally(FormatCSVRow()).without_defaults()
#             | 'WriteOutput' >> beam.io.WriteToText('output_data', file_name_suffix='.csv')
#         )

def run(argv=None):
    schema = (
    "speciesCode:STRING, "
    "comName:STRING, "
    "sciName:STRING, "
    "locId:STRING, "
    "locName:STRING, "
    "obsDt:STRING, "
    "howMany:STRING, "
    "lat:STRING, "
    "lng:STRING, "
    "obsValid:STRING, "
    "obsReviewed:STRING, "
    "locationPrivate:STRING, "
    "subId:STRING"
    )
    pipeline_options = PipelineOptions(argv)
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadInput' >> beam.io.ReadFromText('recent_observations.csv', skip_header_lines=1)
            | 'ParseCSV' >> beam.ParDo(ParseCSVRow())
            | 'Transform' >> beam.Map(transform)
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                'project-bird-430511:project_bird_dataset.project_bird_table',
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location='gs://project-bird-bucket/temp'
            )
        )


if __name__ == '__main__':
    print('running')
    run()
    print('done')