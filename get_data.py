
"""
This script fetches bird observation data from the eBird API and logs the results.
Please upload your API key to the .env file and name it EBIRD_API_TOKEN before running the script.
"""

import requests
import csv
from dotenv import load_dotenv
import os
load_dotenv()
api_key = os.getenv("EBIRD_API_TOKEN")

import logging
logging.basicConfig(level=logging.INFO,filename='ebird.log',format='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M')

import getpass
CURRENT_USER = getpass.getuser()
logging.basicConfig(
    level=logging.INFO,
    filename='ebird.log',
    format='%(asctime)s %(message)s'
)

def get_bird_observations(api_key,lat=40.939999, lng=-73.826111,notable=False):
    """
    Fetches bird observations from the eBird API and writes them to a CSV file.

    Parameters:
    - api_key (str): The API key for accessing the eBird API.
    - lat (float): Latitude for the location to fetch observations. Default is 40.939999.
    - lng (float): Longitude for the location to fetch observations. Default is -73.826111.
    - notable (bool): If True, fetch notable observations. If False, fetch recent observations. Default is True.
    """
    url=(f'https://api.ebird.org/v2/data/obs/geo/recent/notable?lat={lat}&lng={lng}' if notable
         else f"https://api.ebird.org/v2/data/obs/geo/recent?lat={lat}&lng={lng}")

    headers = {
        'X-eBirdApiToken': api_key
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print('Failed to get data:', response.status_code)
        logging.error(f'Failed to get data: {response.status_code} - User {CURRENT_USER}')
    else:
        observations = response.json()
        field_names = list(observations[0].keys()) if len(observations) > 0 else []
        filename = 'recent_notable_observations.csv' if notable else 'recent_observations.csv'
        with open(filename, 'w', newline='',encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile,fieldnames=field_names)
            for observation in observations:
                writer.writerow({field: observation.get(field) for field in field_names})
            message=f'{len(observations)} records written to {filename} - User {CURRENT_USER}'
            logging.info(message)
            print(message)

if __name__ == '__main__':
    lat, lng = 40.939999, -73.826111
    get_bird_observations(api_key,lat,lng,notable=True)
    get_bird_observations(api_key,lat,lng,notable=False)

