from datetime import datetime, timedelta
from airflow.models.dag import DAG # type: ignore
from airflow.operators.bash import BashOperator #type: ignore
import os
import requests
import pandas as pd
import json

seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

API_KEY = os.environ.get('API_KEY')
URL = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={seven_days_ago}&api_key={API_KEY}"
STAGING_PATH = os.environ.get('STAGING_PATH')


def extract_from_api():
    response = requests.get(URL)

    with open(f"{STAGING_PATH}/dataset.json", 'w') as f:
        json.dump(response.json()["near_earth_objects"], f, indent=4)

def transform_data():
    #open the json file
    with open(f"{STAGING_PATH}/dataset.json", 'r') as f:
        data = json.load(f)

    #defining column names for the dataframe
    columns = [
    'id',
    'name',
    'nasa_info_url',
    'absolute_magnitude_h',
    'kilometers_estimated_max_diameter',
    'is_potentially_hazardous_asteroid',
    'close_approach_date',
    'kilometers_per_second_velocity',
    'kilometers_miss_distance',
    'is_sentry_object'
    ]
    df = pd.DataFrame(columns=columns)

    #filing the dataframe with relevant data from the file
    for date in data:
        for key in data[date]:
            id = key['id']
            name = key['name']
            nasa_info_url = key['nasa_jpl_url']
            magnitude = key['absolute_magnitude_h']
            diameter_km = key['estimated_diameter']['kilometers']['estimated_diameter_max']
            hazardous = key['is_potentially_hazardous_asteroid']
            for i in key['close_approach_data']:
                close_approach_date = i['close_approach_date']
                velocity_km_s = i['relative_velocity']['kilometers_per_second']
                miss_distance_km = i['miss_distance']['kilometers']
            is_sentric = key['is_sentry_object']
    
            new_row = {
                'id': id,
                'name': name.split('(')[-1][:-1],
                'nasa_info_url': nasa_info_url,
                'absolute_magnitude_h': round(float(magnitude), 2),
                'kilometers_estimated_max_diameter': round(float(diameter_km), 2),
                'is_potentially_hazardous_asteroid': hazardous,
                'close_approach_date': close_approach_date,
                'kilometers_per_second_velocity': round(float(velocity_km_s), 2),
                'kilometers_miss_distance': round(float(miss_distance_km), 2),
                'is_sentry_object': is_sentric
            }
            df.loc[len(df)] = new_row
    
    print(df)


transform_data()






