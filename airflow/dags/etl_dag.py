from datetime import datetime, timedelta
from airflow.models.dag import DAG # type: ignore
from airflow.operators.python_operator import PythonOperator #type: ignore
import os
import requests
import pandas as pd
import json
from pymongo import MongoClient

seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

API_KEY = os.environ.get('API_KEY')
URL = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={seven_days_ago}&api_key={API_KEY}"
TEMP_PATH = os.environ.get('TEMP_PATH')
DATABASE_STR = os.environ.get('DATABASE_STRING')
final_data_dict = {}


def extract_from_api():
    response = requests.get(URL)

    with open(f"{TEMP_PATH}/dataset.json", 'w') as f:
        json.dump(response.json()["near_earth_objects"], f, indent=4)

def transform_data():
    #open the json file
    with open(f"{TEMP_PATH}/dataset.json", 'r') as f:
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
            is_sentry = key['is_sentry_object']
    
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
                'is_sentry_object': is_sentry
            }
            df.loc[len(df)] = new_row
    # Convert the dataframe into dict
    global final_data_dict 
    final_data_dict = df.to_dict(orient='records')
    with open(f"{TEMP_PATH}/dataset.json", 'w') as f:
        json.dump(final_data_dict, f, indent=4)



def load_into_db():
    with open(f"{TEMP_PATH}/dataset.json", 'r') as f:
        data = json.load(f)
    client = MongoClient(DATABASE_STR)
    db = client['asteroids_db']
    collection = db['asteroids_info']
    collection.insert_many(data)


with DAG(
        "ETL_with_NASA_Asteroids_data",
        default_args={
        "depends_on_past": False,
        "email": ["henokagb@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1)
    },
    schedule=timedelta(days=7),
    start_date=datetime(2024, 4, 29),
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='Extract_data_from_API',
        python_callable=extract_from_api,
        dag=dag
    )

    task2 = PythonOperator(
        task_id='Transform_and_clean_data',
        python_callable=transform_data,
        dag=dag
    )

    task3 = PythonOperator(
        task_id='Load_into_db',
        python_callable=load_into_db,
        dag=dag
    )

    task1 >> task2 >> task3
