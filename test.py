import os
import base64
import requests
import pandas as pd
import json
import pendulum

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup 
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.operators.python import PythonOperator


# from supabase import create_client, Client #type: ignore
# import supabase #type: ignore
# from io import BytesIO


os.environ["AIRFLOW__LOGGING__ENABLE_TASK_INSTANCE_LOGGING"] = "True"

# Setup the Open-Meteo API client with cache and retry on error
# cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
# retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
# openmeteo = openmeteo_requests.Client(session = retry_session)

# Paramètres du DAG
POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "rte_api"

responses = []
start_date = datetime(2020, 1, 1)
end_date = datetime(2025, 5, 3)
six_months = timedelta(days=30*6)

default_args = {
    "owner": "airflow",
    "start_date": pendulum.today('UTC').subtract(days=1)
}

extracts_task_id = ["Extract_energy_consumption", "Extract_energy_solar_pv", "Extract_weather_data"]
transform_task_id = ["Transform_energy_consum_data", "Transform_energy_solar_data", "Transform_weather_data"]
load_task_id = ["Load_energy_consum_data", "Load_energy_solar_data", "Load_weather_data"]


def get_rte_access_token(type_energy_api="energy_consumption"):
    """retourn un token d'authentification depuis RTE"""
    
    # Configuration API RTE
    TOKEN_URL = "https://digital.iservices.rte-france.com/token/oauth/"
    data = {
            "grant_type": "client_credentials"
        }
    conn = BaseHook.get_connection(API_CONN_ID)
    
    if type_energy_api == "energy_consumption":
        client_id = conn.extra_dejson.get("CLIENT_ID")
        client_secret = conn.extra_dejson.get("CLIENT_SECRET")
    
    elif type_energy_api == "generations_per_production_type":
        client_id = conn.extra_dejson.get("CLIENT_ID_2")
        client_secret = conn.extra_dejson.get("CLIENT_SECRET_2")

    auth_str = f"{client_id}:{client_secret}"
    auth_b64 = base64.b64encode(auth_str.encode()).decode()
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {auth_b64}",
    }



    response = requests.post(TOKEN_URL, data=data, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Erreur token API RTE : {response.status_code} {response.text}")
    
    return response.json().get("access_token")

def daterange(start_date, end_date, delta):
    current_date = start_date
    while current_date < end_date:
        next_date = current_date + delta
        yield current_date, min(next_date, end_date)
        current_date = next_date


@task
def extract_energy_consumption():

    """ Extrait la consommation d'énergie via l'API RTE.
        Les identifiants client_id et client_secret sont récupérés depuis le champ Extra JSON de la connexion.
    """
    
    responses_cons = []
    # Récupérer le token
    token = get_rte_access_token()

    api_headers = {
            "Host": "digital.iservices.rte-france.com",
            "Authorization": f"Bearer {token}"
                }
    base_url = "https://digital.iservices.rte-france.com/open_api/consumption/v1/short_term?type=REALISED"
    
    for start, end in daterange(start_date, end_date, six_months):
        url = f"{base_url}&start_date={start.isoformat()}%2B02:00&end_date={end.isoformat()}%2B02:00"
        response = requests.get(url, headers=api_headers)
        if response.status_code == 200:
            responses_cons.append(response.json())  # 🔁 Retourne le contenu JSON
        else:
            raise Exception(f"Erreur API : {response.status_code}")
    return responses_cons

    
    # endpoint = f"https://digital.iservices.rte-france.com/open_api/consumption/v1/short_term?type=REALISED"
    # api_response = requests.get(endpoint, headers=api_headers)

    # if api_response.status_code == 200:
    #     return api_response.json()
    # else:
    #     raise Exception(f"Échec de la récupération des données : {api_response.status_code}")
    
@task
def extract_energy_solar_pv():
    """
    Extrait la production d'énergie solaire via l'API RTE.
    Les identifiants client_id et client_secret sont récupérés depuis le champ Extra JSON de la connexion.
    """
    responses_solar = [] 
    five_months = timedelta(days=30*5)
    # Récupérer le token
    token = get_rte_access_token(type_energy_api="generations_per_production_type")

    api_headers = {
            "Host": "digital.iservices.rte-france.com",
            "Authorization": f"Bearer {token}"
                }

    base_url = "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_production_type?"
    
    for start, end in daterange(start_date, end_date, five_months):
        url = f"{base_url}&start_date={start.isoformat()}%2B02:00&end_date={end.isoformat()}%2B02:00"
        response = requests.get(url, headers=api_headers)
        if response.status_code == 200:
            responses_solar.append(response.json())  # 🔁 Retourne le contenu JSON
        else:
            raise Exception(f"Erreur API : {response.status_code}")
    return responses_solar

    # endpoint = f"https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_production_type"
    # api_response = requests.get(endpoint, headers=api_headers)
    
    # if api_response.status_code == 200:
    #     return api_response.json()
    # else:
    #     raise Exception(f"Échec de la récupération des données : {api_response.status_code}")

@task
def extract_weather_data():

    # Make sure all required weather variables are listed here
    API_KEY  = ""
    LAT = 48.8566
    LON = 2.3522
    URL = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&units=metric&lang=fr&appid={API_KEY}"
    
    # Envoi de la requête GET
    responses = requests.get(URL)
    if responses.status_code == 200:
        return responses.json()
    else:
        raise Exception(f"Échec de la récupération des données : {responses.status_code}")


# def schemas_storage():
#         """
#         Charge les données de consommation dans PostgreSQL.
#         """
#         global cursor, conn
#         pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
#         conn = pg_hook.get_conn()
#         cursor = conn.cursor()
        
#         # Créer une table si elle n'existe pas 
#         cursor.execute("""
#         CREATE TABLE IF NOT EXISTS energies (
#             timestamp TIMESTAMP,
#             Consommations FLOAT,
#             Production_PV FLOAT
#         );
#         """) 

#         conn.commit()
    

@task
def transform_consumption(raw_consumptions):
    """
        Transforme les données extraites en DataFrame pandas.
    """
        
    df_list = [
                {
                    "timestamp": value["start_date"],
                    "Consommations": value["value"]
                }
                for entry in raw_consumptions
                for value in entry["short_term"][0]["values"]
            ]

    dataframe_consumption = pd.DataFrame(df_list)
    # Conversion de la colonne date en datetime avec fuseau horaire UTC
    dataframe_consumption['timestamp'] = pd.to_datetime(dataframe_consumption["timestamp"], 
                                                   format="%Y-%m-%dT%H:%M:%S%z", utc=True)
    
    dataframe_consumption.set_index('timestamp', inplace=True)
    df_hourly = dataframe_consumption.resample('h').mean().reset_index()
    df_hourly["timestamp"] = df_hourly['timestamp'].dt.strftime("%Y-%m-%d %H:%M")

    return df_hourly


@task
def transform_solar_energy(raw_solar_energy):

    data_lst = [

            {
                "timestamp": valeur["start_date"],
                "Production_PV": valeur["value"]
            }

            for entry in raw_solar_energy
            for valeur in entry["actual_generations_per_production_type"][8]["values"]

    ]

    dataframe_solar = pd.DataFrame(data_lst)
    # Conversion de la colonne date en datetime avec fuseau horaire UTC
    dataframe_solar['timestamp'] = pd.to_datetime(dataframe_solar["timestamp"], format="%Y-%m-%dT%H:%M:%S%z", utc=True)
    dataframe_solar['timestamp'] = dataframe_solar['timestamp'].dt.strftime("%Y-%m-%d %H:%M")
    return dataframe_solar


@task(task_id="load_datasets")
def load_datasets(dataframe_consump, dataframe_solar):
    # Inserer les données transformées dans la table
        
    # Jointure sur la colonne timestamp (inner join pour les valeurs communes uniquement)
    df_merged = pd.merge(dataframe_consump, dataframe_solar, on='timestamp')


    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
        
    # Créer une table si elle n'existe pas 
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS energies (
            timestamp TIMESTAMP,
            Consommations FLOAT,
            Production_PV FLOAT
        );
        """)
    
    # Préparation des lignes à insérer
    data = [
        (row["timestamp"], row["Consommations"], row["Production_PV"])
        for _, row in df_merged.iterrows()
    ]

    cursor.executemany("""
            INSERT INTO energies (timestamp, Consommations, Production_PV)
            VALUES (%s, %s, %s)
        """, data)

    conn.commit()
    cursor.close()
    

with DAG(
        dag_id='api_etl_dag', 
         default_args=default_args, 
         schedule="@daily",
        catchup=False) as dag:
    

    start_pipeline = DummyOperator(
            task_id = 'Pipeline_collection_data_is_ready')
    
    # strategy_storage = PythonOperator(
    #     task_id="Schemas_storage", 
    #     python_callable=schemas_storage,
    # )

    with TaskGroup("Data_ingestion") as extract_group: 
        raw_consumption = extract_energy_consumption()
        raw_solar = extract_energy_solar_pv()
        raw_weather = extract_weather_data()

    with TaskGroup("Transform_datasets") as transform_group:

        transformed_consumption = transform_consumption(raw_consumption)
        transformed_solar = transform_solar_energy(raw_solar)

    #with TaskGroup("Load_datasets") as load_group:

    load = load_datasets(transformed_consumption, transformed_solar)
        
    start_pipeline >> extract_group >> transform_group >> load


