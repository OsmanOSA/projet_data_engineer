import os
import base64
import requests
import pandas as pd
import time
import pendulum

from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup 
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.operators.python import PythonOperator



os.environ["AIRFLOW__LOGGING__ENABLE_TASK_INSTANCE_LOGGING"] = "True"

load_dotenv()

# Paramètres du DAG
POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "rte_api"

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

@task
def extract_energy_consumption():
    """
    Extrait les données de consommation énergétique depuis l’API RTE.
    - Utilise des dates dynamiques (hier à aujourd’hui)
    - Réessaie automatiquement en cas d’échec temporaire
    - Affiche les erreurs de réponse utiles pour le débogage
    """
    
    # Récupération du token
    token = get_rte_access_token()
    
    # Headers API
    api_headers = {
        "Host": "digital.iservices.rte-france.com",
        "Authorization": f"Bearer {token}"
    }
    
    # Calcul des dates : hier → aujourd’hui (UTC)
    end = datetime.utcnow()
    start = end - timedelta(days=1)

    endpoint = "https://digital.iservices.rte-france.com/open_api/consumption/v1/short_term?type=REALISED"

    
    response = requests.get(endpoint, headers=api_headers)
        
    if response.status_code == 200:
        print("✅ Succès récupération données consommation")
        return response.json()
    
    raise Exception(f"Échec de la récupération des données : {response.status_code}")
    
@task
def extract_energy_solar_pv():
    """
    Extrait la production d'énergie solaire via l'API RTE.
    Les identifiants client_id et client_secret sont récupérés depuis le champ Extra JSON de la connexion.
    """
        
    # Récupérer le token
    token = get_rte_access_token(type_energy_api="generations_per_production_type")

    api_headers = {
            "Host": "digital.iservices.rte-france.com",
            "Authorization": f"Bearer {token}"
                }

    endpoint = f"https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_production_type"
    api_response = requests.get(endpoint, headers=api_headers)
    
    if api_response.status_code == 200:
        return api_response.json()
    else:
        raise Exception(f"Échec de la récupération des données : {api_response.status_code}")
  

@task
def transform_consumption(raw_consumptions):
    """
        Transforme les données extraites en DataFrame pandas.
    """
        
    df_list = [
            {
                "Date": entry.get("start_date"),
                "Consommations": entry.get("value")
            }
            for entry in raw_consumptions['short_term'][0]['values']
        ]

    dataframe_consumption = pd.DataFrame(df_list)
    # Conversion de la colonne date en datetime avec fuseau horaire UTC
    dataframe_consumption['Date'] = pd.to_datetime(dataframe_consumption["Date"], 
                                                   format="%Y-%m-%dT%H:%M:%S%z", utc=True)
    
    dataframe_consumption.set_index('Date', inplace=True)
    df_hourly = dataframe_consumption.resample('h').mean().reset_index()
    df_hourly["Date"] = df_hourly['Date'].dt.strftime("%Y-%m-%d %H:%M")
    return df_hourly


@task
def transform_solar_energy(raw_solar_energy):

    data_lst = [

            {
                "Date":entry.get("start_date"),
                "Production_PV": entry.get("value")
            }

            for entry in raw_solar_energy["actual_generations_per_production_type"][8]["values"]
    ]

    dataframe_solar = pd.DataFrame(data_lst)
    # Conversion de la colonne date en datetime avec fuseau horaire UTC
    dataframe_solar['Date'] = pd.to_datetime(dataframe_solar["Date"], 
                                             format="%Y-%m-%dT%H:%M:%S%z", utc=True)
    
    dataframe_solar['Date'] = dataframe_solar['Date'].dt.strftime("%Y-%m-%d %H:%M")

    return dataframe_solar


@task(task_id="load_datasets")
def load_datasets(dataframe_consump, dataframe_solar):
    # Inserer les données transformées dans la table
        
    # Jointure sur la colonne Date (inner join pour les valeurs communes uniquement)
    df_merged = pd.merge(dataframe_consump, dataframe_solar, on='Date')

    # ⚠️ Remplir les valeurs manquantes par la moyenne de la colonne
    df_merged["Consommations"] = df_merged["Consommations"].fillna(df_merged["Consommations"].mean())
    df_merged["Production_PV"] = df_merged["Production_PV"].fillna(df_merged["Production_PV"].mean())

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
        
    # Créer une table si elle n'existe pas 
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS energies (
            Date TIMESTAMP,
            Consommations FLOAT,
            Production_PV FLOAT
        );
        """)
    
    # Préparation des lignes à insérer
    data = [
        (row["Date"], row["Consommations"], row["Production_PV"])
        for _, row in df_merged.iterrows()
    ]


    cursor.executemany("""
            INSERT INTO energies (Date, Consommations, Production_PV)
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
    

    with TaskGroup("Data_ingestion") as extract_group: 
        raw_consumption = extract_energy_consumption()
        raw_solar = extract_energy_solar_pv()

    with TaskGroup("Transform_datasets") as transform_group:

        transformed_consumption = transform_consumption(raw_consumption)
        transformed_solar = transform_solar_energy(raw_solar)

    load = load_datasets(transformed_consumption, transformed_solar)
        
    start_pipeline >> extract_group >> transform_group >> load
