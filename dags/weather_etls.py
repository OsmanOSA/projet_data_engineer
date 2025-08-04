import os
import base64
import requests, logging, time
import pendulum #type: ignore
import pandas as pd

from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG #type: ignore
from airflow.operators.dummy_operator import DummyOperator #type: ignore
from airflow.hooks.base import BaseHook #type: ignore
from airflow.providers.http.hooks.http import HttpHook #type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook #type: ignore
from airflow.decorators import task #type: ignore
from airflow.operators.python import PythonOperator #type: ignore
from airflow.models import Variable #type: ignore

load_dotenv()

default_args = {
    "owner": "airflow",
    "start_date": pendulum.today('UTC').subtract(days=1),
    "retries": 5,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False
}

# Paramètres du DAG
POSTGRES_CONN_ID = "postgres_default"

@task
def extract_weather_data():
    """
    Extrait les données météorologiques depuis l'API OpenWeatherMap.

    Returns
    -------
    dict
        Données JSON de la météo actuelle contenant température, humidité, 
        pression, vent, etc.

    Raises
    ------
    ValueError
        Si les variables d'environnement requises sont manquantes
    Exception
        Si l'API retourne une erreur après 5 tentatives
    """
    try:
        # Récupération des variables via Airflow Variables (priorité) puis environnement
        try:
            API_KEY = Variable.get("METEO_API_KEY", default_var=os.getenv("METEO_API_KEY"))
            LAT = Variable.get("LAT", default_var=os.getenv("LAT", "48.8566"))  
            LON = Variable.get("LON", default_var=os.getenv("LON", "2.3522"))   
        except Exception as var_error:
            logging.warning(f"Erreur accès Variables Airflow: {var_error}, fallback sur variables d'environnement")
            API_KEY = os.getenv("METEO_API_KEY")
            LAT = os.getenv("LAT", "48.8566")
            LON = os.getenv("LON", "2.3522")  
        
        # Validation des paramètres requis
        if not API_KEY:
            raise ValueError(
                "Clé API OpenWeatherMap manquante. "
                "Configurez via Interface Airflow > Admin > Variables:\n"
                "- Nom: METEO_API_KEY\n"
                "- Valeur: votre_cle_api_openweathermap\n"
                "Ou via variable d'environnement METEO_API_KEY"
            )
        
        if not LAT or not LON:
            logging.warning("Coordonnées géographiques manquantes, utilisation de Paris par défaut")
            LAT = LAT or "48.8566"
            LON = LON or "2.3522"

        # Log des paramètres utilisés (sans exposer la clé API)
        logging.info(f"Configuration météo: LAT={LAT}, LON={LON}, API_KEY={'***' + API_KEY[-4:] if len(API_KEY) > 4 else '***'}")
        
        URL = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&units=metric&lang=fr&appid={API_KEY}"
        
        for attempt in range(5):
            response = requests.get(URL, timeout=30)
            if response.status_code == 200:
                logging.info("Données météo récupérées avec succès")
                return response.json()
            logging.warning(f"Tentative {attempt+1}: Échec avec code {response.status_code}")
            time.sleep(2 ** attempt)  # Backoff exponentiel
        
        raise Exception(f"Échec de la récupération des données après 5 tentatives : {response.status_code}")
    
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction des données météo: {str(e)}")
        raise

@task
def transform_weather_data(raw_weather_data):
    """
    Transforme les données météo brutes en format structuré simplifié.

    Parameters
    ----------
    raw_weather_data : dict
        Données météo brutes retournées par l'API OpenWeatherMap

    Returns
    -------
    dict
        Dictionnaire contenant timestamp et température uniquement

    Raises
    ------
    Exception
        Si l'extraction des données météo échoue
    """
    try:
        # Extraction des données importantes
        main_data = raw_weather_data.get('main', {})
        
        transformed_data = {
            'timestamp': datetime.utcnow(),
            'temperature': main_data.get('temp')  # Cohérent avec la colonne DB
        }
        
        # Convertir la température en Celsius
        transformed_data['temperature'] = float(transformed_data['temperature']) - 273.15
        
        logging.info(f"Données météo transformées: {transformed_data}")
        return transformed_data
    
    except Exception as e:
        logging.error(f"Erreur lors de la transformation des données météo: {str(e)}")
        raise

@task(task_id="load_data_weather")
def storage_data(transformed_data):
    """
    Stocke les données météo transformées dans PostgreSQL.

    Parameters
    ----------
    transformed_data : dict
        Dictionnaire contenant les données météo transformées avec 
        les clés 'timestamp' et 'temperature'

    Raises
    ------
    Exception
        Si la connexion à la base de données ou l'insertion échoue
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
            
        # Créer une table simplifiée si elle n'existe pas 
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS meteo (
                timestamp TIMESTAMP PRIMARY KEY,
                temperature FLOAT
            );
        """)
        
        # Insertion des données
        cursor.execute("""
            INSERT INTO meteo (
                timestamp, temperature
            ) VALUES (
                %(timestamp)s, %(temperature)s
            )
            ON CONFLICT (timestamp) DO UPDATE SET
                temperature = EXCLUDED.temperature;
        """, transformed_data)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info("Données météo stockées avec succès dans PostgreSQL")
        
    except Exception as e:
        logging.error(f"Erreur lors du stockage des données météo: {str(e)}")
        raise

# Définition du DAG
with DAG(
    dag_id='weather_dag', 
    default_args=default_args, 
    description='DAG pour la collecte, le traitement et le stockage des données météorologiques',
    schedule="@hourly",
    catchup=False,
    tags=['weather', 'etl', 'openweathermap']
) as dag:
    
    start_task = DummyOperator(task_id='Pipeline_collection_data_is_ready')
    end_task = DummyOperator(task_id='Pipeline_completed_successfully')
    
    # Pipeline ETL
    raw_weather = extract_weather_data()
    transformed_weather = transform_weather_data(raw_weather)
    stored_weather = storage_data(transformed_weather)
    
    start_task >> raw_weather >> transformed_weather >> stored_weather >> end_task

    
