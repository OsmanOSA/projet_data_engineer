import os
import requests
import pandas as pd
import time
import logging

from datetime import datetime
from dotenv import load_dotenv
from airflow import DAG #type: ignore
from airflow.utils.task_group import TaskGroup #type: ignore
from airflow.operators.dummy_operator import DummyOperator #type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook #type: ignore
from airflow.decorators import task #type: ignore
from airflow.exceptions import AirflowException #type: ignore
from airflow.models import Variable #type: ignore


from src.constant import (POSTGRES_CONN_ID, BASE_URL_CONSO, 
                          BASE_URL_PROD, DEFAULT_ARGS, DAG_ID, 
                          FREQ_COLLECTE)

from src.main_utils.utils import get_rte_access_token, make_api_request

# Configuration du logging
os.environ["AIRFLOW__LOGGING__ENABLE_TASK_INSTANCE_LOGGING"] = "True"
logging.basicConfig(level=logging.INFO)

load_dotenv()

@task
def extract_energy_consumption():
    """
    Extrait les données de consommation énergétique depuis l'API RTE.

    Utilise des dates dynamiques et intègre une gestion robuste des erreurs 
    avec retry automatique en cas d'échec temporaire.

    Returns
    -------
    dict
        Données JSON de consommation énergétique contenant les valeurs 
        de consommation réalisée sur différentes périodes

    Raises
    ------
    AirflowException
        En cas d'échec d'extraction après toutes les tentatives
    """
    try:
        # Récupération du token
        token = get_rte_access_token(type_energy_api="energy_consumption")
        logging.info(f"Le token pour l'API de consommation est {token}")

        # Headers API
        api_headers = {
            "Host": "digital.iservices.rte-france.com",
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
        
        data = make_api_request(url=BASE_URL_CONSO, headers=api_headers)
        logging.info("Données de consommation récupérées avec succès")
        return data
        
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction de consommation: {str(e)}")
        raise AirflowException(f"Échec extraction consommation: {str(e)}")

@task
def extract_productions():
    """
    Extrait la production d'énergie solaire via l'API RTE.

    Utilise des identifiants spécifiques pour l'API de génération 
    par type de production.

    Returns
    -------
    dict
        Données JSON de production solaire photovoltaïque avec 
        les valeurs de génération par période temporelle

    Raises
    ------
    AirflowException
        En cas d'échec d'extraction après toutes les tentatives
    """
    try:
        # Récupérer le token pour l'API de génération
        token = get_rte_access_token(type_energy_api="generations_per_production_type")
        logging.info(f"Le token pour l'API de génération est {token}")

        # Headers API
        api_headers = {
            "Host": "digital.iservices.rte-france.com",
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
        
        data = make_api_request(url=BASE_URL_PROD, headers=api_headers)
        logging.info("Données de productions récupérées avec succès")

        return data
        
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction productions: {str(e)}")
        raise AirflowException(f"Échec extraction productions: {str(e)}")


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
            LAT = Variable.get("LAT", default_var=os.getenv("LAT"))  
            LON = Variable.get("LON", default_var=os.getenv("LON"))  

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
            time.sleep(2 ** attempt)  
        
        raise Exception(f"Échec de la récupération des données après 5 tentatives : {response.status_code}")
    
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction des données météo: {str(e)}")
        raise


@task
def transform_consumption(raw_consumptions: dict) -> pd.DataFrame:
    """
    Transforme les données de consommation extraites en DataFrame pandas.

    Convertit les données JSON en format tabulaire avec resampling horaire 
    et gestion des valeurs manquantes.

    Parameters
    ----------
    raw_consumptions : dict
        Données brutes de consommation retournées par l'API RTE

    Returns
    -------
    pd.DataFrame
        DataFrame avec colonnes 'timestamp' (format string YYYY-MM-DD HH:MM) 
        et 'Consommations' (valeurs en MW)

    Raises
    ------
    ValueError
        Si les données d'entrée sont invalides ou vides
    AirflowException
        En cas d'erreur lors de la transformation
    """
    try:
        if not raw_consumptions or 'short_term' not in raw_consumptions:
            logging.error(f"Données de consommation invalides ou manqunates")
            raise ValueError("Données de consommation invalides ou manquantes")
        
        values = raw_consumptions['short_term'][0]['values']
        if not values:
            logging.error(f"Aucune valeur de consommation trouvée")
            raise ValueError("Aucune valeur de consommation trouvée")
        
        df_list = [
            {
                "timestamp": entry.get("start_date"),
                "Consommations": entry.get("value")
            }
            for entry in values if entry.get("start_date") and entry.get("value") is not None
        ]

        df_conso = pd.DataFrame(df_list)
        
        # Conversion de la colonne date en datetime
        df_conso['timestamp'] = pd.to_datetime(
            df_conso["timestamp"], 
            utc=True
        ).dt.strftime("%Y-%m-%d %H:%M")
        
        # Suppression des doublons
        df_conso = df_conso.drop_duplicates().set_index('timestamp')
       
        df_conso = df_conso.fillna(
            df_conso.interpolate(method='linear')).reset_index()
        
        logging.info(f"Transformation consommation terminée: {len(df_conso)} enregistrements")
        return df_conso
        
    except Exception as e:
        logging.error(f"Erreur transformation consommation: {str(e)}")
        raise AirflowException(f"Échec transformation consommation: {str(e)}")

@task
def transform_productions(raw_productions: dict):
    """
    Transforme les données de production solaire extraites en DataFrame pandas.

    Recherche intelligemment les données solaires dans la réponse API 
    et les convertit en format tabulaire standardisé.

    Parameters
    ----------
    raw_solar_energy : dict
        Données brutes de production solaire retournées par l'API RTE

    Returns
    -------
    pd.DataFrame
        DataFrame avec colonnes 'timestamp' (format string YYYY-MM-DD HH:MM) 
        et 'Production_PV' (valeurs en MW)

    Raises
    ------
    ValueError
        Si les données solaires ne sont pas trouvées ou sont invalides
    AirflowException
        En cas d'erreur lors de la transformation
    """
    try:
        if not raw_productions or 'actual_generations_per_production_type' not in raw_productions:
            raise ValueError("Données de production énergétique invalides ou manquantes")
        
        production_data = raw_productions["actual_generations_per_production_type"]
        
        # Dictionnaire pour stocker les données
        productions = {}
        type_source = ['SOLAR', 'BIOMASS', 'WIND_ONSHORE', 'NUCLEAR']
        
        # Récupérer les données pour tous les types disponibles
        for item in production_data:
            prod_type = item.get("production_type")
            if prod_type and 'values' in item:
                productions[prod_type] = item["values"]
        
        if not productions:
            raise ValueError("Aucun type de production trouvé")
        
        # Créer le DataFrame
        all_data = []
        for prod_type, values in productions.items():
            for entry in values:
                if entry.get("start_date") and entry.get("value") is not None:
                    all_data.append({
                        "timestamp": entry.get("start_date"),
                        "production_type": prod_type,
                        "value": entry.get("value")
                    })
        
        if not all_data:
            raise ValueError("Aucune donnée valide trouvée")
        
        df = pd.DataFrame(all_data)


        # Pivoter pour avoir une colonne par type de production
        dataframe_productions = df.pivot_table(
            index='timestamp', 
            columns='production_type', 
            values='value', 
            aggfunc='first'
        )

        dataframe_productions = dataframe_productions[type_source]
        dataframe_productions = dataframe_productions.reset_index()
        
        # Conversion de la colonne date en datetime avec fuseau horaire UTC
        dataframe_productions['timestamp'] = pd.to_datetime(
            dataframe_productions["timestamp"],  utc=True
        ).dt.strftime("%Y-%m-%d %H:%M")
        
        df_prod = pd.DataFrame(dataframe_productions).set_index('timestamp')

        df_prod.fillna(
            df_prod.interpolate(method='linear'), inplace=True)
        
        df_prod.reset_index(inplace=True)

        logging.info(f"Transformation solaire terminée: {len(df_prod)} enregistrements")
        return df_prod
        
    except Exception as e:
        logging.error(f"Erreur transformation solaire: {str(e)}")
        raise AirflowException(f"Échec transformation solaire: {str(e)}")

@task
def transform_weather_data(raw_weather_data: dict):
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
        
        dt = raw_weather_data.get("dt")  # timestamp Unix

         # Conversion et formatage YYYY-MM-DD HH:MM
        dt_formatted = datetime.utcfromtimestamp(dt).strftime("%Y-%m-%d %H:%M") if dt else None

        transformed_data = {
            'timestamp':  dt_formatted,
            'temperature': main_data.get('temp')
        }
        
        transformed_data = pd.DataFrame(transformed_data, index=[0])

        # Convertir la température en Celsius
        transformed_data['temperature'] = float(transformed_data['temperature'])
        transformed_data["timestamp"] =  pd.to_datetime(
            transformed_data["timestamp"], 
            format="%Y-%m-%d %H:%M")
        
        transformed_data.set_index("timestamp", inplace=True)
        transformed_data.fillna(
            transformed_data.interpolate(method='linear'), inplace=True)
        transformed_data.reset_index(inplace=True)
        logging.info(f"Données météo transformées: {transformed_data}")

        return transformed_data
    
    except Exception as e:
        logging.error(f"Erreur lors de la transformation des données météo: {str(e)}")
        raise

@task(task_id="load_datasets")
def load_datasets(dataframe_consump: pd.DataFrame, 
                  dataframe_productions: pd.DataFrame,
                  dataframe_temp: pd.DataFrame):
    """
    Charge les données transformées dans la base de données PostgreSQL.

    Effectue une jointure des données de consommation et production, 
    nettoie les valeurs manquantes et insère en base avec gestion des conflits.

    Parameters
    ----------
    dataframe_consump : pd.DataFrame
        DataFrame contenant les données de consommation transformées avec 
        colonnes 'timestamp' et 'Consommations'
    dataframe_solar : pd.DataFrame
        DataFrame contenant les données de production solaire transformées avec 
        colonnes 'timestamp' et 'Production_PV'

    Raises
    ------
    ValueError
        Si les DataFrames sont vides ou si aucune correspondance n'est trouvée
    AirflowException
        En cas d'erreur lors du chargement en base de données
    """
    try:
        # Validation des DataFrames
        if dataframe_consump.empty or dataframe_productions.empty or dataframe_temp.empty:
            raise ValueError("Un ou plusieurs DataFrames sont vides")


        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
            
        # Créer une table si elle n'existe pas avec contraintes appropriées
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS consommation (
                timestamp TIMESTAMP PRIMARY KEY,
                Consommations FLOAT NOT NULL CHECK (Consommations >= 0),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS meteo (
                timestamp TIMESTAMP PRIMARY KEY,
                temperature FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS productions (
                timestamp TIMESTAMP PRIMARY KEY,
                "SOLAR" FLOAT,
                "BIOMASS" FLOAT,
                "WIND_ONSHORE" FLOAT,
                "NUCLEAR" FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
    
        data_cons = [
            (row["timestamp"], float(row["Consommations"]))
            for _, row in dataframe_consump.iterrows()
        ]

        cursor.executemany("""
            INSERT INTO consommation (timestamp, Consommations)
            VALUES (%s, %s)
            ON CONFLICT (timestamp) DO UPDATE SET
                Consommations = EXCLUDED.Consommations,
                created_at = CURRENT_TIMESTAMP
        """, data_cons)


        data_prod = [
            (row["timestamp"], float(row["SOLAR"]), float(row["BIOMASS"]), 
             float(row["WIND_ONSHORE"]), float(row["NUCLEAR"]))
            for _, row in dataframe_productions.iterrows()
        ]

        cursor.executemany("""
            INSERT INTO productions (timestamp, "SOLAR", "BIOMASS", "WIND_ONSHORE", "NUCLEAR")
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (timestamp) DO UPDATE SET
                "SOLAR" = EXCLUDED."SOLAR",
                "BIOMASS" = EXCLUDED."BIOMASS",
                "WIND_ONSHORE" = EXCLUDED."WIND_ONSHORE",
                "NUCLEAR" = EXCLUDED."NUCLEAR",
                created_at = CURRENT_TIMESTAMP
        """, data_prod)


        data_temp = [
            (row["timestamp"], float(row["temperature"]))
            for _, row in dataframe_temp.iterrows()
        ]

        cursor.executemany("""
            INSERT INTO meteo (timestamp, temperature)
            VALUES (%s, %s)
            ON CONFLICT (timestamp) DO UPDATE SET
                temperature = EXCLUDED.temperature,
                created_at = CURRENT_TIMESTAMP
        """, data_temp)

        conn.commit()
        logging.info(f"Chargement terminé: enregistrements insérés/mis à jour")
        
        # Statistiques de contrôle
        cursor.execute("SELECT COUNT(*) FROM productions")
        total_records = cursor.fetchone()[0]
        logging.info(f"Total des enregistrements en base: {total_records}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Erreur lors du chargement: {str(e)}")
        raise AirflowException(f"Échec du chargement: {str(e)}")

# Définition du DAG avec métadonnées enrichies
with DAG(
    dag_id=DAG_ID, 
    default_args=DEFAULT_ARGS, 
    description="Pipeline ETL pour la collecte de données depuis les APIs",
    schedule=FREQ_COLLECTE,
    catchup=False,
    max_active_runs=1,
    tags=['energy', 'etl', 'rte', 'production']
) as dag:
    
    start_pipeline = DummyOperator(
        task_id='Pipeline_collection_data_is_ready',
        doc_md="""
        ## Début du Pipeline ETL
        
        Ce pipeline collecte chaque heure:
        - Données de consommation énergétique française
        - Données de productions (SOLAR, WIND_ONSHORE, 
                                  NUCLEAR, BIOMASS)
        - Données de température.
        """
    )
    
    end_pipeline = DummyOperator(
        task_id='Pipeline_completed_successfully',
        doc_md="Pipeline ETL terminé avec succès"
    )

    with TaskGroup("Extract_datasets", 
                   tooltip="Extraction des données") as extract_group: 
        
        raw_consumption = extract_energy_consumption()
        raw_productions = extract_productions()
        raw_weather_data = extract_weather_data()

    with TaskGroup("Transform_datasets", 
                   tooltip="Transformation des données") as transform_group:
        
        transformed_consumption = transform_consumption(raw_consumption)
        transformed_productions = transform_productions(raw_productions)
        transformed_temperature = transform_weather_data(raw_weather_data)

    load = load_datasets(transformed_consumption, 
                         transformed_productions, 
                         transformed_temperature)
        
    start_pipeline >> extract_group >> transform_group >> load >> end_pipeline
