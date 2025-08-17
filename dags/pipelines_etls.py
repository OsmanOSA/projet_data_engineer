import os
import base64
import requests
import pandas as pd
import time
import pendulum #type: ignore
import logging

from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG #type: ignore
from airflow.utils.task_group import TaskGroup #type: ignore
from airflow.operators.dummy_operator import DummyOperator #type: ignore
from airflow.hooks.base import BaseHook #type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook #type: ignore
from airflow.decorators import task #type: ignore
from airflow.exceptions import AirflowException #type: ignore
from airflow.models import Variable #type: ignore

# Configuration du logging
os.environ["AIRFLOW__LOGGING__ENABLE_TASK_INSTANCE_LOGGING"] = "True"
logging.basicConfig(level=logging.INFO)

load_dotenv()

# Paramètres du DAG
POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "rte_api"

default_args = {
    "owner": "airflow",
    "start_date": pendulum.today('UTC').subtract(days=1),
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False
}

# Constantes pour une meilleure maintenance
MAX_RETRIES = 5
RETRY_DELAY = 2
REQUEST_TIMEOUT = 30

def get_rte_access_token(type_energy_api="energy_consumption"):
    """
    Récupère un token d'authentification depuis l'API RTE.

    Parameters
    ----------
    type_energy_api : str, optional
        Type d'API à utiliser. Doit être 'energy_consumption' ou 
        'generations_per_production_type', by default "energy_consumption"

    Returns
    -------
    str
        Token d'accès Bearer pour l'authentification API

    Raises
    ------
    ValueError
        Si le type d'API n'est pas supporté ou si les identifiants sont manquants
    AirflowException
        En cas d'erreur réseau ou d'échec de récupération du token
    """
    try:
        # Configuration API RTE
        TOKEN_URL = "https://digital.iservices.rte-france.com/token/oauth/"
        data = {"grant_type": "client_credentials"}
        
        conn = BaseHook.get_connection(API_CONN_ID)
        
        if type_energy_api == "energy_consumption":
            client_id = conn.extra_dejson.get("CLIENT_ID")
            client_secret = conn.extra_dejson.get("CLIENT_SECRET")
        elif type_energy_api == "generations_per_production_type":
            client_id = conn.extra_dejson.get("CLIENT_ID_2")
            client_secret = conn.extra_dejson.get("CLIENT_SECRET_2")
        else:
            raise ValueError(f"Type d'API non supporté: {type_energy_api}")

        if not client_id or not client_secret:
            raise ValueError(f"Identifiants manquants pour {type_energy_api}")

        auth_str = f"{client_id}:{client_secret}"
        auth_b64 = base64.b64encode(auth_str.encode()).decode()
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {auth_b64}",
        }
        
        response = requests.post(TOKEN_URL, data=data, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        token = response.json().get("access_token")
        if not token:
            raise AirflowException("Token d'accès non trouvé dans la réponse")
            
        logging.info(f"Token RTE récupéré avec succès pour {type_energy_api}")
        return token
        
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Erreur réseau lors de la récupération du token RTE: {str(e)}")
    except Exception as e:
        raise AirflowException(f"Erreur token API RTE: {str(e)}")

def make_api_request(url, headers, max_retries=MAX_RETRIES):
    """
    Effectue une requête API avec retry automatique et backoff exponentiel.

    Parameters
    ----------
    url : str
        URL de l'API à interroger
    headers : dict
        Headers HTTP à inclure dans la requête
    max_retries : int, optional
        Nombre maximum de tentatives, by default MAX_RETRIES

    Returns
    -------
    dict
        Réponse JSON de l'API

    Raises
    ------
    AirflowException
        En cas d'échec après toutes les tentatives ou d'erreur critique
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 200:
                logging.info(f"Requête API réussie: {url}")
                return response.json()
            elif response.status_code == 429:  # Rate limiting
                wait_time = RETRY_DELAY ** (attempt + 1)
                logging.warning(f"Rate limit atteint, attente de {wait_time}s")
                time.sleep(wait_time)
            else:
                logging.warning(f"Tentative {attempt + 1}: Code {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            logging.warning(f"Tentative {attempt + 1}: Erreur réseau - {str(e)}")
            
        if attempt < max_retries - 1:
            time.sleep(RETRY_DELAY ** attempt)
    
    raise AirflowException(f"Échec de la requête API après {max_retries} tentatives: {url}")

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
        token = get_rte_access_token()
        
        # Headers API
        api_headers = {
            "Host": "digital.iservices.rte-france.com",
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
        
        endpoint = "https://digital.iservices.rte-france.com/open_api/consumption/v1/short_term?type=REALISED"
        
        data = make_api_request(endpoint, api_headers)
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

        api_headers = {
            "Host": "digital.iservices.rte-france.com",
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }

        endpoint = "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_production_type"
        
        data = make_api_request(endpoint, api_headers)
        logging.info("Données de production solaire récupérées avec succès")
        return data
        
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction solaire: {str(e)}")
        raise AirflowException(f"Échec extraction solaire: {str(e)}")


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
            logging.error(f"EDonnées de consommation invalides ou manqunates")
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

        dataframe_consumption = pd.DataFrame(df_list)
        
        # Conversion de la colonne date en datetime
        dataframe_consumption['timestamp'] = pd.to_datetime(
            dataframe_consumption["timestamp"], 
            utc=True
        ).dt.strftime("%Y-%m-%d %H:%M")
        
        # Suppression des doublons
        dataframe_consumption = dataframe_consumption.drop_duplicates().set_index('timestamp', inplace=True)
       
        dataframe_consumption = dataframe_consumption.fillna(
            dataframe_consumption.interpolate(method='linear')).reset_index()
        
        logging.info(f"Transformation consommation terminée: {len(dataframe_consumption)} enregistrements")
        return dataframe_consumption
        
    except Exception as e:
        logging.error(f"Erreur transformation consommation: {str(e)}")
        raise AirflowException(f"Échec transformation consommation: {str(e)}")

@task
def transform_productions(raw_productions):
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
            dataframe_productions["timestamp"], 
            format="%Y-%m-%dT%H:%M:%S%z", 
            utc=True
        ).dt.strftime("%Y-%m-%d %H:%M")
        
        # Définir l'index pour cohérence (optionnel pour le solaire)
        dataframe_productions.set_index('timestamp', inplace=True)
        dataframe_productions.fillna(
            dataframe_productions.interpolate(method='linear'), inplace=True)
        
        dataframe_productions.reset_index(inplace=True)

        logging.info(f"Transformation solaire terminée: {len(dataframe_productions)} enregistrements")
        return dataframe_productions
        
    except Exception as e:
        logging.error(f"Erreur transformation solaire: {str(e)}")
        raise AirflowException(f"Échec transformation solaire: {str(e)}")

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
def load_datasets(dataframe_consump, dataframe_productions, dataframe_temp):
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
    dag_id='api_etl_dag', 
    default_args=default_args, 
    description='Pipeline ETL pour la collecte de données énergétiques depuis l\'API RTE',
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=['energy', 'etl', 'rte', 'production']
) as dag:
    
    start_pipeline = DummyOperator(
        task_id='Pipeline_collection_data_is_ready',
        doc_md="""
        ## Début du Pipeline ETL Énergétique
        
        Ce pipeline collecte quotidiennement:
        - Données de consommation énergétique française
        - Données de production solaire photovoltaïque
        
        Source: API RTE (Réseau de Transport d'Électricité)
        """
    )
    
    end_pipeline = DummyOperator(
        task_id='Pipeline_completed_successfully',
        doc_md="Pipeline ETL terminé avec succès"
    )

    with TaskGroup("Data_ingestion", tooltip="Extraction des données depuis les APIs") as extract_group: 
        raw_consumption = extract_energy_consumption()
        raw_productions = extract_productions()
        raw_weather_data = extract_weather_data()

    with TaskGroup("Transform_datasets", tooltip="Transformation et nettoyage des données") as transform_group:
        transformed_consumption = transform_consumption(raw_consumption)
        transformed_productions = transform_productions(raw_productions)
        transformed_temperature = transform_weather_data(raw_weather_data)

    load = load_datasets(transformed_consumption, transformed_productions, transformed_temperature)
        
    start_pipeline >> extract_group >> transform_group >> load >> end_pipeline
