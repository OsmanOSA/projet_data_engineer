import os
import base64
import requests
import pandas as pd
import time
import pendulum
import logging

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
from airflow.exceptions import AirflowException

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
    "retry_delay": timedelta(minutes=10),
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
def extract_energy_solar_pv():
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
def transform_consumption(raw_consumptions):
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
            raise ValueError("Données de consommation invalides ou manquantes")
        
        values = raw_consumptions['short_term'][0]['values']
        if not values:
            raise ValueError("Aucune valeur de consommation trouvée")
        
        df_list = [
            {
                "timestamp": entry.get("start_date"),
                "Consommations": entry.get("value")
            }
            for entry in values if entry.get("start_date") and entry.get("value") is not None
        ]

        if not df_list:
            raise ValueError("Aucune donnée valide trouvée après filtrage")

        dataframe_consumption = pd.DataFrame(df_list)
        
        # Conversion de la colonne date en datetime avec fuseau horaire UTC
        dataframe_consumption['timestamp'] = pd.to_datetime(
            dataframe_consumption["timestamp"], 
            format="%Y-%m-%dT%H:%M:%S%z", 
            utc=True
        )
        
        # Formatage de la colonne timestamp
        dataframe_consumption['timestamp'] = dataframe_consumption['timestamp'].dt.strftime("%Y-%m-%d %H:%M")
        
        # Suppression des doublons
        dataframe_consumption = dataframe_consumption.drop_duplicates()
        
        # Définir l'index pour le resampling
        dataframe_consumption.set_index('timestamp', inplace=True)
        dataframe_consumption.index = pd.to_datetime(dataframe_consumption.index)
        
        # Interpolation linéaire des valeurs manquantes
        
        # Remplir les valeurs restantes avec forward fill puis backward fill
        dataframe_consumption = dataframe_consumption.fillna(
            dataframe_consumption.interpolate(method='linear'))
        
        # Resample horaire
        df_hourly = dataframe_consumption.resample('h').mean().reset_index()
        
        logging.info(f"Transformation consommation terminée: {len(df_hourly)} enregistrements")
        return df_hourly
        
    except Exception as e:
        logging.error(f"Erreur transformation consommation: {str(e)}")
        raise AirflowException(f"Échec transformation consommation: {str(e)}")

@task
def transform_solar_energy(raw_solar_energy):
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
        if not raw_solar_energy or 'actual_generations_per_production_type' not in raw_solar_energy:
            raise ValueError("Données de production solaire invalides ou manquantes")
        
        # Vérification de l'existence de l'index 8 (généralement solaire)
        production_data = raw_solar_energy["actual_generations_per_production_type"]
        
        # Recherche des données solaires (index peut varier)
        solar_data = None
        for item in production_data:
            if item.get("production_type") in ["SOLAR", "PHOTOVOLTAIC"]:
                solar_data = item
                break
        
        # Fallback sur l'index 8 si pas trouvé par nom
        if not solar_data and len(production_data) > 8:
            solar_data = production_data[8]
        
        if not solar_data or 'values' not in solar_data:
            raise ValueError("Données solaires non trouvées dans la réponse")

        values = solar_data["values"]
        if not values:
            raise ValueError("Aucune valeur de production solaire trouvée")

        data_lst = [
            {
                "timestamp": entry.get("start_date"),
                "Production_PV": entry.get("value")
            }
            for entry in values if entry.get("start_date") and entry.get("value") is not None
        ]

        if not data_lst:
            raise ValueError("Aucune donnée solaire valide trouvée après filtrage")

        dataframe_solar = pd.DataFrame(data_lst)
        
        # Conversion de la colonne date en datetime avec fuseau horaire UTC
        dataframe_solar['timestamp'] = pd.to_datetime(
            dataframe_solar["timestamp"], 
            format="%Y-%m-%dT%H:%M:%S%z", 
            utc=True
        )
        
        # Formatage de la colonne timestamp
        dataframe_solar['timestamp'] = dataframe_solar['timestamp'].dt.strftime("%Y-%m-%d %H:%M")
        
        # Suppression des doublons
        #dataframe_solar = dataframe_solar.drop_duplicates()
        
        
        
        # Définir l'index pour cohérence (optionnel pour le solaire)
        dataframe_solar.set_index('timestamp', inplace=True)
        dataframe_solar.index = pd.to_datetime(dataframe_solar.index)
        # Remplir les valeurs restantes avec forward fill puis backward fill
        dataframe_solar = dataframe_solar.fillna(
            dataframe_solar.interpolate(method='linear'))
        
        df_solar_hourly = dataframe_solar.resample('h').mean().reset_index()

        logging.info(f"Transformation solaire terminée: {len(dataframe_solar)} enregistrements")
        return df_solar_hourly
        
    except Exception as e:
        logging.error(f"Erreur transformation solaire: {str(e)}")
        raise AirflowException(f"Échec transformation solaire: {str(e)}")

@task(task_id="load_datasets")
def load_datasets(dataframe_consump, dataframe_solar):
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
        if dataframe_consump.empty or dataframe_solar.empty:
            raise ValueError("Un ou plusieurs DataFrames sont vides")
        
        # Jointure sur la colonne timestamp (inner join pour les valeurs communes uniquement)
        df_merged = pd.merge(dataframe_consump, dataframe_solar, on='timestamp')
        
        if df_merged.empty:
            raise ValueError("Aucune correspondance trouvée entre les données de consommation et solaire")

        # Gestion plus robuste des valeurs manquantes
        initial_rows = len(df_merged)
        
        # Remplir les valeurs manquantes par la moyenne de la colonne
        #df_merged["Consommations"] = df_merged["Consommations"].fillna(df_merged["Consommations"].mean())
        #df_merged["Production_PV"] = df_merged["Production_PV"].fillna(df_merged["Production_PV"].mean())
        
        
        logging.info(f"Nettoyage terminé: {initial_rows} -> {len(df_merged)} enregistrements")

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
            
        # Créer une table si elle n'existe pas avec contraintes appropriées
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS energies (
                timestamp TIMESTAMP PRIMARY KEY,
                Consommations FLOAT NOT NULL CHECK (Consommations >= 0),
                Production_PV FLOAT NOT NULL CHECK (Production_PV >= 0),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Migration complète : renommer colonne date vers timestamp et autres améliorations
        cursor.execute("""
            DO $$ 
            BEGIN
                -- Renommer la colonne 'date' en 'timestamp' si elle existe
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'energies' AND column_name = 'date'
                ) AND NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'energies' AND column_name = 'timestamp'
                ) THEN
                    ALTER TABLE energies RENAME COLUMN date TO timestamp;
                    RAISE NOTICE 'Colonne date renommée en timestamp';
                END IF;
                
                -- Ajouter la colonne created_at si elle n'existe pas
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'energies' AND column_name = 'created_at'
                ) THEN
                    ALTER TABLE energies ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                    RAISE NOTICE 'Colonne created_at ajoutée';
                END IF;
                
                -- Supprimer l'ancienne PRIMARY KEY si elle existe sur 'date'
                IF EXISTS (
                    SELECT 1 FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_name = 'energies' 
                    AND tc.constraint_type = 'PRIMARY KEY'
                    AND kcu.column_name = 'date'
                ) THEN
                    ALTER TABLE energies DROP CONSTRAINT IF EXISTS energies_pkey;
                    RAISE NOTICE 'Ancienne PRIMARY KEY supprimée';
                END IF;
                
                -- Vérifier et ajouter PRIMARY KEY sur timestamp si elle n'existe pas
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.table_constraints 
                    WHERE table_name = 'energies' 
                    AND constraint_type = 'PRIMARY KEY'
                ) THEN
                    -- Supprimer les doublons potentiels avant d'ajouter la PRIMARY KEY
                    DELETE FROM energies a USING energies b 
                    WHERE a.ctid < b.ctid AND a.timestamp = b.timestamp;
                    
                    -- Ajouter la PRIMARY KEY
                    ALTER TABLE energies ADD PRIMARY KEY (timestamp);
                    RAISE NOTICE 'Nouvelle PRIMARY KEY ajoutée sur timestamp';
                END IF;
            END $$;
        """)
        
        # Vérification de la structure de la table après migration
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'energies' 
            ORDER BY ordinal_position;
        """)
        columns_info = cursor.fetchall()
        logging.info(f"Structure de la table energies après migration: {columns_info}")
        
        # Préparation des lignes à insérer avec gestion des conflits
        data = [
            (row["timestamp"], float(row["Consommations"]), float(row["Production_PV"]))
            for _, row in df_merged.iterrows()
        ]

        cursor.executemany("""
            INSERT INTO energies (timestamp, Consommations, Production_PV)
            VALUES (%s, %s, %s)
            ON CONFLICT (timestamp) DO UPDATE SET
                Consommations = EXCLUDED.Consommations,
                Production_PV = EXCLUDED.Production_PV,
                created_at = CURRENT_TIMESTAMP
        """, data)

        conn.commit()
        logging.info(f"Chargement terminé: {len(data)} enregistrements insérés/mis à jour")
        
        # Statistiques de contrôle
        cursor.execute("SELECT COUNT(*) FROM energies")
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
    schedule="@daily",
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
        raw_solar = extract_energy_solar_pv()

    with TaskGroup("Transform_datasets", tooltip="Transformation et nettoyage des données") as transform_group:
        transformed_consumption = transform_consumption(raw_consumption)
        transformed_solar = transform_solar_energy(raw_solar)

    load = load_datasets(transformed_consumption, transformed_solar)
        
    start_pipeline >> extract_group >> transform_group >> load >> end_pipeline
