"""
Fonctions utilitaires pour le projet ETL Data Engineering.
Réutilisables dans différents DAGs Airflow.
"""

import requests
import logging
import time
import os
import base64
from datetime import datetime


def fetch_data_with_retry(api_url, headers, max_retries=5, delay=2):
    """
    Effectue une requête API avec retry automatique.
    
    Parameters
    ----------
    api_url : str
        URL de l'API à interroger
    headers : dict
        Headers HTTP pour la requête
    max_retries : int, optional
        Nombre maximum de tentatives, by default 5
    delay : int, optional
        Délai entre tentatives en secondes, by default 2
        
    Returns
    -------
    dict
        Réponse JSON de l'API
        
    Raises
    ------
    Exception
        Si toutes les tentatives échouent
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(api_url, headers=headers, timeout=30)
            if response.status_code == 200:
                logging.info(f"API call successful: {api_url}")
                return response.json()
            elif response.status_code == 429:  # Rate limiting
                wait_time = delay * (2 ** attempt)  # Backoff exponentiel
                logging.warning(f"Rate limit hit, waiting {wait_time}s")
                time.sleep(wait_time)
            else:
                logging.warning(f"Attempt {attempt+1}: Failed with {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt+1}: Network error - {str(e)}")
            
        if attempt < max_retries - 1:
            time.sleep(delay)
    
    raise Exception(f"API call failed after {max_retries} retries: {api_url}")


def encode_credentials(client_id, client_secret):
    """
    Encode les identifiants en base64 pour l'authentification Basic.
    
    Parameters
    ----------
    client_id : str
        Identifiant client
    client_secret : str
        Secret client
        
    Returns
    -------
    str
        Chaîne encodée en base64
    """
    auth_str = f"{client_id}:{client_secret}"
    return base64.b64encode(auth_str.encode()).decode()


def validate_environment_variables(*required_vars):
    """
    Valide que toutes les variables d'environnement requises sont présentes.
    
    Parameters
    ----------
    *required_vars : str
        Noms des variables d'environnement à vérifier
        
    Raises
    ------
    ValueError
        Si une variable d'environnement est manquante
    """
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"Variables d'environnement manquantes: {', '.join(missing_vars)}")


def log_dataframe_info(df, name="DataFrame"):
    """
    Log des informations sur un DataFrame pandas.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame à analyser
    name : str, optional
        Nom du DataFrame pour les logs, by default "DataFrame"
    """
    logging.info(f"{name} - Shape: {df.shape}")
    logging.info(f"{name} - Columns: {list(df.columns)}")
    logging.info(f"{name} - Memory usage: {df.memory_usage(deep=True).sum() / 1024:.1f} KB")
    
    # Vérifier les valeurs manquantes
    null_counts = df.isnull().sum()
    if null_counts.sum() > 0:
        logging.warning(f"{name} - Null values found: {null_counts[null_counts > 0].to_dict()}")


def format_timestamp(timestamp_str, input_format="%Y-%m-%dT%H:%M:%S%z", output_format="%Y-%m-%d %H:%M"):
    """
    Formate un timestamp selon le format souhaité.
    
    Parameters
    ----------
    timestamp_str : str
        Timestamp au format string
    input_format : str, optional
        Format d'entrée, by default "%Y-%m-%dT%H:%M:%S%z"
    output_format : str, optional
        Format de sortie, by default "%Y-%m-%d %H:%M"
        
    Returns
    -------
    str
        Timestamp formaté
    """
    try:
        dt = datetime.strptime(timestamp_str, input_format)
        return dt.strftime(output_format)
    except ValueError as e:
        logging.error(f"Error formatting timestamp {timestamp_str}: {e}")
        return timestamp_str


def calculate_compression_ratio(original_size, compressed_size):
    """
    Calcule le ratio de compression.
    
    Parameters
    ----------
    original_size : float
        Taille originale en bytes
    compressed_size : float
        Taille compressée en bytes
        
    Returns
    -------
    float
        Ratio de compression en pourcentage
    """
    if original_size == 0:
        return 0
    return (1 - compressed_size / original_size) * 100


# Constantes réutilisables
DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_DELAY = 2
DEFAULT_REQUEST_TIMEOUT = 30

# Format de timestamp par défaut pour les APIs
DEFAULT_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
OUTPUT_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M" 