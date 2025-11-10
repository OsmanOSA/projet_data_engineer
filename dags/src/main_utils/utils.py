import os
import requests
import base64
import time
import logging

from typing import Literal
from airflow.hooks.base import BaseHook #type: ignore
from airflow.exceptions import AirflowException #type: ignore
from airflow.models import Variable #type: ignore

from src.constant import (MAX_RETRIES, RETRY_DELAY, 
                          REQUEST_TIMEOUT, API_CONN_ID,
                          TOKEN_URL, DATA)



def get_rte_access_token(
    type_energy_api: Literal["energy_consumption", 
    "generations_per_production_type"] = "energy_consumption") -> str:

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
        
        conn = BaseHook.get_connection(API_CONN_ID)
        
        if type_energy_api == "energy_consumption":
            client_id = Variable.get("CLIENT_ID", default_var=os.getenv("CLIENT_ID"))
            client_secret = Variable.get("CLIENT_SECRET", default_var=os.getenv("CLIENT_SECRET"))
        elif type_energy_api == "generations_per_production_type":
            client_id = Variable.get("CLIENT_ID_2", default_var=os.getenv("CLIENT_ID_2"))
            client_secret = Variable.get("CLIENT_SECRET_2", default_var=os.getenv("CLIENT_SECRET_2"))
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
        
        response = requests.post(TOKEN_URL, data=DATA, 
                                 headers=headers, timeout=REQUEST_TIMEOUT)
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

def make_api_request(url: str, 
                     headers: dict, 
                     max_retries: int = MAX_RETRIES):
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
            
            elif response.status_code == 429:  

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