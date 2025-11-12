import pendulum #type: ignore

from datetime import timedelta

# Constantes pour une meilleure maintenance
MAX_RETRIES: int = 5
RETRY_DELAY: int= 1
REQUEST_TIMEOUT: int = 2

POSTGRES_CONN_ID: str = "postgres_default"
API_CONN_ID: str = "rte_api"

# Configuration API RTE
TOKEN_URL: str = "https://digital.iservices.rte-france.com/token/oauth/"
DATA: dict = {"grant_type": "client_credentials"}

# ENDPOINTS
BASE_URL_CONSO: str = "https://digital.iservices.rte-france.com/open_api/consumption/v1/short_term?type=REALISED"
BASE_URL_PROD: str = "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_production_type"

DAG_ID: str = "api_etl_dag"
FREQ_COLLECTE: str = "@hourly"

DEFAULT_ARGS: dict = {
    "owner": "airflow",
    "start_date": pendulum.today('UTC').subtract(days=1),
    "retries": MAX_RETRIES,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False
}
