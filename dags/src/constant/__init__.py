# Constantes pour une meilleure maintenance
MAX_RETRIES = 5
RETRY_DELAY = 1
REQUEST_TIMEOUT = 2

POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "rte_api"

# Configuration API RTE
TOKEN_URL = "https://digital.iservices.rte-france.com/token/oauth/"
DATA = {"grant_type": "client_credentials"}

# ENDPOINTS
BASE_URL_CONSO = "https://digital.iservices.rte-france.com/open_api/consumption/v1/short_term?type=REALISED"
BASE_URL_PROD = "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_production_type"