# Projet Titre RNCP - Data Engineer

## Description du Projet

Ce projet implémente un pipeline ETL (Extract, Transform, Load) automatisé pour la collecte et l'analyse de données énergétiques françaises. Il utilise **Apache Airflow** pour orchestrer la collecte quotidienne de données depuis les APIs officielles et stocke les informations dans une base de données PostgreSQL.

### Objectifs
- Collecter les données de **consommation énergétique française** (API RTE)
- Récupérer les données de **productions** (API RTE)  
- Intégrer les **données météorologiques** (OpenWeatherMap)
- Automatiser le traitement et le stockage des données
- Fournir une base de données consolidée pour l'analyse et la prédiction

### Stack Technique

- **Orchestrateur** : Apache Airflow 2.10.5
- **Base de données** : PostgreSQL 13
- **Containerisation** : Docker & Docker Compose
- **Langage** : Python 3.12
- **Librairies principales** : pandas, requests, python-dotenv


### Comptes API requis
1. **Compte RTE** : [digital.iservices.rte-france.com](https://digital.iservices.rte-france.com)
   - Application "Consommation" 
   - Application "Génération par filière"
2. **OpenWeatherMap** : [openweathermap.org](https://openweathermap.org/api)

## Configuration et Paramétrage

### 1. Variables d'Environnement

Créez un fichier `.env` à la racine du projet :

```bash
# Configuration Apache Airflow
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# API OpenWeatherMap
METEO_API_KEY=votre_cle_openweathermap
LAT=48.8566          
LON=2.3522           

# Configuration PostgreSQL (optionnel, utilise les valeurs par défaut)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
```


## APIs Utilisées

### 1. API RTE (Réseau de Transport d'Électricité)

#### Consommation Énergétique
- **Endpoint** : `/open_api/consumption/v1/short_term`
- **Données** : Consommation électrique réalisée en France
- **Granularité** : 15 minutes
- **Format** : JSON

#### Production
- **Endpoint** : `/open_api/actual_generation/v1/actual_generations_per_production_type`
- **Données** : Production par filière (focus sur 4 sources "SOLAR", "BIOMASS", "WIND_ONSHORE", "NUCLEAR")
- **Granularité** : 1 heure 
- **Format** : JSON

### 2. OpenWeatherMap API
- **Endpoint** : `/data/2.5/weather`
- **Données** : Météo actuelle (température, humidité, pression, vent)
- **Fréquence** : 1 heure
- **Format** : JSON

## Base de Données

### Table `consommation`
```sql
CREATE TABLE consommation (
    timestamp TIMESTAMP PRIMARY KEY,
    Consommations FLOAT NOT NULL CHECK (Consommations >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Table `meteo`
```sql
CREATE TABLE meteo (
    timestamp TIMESTAMP PRIMARY KEY,
    temperature FLOAT
);
```

## Monitoring et Observabilité

### Interface Airflow
- **Web UI** : `http://localhost:8080`
- **Monitoring** : Statut des DAGs, logs, métriques
- **Gestion** : Retry, pause/unpause, visualisation des graphiques

### Logs
```bash
# Logs généraux
docker-compose logs

# Logs spécifiques
docker-compose logs airflow-scheduler
docker-compose logs airflow-worker
```

### Base de Données
```bash
# Connexion PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow

# Vérification des données
SELECT COUNT(*) FROM consommation;
SELECT * FROM meteo ORDER BY timestamp DESC LIMIT 10;
```




## Ressources

- [Documentation Apache Airflow](https://airflow.apache.org/docs/)
- [API RTE Documentation](https://digital.iservices.rte-france.com/apis)
- [OpenWeatherMap API](https://openweathermap.org/api)
- [Docker Compose Guide](https://docs.docker.com/compose/)
