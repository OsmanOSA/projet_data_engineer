# 📊 TABLEAU DE PARAMÉTRAGE DU SYSTÈME DE SAUVEGARDE

## 🎯 **Paramètres Principaux de Configuration**

| **Paramètre** | **Valeur / Règle** | **Fichier** | **Objectif** |
|---------------|-------------------|-------------|--------------|
| **Fréquence d'exécution** | `@daily` à 2h00 du matin | `backup_maintenance_dag.py` | Sauvegarde quotidienne automatique |
| **Nom du DAG** | `backup_maintenance_dag` | `backup_maintenance_dag.py` | Identification unique dans Airflow |
| **Propriétaire** | `airflow` | `backup_maintenance_dag.py` | Responsable du DAG |
| **Catchup** | `False` | `backup_maintenance_dag.py` | Pas de rattrapage historique |
| **Exécutions simultanées** | `max_active_runs=1` | `backup_maintenance_dag.py` | Une seule sauvegarde à la fois |

## 🔒 **Paramètres de Sécurité et Fiabilité**

| **Paramètre** | **Valeur / Règle** | **Fichier** | **Objectif** |
|---------------|-------------------|-------------|--------------|
| **Tentatives de retry** | `5 tentatives` ⭐ | `backup_maintenance_dag.py` | Résistance renforcée aux pannes |
| **Délai entre retry** | `10 minutes` | `backup_maintenance_dag.py` | Attente avant nouvelle tentative |
| **Timeout maximum** | `30 minutes` | `backup_maintenance_dag.py` | Protection contre les blocages |
| **Email en cas d'échec** | `False` | `backup_maintenance_dag.py` | Notifications par email |
| **Email sur retry** | `False` | `backup_maintenance_dag.py` | Notifications retry |
| **Permissions répertoires** | `755` (AIRFLOW_UID:0) | `setup_backup_system.sh` | Sécurité des accès fichiers |

## 🔐 **Paramètres de Contrôle d'Accès Base de Données (NOUVEAU ⭐)**

| **Paramètre** | **Valeur / Règle** | **Fichier** | **Objectif** |
|---------------|-------------------|-------------|--------------|
| **Rôles de sécurité** | `role_lecteur_energie`, `role_analyste_energie`, `role_admin_energie` | `setup_database_security.sql` | Hiérarchie des permissions |
| **Utilisateur ETL** | `user_etl_airflow` (admin) | `setup_database_security.sql` | Connexion DAGs Airflow |
| **Utilisateur Analyste** | `user_data_analyst` (analyste) | `setup_database_security.sql` | Analyse de données |
| **Utilisateur Readonly** | `user_readonly` (lecteur) | `setup_database_security.sql` | Dashboards/monitoring |
| **Utilisateur Backup** | `user_backup` (backup) | `setup_database_security.sql` | Sauvegardes spécialisées |
| **Row Level Security** | Activé sur `energies` et `meteo` | `setup_database_security.sql` | Filtrage automatique par rôle |
| **Audit automatique** | Triggers sur toutes modifications | `setup_database_security.sql` | Traçabilité complète |
| **Connexion ETL** | `postgres_etl` (user_etl_airflow) | `setup_database_access_control.sh` | DAGs avec permissions admin |
| **Connexion Backup** | `postgres_backup` (user_backup) | `setup_database_access_control.sh` | Sauvegardes avec accès lecture |
| **Politique accès lecteur** | Données 30 jours uniquement | `setup_database_security.sql` | Limitation accès données récentes |
| **Politique accès analyste** | Données 6 mois + vues temporaires | `setup_database_security.sql` | Accès étendu pour analyses |
| **Mots de passe sécurisés** | Minimum 12 caractères | `setup_database_security.sql` | Complexité forcée |
| **Vues sécurisées** | `daily_energy_summary`, `energy_weather_correlation` | `setup_database_security.sql` | Accès contrôlé aux analyses |
| **Surveillance connexions** | Vue `active_connections` | `setup_database_security.sql` | Monitoring temps réel |
| **Audit sécurité** | Vue `security_audit_summary` | `setup_database_security.sql` | Rapport activités 7 jours |

## 📁 **Paramètres de Stockage et Fichiers**

| **Paramètre** | **Valeur / Règle** | **Fichier** | **Objectif** |
|---------------|-------------------|-------------|--------------|
| **Répertoire principal** | `/opt/airflow/backups` | `backup_maintenance_dag.py` | Centralisation des sauvegardes |
| **Volume Docker** | `${AIRFLOW_PROJ_DIR}/backups` | `docker-compose.yaml` | Persistance hors container |
| **Format de nommage** | `backup_airflow_YYYYMMDD_HHMMSS.sql.gz` | `backup_maintenance_dag.py` | Identification chronologique |
| **Rétention des données** | `30 jours` | `backup_maintenance_dag.py` | Gestion automatique espace disque |
| **Taille limite alerte** | `500 MB` | `backup_maintenance_dag.py` | Détection d'anomalies |
| **Niveau de compression** | `gzip -9` (maximum) | `backup_maintenance_dag.py` | Optimisation espace stockage |

## 🗄️ **Paramètres Base de Données PostgreSQL**

| **Paramètre** | **Valeur / Règle** | **Fichier** | **Objectif** |
|---------------|-------------------|-------------|--------------|
| **ID Connexion Airflow** | `postgres_default` | `backup_maintenance_dag.py` | Réutilisation config existante |
| **Commande de sauvegarde** | `pg_dump` | `backup_maintenance_dag.py` | Outil standard PostgreSQL |
| **Options pg_dump** | `--clean --create --if-exists --verbose` | `backup_maintenance_dag.py` | Sauvegarde complète restaurable |
| **Format de sortie** | `--format=plain` (SQL) | `backup_maintenance_dag.py` | Compatibilité et lisibilité |
| **Host par défaut** | `postgres` | `backup_maintenance_dag.py` | Nom service Docker |
| **Port par défaut** | `5432` | `backup_maintenance_dag.py` | Port standard PostgreSQL |
| **Authentification** | Variable `PGPASSWORD` | `backup_maintenance_dag.py` | Sécurité connexion |

## 📊 **Paramètres de Monitoring et Logs**

| **Paramètre** | **Valeur / Règle** | **Fichier** | **Objectif** |
|---------------|-------------------|-------------|--------------|
| **Niveau de log** | `logging.INFO` | `backup_maintenance_dag.py` | Traçabilité détaillée |
| **Interface monitoring** | Airflow Web UI (port 8080) | N/A | Monitoring visuel temps réel |
| **Métriques JSON** | `backup_metrics.json` | `backup_metrics.py` | Intégration Prometheus/Grafana |
| **Rapports de session** | JSON complet par sauvegarde | `backup_maintenance_dag.py` | Historique et analytics |
| **Health checks** | Vérification DB avant sauvegarde | `backup_maintenance_dag.py` | Prévention sauvegardes corrompues |
| **Tags du DAG** | `backup, maintenance, database, postgresql` | `backup_maintenance_dag.py` | Classification et filtrage |

## ⚙️ **Paramètres des Scripts Utilitaires**

| **Paramètre** | **Valeur / Règle** | **Fichier** | **Objectif** |
|---------------|-------------------|-------------|--------------|
| **Script de vérification** | `verify_backup.sh` | `setup_backup_system.sh` | Contrôle manuel sauvegardes |
| **Script de restauration** | `restore_backup.sh` | `setup_backup_system.sh` | Restauration d'urgence guidée |
| **Configuration centralisée** | `backup_config.env` | `setup_backup_system.sh` | Paramètres modifiables |
| **Générateur métriques** | `backup_metrics.py` | `setup_backup_system.sh` | Monitoring avancé |
| **Répertoire scripts** | `/opt/airflow/scripts` | `setup_backup_system.sh` | Centralisation utilitaires |

## 🔍 **Paramètres de Vérification et Validation**

| **Paramètre** | **Valeur / Règle** | **Fichier** | **Objectif** |
|---------------|-------------------|-------------|--------------|
| **Test d'intégrité** | `gzip -t` + validation SQL | `backup_maintenance_dag.py` | Garantie qualité fichiers |
| **Validation contenu** | Recherche "PostgreSQL database dump" | `backup_maintenance_dag.py` | Vérification format SQL |
| **Taille minimum** | `> 1KB` | `backup_maintenance_dag.py` | Détection sauvegarde vide |
| **Confirmation restauration** | Prompt `yes/no` obligatoire | `restore_backup.sh` | Protection suppression accidentelle |
| **Vérification post-restauration** | `\dt` pour lister tables | `restore_backup.sh` | Contrôle restauration réussie |

## 🚨 **Paramètres d'Alertes et Notifications (Optionnels)**

| **Paramètre** | **Valeur / Règle** | **Fichier** | **Objectif** |
|---------------|-------------------|-------------|--------------|
| **Alertes email** | `ENABLE_EMAIL_ALERTS=false` | `backup_config.env` | Notifications par email |
| **Destinataires email** | `admin@example.com` | `backup_config.env` | Liste contacts alertes |
| **Webhook Slack** | `SLACK_WEBHOOK_URL=""` | `backup_config.env` | Alertes équipe temps réel |
| **Sauvegarde cloud** | `ENABLE_CLOUD_BACKUP=false` | `backup_config.env` | Backup externe AWS S3 |
| **Région AWS** | `eu-west-1` | `backup_config.env` | Localisation services cloud |

## 📈 **Paramètres de Performance**

| **Paramètre** | **Valeur / Règle** | **Fichier** | **Objectif** |
|---------------|-------------------|-------------|--------------|
| **Task Groups** | Préparation → Sauvegarde → Maintenance | `backup_maintenance_dag.py` | Organisation workflow |
| **Séquencement** | Dépendances automatiques | `backup_maintenance_dag.py` | Ordre exécution garanti |
| **Gestion mémoire** | Streaming pour gros fichiers | `backup_maintenance_dag.py` | Efficacité mémoire |
| **Parallélisation** | Tasks séquentiels | `backup_maintenance_dag.py` | Éviter conflits ressources |
| **Calcul ratio compression** | Automatique | `backup_maintenance_dag.py` | Monitoring efficacité |

## 🔧 **Paramètres Techniques Avancés**

| **Paramètre** | **Valeur / Règle** | **Fichier** | **Objectif** |
|---------------|-------------------|-------------|--------------|
| **Version Airflow** | `2.10.5` compatible | `docker-compose.yaml` | Compatibilité DAG |
| **Hook PostgreSQL** | `PostgresHook` Airflow | `backup_maintenance_dag.py` | Intégration native |
| **Exception handling** | `AirflowException` avec contexte | `backup_maintenance_dag.py` | Gestion erreurs robuste |
| **Format docstring** | numpydoc standard | `backup_maintenance_dag.py` | Documentation professionnelle |
| **Variables environnement** | Automatiques via Hook | `backup_maintenance_dag.py` | Configuration dynamique |

## 📋 **Paramètres de Documentation**

| **Paramètre** | **Valeur / Règle** | **Fichier** | **Objectif** |
|---------------|-------------------|-------------|--------------|
| **Description DAG** | "DAG de sauvegarde automatique..." | `backup_maintenance_dag.py` | Documentation interface |
| **Documentation Markdown** | Intégrée aux tasks | `backup_maintenance_dag.py` | Aide contextuelle |
| **Commentaires code** | Français, détaillés | `backup_maintenance_dag.py` | Maintenabilité |
| **README système** | `README.md` complet | `README.md` | Guide utilisateur |
| **Ce fichier paramètrage** | `PARAMETRAGE_SAUVEGARDE.md` | Ce fichier | Référence technique |

## 🎯 **Résumé des Paramètres Critiques**

| **Catégorie** | **Paramètres Essentiels** | **Statut** |
|---------------|---------------------------|------------|
| **⏰ Temporisation** | Quotidien 2h00, retry 5×10min | ✅ Configuré |
| **💾 Stockage** | 30 jours, gzip -9, 500MB limit | ✅ Configuré |
| **🔒 Sécurité** | Permissions, validation, timeout | ✅ Configuré |
| **🔐 Contrôle d'accès** | Rôles, RLS, audit, utilisateurs ⭐ | ✅ Configuré |
| **📊 Monitoring** | Logs, métriques, rapports | ✅ Configuré |
| **🔄 Automatisation** | DAG Airflow, nettoyage auto | ✅ Configuré |
| **🚨 Alertes** | Email, Slack, cloud (opt.) | ⚙️ Configurable |

---

## 📝 **Notes de Configuration**

1. **Modification des paramètres** : Éditer les fichiers correspondants puis redémarrer Airflow
2. **Test après modification** : Utiliser `./scripts/verify_backup.sh`
3. **Monitoring actif** : Interface Airflow → DAG → Graph View
4. **Logs détaillés** : Interface Airflow → DAG → Logs
5. **Métriques avancées** : Exécuter `python3 scripts/backup_metrics.py`
6. **⭐ Configuration contrôle d'accès** : Exécuter `./scripts/setup_database_access_control.sh`
7. **⭐ Test des permissions** : Utiliser `./scripts/setup_database_access_control.sh -t`
8. **⭐ Surveillance sécurité** : Consulter les vues `active_connections` et `security_audit_summary`

**✅ Système configuré pour la production avec contrôle d'accès sécurisé et 5 tentatives de retry !** 🚀🔐 