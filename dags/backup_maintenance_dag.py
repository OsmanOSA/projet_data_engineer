import os
import logging
import pendulum
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup

# Configuration logging
logging.basicConfig(level=logging.INFO)

# Paramètres du DAG
POSTGRES_CONN_ID = "postgres_default"
BACKUP_DIR = "/opt/airflow/backups"
RETENTION_DAYS = 30
MAX_BACKUP_SIZE_MB = 500

default_args = {
    "owner": "airflow",
    "start_date": pendulum.today('UTC').subtract(days=1),
    "retries": 5,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False
}

@task
def check_database_health():
    """
    Vérifie l'état de santé de la base de données avant sauvegarde.

    Returns
    -------
    dict
        Statistiques de santé de la base de données

    Raises
    ------
    AirflowException
        Si la base de données n'est pas accessible ou en mauvais état
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Test de connexion
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Vérifications de base
                cursor.execute("SELECT version();")
                db_version = cursor.fetchone()[0]
                
                # Statistiques des tables principales
                cursor.execute("""
                    SELECT 
                        schemaname, 
                        tablename, 
                        n_tup_ins + n_tup_upd + n_tup_del as total_operations,
                        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
                    FROM pg_stat_user_tables 
                    WHERE tablename IN ('energies', 'meteo')
                    ORDER BY total_operations DESC;
                """)
                
                table_stats = cursor.fetchall()
                
                # Vérification de l'espace disque
                cursor.execute("SELECT pg_size_pretty(pg_database_size('airflow'));")
                db_size = cursor.fetchone()[0]
                
                health_report = {
                    "status": "healthy",
                    "database_version": db_version,
                    "database_size": db_size,
                    "table_statistics": table_stats,
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                logging.info(f"Base de données en bon état: {db_size}")
                return health_report
                
    except Exception as e:
        logging.error(f"Erreur lors du check de santé: {str(e)}")
        raise AirflowException(f"Base de données inaccessible: {str(e)}")

@task
def create_backup_directory():
    """
    Crée le répertoire de sauvegarde s'il n'existe pas.

    Returns
    -------
    str
        Chemin du répertoire de sauvegarde créé
    """
    try:
        backup_path = Path(BACKUP_DIR)
        backup_path.mkdir(parents=True, exist_ok=True)
        
        # Vérifier les permissions
        if not os.access(backup_path, os.W_OK):
            raise AirflowException(f"Pas de permission d'écriture sur {BACKUP_DIR}")
        
        logging.info(f"Répertoire de sauvegarde prêt: {backup_path}")
        return str(backup_path)
        
    except Exception as e:
        logging.error(f"Erreur création répertoire backup: {str(e)}")
        raise AirflowException(f"Impossible de créer {BACKUP_DIR}: {str(e)}")

@task
def backup_database(backup_directory: str):
    """
    Effectue la sauvegarde complète de la base de données PostgreSQL.

    Parameters
    ----------
    backup_directory : str
        Répertoire où stocker la sauvegarde

    Returns
    -------
    dict
        Informations sur la sauvegarde créée

    Raises
    ------
    AirflowException
        En cas d'échec de la sauvegarde
    """
    try:
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        backup_filename = f"backup_airflow_{timestamp}.sql"
        backup_filepath = os.path.join(backup_directory, backup_filename)
        
        # Commande pg_dump
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        connection = pg_hook.get_connection(POSTGRES_CONN_ID)
        
        cmd = [
            "pg_dump",
            f"--host={connection.host or 'postgres'}",
            f"--port={connection.port or 5432}",
            f"--username={connection.login}",
            "--verbose",
            "--clean",
            "--create",
            "--if-exists",
            "--format=plain",
            connection.schema or "airflow"
        ]
        
        # Variables d'environnement pour pg_dump
        env = os.environ.copy()
        env['PGPASSWORD'] = connection.password
        
        # Exécution de la sauvegarde
        with open(backup_filepath, 'w') as backup_file:
            result = subprocess.run(
                cmd, 
                stdout=backup_file, 
                stderr=subprocess.PIPE,
                env=env,
                timeout=1800,  # 30 minutes max
                check=True
            )
        
        # Vérification de la sauvegarde
        backup_size_bytes = os.path.getsize(backup_filepath)
        backup_size_mb = backup_size_bytes / (1024 * 1024)
        
        if backup_size_mb > MAX_BACKUP_SIZE_MB:
            logging.warning(f"Sauvegarde volumineuse: {backup_size_mb:.2f} MB")
        
        if backup_size_bytes < 1024:  # Moins de 1KB = problème
            raise AirflowException("Sauvegarde trop petite, probablement échouée")
        
        backup_info = {
            "filename": backup_filename,
            "filepath": backup_filepath,
            "size_mb": round(backup_size_mb, 2),
            "timestamp": timestamp,
            "status": "success"
        }
        
        logging.info(f"Sauvegarde créée: {backup_filename} ({backup_size_mb:.2f} MB)")
        return backup_info
        
    except subprocess.TimeoutExpired:
        raise AirflowException("Timeout de sauvegarde (30 minutes dépassées)")
    except subprocess.CalledProcessError as e:
        stderr_output = e.stderr.decode() if e.stderr else "Erreur inconnue"
        raise AirflowException(f"Échec pg_dump: {stderr_output}")
    except Exception as e:
        logging.error(f"Erreur lors de la sauvegarde: {str(e)}")
        raise AirflowException(f"Sauvegarde échouée: {str(e)}")

@task
def compress_backup(backup_info: dict):
    """
    Compresse la sauvegarde pour économiser l'espace disque.

    Parameters
    ----------
    backup_info : dict
        Informations sur la sauvegarde à compresser

    Returns
    -------
    dict
        Informations sur la sauvegarde compressée

    Raises
    ------
    AirflowException
        En cas d'échec de compression
    """
    try:
        original_filepath = backup_info['filepath']
        compressed_filepath = f"{original_filepath}.gz"
        
        # Compression avec gzip
        cmd = ["gzip", "-9", original_filepath]  # Compression maximale
        
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        # Vérification du fichier compressé
        if not os.path.exists(compressed_filepath):
            raise AirflowException("Fichier compressé non créé")
        
        compressed_size_bytes = os.path.getsize(compressed_filepath)
        compressed_size_mb = compressed_size_bytes / (1024 * 1024)
        
        # Calcul du taux de compression
        original_size_mb = backup_info['size_mb']
        compression_ratio = (1 - compressed_size_mb / original_size_mb) * 100
        
        compressed_info = backup_info.copy()
        compressed_info.update({
            "compressed_filepath": compressed_filepath,
            "compressed_size_mb": round(compressed_size_mb, 2),
            "compression_ratio": round(compression_ratio, 1),
            "status": "compressed"
        })
        
        logging.info(f"Compression terminée: {compression_ratio:.1f}% d'économie")
        return compressed_info
        
    except Exception as e:
        logging.error(f"Erreur lors de la compression: {str(e)}")
        raise AirflowException(f"Compression échouée: {str(e)}")

@task
def verify_backup_integrity(backup_info: dict):
    """
    Vérifie l'intégrité de la sauvegarde compressée.

    Parameters
    ----------
    backup_info : dict
        Informations sur la sauvegarde à vérifier

    Returns
    -------
    dict
        Rapport de vérification d'intégrité

    Raises
    ------
    AirflowException
        Si la vérification d'intégrité échoue
    """
    try:
        compressed_filepath = backup_info['compressed_filepath']
        
        # Test d'intégrité avec gzip
        cmd = ["gzip", "-t", compressed_filepath]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        # Test de lecture des premières lignes (validation SQL)
        cmd_head = ["zcat", compressed_filepath]
        result_head = subprocess.run(
            cmd_head, 
            capture_output=True, 
            text=True, 
            check=True
        )
        
        # Vérifier que c'est bien un dump SQL
        sql_content = result_head.stdout[:1000]  # Premiers 1000 caractères
        
        if "PostgreSQL database dump" not in sql_content:
            raise AirflowException("Le fichier ne semble pas être un dump PostgreSQL valide")
        
        integrity_report = {
            "integrity_status": "valid",
            "compression_test": "passed",
            "sql_validation": "passed",
            "verified_at": datetime.utcnow().isoformat()
        }
        
        logging.info("Intégrité de la sauvegarde vérifiée avec succès")
        return integrity_report
        
    except subprocess.CalledProcessError as e:
        raise AirflowException(f"Test d'intégrité échoué: {e.stderr}")
    except Exception as e:
        logging.error(f"Erreur lors de la vérification: {str(e)}")
        raise AirflowException(f"Vérification d'intégrité échouée: {str(e)}")

@task
def cleanup_old_backups(backup_directory: str):
    """
    Nettoie les anciennes sauvegardes selon la politique de rétention.

    Parameters
    ----------
    backup_directory : str
        Répertoire contenant les sauvegardes

    Returns
    -------
    dict
        Rapport de nettoyage

    Raises
    ------
    AirflowException
        En cas d'erreur lors du nettoyage
    """
    try:
        backup_path = Path(backup_directory)
        cutoff_date = datetime.utcnow() - timedelta(days=RETENTION_DAYS)
        
        deleted_files = []
        total_space_freed = 0
        
        # Recherche des fichiers de sauvegarde anciens
        for backup_file in backup_path.glob("backup_*.sql.gz"):
            file_mtime = datetime.fromtimestamp(backup_file.stat().st_mtime)
            
            if file_mtime < cutoff_date:
                file_size = backup_file.stat().st_size
                backup_file.unlink()
                
                deleted_files.append({
                    "filename": backup_file.name,
                    "size_mb": round(file_size / (1024 * 1024), 2),
                    "age_days": (datetime.utcnow() - file_mtime).days
                })
                total_space_freed += file_size
        
        cleanup_report = {
            "deleted_files_count": len(deleted_files),
            "deleted_files": deleted_files,
            "total_space_freed_mb": round(total_space_freed / (1024 * 1024), 2),
            "retention_policy_days": RETENTION_DAYS,
            "cleanup_date": datetime.utcnow().isoformat()
        }
        
        if deleted_files:
            logging.info(f"Nettoyage: {len(deleted_files)} fichiers supprimés "
                        f"({cleanup_report['total_space_freed_mb']} MB libérés)")
        else:
            logging.info("Aucun fichier ancien à supprimer")
        
        return cleanup_report
        
    except Exception as e:
        logging.error(f"Erreur lors du nettoyage: {str(e)}")
        raise AirflowException(f"Nettoyage échoué: {str(e)}")

@task
def generate_backup_report(health_report: dict, backup_info: dict, 
                          integrity_report: dict, cleanup_report: dict):
    """
    Génère un rapport complet de la session de sauvegarde.

    Parameters
    ----------
    health_report : dict
        Rapport de santé de la base de données
    backup_info : dict
        Informations sur la sauvegarde créée
    integrity_report : dict
        Rapport de vérification d'intégrité
    cleanup_report : dict
        Rapport de nettoyage

    Returns
    -------
    dict
        Rapport consolidé de la session de sauvegarde
    """
    try:
        final_report = {
            "backup_session": {
                "session_id": f"backup_{backup_info['timestamp']}",
                "session_date": datetime.utcnow().isoformat(),
                "status": "completed_successfully"
            },
            "database_health": health_report,
            "backup_details": backup_info,
            "integrity_verification": integrity_report,
            "cleanup_summary": cleanup_report,
            "recommendations": []
        }
        
        # Ajout de recommandations basées sur les données
        if backup_info['size_mb'] > MAX_BACKUP_SIZE_MB:
            final_report["recommendations"].append(
                f"Sauvegarde volumineuse ({backup_info['size_mb']} MB). "
                "Considérer un archivage des données anciennes."
            )
        
        if cleanup_report['deleted_files_count'] == 0:
            final_report["recommendations"].append(
                "Aucun fichier ancien supprimé. Vérifier la politique de rétention."
            )
        
        if not final_report["recommendations"]:
            final_report["recommendations"].append("Aucune action requise. Session parfaite !")
        
        logging.info(f"Rapport de sauvegarde généré: Session {final_report['backup_session']['session_id']}")
        return final_report
        
    except Exception as e:
        logging.error(f"Erreur génération rapport: {str(e)}")
        return {"status": "report_generation_failed", "error": str(e)}

# Définition du DAG principal
with DAG(
    dag_id='backup_maintenance_dag',
    default_args=default_args,
    description='DAG de sauvegarde automatique et maintenance de la base de données PostgreSQL',
    schedule="0 2 * * *",  # Tous les jours à 2h du matin
    catchup=False,
    max_active_runs=1,  # Une seule exécution à la fois
    tags=['backup', 'maintenance', 'database', 'postgresql']
) as dag:
    
    # Tâches de début et fin
    start_backup_session = DummyOperator(
        task_id='start_backup_session',
        doc_md="""
        ## Début de Session de Sauvegarde
        
        Initialise la session de sauvegarde quotidienne de la base PostgreSQL.
        
        **Fréquence**: Quotidienne à 2h du matin
        **Rétention**: 30 jours
        """
    )
    
    end_backup_session = DummyOperator(
        task_id='end_backup_session',
        doc_md="Fin de session de sauvegarde - Toutes les tâches terminées avec succès"
    )
    
    # Groupe de tâches de préparation
    with TaskGroup("preparation", tooltip="Préparation et vérifications") as prep_group:
        health_check = check_database_health()
        backup_dir = create_backup_directory()
    
    # Groupe de tâches de sauvegarde
    with TaskGroup("backup_process", tooltip="Processus de sauvegarde principal") as backup_group:
        backup_task = backup_database(backup_dir)
        compress_task = compress_backup(backup_task)
        verify_task = verify_backup_integrity(compress_task)
    
    # Groupe de tâches de maintenance
    with TaskGroup("maintenance", tooltip="Maintenance et nettoyage") as maintenance_group:
        cleanup_task = cleanup_old_backups(backup_dir)
    
    # Rapport final
    final_report = generate_backup_report(
        health_check, 
        verify_task, 
        verify_task, 
        cleanup_task
    )
    
    # Définition des dépendances
    start_backup_session >> prep_group >> backup_group >> maintenance_group >> final_report >> end_backup_session 