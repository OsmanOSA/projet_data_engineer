# Utilise l'image officielle Apache Airflow comme base
FROM apache/airflow:2.10.5

# Passe en utilisateur root pour installer des dépendances système
USER root

# Mise à jour et installation de paquets système si besoin (exemple)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Repasser à l'utilisateur airflow pour sécurité
USER airflow

# Copier les fichiers requirements.txt (si tu as des dépendances python à ajouter)
# Place un fichier requirements.txt à côté de ce Dockerfile avec tes packages Python personnalisés
COPY requirements.txt /requirements.txt

# Installer les dépendances Python supplémentaires
RUN pip install --no-cache-dir -r /requirements.txt

# Copier les DAGs, plugins, etc. (optionnel, tu peux aussi le faire via volumes Docker)
# COPY dags/ /opt/airflow/dags/
# COPY plugins/ /opt/airflow/plugins/

# Commande par défaut (on peut la laisser comme dans l'image officielle)
ENTRYPOINT ["/entrypoint"]
CMD ["webserver"]


