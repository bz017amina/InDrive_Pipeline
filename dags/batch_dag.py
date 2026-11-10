from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2


API_URLS = {
    "drivers": "https://api.mockaroo.com/api/101f8700?count=100&key=7a4ce2c0",
    "vehicles": "https://api.mockaroo.com/api/c2232300?count=100&key=7a4ce2c0",
    "zones": "https://api.mockaroo.com/api/d8be0870?count=100&key=7a4ce2c0",
}

def get_pg_connection():
    """Établit la connexion avec la base de données PostgreSQL dans Docker"""
    return psycopg2.connect(
        host="postgres",        # Nom du service défini dans le docker-compose
        database="indrive_db",  # Nom de la base de données
        user="indrive",         # Utilisateur Postgres
        password="indrive"      # Mot de passe Postgres
    )

def fetch_and_load(table_name, api_url, columns):
    """
    Fonction générique d'ingestion Batch :
    - Télécharge le JSON depuis l'API Mockaroo.
    - Effectue un TRUNCATE de la table raw pour garantir l'idempotence.
    - Insère les données ligne par ligne.
    """
    # 1. Requête HTTP vers l'API Mockaroo
    response = requests.get(api_url, timeout=15)
    response.raise_for_status() # Lève une erreur si la requête échoue
    rows = response.json()
    
    if not rows:
        return

    # 2. Connexion et injection dans PostgreSQL
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            # Idempotence : on vide la table raw avant d'insérer
            cur.execute(f"TRUNCATE TABLE raw.{table_name} RESTART IDENTITY CASCADE;")
            
            # Construction dynamique de la requête SQL
            cols_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            query = f"INSERT INTO raw.{table_name} ({cols_str}) VALUES ({placeholders})"
            
            # Injection des données
            for row in rows:
                values = [row.get(col) for col in columns]
                cur.execute(query, values)
                
        conn.commit()
    finally:
        conn.close()

# 2. Paramètres par défaut du DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# 3. Définition du DAG Airflow
with DAG(
    'indrive_batch_ingestion',
    default_args=default_args,
    description='Pipeline Batch pour charger drivers, vehicles et zones depuis Mockaroo vers raw',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # Tâche 1 : Chargement des chauffeurs
    t_drivers = PythonOperator(
        task_id='load_drivers',
        python_callable=fetch_and_load,
        op_kwargs={
            'table_name': 'drivers',
            'api_url': API_URLS['drivers'],
            'columns': ['driver_id', 'driver_name', 'phone_number', 'rating']
        }
    )

    # Tâche 2 : Chargement des véhicules
    t_vehicles = PythonOperator(
        task_id='load_vehicles',
        python_callable=fetch_and_load,
        op_kwargs={
            'table_name': 'vehicles',
            'api_url': API_URLS['vehicles'],
            'columns': ['vehicle_id', 'driver_id', 'vehicle_type', 'model']
        }
    )

    # Tâche 3 : Chargement des zones géographiques
    t_zones = PythonOperator(
        task_id='load_zones',
        python_callable=fetch_and_load,
        op_kwargs={
            'table_name': 'zones',
            'api_url': API_URLS['zones'],
            'columns': ['zone_id', 'zone_name', 'city']
        }
    )

    # Exécution des trois tâches en parallèle dans le DAG
    [t_drivers, t_vehicles, t_zones]