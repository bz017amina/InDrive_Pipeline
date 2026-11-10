from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2


def get_pg_connection():
    """Se connecte à la base PostgreSQL dans Docker"""
    return psycopg2.connect(
        host="postgres",
        database="indrive_db",
        user="indrive",
        password="indrive"
    )
def fetch_and_append_rides():
    """
    1. Appelle l'API Mockaroo pour récupérer un lot de nouvelles courses.
    2. Les insère (INSERT) dans la table raw.rides sans effacer l'historique.
    """
    # Remplacez par votre lien d'API Mockaroo pour les rides et votre clé
    api_url = "https://api.mockaroo.com/api/0e2c8140?count=100&key=7a4ce2c0"
    
    # 1. Appel HTTP vers l'API
    response = requests.get(api_url, timeout=15)
    response.raise_for_status()
    rows = response.json()
    
    if not rows:
        return

    # 2. Connexion et insertion ligne par ligne dans Postgres
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            query = """
                INSERT INTO raw.rides (ride_id, driver_id, pickup_zone_id, dropoff_zone_id, distance_km, fare_amount)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            for row in rows:
                cur.execute(query, (
                    row.get('ride_id'),
                    row.get('driver_id'),
                    row.get('pickup_zone_id'),
                    row.get('dropoff_zone_id'),
                    row.get('distance_km'),
                    row.get('fare_amount')
                ))
        conn.commit()
    finally:
        conn.close()

# Paramètres du DAG de Streaming
default_args = {
    'owner':'airflow',
    'retries':1,
}
with DAG (
    'indrive_streaming_rides',
    default_args= default_args,
    description=' Rides Ingestion to raw.rides',
    schedule_interval='*/5 * * * *',
    start_date= datetime(2026, 1, 1),
    catchup=False
) as dag:

    t_stream_rides = PythonOperator(
        task_id='fetch_and_load_rides_stream',
        python_callable=fetch_and_append_rides  # <-- Corrigé (python_callable)
    )