import psycopg2
from psycopg2 import OperationalError

def get_pg_connection():
    """
    Établit et retourne une connexion à la base PostgreSQL dans Docker.
    Gère les erreurs de connexion de manière propre.
    """
    try:
        conn = psycopg2.connect(
            host="postgres",  
            user="indrive",
            password="indrive",
            port=5432
        )
        return conn
    except OperationalError as e:
        print(f"Erreur de connexion à la base de données : {e}")
        raise e