from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    'indrive_marts_pipeline',
    default_args={'owner': 'airflow', 'retries': 1},
    description='Pipeline Marts',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    template_searchpath=['/opt/airflow/sql'],
) as dag:

    t_mart_rides = PostgresOperator(
        task_id='create_mart_rides',
        postgres_conn_id='postgres_default',
        sql='mart/rides.sql',
    )

    t_revenue_by_zone = PostgresOperator(
        task_id='create_revenue_by_zone',
        postgres_conn_id='postgres_default',
        sql='mart/revenue_by_zone.sql',
    )

    t_driver_leaderboard = PostgresOperator(
        task_id='create_driver_leaderboard',
        postgres_conn_id='postgres_default',
        sql='mart/driver_leaderboard.sql',
    )

    t_mart_rides >> [t_revenue_by_zone, t_driver_leaderboard]