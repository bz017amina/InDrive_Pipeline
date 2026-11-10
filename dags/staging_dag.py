from datetime import datetime
from airflow import DAG
from airflow.models import DagRun
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator


def latest_success(dag_id):
    """Pointe vers le dernier run réussi de l'autre DAG (plannings différents)."""
    def _fn(logical_date, **_):
        runs = DagRun.find(dag_id=dag_id, state="success")
        return max(r.execution_date for r in runs) if runs else logical_date
    return _fn


with DAG(
    'indrive_staging_pipeline',
    default_args={'owner': 'airflow', 'retries': 1},
    description='Pipeline Staging : Nettoyage et transformation',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,                       # évite l'empilement de runs
    template_searchpath=['/opt/airflow/sql'],
) as dag:

    wait_for_streaming = ExternalTaskSensor(
        task_id='wait_for_streaming_ride',
        external_dag_id='indrive_streaming_rides',
        external_task_id=None,
        execution_date_fn=latest_success('indrive_streaming_rides'),
        mode='reschedule',
        timeout=600,
        poke_interval=30,
    )

    wait_for_batch = ExternalTaskSensor(
        task_id='wait_for_batch_ingestion',
        external_dag_id='indrive_batch_ingestion',
        external_task_id=None,
        execution_date_fn=latest_success('indrive_batch_ingestion'),
        mode='reschedule',
        timeout=600,
        poke_interval=30,
    )

    def staging_task(name):
        return PostgresOperator(
            task_id=f'staging_{name}',
            postgres_conn_id='postgres_default',
            sql=f'staging/{name}.sql',       # /opt/airflow/sql/staging/<name>.sql
        )

    staging = [staging_task(n) for n in ('drivers', 'vehicles', 'zones', 'rides')]

    trigger_marts = TriggerDagRunOperator(
        task_id='trigger_marts_dag',
        trigger_dag_id='indrive_marts_pipeline',
    )

    sensors_done = EmptyOperator(task_id='sensors_done')

[wait_for_streaming, wait_for_batch] >> sensors_done
sensors_done >> staging
staging >> trigger_marts