from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

DBT_PROFILE_DIR = "/home/mahbubah/Desktop/DataWarehouse/traffic_dbt"

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'dbt_run_and_test_dag',
    default_args=default_args,
    description='DAG to run dbt run and dbt test',
    schedule_interval=timedelta(days=1),  # Run daily
)

# Define the dbt run command
dbt_run_command = "dbt run --profiles-dir {DBT_PROJECT_DIR} --target target"
dbt_run_task = BashOperator(
    task_id='dbt_run_task',
    bash_command=dbt_run_command,
    dag=dag,
)

# Define the dbt test command
dbt_test_command = "dbt test --profiles-dir {DBT_PROJECT_DIR}  --target target"
dbt_test_task = BashOperator(
    task_id='dbt_test_task',
    bash_command=dbt_test_command,
    dag=dag,
)

# Set task dependencies
dbt_run_task >> dbt_test_task

# You can define more tasks here if needed, and set their dependencies

if __name__ == "__main__":
    dag.cli()
