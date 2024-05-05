from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2

# Function to read data from the database
def read_from_database():
    # Connect to the database
    conn = psycopg2.connect(
        dbname="traffic",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    
    # Create a cursor
    cursor = conn.cursor()

    # Execute a query (replace this with your query)
    query = "SELECT * FROM trajectories"
    cursor.execute(query)

    # Fetch the results
    results = cursor.fetchall()

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return results

# Function to display the top 10 values
def display_top_10(results):
    # Use the Unix pipe to display the top 10 values
    top_10_command = "head -n 10"
    top_10_values = results | top_10_command
    print(top_10_values)

# Define default arguments for the DAG
default_args = {
    'owner': 'mahbubah',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'read_and_display_top_10_dag',
    default_args=default_args,
    description='DAG to read data from a database and display the top 10 values',
    schedule_interval=timedelta(days=1),  # Run daily
)

# Define the task to read from the database
read_from_database_task = PythonOperator(
    task_id='read_from_database_task',
    python_callable=read_from_database,
    dag=dag,
)

# Define the task to display the top 10 values
display_top_10_task = PythonOperator(
    task_id='display_top_10_task',
    python_callable=display_top_10,
    provide_context=True,  # Pass results from the previous task
    dag=dag,
)

# Set task dependencies
read_from_database_task >> display_top_10_task

if __name__ == "__main__":
    dag.cli()
