# This Python program doing job to do extract, transform
# and load batch data from multiple source (csv, tsv, and fixed width) into single file
# import the libraries
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago

# Defining DAG arguments
default_args={
    'owner': 'Khairil Azmi Ashari',
    'start_date': days_ago(0),
    'email': ['khairilazmiashari@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
   'ETL_toll_data_bash',
   default_args=default_args,
   description='Apache Airflow Final Assignment',
   schedule_interval=timedelta(days=1),
)

# Define the tasks

# extract_transform_load by calling Extract_Transform_data.sh
# to do extract, transform, and load job
extract_transform_load = BashOperator(
    task_id = 'extract_transform_load',
    bash_command = 'Extract_Transform_data.sh',
    dag=dag,
)

# Define task pipeline
extract_transform_load

if __name__ == "__main__":
    dag.cli()