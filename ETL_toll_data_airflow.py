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
   'ETL_toll_data_airflow',
   default_args=default_args,
   description='Apache Airflow Final Assignment',
   schedule_interval=timedelta(days=1),
)

# Define the tasks

# Unzip data
unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -xzf tolldata.tgz',
    dag=dag,
)

# Extract data from csv file
extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d"," -f1-4 vehicle-data.csv > csv_data.csv',
    dag=dag,
)

# Extract data from tsv file
extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'cut -f5-7 tollplaza-data.tsv | tr "\t" "," > tsv_data.csv',
    dag=dag,
)

# Extract data from fixed width file
extract_data_from_fixed_width = BashOperator(
    task_id = 'extarct_data_from_fixed_width',
    bash_command = 'cut -c59-67 payment-data.txt | tr " " "," > fixed_width_data.csv',
    dag=dag,
)

# Consolidate data
consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)

# Transform data
transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = "awk '$5 = toupper($5)' < extracted_data.csv > transformed_data.csv",
    dag=dag,
)

# Define task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width \
    >> consolidate_data >> transform_data

if __name__ == "__main__":
    dag.cli()