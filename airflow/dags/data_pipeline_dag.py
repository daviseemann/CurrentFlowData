from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from core_functions.create_tables import create_tables
from core_functions.check_etags_and_download import check_etags_and_download
from core_functions.load_to_db import process_and_load_files

default_args = {
    "owner": "davi",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 28),
    "retries": 1,
}

# Define the DAG
dag = DAG(
    "data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",  # Set this to your desired interval
)

# Task 1: Create database tables
create_tables_task = PythonOperator(
    task_id="create_tables",
    python_callable=create_tables,
    dag=dag,
)

# Task 2: Check ETags and download files
check_etags_task = PythonOperator(
    task_id="check_etags_and_download",
    python_callable=check_etags_and_download,
    dag=dag,
)

# Task 3: Process and load files into the database
load_to_db_task = PythonOperator(
    task_id="process_and_load_files",
    python_callable=process_and_load_files,
    dag=dag,
)

# Task dependencies: define order of execution
create_tables_task >> check_etags_task >> load_to_db_task

default_args = {
    "owner": "davi",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 28),
    "retries": 1,
}

# Define the DAG
dag = DAG(
    "data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",  # Set this to your desired interval
)

# Task 1: Create database tables
create_tables_task = PythonOperator(
    task_id="create_tables",
    python_callable=create_tables,
    dag=dag,
)

# Task 2: Check ETags and download files
check_etags_task = PythonOperator(
    task_id="check_etags_and_download",
    python_callable=check_etags_and_download,
    dag=dag,
)

# Task 3: Process and load files into the database
load_to_db_task = PythonOperator(
    task_id="process_and_load_files",
    python_callable=process_and_load_files,
    dag=dag,
)

# Task dependencies: define order of execution
create_tables_task >> check_etags_task >> load_to_db_task
