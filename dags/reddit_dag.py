from airflow import DAG
from datetime import datetime
import os
import sys

from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.reddit_pipeline import reddit_pipeline
from pipelines.load_to_s3 import load_from_db_to_s3

default_args = {
    'owner' : 'Virtue',
    'start_date' : datetime(2025,3,13)
}

file_postfix = datetime.now().strftime("%Y%m%D")

dag = DAG (
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit','etl','pipeline']
)

#extraction from reddit
extract_and_transform = PythonOperator(
    task_id='extract_reddit_posts_to_db',
    python_callable=reddit_pipeline,
    dag = dag
)

load_to_s3 = PythonOperator(
    task_id='load_from_db_to_s3',
    python_callable=load_from_db_to_s3,
    dag=dag
)

extract_and_transform >> load_to_s3
