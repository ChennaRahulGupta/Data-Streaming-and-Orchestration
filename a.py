from airflow import DAG
import time
from airflow.operators.python import PythonOperator
import pendulum

default_args = {
    'owner':'airflow',
    'start_date': pendulum.datetime(2024,12,13)
    }

#
res = ['Rahul',20,'Male']

def print_data(res):
    print(f'Name of user is {res[0]}')
    print(f'Age of user is {res[1]}')
    print(f'Gender of user is {res[2]}')

with DAG('new_airflow',
         default_args=default_args,
         schedule= '@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
         task_id = 'USER-DATA',
         python_callable=print_data
    )
