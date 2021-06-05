# airflow related
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# other packages
from datetime import datetime
from datetime import timedelta

default_args = {
  'owner': 'ie',
  'depends_on_past': False,
  'start_date': datetime(2021, 6, 5),
  'email_on_failure': False,
  'email_on_retry': False,
  'schedule_interval': '@daily',
  'retries': 1,
  'retry_delay': timedelta(seconds=5),
}

def task_1():
  print("I clean data")

def task_2():
  print("I impute data")
    
def task_3():
  print("I select data")

dag = DAG(
  dag_id='my_dag', 
  description='IE Data Analytics Example',
  default_args=default_args)

task_1 = PythonOperator(
  task_id='task_1', 
  python_callable=task_1, 
  dag=dag)

src2_hdfs = PythonOperator(
  task_id='task_2', 
  python_callable=task_2,
  dag=dag
)

src3_s3 = PythonOperator(
  task_id='task_3', 
  python_callable=task_3, 
  dag=dag)

# setting dependencies
task_1 >> task_2
task_2 >> task_3
