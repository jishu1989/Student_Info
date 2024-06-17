import pyarrow as pa
import pyarrow.parquet as pq
import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


#owner:Indicates the owner of the DAG.
#email:where notification will be send.
#retries:number of retries when task failed.
#retry_delay:declaration time between retries.
default_args = {
'owner':'soumya',
'email':'soumyadas.edu@gmail.com',
'retries':'5',
'retry_delay': timedelta(minutes=10)
}

def mysql_to_s3():
    #step1:query data from mysqldb
    hook=MySqlHook(mysql_conn_id="mysql_localhost")
    df=hook.get_pandas_df("select student_code, honors_subject, percentage_of_mark from STUDENT;")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, 'STUDENTS.parquet')
    #step2:upload textfile to s3
    dir=os.getcwd()
    s3_hook=S3Hook(aws_conn_id="<AwsBaseHook.default_conn_name>")
    s3_hook.load_file(
        filename= os.path.join(dir,'STUDENTS.parquet'),
        key= <s3 key that point to the file>,
        bucket_name= assignment-eu-central-1,
        replace= True
    )

with DAG(
    dag_id="dag_with_mysql_hooks_v0xx",
    default_args=default_args,
    start_date=datetime(2024,6,17),
    schedule_interval='@daily'
) as dag:
     task1= PythonOperator(
        task_id="mysql_to_s3",
        python_callable=postgres_to_s3
     )
     task1

