import pyarrow as pa
import pyarrow.parquet as pq

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


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



    #step2:upload textfile to s3

