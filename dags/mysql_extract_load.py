from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='mysql_etl',
    default_args=default_args,
    start_date=datetime(2024,1,1),
    schedule_interval='@daily',
    catchup=False,
    description='Extract transaction from mysql source db and load to mysql db',
    tags=['mysql', 'etl'],

) as dag:

extract_transaction_mysql_source = PythonOperator(
    task_id='extract_transactions',
    python_callable=extract_transactions,
    provide_context=True
)
load_transaction_mysql_dest = PythonOperator (
    task_id='load_transactions',
    python_callable=load_transactions,
    provide_context=True
)
extract_transaction_mysql_source >> load_transaction_mysql_dest

def extract_transactions(**context):
    hook = MySQlHook(mysql_conn_id='devopssourcedb')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id, amount, customer_id, status, created_at FROM transactions")
    rows = cursor.fetchall()
    context['ti'].xcom_push(key='transaction_data', value=rows)

def load_transaction_mysql_dest(**context):
    data = context['ti'].xcom_pull(task_ids='extract_transactions', key='transaction_data')
    hook = MySqlHook(mysql_conn_id='devopsdestdb')
    conn= hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('TRUNCATE TABLE transactions')
    for row in data:
        cursor.execute("INSERT INTO transactions (customer_id, amount, status) VALUES (%s, %s, %s)", row)
    conn.commit()
