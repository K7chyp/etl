from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from clickhouse_driver import Client
from lib import default_args

def postgres_to_clickhouse():
    # Подключение к PostgreSQL
    pg_conn = psycopg2.connect(
        dbname='airflow',
        user='airflow',
        password='airflow',
        host='postgres'
    )
    
    # Подключение к ClickHouse
    ch_client = Client(host='clickhouse', port=9000)
    
    # Перенос данных и создание витрин
    # Пример витрины: ежедневная активность пользователей
    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS user_activity_daily
        (
            date Date,
            user_id Int32,
            session_count Int32,
            avg_session_duration Float32
        ) ENGINE = MergeTree()
        ORDER BY (date, user_id)
    ''')
    
    # Заполнение витрины
    data = pg_conn.cursor().execute('''
        SELECT DATE(start_time) as date, 
               user_id,
               COUNT(*) as session_count,
               AVG(EXTRACT(EPOCH FROM (end_time - start_time))) 
        FROM user_sessions 
        GROUP BY 1, 2
    ''').fetchall()
    
    ch_client.execute(
        'INSERT INTO user_activity_daily VALUES',
        data
    )

dag = DAG(
    'postgres_to_clickhouse',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

transfer_task = PythonOperator(
    task_id='transfer_postgres_to_clickhouse',
    python_callable=postgres_to_clickhouse,
    dag=dag
)