from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import psycopg2

from lib import default_args

def mongo_to_postgres():
    # Подключение к MongoDB
    mongo_client = MongoClient("mongodb://root:example@mongodb:27017/")
    db = mongo_client["mydatabase"]
    
    # Подключение к PostgreSQL
    pg_conn = psycopg2.connect(
        dbname='airflow',
        user='airflow',
        password='airflow',
        host='postgres'
    )
    cursor = pg_conn.cursor()
    
    # Трансформация и загрузка данных
    # Пример для UserSessions
    sessions = db.UserSessions.find()
    for session in sessions:
        cursor.execute("""
            INSERT INTO user_sessions 
            (session_id, user_id, start_time, end_time, device)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (session_id) DO NOTHING
        """, (
            session['session_id'],
            session['user_id'],
            session['start_time'],
            session['end_time'],
            session['device']
        ))
    
    pg_conn.commit()
    cursor.close()
    pg_conn.close()

dag = DAG(
    'mongo_to_postgres',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
)

transfer_task = PythonOperator(
    task_id='transfer_mongo_to_postgres',
    python_callable=mongo_to_postgres,
    dag=dag
)