from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
from faker import Faker
from lib import default_args

def generate_mongo_data():
    fake = Faker()
    client = MongoClient("mongodb://root:example@mongodb:27017/")
    db = client["mydatabase"]
    
    # Генерация UserSessions
    db.UserSessions.insert_one({
        "session_id": fake.uuid4(),
        "user_id": fake.random_int(1, 1000),
        "start_time": fake.date_time_this_month(),
        "end_time": fake.date_time_this_month(),
        "pages_visited": [fake.uri() for _ in range(5)],
        "device": fake.user_agent(),
        "actions": [fake.word() for _ in range(3)]
    })
    
    # Аналогично для других коллекций...

dag = DAG(
    'generate_mongo_data',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    catchup=False
)

generate_task = PythonOperator(
    task_id='generate_mongo_data',
    python_callable=generate_mongo_data,
    dag=dag
)