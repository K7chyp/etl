from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import random


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 5),  
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def generate_data():
    import psycopg2
    conn = psycopg2.connect("dbname='airflow' user='airflow' host='postgres' password='airflow'")
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO source_table (id, name, value, updated_at)
        VALUES (generate_series(1, 100), 
                md5(random()::text), 
                random() * 100, 
                NOW() - INTERVAL '1 day' * floor(random() * 10));
    """)
    conn.commit()
    cur.close()
    conn.close()


with DAG(
    'etl_data_load',
    default_args=default_args,
    description='ETL загрузка данных в целевую систему',
    schedule_interval='@daily',  
    catchup=False,
) as dag:
    
    
    generate_data_task = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data
    )
    
    
    full_load = PostgresOperator(
        task_id='full_load',
        postgres_conn_id='postgres_default',  
        sql="""
            INSERT INTO target_table (id, name, value, updated_at)
            SELECT id, name, value, updated_at FROM source_table;
        """,
    )

    
    incremental_load = PostgresOperator(
        task_id='incremental_load',
        postgres_conn_id='postgres_default',
        sql="""
            INSERT INTO target_table (id, name, value, updated_at)
            SELECT id, name, value, updated_at FROM source_table
            WHERE updated_at >= NOW() - INTERVAL '3 days'
            ON CONFLICT (id) DO UPDATE 
            SET name = EXCLUDED.name, 
                value = EXCLUDED.value, 
                updated_at = EXCLUDED.updated_at;
        """,
    )

    generate_data_task >> full_load >> incremental_load
