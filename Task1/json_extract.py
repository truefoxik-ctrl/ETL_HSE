from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

with DAG('extract', schedule_interval=timedelta(minutes=30),
         start_date=datetime(2026, 8, 2),
         catchup=False,) as dag:
         history=PostgresOperator(
                 task_id='extract_json',
                 postgres_conn_id='pg1',
                 sql= """
                 CREATE SCHEMA IF NOT EXISTS extract_demo
                 CREATE TABLE extract_demo.data_from_json AS
                 select 
                 post.value->>'name' AS name,
                 post.value->>'species' AS species,
                 post.value->>'favFoods' AS favFoods,
                 post.value->>'birthYear' AS birthYear,
                 post.value->>'photo' AS photo
                 from extract_demo.json_content,
                 jsonb_array_elements(json_data->'pets') AS post(value);

                 """

         )
