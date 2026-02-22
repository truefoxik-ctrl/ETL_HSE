from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG('iot_temp_full', schedule_interval=None,
         start_date=datetime(2026, 1, 1),
         catchup=False,) as full_dag:

         hot_days=PostgresOperator(
                 task_id='hot_days',
                 postgres_conn_id='pg1',
                 sql= """
                 CREATE TABLE IF NOT EXISTS airflow.hot_days AS

                 SELECT MAX(temp_c) AS max_temp, DATE(noted_date) AS noted_day FROM airflow.airflow 
                 GROUP BY DATE(noted_date)
                 ORDER BY MAX(temp_c) DESC
                 LIMIT 5;
                 """
         )

         cold_days=PostgresOperator(
                 task_id='cold_days',
                 postgres_conn_id='pg1',
                 sql= """
                 CREATE TABLE IF NOT EXISTS airflow.cold_days AS

                 SELECT MIN(temp_c) AS min_temp, DATE(noted_date) AS noted_day FROM airflow.airflow 
                 GROUP BY DATE(noted_date)
                 ORDER BY MIN(temp_c) ASC
                 LIMIT 5;
                 """
         )

         filter_in=PostgresOperator(
                 task_id='filter_in',
                 postgres_conn_id='pg1',
                 sql= """
                 CREATE TABLE IF NOT EXISTS airflow.only_in AS

                 SELECT * FROM airflow.airflow
                 WHERE side = 'In';
                 """
         )

         correct_date=PostgresOperator(
                 task_id='correct_date',
                 postgres_conn_id='pg1',
                 sql= """
                 CREATE TABLE IF NOT EXISTS airflow.corr_dt AS

                 SELECT id, room_id, DATE(noted_date), temp_c, side FROM airflow.airflow;
                 """
         )
         [hot_days, cold_days, filter_in, correct_date]

with DAG('iot_temp_incremental', schedule_interval='0 2 * * *',
         start_date=datetime(2026, 1, 1),
         catchup=False,) as incr_dag:

         hot_days=PostgresOperator(
                 task_id='hot_days',
                 postgres_conn_id='pg1',
                 sql= """
                 INSERT INTO airflow.hot_days (max_temp, noted_day)

                 SELECT MAX(temp_c) AS max_temp, DATE(noted_date) AS noted_day FROM airflow.airflow 
                 WHERE DATE(noted_date) >= '01-12-2018' AND DATE(noted_date) NOT IN (SELECT noted_day FROM airflow.hot_days)
                 GROUP BY DATE(noted_date)
                 ORDER BY MAX(temp_c) DESC
                 LIMIT 5;
                 """
         )

         cold_days=PostgresOperator(
                 task_id='cold_days',
                 postgres_conn_id='pg1',
                 sql= """
                 INSERT INTO airflow.cold_days (min_temp, noted_day)

                 SELECT MIN(temp_c) AS min_temp, DATE(noted_date) AS noted_day FROM airflow.airflow
                 WHERE DATE(noted_date) >= '01-12-2018' AND DATE(noted_date) NOT IN (SELECT noted_day FROM airflow.cold_days) 
                 GROUP BY DATE(noted_date)
                 ORDER BY MIN(temp_c) ASC
                 LIMIT 5;
                 """
         )

         filter_in=PostgresOperator(
                 task_id='filter_in',
                 postgres_conn_id='pg1',
                 sql= """
                 INSERT INTO airflow.only_in 

                 SELECT * FROM airflow.airflow
                 WHERE side = 'In' AND DATE(noted_date) >= '01-12-2018' AND id NOT IN (SELECT id FROM airflow.only_in) ;
                 """
         )

         correct_date=PostgresOperator(
                 task_id='correct_date',
                 postgres_conn_id='pg1',
                 sql= """
                 INSERT INTO airflow.corr_dt

                 SELECT id, room_id, DATE(noted_date), temp_c, side FROM airflow.airflow
                 WHERE side = 'In' AND DATE(noted_date) >= '01-12-2018' AND id NOT IN (SELECT id FROM airflow.corr_dt) ;
                 """

         )
