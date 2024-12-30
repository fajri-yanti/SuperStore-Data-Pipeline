from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from sqlalchemy.types import Date
from pytz import timezone
import glob
import os

def insert_data_from_csv():
   
    file_path = '/opt/airflow/data/SuperStore/data_test'
    files = glob.glob(os.path.join(file_path, '*.csv'))
    csv_append = []

    for file in files:
        df = pd.read_csv(file, encoding="latin1")  
        csv_append.append(df)

    combine_df = pd.concat(csv_append, ignore_index=True)
    def date_transform(df, columns):  
        for column in columns:
            df[column] = pd.to_datetime(df[column])
        return df

    columns = ['Ship Date', 'Order Date']
    data = date_transform(combine_df, columns)
    data['last_update'] = pd.Timestamp.now(tz=timezone('Asia/Jakarta'))
    data = data.drop(['year_month'], axis=1)

    engine = create_engine("mysql+mysqlconnector://root:dibimbing@host.docker.internal:3303/SuperStore",isolation_level="AUTOCOMMIT")
    try:
        data.to_sql('bronze_transaction', con=engine, if_exists='append', index=False)
        print("Data inserted successfully into MySQL.")
    except Exception as e:
            print(f"An error occurred: {e}")
with DAG(
    dag_id='load_csv_to_mysql',
    schedule_interval=None,
    start_date=datetime(2024, 12, 19),
    catchup=False,
) as dag:
    start_task = EmptyOperator(task_id="start_task")
    insert_data_task = PythonOperator(
        task_id='insert_data_from_csv',
        python_callable=insert_data_from_csv
    )
    end_task = EmptyOperator(task_id="end_task")

start_task >> insert_data_task >> end_task
