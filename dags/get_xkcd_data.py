import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import requests
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'Natalia',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 16),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}
@dag(
    default_args=default_args,
    description='DAG to compare XKCD values',
    schedule_interval='@daily',
    catchup=False
)
def get_xkcd_data():

    def find_max_value():
        url = 'https://xkcd.com/info.0.json'
        header = {'Content-Type': 'application/json'}
        api_call = requests.get(url=url, headers=header)
        max_value = api_call.json()['num']
        return max_value

    def find_last_value():
        conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
        sql_query = "SELECT MAX(max_value) FROM etl.load_status WHERE table_name = 'xkcd_records'"
        cursor = conn.cursor()
        cursor.execute(sql_query)
        result = cursor.fetchone()
        last_value = result[0] if result[0] is not None else 0
        cursor.close()
        conn.close()
        return last_value

    def get_data_from_xkcd(num):
        url = 'https://xkcd.com/' + str(num) + '/info.0.json'
        header = {'Content-Type': 'application/json'}

        try:
            api_call = requests.get(url=url, headers=header)
            api_call.raise_for_status()
            result = api_call.json()
            df = pd.DataFrame([result])
            return df

        except requests.exceptions.RequestException as e:
            print("Error fetching data:", e)

        except Exception as e:
            print("An error occurred:", e)

    def validate_df(df):
        expected_columns = ['month', 'num', 'link', 'year', 'news', 'safe_title', 'transcript', 'alt', 'img', 'title',
                            'day']
        passed_columns = df.columns.tolist()

        if expected_columns == passed_columns:
            # print('Check passed')
            return df
        else:
            print('Columns dont match')
            if len(passed_columns) > len(expected_columns):
                columns_to_drop = [col for col in passed_columns if col not in expected_columns]
                df = df.drop(columns=columns_to_drop)
                print('Dropped extra columns')
                return df
            else:
                print('Fewer columns passed, continue')
                return df

    def fetch_xkcd_data_loop(**kwargs):
        ti = kwargs['ti']
        last_value = ti.xcom_pull(task_ids='fetch_last_value')
        max_value = ti.xcom_pull(task_ids='fetch_max_value')
        print(last_value, max_value)
        df = pd.DataFrame()
        for i in range(last_value + 1, max_value + 1):
            print(i)
            df1 = get_data_from_xkcd(i)
            if df1 is not None:
                df1 = validate_df(df1)
                df = pd.concat([df, df1])
            else:
                print('Entry does not exist:', i)
                continue
        return df

    def process_data(**kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='fetch_xkcd_data_loop')
        df = df.assign(
            cost_eur=df['title'].apply(lambda x: len(str(x.replace(' ', '')))) * 5,
            created_at=pd.to_datetime(
                df['year'].astype(str) + '-' + df['month'].astype(str) + '-' + df['day'].astype(str))
        )
        df.drop(['month', 'year', 'day'], inplace=True, axis=1)
        return df

    def get_credentials():
        load_dotenv()
        credentials = {'host': os.environ['host'], 'database': os.environ['database'], 'user': os.environ['user'],
                       'password': os.environ['password'], 'port': os.environ['port']}
        return credentials

    def load_to_db(table_name, schema_name, **kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='process_data')
        print(df.head())
        try:
            credentials = get_credentials()
            host = credentials['host']
            database = credentials['database']
            user = credentials['user']
            password = credentials['password']
            port = credentials['port']
            params = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
            engine = create_engine(params, isolation_level='AUTOCOMMIT')
            print("############### Loading to table : ", table_name)
            df.to_sql(name=table_name, con=engine, schema=schema_name, if_exists='append', index=False, method='multi')
        except SQLAlchemyError as e:
            error_message = f"Error loading data to table {schema_name}.{table_name}: {str(e)}"
            raise RuntimeError(error_message)

    def write_etl_status(table_name, **kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='process_data')
        rows = df.shape[0]
        min_value = df['num'].min()
        max_value = df['num'].max()
        executed_at = datetime.utcnow()
        df = pd.DataFrame([{
            'table_name': table_name,
            'count_records': rows,
            'min_value': min_value,
            'max_value': max_value,
            'executed_at': executed_at
        }])
        print(df)
        load_to_db('load_status', 'etl', df)

    def comparison_result(**kwargs):
        ti = kwargs['ti']
        max_value = ti.xcom_pull(task_ids='fetch_max_value')
        last_value = ti.xcom_pull(task_ids='fetch_last_value')
        if max_value > last_value:
            return 'fetch_xkcd_data_loop'
        else:
            return 'end_pipeline'

    fetch_max_value = PythonOperator(
        task_id='fetch_max_value',
        python_callable=find_max_value,
    )

    fetch_last_value = PythonOperator(
        task_id='fetch_last_value',
        python_callable=find_last_value,
    )

    branching = BranchPythonOperator(
        task_id='branching',
        provide_context=True,
        python_callable=comparison_result,
    )

    fetch_xkcd_data_loop = PythonOperator(
        task_id='fetch_xkcd_data_loop',
        provide_context=True,
        python_callable=fetch_xkcd_data_loop
    )

    process_data_task = PythonOperator(
        task_id='process_data',
        provide_context=True,
        python_callable=process_data,
    )

    load_to_db_task = PythonOperator(
        task_id='load_to_db',
        provide_context=True,
        python_callable=load_to_db,
        op_kwargs={'table_name': 'xkcd_records', 'schema_name': 'public'}
    )

    write_etl_status_task = PythonOperator(
        task_id='write_etl_status',
        provide_context=True,
        python_callable=write_etl_status,
        op_kwargs={'table_name': 'xkcd_records'}
    )

    end_pipeline = DummyOperator(
        task_id='end_pipeline',
    )

    fetch_max_value >> fetch_last_value >> branching

    branching >> fetch_xkcd_data_loop

    fetch_xkcd_data_loop >> process_data_task >> load_to_db_task >> write_etl_status_task >> end_pipeline

    branching >> end_pipeline

dag = get_xkcd_data()
