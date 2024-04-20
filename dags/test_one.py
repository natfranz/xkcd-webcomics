import psycopg2
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

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
    description='DAG to extract XKCD data',
    schedule_interval='59 23 * * 1,3,5',
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
        sql_query = "SELECT MAX(max_value) FROM etl.load_status WHERE table_name = 'xkcd_webcomics'"
        cursor = conn.cursor()
        cursor.execute(sql_query)
        result = cursor.fetchone()
        last_value = result[0] if result[0] is not None else 0
        cursor.close()
        conn.close()
        return last_value

    def find_next_key():
        conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
        sql_query = "SELECT MAX(load_status_key) FROM etl.load_status"
        cursor = conn.cursor()
        cursor.execute(sql_query)
        result = cursor.fetchone()
        key_value = result[0] + 1 if result[0] is not None else 0
        cursor.close()
        conn.close()
        return key_value

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
        expected_columns = ['month', 'num', 'year', 'safe_title', 'transcript', 'alt', 'img', 'title',
                            'day']
        passed_columns = df.columns.tolist()

        for col in expected_columns:
            if (col in passed_columns) and (df[col].values == ''):
                df[col] = None

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

    def load_to_db(table_name, **kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='process_data')
        try:
            print("############### Loading to table : ", table_name)
            pg_hook = PostgresHook(postgres_conn_id='postgres_default')
            pg_hook.insert_rows(table=table_name, rows=df.values.tolist())
        except Exception as e:
            print(f"Error occurred while loading data to PostgreSQL: {str(e)}")
            raise e

    def write_etl_status(table_name, **kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='process_data')
        rows = df.shape[0]
        key = find_next_key()
        min_value = df['num'].min()
        max_value = df['num'].max()
        executed_at = datetime.utcnow()
        procedure_name = 'get_xkcd_data'
        df = pd.DataFrame([{
            'load_status_key': key,
            'procedure_name': procedure_name,
            'table_name': table_name,
            'count_records': rows,
            'min_value': min_value,
            'max_value': max_value,
            'executed_at': executed_at
        }])
        try:
            print("############### Loading to table : etl.load_status")
            pg_hook = PostgresHook(postgres_conn_id='postgres_default')
            pg_hook.insert_rows(table='etl.load_status', rows=df.values.tolist())
        except Exception as e:
            print(f"Error occurred while loading data to PostgreSQL: {str(e)}")
            raise e

    def comparison_result(**kwargs):
        ti = kwargs['ti']
        max_value = ti.xcom_pull(task_ids='fetch_max_value')
        last_value = ti.xcom_pull(task_ids='fetch_last_value')
        if max_value > last_value:
            return 'fetch_xkcd_data_loop'
        else:
            return 'end_pipeline'

    def get_sample_data():
        conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
        sql_query = "select num , created_at from public.xkcd_webcomics xr"
        cursor = conn.cursor()
        cursor.execute(sql_query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data, columns=columns)
        cnt = np.random.randint(1, 50)
        reviews_df = df.sample(n=cnt)
        reviews_df.reset_index(drop=True, inplace=True)
        cursor.close()
        conn.close()
        return reviews_df

    def generate_ratings():
        reviews_df = get_sample_data()
        reviews_df['rating'] = reviews_df.apply(lambda x: (float(np.random.randint(2, 20)) / 2.0), axis=1)
        reviews_df['review_date'] = reviews_df.apply(lambda x: x.created_at + timedelta(days=np.random.randint(200)),
                                                     axis=1)
        reviews_df = reviews_df.drop(['created_at'], axis=1)
        reviews_df['snapshot_date'] = datetime.utcnow()
        table_name = 'reviews'
        try:
            print("############### Loading to table : ", table_name)
            pg_hook = PostgresHook(postgres_conn_id='postgres_default')
            pg_hook.insert_rows(table=table_name, rows=reviews_df.values.tolist())
        except Exception as e:
            print(f"Error occurred while loading data to PostgreSQL: {str(e)}")
            raise e

    def update_xkcd_table_values():
        conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
        sql_query = "select public.func_xkcd_webcomics()"
        cursor = conn.cursor()
        cursor.execute(sql_query)
        cursor.close()
        conn.close()


    update_xkcd_table = PythonOperator(
        task_id='update_xkcd_table_values',
        provide_context=True,
        python_callable=update_xkcd_table_values
    )

    end_pipeline = DummyOperator(
        task_id='end_pipeline',
    )

    update_xkcd_table >> end_pipeline


dag = get_xkcd_data()
