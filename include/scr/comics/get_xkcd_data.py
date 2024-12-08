import requests
import pandas as pd
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook


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
    key_value = result[0]+1 if result[0] is not None else 0
    cursor.close()
    conn.close()
    return key_value


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
    df = ti.xcom_pull(task_ids='ingest_xkcd_data')
    df = df.assign(
        cost_eur=df['title'].apply(lambda x: len(str(x.replace(' ', '')))) * 5,
        created_at=pd.to_datetime(
            df['year'].astype(str) + '-' + df['month'].astype(str) + '-' + df['day'].astype(str))
    )
    df.drop(['month', 'year', 'day'], inplace=True, axis=1)
    return df


def comparison_result(**kwargs):
    ti = kwargs['ti']
    max_value = ti.xcom_pull(task_ids='fetch_max_value')
    last_value = ti.xcom_pull(task_ids='fetch_last_value')
    if max_value > last_value:
        return 'ingest_xkcd_data'
    else:
        return 'end_pipeline'


def update_xkcd_table_values():
    conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    sql_query = "select public.func_xkcd_webcomics()"
    cursor = conn.cursor()
    cursor.execute(sql_query)
    cursor.close()
    conn.close()

