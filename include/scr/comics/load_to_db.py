from airflow.hooks.postgres_hook import PostgresHook


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