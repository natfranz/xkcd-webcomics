from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from include.scr.comics.get_xkcd_data import find_max_value, find_last_value, fetch_xkcd_data_loop, process_data, comparison_result, update_xkcd_table_values, write_etl_status
from include.scr.comics.load_to_db import load_to_db

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
def xkcd_all_steps():

    fetch_max_value = PythonOperator(
        task_id='fetch_max_value',
        python_callable=find_max_value
    )

    fetch_last_value = PythonOperator(
        task_id='fetch_last_value',
        python_callable=find_last_value
    )

    branching = BranchPythonOperator(
        task_id='branching',
        provide_context=True,
        python_callable=comparison_result
    )

    ingest_xkcd_data = PythonOperator(
        task_id='ingest_xkcd_data',
        provide_context=True,
        python_callable=fetch_xkcd_data_loop
    )

    process_data_task = PythonOperator(
        task_id='process_data',
        provide_context=True,
        python_callable=process_data
    )

    load_to_db_task = PythonOperator(
        task_id='load_to_db',
        provide_context=True,
        python_callable=load_to_db,
        op_kwargs={'table_name': 'xkcd_webcomics'}
    )

    write_etl_status_task = PythonOperator(
        task_id='write_etl_status',
        provide_context=True,
        python_callable=write_etl_status,
        op_kwargs={'table_name': 'xkcd_webcomics'}
    )

    update_xkcd_table = PythonOperator(
        task_id='update_xkcd_table_values',
        provide_context=True,
        python_callable=update_xkcd_table_values
    )

    end_pipeline = DummyOperator(
        task_id='end_pipeline',
    )

    fetch_max_value >> fetch_last_value >> branching

    branching >> ingest_xkcd_data

    ingest_xkcd_data >> process_data_task >> load_to_db_task >> write_etl_status_task >> update_xkcd_table >> end_pipeline

    branching >> end_pipeline

xkcd_all_steps()
