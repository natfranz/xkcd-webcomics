import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv


def get_credentials():
    load_dotenv()
    credentials = {'host': os.environ['host'], 'database': os.environ['database'], 'user': os.environ['user'],
                   'password': os.environ['password'], 'port': os.environ['port']}
    return credentials


def load_to_db(table_name, schema_name, df):
    try:
        credentials = get_credentials()
        host = credentials['host']
        database = credentials['database']
        user = credentials['user']
        password = credentials['password']
        port = credentials['port']
        params = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
        # print("params: " + params)
        engine = create_engine(params, isolation_level='AUTOCOMMIT')
        print("############### Loading to table : ", table_name)
        df.to_sql(name=table_name, con=engine, schema=schema_name, if_exists='append', index=False, method='multi')
    except SQLAlchemyError as e:
        error_message = f"Error loading data to table {schema_name}.{table_name}: {str(e)}"
        raise RuntimeError(error_message)


def connect_db():
    credentials = get_credentials()
    try:
        conn = psycopg2.connect(**credentials)
        return conn
    except:
        raise