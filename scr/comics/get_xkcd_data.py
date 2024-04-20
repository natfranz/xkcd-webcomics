import requests
import pandas as pd
from datetime import datetime
from load_to_db import load_to_db, connect_db


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
    conn = connect_db()
    sql_query = "select max(max_value) from etl.load_status where table_name='xkcd_webcomics'"
    cursor = conn.cursor()
    cursor.execute(sql_query)
    result = cursor.fetchone()
    if result[0] is not None:
        last_value = result[0]
    else:
        last_value = 0
    cursor.close()
    conn.close()
    return last_value


def write_etl_status(df, table_name):
    rows = df.shape[0]
    min_value = df['num'].min()
    max_value = df['num'].max()
    executed_at = datetime.utcnow()
    procedure_name = 'get_xkcd_data'
    df = pd.DataFrame([{
        'procedure_name': procedure_name,
        'table_name': table_name,
        'count_records': rows,
        'min_value': min_value,
        'max_value': max_value,
        'executed_at': executed_at
    }])
    print(df)
    load_to_db('load_status', 'etl', df)


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


def main():
    last_value = find_last_value()
    max_value = find_max_value()

    # Check if there are any new records
    # If yes, extract them, validate them, and append into a df
    if max_value > last_value:
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

        # Create cost and creation date columns
        df = df.assign(
            cost_eur=df['title'].apply(lambda x: len(str(x.replace(' ', '')))) * 5,
            created_at=pd.to_datetime(
                df['year'].astype(str) + '-' + df['month'].astype(str) + '-' + df['day'].astype(str))

        )
        df.drop(['month', 'year', 'day'], inplace=True, axis=1)

        # Load df to the db and update etl load status
        table_name = 'xkcd_webcomics'
        schema_name = 'public'
        load_to_db(table_name, schema_name, df)
        write_etl_status(df, table_name)

    else:
        print('No new entries')


if __name__ == '__main__':
    main()
