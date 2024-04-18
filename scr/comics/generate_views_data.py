import pandas as pd
import numpy as np
from itertools import product
from datetime import datetime, timedelta
from load_to_db import connect_db, load_to_db


def generate_dates(start_date, end_date):
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    return dates


def get_comic_ids():
    conn = connect_db()
    sql_query = "select num from public.xkcd_records xr"
    df = pd.read_sql_query(sql_query, conn)
    conn.close()
    num_list = df['num'].tolist()
    return num_list


def main():
    # Get a list of dates and
    start_date = '2024-01-01'
    end_date = '2024-01-05'
    dates_list = generate_dates(start_date, end_date)
    num_list = get_comic_ids()

    # Create all combinations of numbers and dates
    combinations = list(product(dates_list, num_list))
    views_df = pd.DataFrame(combinations, columns=['view_date', 'num'])

    # Generate daily views data
    views_df['views_count'] = views_df.apply(lambda x: int(np.random.random()*10000), axis=1)
    views_df['snapshot_date'] = datetime.utcnow()

    load_to_db('daily_views','public',views_df)


if __name__ == '__main__':
    main()
