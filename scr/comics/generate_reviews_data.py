import pandas as pd
from load_to_db import load_to_db, connect_db
import numpy as np
from datetime import timedelta, datetime


def get_sample_data():
    # Get the ids of the existing items in the db
    conn = connect_db()
    sql_query = "select num , created_at from public.xkcd_records xr"
    df = pd.read_sql_query(sql_query, conn)
    conn.close()

    # Get a random sample of the df
    cnt = np.random.randint(1, 50)
    reviews_df = df.sample(n=cnt)
    reviews_df.reset_index(drop=True, inplace=True)
    return reviews_df


def generate_ratings(reviews_df):
    # Generate reviews data
    reviews_df['rating'] = reviews_df.apply(lambda x: (float(np.random.randint(2, 20)) / 2.0), axis=1)
    reviews_df['review_date'] = reviews_df.apply(lambda x: x.created_at + timedelta(days=np.random.randint(200)), axis=1)
    reviews_df = reviews_df.drop(['created_at'], axis=1)
    reviews_df['snapshot_date'] = datetime.utcnow()
    return reviews_df


def main():
    reviews_df = get_sample_data()
    reviews_df = generate_ratings(reviews_df)
    load_to_db('reviews','public',reviews_df)


if __name__ == '__main__':
    main()