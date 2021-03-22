import re
import emoji
import pandas as pd
import sqlite3 as sql

def get_query(query, db):
    with sql.connect(db) as conn:
        df = pd.read_sql_query(query, conn)
    df.columns = [str(col).lower() for col in df.columns]
    return df
    
def clean(tweet):
    tweet = re.sub("@[A-Za-z0-9]+","",tweet)
    tweet = re.sub(r"(?:\@|http?\://|https?\://|www)\S+", "", tweet)
    tweet = " ".join(tweet.split())
    tweet = emoji.demojize(tweet, delimiters=(" ", " "))
    tweet = tweet.replace("#", "").replace("_", " ")
    return tweet