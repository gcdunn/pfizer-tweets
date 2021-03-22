import os
import tweepy
import pandas as pd
import sqlite3 as sql

local_db = '../data/pfizer_tweets.db'

api_key = os.environ['API_KEY']
api_key_secret = os.environ['API_KEY_SECRET']
access_token = os.environ['ACCESS_TOKEN']
access_token_secret = os.environ['ACCESS_TOKEN_SECRET']

def authenticate():
    auth = tweepy.OAuthHandler(api_key, api_key_secret)
    auth.set_access_token(access_token, access_token_secret)
    return tweepy.API(auth,wait_on_rate_limit=True)

def db_create(db):
  conn = sql.connect(db)
  create_table = '''CREATE TABLE tweets(id, created_at, screen_name, tweet, favorited, favorite_count, retweeted, retweet_count, source, follower_count, location, time_zone, utc_offset)'''
  conn.cursor().execute(create_table)
  conn.commit()
  conn.close()

if __name__ == "__main__":

    api = authenticate()
    if not os.path.exists(local_db):
        db_create(local_db)

    conn = sql.connect(local_db)
    c = conn.cursor()
    for tweet in tweepy.Cursor(api.search,q="#PfizerBioNTech",count=250000,
                           lang="en",
                           since="2020-01-01").items():
        print (tweet.created_at, tweet.text)
        query = '''INSERT INTO tweets(id, created_at, screen_name, tweet, 
        favorited, favorite_count, retweeted, retweet_count, source, follower_count, 
        location, time_zone, utc_offset) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        c.execute(query, (tweet.id, tweet.created_at, tweet.user.screen_name, tweet.text, 
        tweet.favorited, tweet.favorite_count, tweet.retweeted, tweet.retweet_count, tweet.source, tweet.user.followers_count, 
        tweet.user.location, tweet.user.time_zone, tweet.user.utc_offset))
        conn.commit()
    c.execute('CREATE INDEX id_index ON tweets(id)')
    conn.close()

