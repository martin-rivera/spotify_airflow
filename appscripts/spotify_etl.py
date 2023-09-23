import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3
import base64
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import json

def check_if_data_valid(df) -> bool:

    # check if dataframe is empty
    if df.empty:
        print("No songs downloaded")
        return False

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    return True

def etl_spotify():

    with open('credentials.json', 'r') as credentials:
        credentials_dict = json.load(credentials)

    DATABASE_LOCATION = "sqlite:///appscripts/my_played_tracks.sqlite"
    AUTH_URL = 'https://accounts.spotify.com/authorize'
    USER_ID = credentials_dict["USER_ID"]
    CLIENT_ID = credentials_dict["CLIENT_ID"]
    CLIENT_SECRET_ID = credentials_dict["CLIENT_SECRET_ID"]
    SCOPE = "user-read-recently-played"
    SPOTIPY_REDIRECT_URI = "https://google.com"

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    today_unix_timestamp = int(today.timestamp()) * 1000

    auth = SpotifyOAuth(client_id=CLIENT_ID,
                        client_secret=CLIENT_SECRET_ID,
                        redirect_uri=SPOTIPY_REDIRECT_URI,
                        scope=SCOPE,
                        cache_path="./dags/spotipycache",
                        )

    sp = spotipy.Spotify(auth_manager=auth)


    r = sp.current_user_recently_played(limit=50, before=today_unix_timestamp)

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in r['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['artists'][0]['name'])
        played_at_list.append(song['played_at'])
        timestamps.append(song['played_at'][0:10])

    song_dict={
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps,
    }

    df = pd.DataFrame(song_dict)
    df = df.sort_values(['played_at'], ascending=True)


    # Validation
    if check_if_data_valid(df):
        print("Data valid: proceed to Load stage")

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)

    conn = sqlite3.connect("./appscripts/my_played_tracks.sqlite")
    cursor = conn.cursor()

    query= """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)    
    )
    """
    cursor.execute(query)
    conn.commit()
    print("Database opened successfully")

    df.to_sql('tempTable', engine, if_exists='replace', index=False)

    query = """
    INSERT INTO my_played_tracks
    SELECT * FROM tempTable
    WHERE NOT EXISTS (
        SELECT 1
        FROM my_played_tracks AS m
        WHERE m.played_at = tempTable.played_at
    );
    """

    try:
        cursor.execute(query)
        conn.commit()
        
    except sqlite3.IntegrityError:
        print("Data already in Database")

    conn.close()
    print("Database closed successfully")

