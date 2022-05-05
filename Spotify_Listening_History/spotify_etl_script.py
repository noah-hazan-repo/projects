#!/usr/bin/env python
# coding: utf-8

# # Setup: import packages, instantiate engine, and create helper variables and functions

# In[10]:


import os
import sys
import json
import spotipy
import pandas as pd
import sqlalchemy
import mysql.connector
import spotipy.util as util
from mysql.connector import Error
from sqlalchemy import create_engine
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime, timezone
from spotipy.oauth2 import SpotifyClientCredentials

engine = sqlalchemy.create_engine('mysql+pymysql://root:Babusafti33@localhost/spotify_etl_project') # connect to server
engine.execute("CREATE DATABASE IF NOT EXISTS spotify_etl_project") #create db
engine.execute("USE spotify_etl_project") # select new db

DAG = ["""CREATE TABLE IF NOT EXISTS spotify_etl_project.f_spotify_track(
unique_identifier varchar(100) PRIMARY KEY NOT NULL,
song_id varchar(200) NOT NULL,
song_name varchar(512),
duration_ms int,
url varchar(512),
popularity int,
date_time_played TIMESTAMP,
album_id varchar(100),
artist_id varchar(100));""",

""" CREATE TABLE IF NOT EXISTS spotify_etl_project.d_spotify_album(
album_id varchar(100) NOT NULL PRIMARY KEY,
name varchar(512),
release_date varchar(30),
total_tracks int,
url varchar(512));""",

"""CREATE TABLE IF NOT EXISTS spotify_etl_project.d_spotify_artist(
artist_id varchar(100) PRIMARY KEY NOT NULL,
name varchar(512),
url varchar(512));""",

"USE  SPOTIFY_ETL_PROJECT;",
"CREATE TABLE IF NOT EXISTS tmp_track AS SELECT * FROM SPOTIFY_ETL_PROJECT.f_spotify_track;",
"CREATE TABLE IF NOT EXISTS tmp_album AS SELECT * FROM SPOTIFY_ETL_PROJECT.d_spotify_album;",
"CREATE TABLE IF NOT EXISTS tmp_artist as SELECT * FROM SPOTIFY_ETL_PROJECT.d_spotify_artist;"]


DAG2 = [
"USE SPOTIFY_ETL_PROJECT;",

"""INSERT INTO f_spotify_track SELECT tmp_track.*
FROM   tmp_track
LEFT   JOIN f_spotify_track USING (unique_identifier)
WHERE  f_spotify_track.unique_identifier IS NULL; 
""",

"DROP TABLE TMP_TRACK;",

"USE SPOTIFY_ETL_PROJECT;",

""" INSERT INTO d_spotify_album
SELECT tmp_album.*
FROM   tmp_album
LEFT   JOIN d_spotify_album USING (album_id)
WHERE  d_spotify_album.album_id IS NULL;
""",
       
"DROP TABLE TMP_ALBUM;",

"USE SPOTIFY_ETL_PROJECT;",
       
"""INSERT INTO d_spotify_artist
SELECT tmp_artist.*
FROM   tmp_artist
LEFT   JOIN d_spotify_artist USING (artist_id)
WHERE  d_spotify_artist.artist_id IS NULL;
""",
"DROP TABLE TMP_ARTIST;"]


def execute_sql(sql, cursor):
    try:
        print("Executing SQL - {}: ".format(sql[:20]), end='')
        cursor.execute(sql)
    except mysql.connector.Error as err:
        print(err.msg)
    else:
        print("OK")

def confirm_db_conn(password): # this one's not used, but is helpful to confirm we can establish conn if needed
    try: 
        connection = mysql.connector.connect(host='localhost',database='spotify_etl_project',user='root',password=password, autocommit=True)

        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
            
    except Error as e:
        print("Error while connecting to MySQL", e)
        return False

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
            return True
        
def get_cursor(password):
    connection = mysql.connector.connect(host='localhost',database='spotify_etl_project',user='root',password=password, autocommit=True)
    cursor = connection.cursor()
    print("MySQL cursor now connected")
    return connection,cursor 


# # Authenticate Spotify API and Extract/Transform Data

# In[11]:


# Enter credentials and instantiate API

spotify_client_id = '3abb01150b67436089942c63d5cedde0'
spotify_client_secret = '70ff498bafa748b09c65bb9ebcc92da3'
spotify_redirect_url = 'http://localhost:8081/callback'

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=spotify_client_id,
                                               client_secret=spotify_client_secret,
                                               redirect_uri=spotify_redirect_url,
                                               scope="user-read-recently-played"))

# Retrieve data
recently_played = sp.current_user_recently_played(limit=50)

# Handling edge case of no results
if len(recently_played) ==0:
    sys.exit("No results recieved from Spotify")

# Organize Albums Data
album_list = []
for row in recently_played['items']:
    album_id = row['track']['album']['id']
    album_name = row['track']['album']['name']
    album_release_date = row['track']['album']['release_date']
    album_total_tracks = row['track']['album']['total_tracks']
    album_url = row['track']['album']['external_urls']['spotify']
    album_element = {'album_id':album_id,'name':album_name,'release_date':album_release_date,
                    'total_tracks':album_total_tracks,'url':album_url}
    album_list.append(album_element)

    
artist_dict = {}
id_list = []
name_list = []
url_list = []
for item in recently_played['items']:
    for key,value in item.items():
        if key == "track":
            for data_point in value['artists']:
                id_list.append(data_point['id'])
                name_list.append(data_point['name'])
                url_list.append(data_point['external_urls']['spotify'])
artist_dict = {'artist_id':id_list,'name':name_list,'url':url_list}

# Organize Tracks Data
song_list = []
for row in recently_played['items']:
    song_id = row['track']['id']
    song_name = row['track']['name']
    song_duration = row['track']['duration_ms']
    song_url = row['track']['external_urls']['spotify']
    song_popularity = row['track']['popularity']
    song_time_played = row['played_at']
    album_id = row['track']['album']['id']
    artist_id = row['track']['album']['artists'][0]['id']
    song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                    'popularity':song_popularity,'date_time_played':song_time_played,'album_id':album_id,
                    'artist_id':artist_id
                   }
    song_list.append(song_element)

# Organize DataFrames and UniqueIds

#Album DF and Deduplication Step
album_df = pd.DataFrame.from_dict(album_list)
album_df = album_df.drop_duplicates(subset=['album_id'])

#Artist DF and Deduplication Step
artist_df = pd.DataFrame.from_dict(artist_dict)
artist_df = artist_df.drop_duplicates(subset=['artist_id'])

# Tracks DF
song_df = pd.DataFrame.from_dict(song_list)
#date_time_played is an object (data type) changing to a timestamp
song_df['date_time_played'] = pd.to_datetime(song_df['date_time_played'])
#converting to my timezone of Eastern
song_df['date_time_played'] = song_df['date_time_played'].dt.tz_convert('US/Eastern')
#Cleaning the date
song_df['date_time_played'] = song_df['date_time_played'].astype(str).str[:-7]
song_df['date_time_played'] = pd.to_datetime(song_df['date_time_played'])
#Creating a Unix Timestamp for Time Played. This will be one half of our unique identifier
song_df['UNIX_Time_Stamp'] = (song_df['date_time_played'] - pd.Timestamp("1970-01-01"))//pd.Timedelta('1s')
# Generating uniqueidentifer for duplicate case and for future deduplication 
song_df['unique_identifier'] = song_df['song_id'] + "-" + song_df['UNIX_Time_Stamp'].astype(str)
song_df = song_df[['unique_identifier','song_id','song_name','duration_ms','url','popularity','date_time_played','album_id','artist_id']]


# # Run Makeshift "Dag"

# In[14]:


connection, cursor = get_cursor('Babusafti33')

for sql_task in DAG:
    execute_sql(sql_task, cursor)
    
song_df.to_sql("tmp_track", con = engine, if_exists='append', index = False)
album_df.to_sql("tmp_album", con = engine, if_exists='append', index = False)
artist_df.to_sql("tmp_artist", con = engine, if_exists='append', index = False)
    
for sql_task in DAG2:
    execute_sql(sql_task, cursor)
    


# In[ ]:





