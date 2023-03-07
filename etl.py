import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes a song file specified by the file name in 
    "filepath". Data is taken and recorded in the database 
    associated with the cursor "cur". This data corresponds
    to columns in the song and artist tables.
    
    Inputs:
        cur: Database cursor reference
        filepath : File system path to a song file (string)
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id","title","artist_id","year","duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes a log file specified by the file name in 
    "filepath". Data is taken and recorded in the database 
    associated with the cursor "cur". This data corresponds
    to columns in the user and timestamp tables. Using the 
    song_select statement, it also retrieves data from the 
    song and artist tables and inserts this data in addition
    to the log file data into the songplay table.
    
    Inputs:
        cur: Database cursor reference
        filepath : File system path to a song file (string)
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.month, t.dt.weekday]
    column_labels = ["start_time","hour","day","month","weekday"] 
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts), row.userId, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    The primary function for utilizing the process_song_file
    and process_log_file functions. Iteratively retrieves files' 
    filepaths within the given filepath, and uses the function 
    specified by the func parameter to retrieve data from the file
    and record it into the database associated with the cursor and
    connection specified by the cur and conn parameters, respectively 
    
    Inputs:
        cur: Database cursor reference
        filepath : File system path to a song file (string)
        conn: Database connector 
        func: Processing function to be used
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    The main function, which accomplishes the following:
    -Connects to the sparkify database and sets a cursor
    -Iteratively processes through each song file, and then
    through each log file to populate the tables in the database
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()