#!/usr/bin/env python
# coding: utf-8

# In[162]:


import datetime
from googleapiclient.discovery import build
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import psycopg2
import pymongo
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu


# In[163]:


pd.set_option('display.max_rows', None)


# In[164]:


pd.set_option('display.max_columns', None)


# In[165]:


def streamlit_config():

    # page configuration
    st.set_page_config(page_title='YouTube Data Harvesting and Warehousing',
                       page_icon=':bar_chart:', layout="wide")

    # page header transparent color
    page_background_color = """
    <style>

    [data-testid="stHeader"] 
    {
    background: rgba(0,0,0,0);
    }

    </style>
    """
    st.markdown(page_background_color, unsafe_allow_html=True)

    # title and position
    st.markdown(f'<h1 style="text-align: center;">YouTube Data Harvesting and Warehousing</h1>',
                unsafe_allow_html=True)


# In[166]:


def Api_connect():
    api_key = "AIzaSyAaWXGZIBT9mH8qhGzgl-TFOIBCq20JVKQ"
    
    api_service_name="youtube"
    api_version="v3"
    
    youtube = build(api_service_name, api_version, developerKey=api_key)
                    
    return youtube
                    
youtube = Api_connect()


# In[167]:


class youtube_extract:

    # Function To Get Channel Data
    def get_channel_data(youtube, channel_id):
        request = youtube.channels().list(
            part="snippet, contentDetails, statistics, status",
            id=channel_id
        )

        response = request.execute()

        data = {
            'channel_name': response['items'][0]['snippet']['title'],
            'channel_id': response['items'][0]['id'],
            'subscribers': response['items'][0]['statistics']['subscriberCount'],
            'total_videos': response['items'][0]['statistics']['videoCount'],
            'views': response['items'][0]['statistics']['viewCount'],
            'channel_description': response['items'][0]['snippet']['description'],
            'upload_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        }

        return data

    # Function To Get Playlist Data
    def get_playlist_data(youtube, channel_id, upload_id):
        playlist = []

        request = youtube.playlists().list(
            part="snippet,contentDetails,status",
            channelId=channel_id,
            maxResults=50
        )
        response = request.execute()

        for i in range(0, len(response['items'])):
            data = {
                'playlist_id': response['items'][i]['id'],
                'playlist_name': response['items'][i]['snippet']['title'],
                'channel_id': channel_id,
                'upload_id': upload_id
            }

            playlist.append(data)

        # Check if there are more pages of results
        while 'nextPageToken' in response:
            nextPageToken = response['nextPageToken']
            request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=nextPageToken
            )
            response = request.execute()

            for i in range(0, len(response['items'])):
                data = {
                    'playlist_id': response['items'][i]['id'],
                    'playlist_name': response['items'][i]['snippet']['title'],
                    'channel_id': channel_id,
                    'upload_id': upload_id
                }

                playlist.append(data)

        return playlist

    # Function To Get Video Ids
    def get_video_ids(youtube, upload_id):
        video_ids = []

        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=upload_id,
            maxResults=50
        )
        response = request.execute()

        for i in range(0, len(response['items'])):
            data = response['items'][i]['contentDetails']['videoId']
            video_ids.append(data)

        nextPageToken = None

        while True:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=upload_id,
                maxResults=50,
                pageToken=nextPageToken
            )
            response = request.execute()

            for i in range(0, len(response['items'])):
                data = response['items'][i]['contentDetails']['videoId']
                video_ids.append(data)

            if 'nextPageToken' in response:
                nextPageToken = response['nextPageToken']
            else:
                break

        return video_ids

    # Function to Get Video Info
    def get_video_info(youtube, video_id, upload_id):
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )
        response = request.execute()

        caption = {'true': 'Available', 'false': 'Not Available'}

        # convert duration format using Timedelta function in pandas
        def time_duration(t):
            a = pd.Timedelta(t)
            b = str(a).split()[-1]
            return b

        data = {
            'video_id': response['items'][0]['id'],
            'video_name': response['items'][0]['snippet']['title'],
            'video_description': response['items'][0]['snippet'].get('description'),
            'upload_id': upload_id,
            'tags': response['items'][0]['snippet'].get('tags', []),
            'published_date': response['items'][0]['snippet']['publishedAt'],
            'published_time': response['items'][0]['snippet']['publishedAt'],
            'view_count': response['items'][0]['statistics']['viewCount'],
            'like_count': response['items'][0]['statistics'].get('likeCount', 0),
            'favorite_count': response['items'][0]['statistics'].get('favoriteCount', 0),
            'comment_count': response['items'][0]['statistics'].get('commentCount', 0),
            'duration': time_duration(response['items'][0]['contentDetails']['duration']),
            'thumbnail': response['items'][0]['snippet']['thumbnails']['default']['url'],
            'caption_status': caption[response['items'][0]['contentDetails']['caption']]
        }

        if data['tags'] == []:
            del data['tags']

        return data

    # Function to Get Comment Info
    def get_comment_info(youtube, video_id):
        comment_data = []

        request = youtube.commentThreads().list(
            part='id, snippet',
            videoId=video_id,
            maxResults=50
        )
        response = request.execute()

        for i in range(0, len(response['items'])):
            data = {
                'comment_id': response['items'][i]['snippet']['topLevelComment']['id'],
                'comment_text': response['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal'],
                'comment_author': response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'comment_published_date': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'],
                'comment_published_time': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'],
                'video_id': video_id
            }

            comment_data.append(data)

        return comment_data

    def main(channel_id):
        
        channel = youtube_extract.get_channel_data(youtube, channel_id)
        upload_id = channel_data['upload_id']
        playlist = youtube_extract.get_playlist_data(youtube, channel_id, upload_id)
        video_ids = youtube_extract.get_video_ids(youtube, upload_id)

        video = []
        comment_data = []

        for i in video_ids:
            v = youtube_extract.get_video_info(youtube, i, upload_id)
            video.append(v)

            # skip disabled comments error in looping function
            try:
                c = youtube_extract.get_comment_info(youtube, i)
                comment_data.append(c)
            except:
                pass

        final = {
            'channel': channel,
            'playlist': playlist,
            'video': video,
            'comment': comment_data
        }

        return final

    def display_sample_data(channel_id):
        
        channel = youtube_extract.get_channel_data(youtube, channel_id)
        upload_id = channel_data['upload_id']
        playlist = youtube_extract.get_playlist_data(youtube, channel_id, upload_id)
        video_ids = youtube_extract.get_video_ids(youtube, upload_id)

        video = []
        comment_data = []

        for i in video_ids:
            v = youtube_extract.get_video_info(youtube, i, upload_id)
            video.append(v)

            # skip disabled comments error in looping function
            try:
                c = youtube_extract.get_comment_info(youtube, i)
                comment_data.append(c)
            except:
                pass

        final = {
            'channel': channel,
            'playlist': playlist,
            'video': video,
            'comment': comment_data
        }

        return final


# In[168]:


class mongodb:
  
    def data_storage(channel_name, database, data):
        client = pymongo.MongoClient("mongodb+srv://santhosh:santhosh1992@cluster0.idw4nqi.mongodb.net/?retryWrites=true&w=majority")
        db = client[database]
        col = db[channel_name]
        col.insert_one(data)


    def list_collection_names(database):
        client = pymongo.MongoClient("mongodb+srv://santhosh:santhosh1992@cluster0.idw4nqi.mongodb.net/?retryWrites=true&w=majority")
        db = client[database]
        col = db.list_collection_names()
        col.sort(reverse=False)
        return col


    def order_collection_names(database):

        m = mongodb.list_collection_names(database)

        if m == []:
            st.info("The Mongodb database is currently empty")

        else:
            st.subheader('List of collections in MongoDB database')
            m = mongodb.list_collection_names(database)
            c = 1
            for i in m:
                st.write(str(c) + ' - ' + i)
                c += 1


    def drop_temp_collection():
        client = pymongo.MongoClient("mongodb+srv://santhosh:santhosh1992@cluster0.idw4nqi.mongodb.net/?retryWrites=true&w=majority")
        db = client['temp']
        col = db.list_collection_names()
        if len(col) > 0:
            for i in col:
                db.drop_collection(i)


    def main(database):

        client = pymongo.MongoClient("mongodb+srv://santhosh:santhosh1992@cluster0.idw4nqi.mongodb.net/?retryWrites=true&w=majority")
        db = client['temp']
        col = db.list_collection_names()

        if len(col) == 0:
            st.info("There is no data retrived from youtube")

        else:
            client = pymongo.MongoClient("mongodb+srv://santhosh:santhosh1992@cluster0.idw4nqi.mongodb.net/?retryWrites=true&w=majority")
            db = client['temp']
            col = db.list_collection_names()
            channel_name = col[0]

            # Now we get the channel name and access channel data
            data_youtube = {}
            col1 = db[channel_name]
            for i in col1.find():
                data_youtube.update(i)

            # verify channel name already exists in database
            list_collection_names = mongodb.list_collection_names(database)

            if channel_name not in list_collection_names:
                mongodb.data_storage(channel_name, database, data_youtube)
                st.success(
                    "The data has been successfully stored in the MongoDB database")
                st.balloons()
                mongodb.drop_temp_collection()

            else:
                st.warning(
                    "The data has already been stored in MongoDB database")
                option = st.radio('Do you want to overwrite the data currently stored?',
                                  ['Select one below', 'Yes', 'No'])

                if option == 'Yes':
                    client = pymongo.MongoClient("localhost:27017")
                    db = client[database]

                    # delete existing data
                    db[channel_name].drop()

                    # add new data
                    mongodb.data_storage(channel_name, database, data_youtube)
                    st.success(
                        "The data has been successfully overwritten and updated in MongoDB database")
                    st.balloons()
                    mongodb.drop_temp_collection()

                elif option == 'No':
                    mongodb.drop_temp_collection()
                    st.info("The data overwrite process has been skipped")


# In[169]:


class sql:

    def create_tables():

        client = psycopg2.connect(host='localhost',
                                user='postgres',
                                password='santhosh1992',
                                database='youtube')
        cursor = client.cursor()

        cursor.execute(f"""create table if not exists channel(
                                    channel_name		varchar(255),
                                    channel_id 			varchar(255) primary key,
                                    subscribers	int,
                                    total_videos	int,
                                    views		int,
                                    channel_description	text,
                                    upload_id			varchar(255));""")

        cursor.execute(f"""create table if not exists playlist(
                                    playlist_id		varchar(255) primary key,
                                    playlist_name	varchar(255),
                                    channel_id		varchar(255),
                                    upload_id		varchar(255));""")

        cursor.execute(f"""create table if not exists video(
                                    video_id			varchar(255) primary key,
                                    video_name			varchar(255),
                                    video_description	text,
                                    upload_id			varchar(255),
                                    tags				text,
                                    published_date		date,
                                    published_time		time,
                                    view_count			int,
                                    like_count			int,
                                    favourite_count		int,
                                    comment_count		int,
                                    duration			time,
                                    thumbnail			varchar(255),
                                    caption_status		varchar(255));""")

        cursor.execute(f"""create table if not exists comment(
                                    comment_id				varchar(255) primary key,
                                    comment_text			text,
                                    comment_author			varchar(255),
                                    comment_published_date	date,
                                    comment_published_time	time,
                                    video_id				varchar(255));""")

        client.commit()


    def list_channel_names():

        client = psycopg2.connect(host='localhost',
                                user='postgres',
                                password='santhosh1992',
                                database='youtube')
        cursor = client.cursor()
        cursor.execute("select channel_name from channel")
        s = cursor.fetchall()
        s = [i[0] for i in s]
        s.sort(reverse=False)
        return s


    def order_channel_names():

        s = sql.list_channel_names()

        if s == []:
            st.info("The SQL database is currently empty")

        else:
            st.subheader("List of channels in SQL database")
            c = 1
            for i in s:
                st.write(str(c) + ' - ' + i)
                c += 1


    def channel(database, channel_name):

        client = pymongo.MongoClient("mongodb+srv://santhosh:santhosh1992@cluster0.idw4nqi.mongodb.net/?retryWrites=true&w=majority")
        db = client[database]
        col = db[channel_name]

        data = []
        for i in col.find({}, {'_id': 0, 'channel': 1}):
            data.append(i['channel'])

        df = pd.DataFrame(data)
        df = df.reindex(columns=['channel_id', 'channel_name', 'subscribers', 'views', 'channel_description', 'upload_id'])
        df['subscribers'] = pd.to_numeric(df['subscribers'])
        df['views'] = pd.to_numeric(df['views'])
        
        return df


    def playlist(database, channel_name):

        client = pymongo.MongoClient("mongodb+srv://santhosh:santhosh1992@cluster0.idw4nqi.mongodb.net/?retryWrites=true&w=majority")         
        db = client[database]
        col = db[channel_name]

        data = []
        for i in col.find({}, {'_id': 0, 'playlist': 1}):
            data.extend(i['playlist'])

        df = pd.DataFrame(data)
        df = df.reindex(
            columns=['playlist_id', 'playlist_name', 'channel_id', 'upload_id'])
        return df


    def video(database, channel_name):

        client = pymongo.MongoClient("mongodb+srv://santhosh:santhosh1992@cluster0.idw4nqi.mongodb.net/?retryWrites=true&w=majority")
        db = client[database]
        col = db[channel_name]

        data = []
        for i in col.find({}, {'_id': 0, 'video': 1}):
            data.extend(i['video'])

        df = pd.DataFrame(data)
        df = df.reindex(columns=['video_id', 'video_name', 'video_description', 'upload_id',
                                 'tags', 'published_date', 'published_time', 'view_count',
                                 'like_count', 'favourite_count', 'comment_count', 'duration',
                                 'thumbnail', 'caption_status'])

        df['published_date'] = pd.to_datetime(df['published_date']).dt.date
        df['published_time'] = pd.to_datetime(df['published_time'], format='%H:%M:%S').dt.time
        df['view_count'] = pd.to_numeric(df['view_count'])
        df['like_count'] = pd.to_numeric(df['like_count'])
        df['favourite_count'] = pd.to_numeric(df['favourite_count'])
        df['comment_count'] = pd.to_numeric(df['comment_count'])
        df['duration'] = pd.to_datetime(df['duration'], format='%H:%M:%S').dt.time
        
        return df


    def comment(database, channel_name):
        client = pymongo.MongoClient("mongodb+srv://santhosh:santhosh1992@cluster0.idw4nqi.mongodb.net/?retryWrites=true&w=majority")
        db = client[database]
        col = db[channel_name]

        data = []
        for i in col.find({}, {'_id': 0, 'comment': 1}):
            data.extend(i['comment'][0])

        df = pd.DataFrame(data)
        df = df.reindex(columns=['comment_id', 'comment_text', 'comment_author',
                                 'comment_published_date', 'comment_published_time', 'video_id'])
        df['comment_published_date'] = pd.to_datetime(df['comment_published_date']).dt.date
        df['comment_published_time'] = pd.to_datetime(df['comment_published_time'], format='%H:%M:%S').dt.time
        
        return df


    def main(mdb_database, sql_database):

        # create table in sql
        sql.create_tables()

        # mongodb and sql channel names
        m = mongodb.list_collection_names(mdb_database)
        s = sql.list_channel_names()

        if s == m == []:
            st.info("Both Mongodb and SQL databases are currently empty")

        else:
            # mongodb and sql channel names
            mongodb.order_collection_names(mdb_database)
            sql.order_channel_names()

            # remaining channel name for migration
            list_mongodb_notin_sql = ['Select one']
            m = mongodb.list_collection_names(mdb_database)
            s = sql.list_channel_names()

            # verify channel name not in sql
            for i in m:
                if i not in s:
                    list_mongodb_notin_sql.append(i)

            # channel name for user selection
            option = st.selectbox('', list_mongodb_notin_sql)

            if option == 'Select one':
                col1, col2 = st.columns(2)
                with col1:
                    st.warning('Please select the channel')

            else:
                channel = sql.channel(sql_database, option)
                playlist = sql.playlist(sql_database, option)
                video = sql.video(sql_database, option)
                comment = sql.comment(sql_database, option)

                client = psycopg2.connect(host='localhost',
                                        user='postgres',
                                        password='santhosh1992',
                                        database='youtube')
                cursor = client.cursor()

                cursor.executemany(f"""insert into channel(channel_id, channel_name, subscribers,
                                        views, channel_description, upload_id) 
                                        values(%s,%s,%s,%s,%s,%s,%s)""", channel.values.tolist())

                cursor.executemany(f"""insert into playlist(playlist_id, playlist_name, channel_id, 
                                        upload_id) 
                                        values(%s,%s,%s,%s)""", playlist.values.tolist())

                cursor.executemany(f"""insert into video(video_id, video_name, video_description, 
                                        upload_id, tags, published_date, published_time, view_count, 
                                        like_count, favourite_count, comment_count, duration, thumbnail, 
                                        caption_status) 
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                                   video.values.tolist())

                cursor.executemany(f"""insert into comment(comment_id, comment_text, comment_author, 
                                        comment_published_date, comment_published_time, video_id) 
                                        values(%s,%s,%s,%s,%s,%s)""", comment.values.tolist())

                client.commit()
                st.success("Migrated Data Successfully to SQL Data Warehouse")
                st.balloons()
                client.close()


# In[170]:


class sql_queries:

    def q1_allvideoname_channelname():

        client_s = psycopg2.connect(host='localhost', 
                                user='postgres', 
                                password='santhosh1992', 
                                database='youtube')
        
        cursor = client_s.cursor()

        # using Inner Join to join the tables
        cursor.execute(f'''select video.video_name, channel.channel_name
                            from video
                            inner join playlist on playlist.upload_id = video.upload_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            group by video.video_id, channel.channel_id
                            order by channel.channel_name ASC''')
        
        s = cursor.fetchall()

        # add index for dataframe and set a column names
        i = [i for i in range(1, len(s) + 1)]
        data = pd.DataFrame(s, columns=['Video Names', 'Channel Names'], index=i)

        # add name for 'S.No'
        data = data.rename_axis('S.No')

        # index in center position of dataframe
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

        return data


    def q2_channelname_totalvideos():

        client_s = psycopg2.connect(host='localhost', 
                                user='postgres', 
                                password='santhosh1992', 
                                database='youtube')
        cursor = client_s.cursor()

        cursor.execute(f'''select distinct channel.channel_name, count(distinct video.video_id) as total
                        from video
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        group by channel.channel_id
                        order by total DESC''')
        
        s = cursor.fetchall()

        i = [i for i in range(1, len(s) + 1)]
        data = pd.DataFrame(s, columns=['Channel Names', 'Total Videos'], index=i)

        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

        return data


    def q3_mostviewvideos_channelname():

        client_s = psycopg2.connect(host='localhost', 
                                user='postgres', 
                                password='santhosh1992', 
                                database='youtube')
        cursor = client_s.cursor()

        cursor.execute(f'''select distinct video.video_name, video.view_count, channel.channel_name
                            from video
                            inner join playlist on playlist.upload_id = video.upload_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            order by video.view_count DESC
                            limit 10''')
        
        s = cursor.fetchall()

        i = [i for i in range(1, len(s) + 1)]
        data = pd.DataFrame(s, columns=['Video Names', 'Total Views', 'Channel Names'], index=i)

        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

        return data


    def q4_videonames_totalcomments():
            
            client_s = psycopg2.connect(host='localhost', 
                                    user='postgres', 
                                    password='santhosh1992', 
                                    database='youtube')
            cursor = client_s.cursor()

            cursor.execute(f'''select video.video_name, video.comment_count, channel.channel_name
                            from video
                            inner join playlist on playlist.upload_id = video.upload_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            group by video.video_id, channel.channel_name
                            order by video.comment_count DESC''')
            
            s = cursor.fetchall()

            i = [i for i in range(1, len(s) + 1)]
            data = pd.DataFrame(s, columns=['Video Names', 'Total Comments', 'Channel Names'], index=i)

            data = data.rename_axis('S.No')
            data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

            return data


    def q5_videonames_highestlikes_channelname():
            
            client_s = psycopg2.connect(host='localhost', 
                                    user='postgres', 
                                    password='santhosh1992', 
                                    database='youtube')
            cursor = client_s.cursor()

            cursor.execute(f'''select distinct video.video_name, channel.channel_name, video.like_count
                            from video
                            inner join playlist on playlist.upload_id = video.upload_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            where video.like_count = (select max(like_count) from video)''')
            
            s = cursor.fetchall()

            i = [i for i in range(1, len(s) + 1)]
            data = pd.DataFrame(s, columns=['Video Names', 'Channel Names', 'Most Likes'], index=i)

            data = data.reindex(columns=['Video Names', 'Most Likes', 'Channel Names'])
            data = data.rename_axis('S.No')
            data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

            return data


    def q6_videonames_totallikes_channelname():
            
            client_s = psycopg2.connect(host='localhost', 
                                    user='postgres', 
                                    password='santhosh1992', 
                                    database='youtube')
            cursor = client_s.cursor()

            cursor.execute(f'''select distinct video.video_name, video.like_count, channel.channel_name
                            from video
                            inner join playlist on playlist.upload_id = video.upload_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            group by video.video_id, channel.channel_id
                            order by video.like_count DESC''')
            
            s = cursor.fetchall()

            i = [i for i in range(1, len(s) + 1)]
            data = pd.DataFrame(s, columns=['Video Names', 'Total Likes', 'Channel Names'], index=i)

            data = data.rename_axis('S.No')
            data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
            
            return data


    def q7_channelnames_totalviews():
            
            client_s = psycopg2.connect(host='localhost', 
                                    user='postgres', 
                                    password='santhosh1992', 
                                    database='youtube')
            cursor = client_s.cursor()

            cursor.execute(f'''select channel_name, channel_views from channel
                            order by channel_views DESC''')
            
            s = cursor.fetchall()

            i = [i for i in range(1, len(s) + 1)]
            data = pd.DataFrame(s, columns=['Channel Names', 'Total Views'], index=i)
            
            data = data.rename_axis('S.No')
            data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

            return data


    def q8_channelnames_releasevideos(year):
            
            client_s = psycopg2.connect(host='localhost', 
                                    user='postgres', 
                                    password='santhosh1992', 
                                    database='youtube')
            cursor = client_s.cursor()

            cursor.execute(f"""select distinct channel.channel_name, count(distinct video.video_id) as total
                            from video
                            inner join playlist on playlist.upload_id = video.upload_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            where extract(year from video.published_date) = '{year}'
                            group by channel.channel_id
                            order by total DESC""")
            
            s = cursor.fetchall()

            i = [i for i in range(1, len(s) + 1)]
            data = pd.DataFrame(s, columns=['Channel Names', 'Published Videos'], index=i)

            data = data.rename_axis('S.No')
            data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
            
            return data


    def q9_channelnames_avgvideoduration():
            
            client_s = psycopg2.connect(host='localhost', 
                                    user='postgres', 
                                    password='santhosh1992', 
                                    database='youtube')
            cursor = client_s.cursor()

            cursor.execute(f'''select channel.channel_name, substring(cast(avg(video.duration) as varchar), 1, 8) as average
                            from video
                            inner join playlist on playlist.upload_id = video.upload_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            group by channel.channel_id
                            order by average DESC''')
            
            s = cursor.fetchall()

            i = [i for i in range(1, len(s) + 1)]
            data = pd.DataFrame(s, columns=['Channel Names', 'Average Video Duration'], index=i)
            
            data = data.rename_axis('S.No')
            data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

            return data


    def q10_videonames_channelnames_mostcomments():
            
            client_s = psycopg2.connect(host='localhost', 
                                    user='postgres', 
                                    password='santhosh1992', 
                                    database='youtube')
            cursor = client_s.cursor()

            cursor.execute(f'''select video.video_name, video.comment_count, channel.channel_name
                            from video
                            inner join playlist on playlist.upload_id = video.upload_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            group by video.video_id, channel.channel_name
                            order by video.comment_count DESC
                            limit 1''')
            
            s = cursor.fetchall()

            i = [i for i in range(1, len(s) + 1)]
            data = pd.DataFrame(s, columns=['Video Names', 'Channel Names', 'Total Comments'], index=i)

            data = data.rename_axis('S.No')
            data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

            return data


    def main():
        st.subheader('Select the Query below')
        q1 = 'Q1-What are the names of all the videos and their corresponding channels?'
        q2 = 'Q2-Which channels have the most number of videos, and how many videos do they have?'
        q3 = 'Q3-What are the top 10 most viewed videos and their respective channels?'
        q4 = 'Q4-How many comments were made on each video with their corresponding video names?'
        q5 = 'Q5-Which videos have the highest number of likes with their corresponding channel names?'
        q6 = 'Q6-What is the total number of likes for each video with their corresponding video names?'
        q7 = 'Q7-What is the total number of views for each channel with their corresponding channel names?'
        q8 = 'Q8-What are the names of all the channels that have published videos in the particular year?'
        q9 = 'Q9-What is the average duration of all videos in each channel with corresponding channel names?'
        q10 = 'Q10-Which videos have the highest number of comments with their corresponding channel names?'

        query_option = st.selectbox(
            '', ['Select One', q1, q2, q3, q4, q5, q6, q7, q8, q9, q10])

        if query_option == q1:
            st.dataframe(sql_queries.q1_allvideoname_channelname())

        elif query_option == q2:
            st.dataframe(sql_queries.q2_channelname_totalvideos())

        elif query_option == q3:
            st.dataframe(sql_queries.q3_mostviewvideos_channelname())

        elif query_option == q4:
            st.dataframe(sql_queries.q4_videonames_totalcomments())

        elif query_option == q5:
            st.dataframe(sql_queries.q5_videonames_highestlikes_channelname())

        elif query_option == q6:
            st.dataframe(sql_queries.q6_videonames_totallikes_channelname())

        elif query_option == q7:
            st.dataframe(sql_queries.q7_channelnames_totalviews())

        elif query_option == q8:
            year = st.text_input('Enter the year')
            submit = st.button('Submit')
            if submit:
                st.dataframe(sql_queries.q8_channelnames_releasevideos(year))

        elif query_option == q9:
            st.dataframe(sql_queries.q9_channelnames_avgvideoduration())

        elif query_option == q10:
            st.dataframe(
                sql_queries.q10_videonames_channelnames_mostcomments())


# In[171]:


streamlit_config()
st.write('')
st.write('')


# In[172]:


with st.sidebar:
    image_url = 'https://raw.githubusercontent.com/SanthoshDhamodharan/YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit/main/youtube_logo.JPG'
    st.image(image_url, use_column_width=True)

    option = option_menu(menu_title='', options=['Data Retrive from YouTube API', 'Store data to MongoDB',
                                                 'Migrating Data to SQL', 'SQL Queries', 'Exit'],
                         icons=['youtube', 'database-add', 'database-fill-check', 'list-task', 'pencil-square', 'sign-turn-right-fill'])


# In[173]:


if option == 'Data Retrive from YouTube API':

    try:

        # get input from user
        col1, col2 = st.columns(2, gap='medium')
        with col1:
            channel_id = st.text_input("Enter Channel ID: ")
        with col2:
            api_key = st.text_input("Enter Your API Key:", type='password')
        
        submit = st.button(label='Submit')

        if submit and option is not None:

            api_service_name = "youtube"
            api_version = "v3"
            youtube = googleapiclient.discovery.build(api_service_name,
                                                    api_version, developerKey=api_key)

            data = {}
            final = youtube_extract.main(channel_id)
            data.update(final)
            channel_name = data['channel']['channel_name']

            mongodb.drop_temp_collection()
            mongodb.data_storage(channel_name=channel_name,
                                 database='temp', data=final)

            # display the sample data in streamlit
            st.json(youtube_extract.display_sample_data(channel_id))
            st.success('Retrived data from YouTube successfully')
            st.balloons()

    except:
        col1,col2 = st.columns([0.45,0.55])
        with col1:
            st.warning("Please enter the valid Channel ID and API key")
            
elif option == 'Store data to MongoDB':
    mongodb.main('project_youtube')
    
elif option == 'Migrating Data to SQL':
    sql.main(mdb_database='project_youtube', sql_database='youtube')
    
elif option == 'SQL Queries':
    s1 = sql.list_channel_names()
    if s1 == []:
        st.info("The SQL database is currently empty")
    else:
        sql_queries.main()
        
elif option == 'Exit':
    mongodb.drop_temp_collection()
    st.write('')
    st.write('')
    st.success('Thank you for your time. Exiting the application')
    st.balloons()

