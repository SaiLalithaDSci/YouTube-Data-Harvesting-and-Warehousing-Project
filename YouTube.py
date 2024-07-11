# Packages to import
import streamlit as st
from googleapiclient.discovery import build
import psycopg2
import pandas as pd
import datetime

# API key connection
def api_connect():
    api_id = "your_api_key" # change with your api key
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_id)
    return youtube

youtube = api_connect()

# Scraping Functions

# Channel info
def channel_details_scrape(channel_id):
    request = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id)
    response = request.execute()
    for i in response['items']:
        channel_scrape_details = dict(channel_title=i['snippet']['title'],
                                      channel_id=i['id'],
                                      channel_description=i['snippet']['description'],
                                      channel_published_date=i['snippet']['publishedAt'],
                                      channel_subscribers=i['statistics']['subscriberCount'],
                                      channel_videos=i['statistics']['videoCount'],
                                      channel_views=i['statistics']['viewCount'],
                                      channel_playList_id=i['contentDetails']['relatedPlaylists']['uploads'])
    return channel_scrape_details


# Video ids scraping
def video_ids_scraping(channel_id):
    video_ids = []
    response = youtube.channels().list(part='contentDetails', id=channel_id).execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        response1 = youtube.playlistItems().list(part='snippet',
                                                 playlistId=playlist_id,
                                                 maxResults=50,
                                                 pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

# Getting video info
def video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id)
        response = request.execute()
        for item in response['items']:
            data = dict(ChannelName=item['snippet']['channelTitle'],
                        ChannelId=item['snippet']['channelId'],
                        VideoId=item['id'],
                        Title=item['snippet']['title'],
                        Tags=item['snippet'].get('tags'),
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Description=item['snippet'].get('description'),
                        PublishedDate=item['snippet']['publishedAt'],
                        Duration=item['contentDetails']['duration'],
                        Views=item['statistics'].get('viewCount'),
                        Likes=item['statistics'].get('likeCount'),
                        Comments=item['statistics'].get('commentCount'),
                        FavouriteCount=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        CaptionStatus=item['contentDetails']['caption'])
            video_data.append(data)
    return video_data

# Comment info scraping
def comment_scraping(videoids):
    commentData = []
    try:
        for videoid in videoids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=videoid,
                maxResults=50
            )
            response = request.execute()
            for item in response['items']:
                data_comment = dict(Comment_id=item['snippet']['topLevelComment']['id'],
                                    Video_Id_Comment=item['snippet']['topLevelComment']['snippet']['videoId'],
                                    Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                    Comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                    Comment_publishedAt=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                commentData.append(data_comment)
    except:
        pass
    return commentData

# Playlist info
def playlist_details_scraping(channel_id):
    all_playlist_info = []
    next_page_token = None
    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response['items']:
            playlist_id_dict = dict(id_of_playlist=item['id'],
                                    title_of_playlist=item['snippet']['title'],
                                    channelId_of_playlist=item['snippet']['channelId'],
                                    publishedAt_of_playlist=item['snippet']['publishedAt'],
                                    channelName_of_playlist=item['snippet']['channelTitle'],
                                    no_of_videos=item['contentDetails']['itemCount'])
            all_playlist_info.append(playlist_id_dict)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return all_playlist_info

# PostgreSQL connection
def connect_to_db():
    my_db = psycopg2.connect(host='localhost',
                             user='postgres',
                             password='your_password', # change with your password
                             database='YouTube_Data',
                             port='5432')
    return my_db

# Function for compiling all details of channels
def allDataOfChannel(channel_id):
    CH_details = channel_details_scrape(channel_id)
    PL_details = playlist_details_scraping(channel_id)
    VD_Ids_details = video_ids_scraping(channel_id)
    VD_details = video_info(VD_Ids_details)
    CM_details = comment_scraping(VD_Ids_details)
    
    # Insert into PostgreSQL
    my_db = connect_to_db()
    cursor = my_db.cursor()

    # Channel table
    create_channel_query = '''CREATE TABLE IF NOT EXISTS channels(
                                channel_title VARCHAR(100),
                                channel_id VARCHAR(100) PRIMARY KEY,
                                channel_description TEXT,
                                channel_published_date TIMESTAMP,
                                channel_subscribers BIGINT,
                                channel_videos INT,
                                channel_views BIGINT,
                                channel_playList_id VARCHAR(100))'''
    cursor.execute(create_channel_query)

    insert_channel_query = '''INSERT INTO channels(
                                channel_title,
                                channel_id,
                                channel_description,
                                channel_published_date,
                                channel_subscribers,
                                channel_videos,
                                channel_views,
                                channel_playList_id)
                              VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                              ON CONFLICT (channel_id) DO NOTHING'''
    channel_values = (CH_details['channel_title'], CH_details['channel_id'], CH_details['channel_description'],
                      CH_details['channel_published_date'], CH_details['channel_subscribers'], CH_details['channel_videos'],
                      CH_details['channel_views'], CH_details['channel_playList_id'])
    cursor.execute(insert_channel_query, channel_values)

    # Playlist table
    create_playlist_query = '''CREATE TABLE IF NOT EXISTS playlists(
                                id_of_playlist VARCHAR(100) PRIMARY KEY,
                                title_of_playlist VARCHAR(100),
                                channelId_of_playlist VARCHAR(100),
                                publishedAt_of_playlist TIMESTAMP,
                                channelName_of_playlist VARCHAR(100),
                                no_of_videos INT)'''
    cursor.execute(create_playlist_query)

    insert_playlist_query = '''INSERT INTO playlists(
                                id_of_playlist,
                                title_of_playlist,
                                channelId_of_playlist,
                                publishedAt_of_playlist,
                                channelName_of_playlist,
                                no_of_videos)
                              VALUES(%s, %s, %s, %s, %s, %s)
                              ON CONFLICT (id_of_playlist) DO NOTHING'''
    for playlist in PL_details:
        playlist_values = (playlist['id_of_playlist'], playlist['title_of_playlist'], playlist['channelId_of_playlist'],
                           playlist['publishedAt_of_playlist'], playlist['channelName_of_playlist'], playlist['no_of_videos'])
        cursor.execute(insert_playlist_query, playlist_values)

    # Video table
    create_video_query = '''CREATE TABLE IF NOT EXISTS videos(
                            ChannelName VARCHAR(100),
                            ChannelId VARCHAR(100),
                            VideoId VARCHAR(100) PRIMARY KEY,
                            Title VARCHAR(200),
                            Tags TEXT,
                            Thumbnail VARCHAR(150),
                            Description TEXT,
                            PublishedDate TIMESTAMP,
                            Duration INTERVAL,
                            Views BIGINT,
                            Likes BIGINT,
                            Comments INT,
                            FavouriteCount INT,
                            Definition VARCHAR(50),
                            CaptionStatus VARCHAR(50))'''
    cursor.execute(create_video_query)

    insert_video_query = '''INSERT INTO videos(
                            ChannelName,
                            ChannelId,
                            VideoId,
                            Title,
                            Tags,
                            Thumbnail,
                            Description,
                            PublishedDate,
                            Duration,
                            Views,
                            Likes,
                            Comments,
                            FavouriteCount,
                            Definition,
                            CaptionStatus)
                          VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                          ON CONFLICT (VideoId) DO NOTHING'''
    for video in VD_details:
        video_values = (video['ChannelName'], video['ChannelId'], video['VideoId'], video['Title'], video['Tags'], video['Thumbnail'],
                        video['Description'], video['PublishedDate'], video['Duration'], video['Views'], video['Likes'],
                        video['Comments'], video['FavouriteCount'], video['Definition'], video['CaptionStatus'])
        cursor.execute(insert_video_query, video_values)

    # Comment table
    create_comment_query = '''CREATE TABLE IF NOT EXISTS comments(
                                Comment_id VARCHAR(100) PRIMARY KEY,
                                Video_Id_Comment VARCHAR(100),
                                Comment_text TEXT,
                                Comment_author VARCHAR(100),
                                Comment_publishedAt TIMESTAMP)'''
    cursor.execute(create_comment_query)

    insert_comment_query = '''INSERT INTO comments(
                                Comment_id,
                                Video_Id_Comment,
                                Comment_text,
                                Comment_author,
                                Comment_publishedAt)
                            VALUES(%s, %s, %s, %s, %s)
                            ON CONFLICT (Comment_id) DO NOTHING'''
    for comment in CM_details:
        comment_values = (comment['Comment_id'], comment['Video_Id_Comment'], comment['Comment_text'],
                          comment['Comment_author'], comment['Comment_publishedAt'])
        cursor.execute(insert_comment_query, comment_values)

    my_db.commit()
    cursor.close()
    my_db.close()

# Function to Update details of given Channel
def update_channel(channel_id):

    CH_details = channel_details_scrape(channel_id)
    PL_details = playlist_details_scraping(channel_id)
    VD_Ids_details = video_ids_scraping(channel_id)
    VD_details = video_info(VD_Ids_details)
    CM_details = comment_scraping(VD_Ids_details)
    
    # Insert into PostgreSQL
    my_db = connect_to_db()
    cursor = my_db.cursor()
    

    # Insert or update channels
    insert_channel_query = '''
        INSERT INTO channels(
            channel_title,
            channel_id,
            channel_description,
            channel_published_date,
            channel_subscribers,
            channel_videos,
            channel_views,
            channel_playList_id
        ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (channel_id) DO UPDATE SET
            channel_title = EXCLUDED.channel_title,
            channel_description = EXCLUDED.channel_description,
            channel_published_date = EXCLUDED.channel_published_date,
            channel_subscribers = EXCLUDED.channel_subscribers,
            channel_videos = EXCLUDED.channel_videos,
            channel_views = EXCLUDED.channel_views,
            channel_playList_id = EXCLUDED.channel_playList_id
    '''
    channel_values = (
        CH_details['channel_title'], 
        CH_details['channel_id'], 
        CH_details['channel_description'],
        CH_details['channel_published_date'], 
        CH_details['channel_subscribers'], 
        CH_details['channel_videos'],
        CH_details['channel_views'], 
        CH_details['channel_playList_id']
    )
    cursor.execute(insert_channel_query, channel_values)

    # Insert or update playlists
    insert_playlist_query = '''
        INSERT INTO playlists(
            id_of_playlist,
            title_of_playlist,
            channelId_of_playlist,
            publishedAt_of_playlist,
            channelName_of_playlist,
            no_of_videos
        ) VALUES(%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_of_playlist) DO UPDATE SET
            title_of_playlist = EXCLUDED.title_of_playlist,
            channelId_of_playlist = EXCLUDED.channelId_of_playlist,
            publishedAt_of_playlist = EXCLUDED.publishedAt_of_playlist,
            channelName_of_playlist = EXCLUDED.channelName_of_playlist,
            no_of_videos = EXCLUDED.no_of_videos
    '''
    for playlist in PL_details:
        playlist_values = (
            playlist['id_of_playlist'], 
            playlist['title_of_playlist'], 
            playlist['channelId_of_playlist'],
            playlist['publishedAt_of_playlist'], 
            playlist['channelName_of_playlist'], 
            playlist['no_of_videos']
        )
        cursor.execute(insert_playlist_query, playlist_values)

    # Insert or update videos
    insert_video_query = '''
        INSERT INTO videos(
            ChannelName,
            ChannelId,
            VideoId,
            Title,
            Tags,
            Thumbnail,
            Description,
            PublishedDate,
            Duration,
            Views,
            Likes,
            Comments,
            FavouriteCount,
            Definition,
            CaptionStatus
        ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (VideoId) DO UPDATE SET
            ChannelName = EXCLUDED.ChannelName,
            ChannelId = EXCLUDED.ChannelId,
            Title = EXCLUDED.Title,
            Tags = EXCLUDED.Tags,
            Thumbnail = EXCLUDED.Thumbnail,
            Description = EXCLUDED.Description,
            PublishedDate = EXCLUDED.PublishedDate,
            Duration = EXCLUDED.Duration,
            Views = EXCLUDED.Views,
            Likes = EXCLUDED.Likes,
            Comments = EXCLUDED.Comments,
            FavouriteCount = EXCLUDED.FavouriteCount,
            Definition = EXCLUDED.Definition,
            CaptionStatus = EXCLUDED.CaptionStatus
    '''
    for video in VD_details:
        video_values = (
            video['ChannelName'], video['ChannelId'], video['VideoId'], video['Title'], video['Tags'], video['Thumbnail'],
            video['Description'], video['PublishedDate'], video['Duration'], video['Views'], video['Likes'],
            video['Comments'], video['FavouriteCount'], video['Definition'], video['CaptionStatus']
        )
        cursor.execute(insert_video_query, video_values)

    # Insert or update comments
    insert_comment_query = '''
        INSERT INTO comments(
            Comment_id,
            Video_Id_Comment,
            Comment_text,
            Comment_author,
            Comment_publishedAt
        ) VALUES(%s, %s, %s, %s, %s)
        ON CONFLICT (Comment_id) DO UPDATE SET
            Video_Id_Comment = EXCLUDED.Video_Id_Comment,
            Comment_text = EXCLUDED.Comment_text,
            Comment_author = EXCLUDED.Comment_author,
            Comment_publishedAt = EXCLUDED.Comment_publishedAt
    '''
    for comment in CM_details:
        comment_values = (
            comment['Comment_id'], comment['Video_Id_Comment'], comment['Comment_text'],
            comment['Comment_author'], comment['Comment_publishedAt']
        )
        cursor.execute(insert_comment_query, comment_values)

    my_db.commit()
    cursor.close()
    my_db.close()

# Function to check the existence of channel
def channel_exists(cursor, channel_id):
    check_query = "SELECT COUNT(*) FROM channels WHERE channel_id = %s"
    cursor.execute(check_query, (channel_id,))
    return cursor.fetchone()[0] > 0


# Streamlit app
def main():
    with st.sidebar:
        st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
        st.header("KEY SKILL TAKEAWAYS")
        st.caption("PYTHON SCRIPTING: Automate tasks, schedule data collection, and streamline the ETL process with Python scripts.")
        st.caption("DATA HARVESTING: Fetch data from YouTube API.")
        st.caption("DATA CLEANING: Clean and preprocess the data.")
        st.caption("DATA STORAGE: Store the data in a structured PostgreSQL database.")
        st.caption("DATA ANALYSIS: Analyze the data using SQL and visualization tools.")
        st.caption("APPLICATION DEVELOPMENT: Create an interactive application using Streamlit.")
        st.caption("DEPLOYMENT AND MAINTENANCE: Deploy and maintain the application, ensuring scalability and security.")

    channel_id = st.text_input("Enter the Channel Id of the Channel you want to get information of:")
    scrape=st.button("Collect and store the data of the given channel!")

    my_db = connect_to_db()
    cursor = my_db.cursor()
    
    if channel_exists(cursor, channel_id):
        st.warning("This channel ID exists.")
        delete = st.button("Delete this channel's existing details!")
        update=st.button("Update the details of this channel!")
        if delete:
            try:
                cursor = my_db.cursor()

                # Delete from comments where video_id is in videos with the given channel_id
                cursor.execute("""DELETE FROM comments WHERE Video_Id_Comment IN (SELECT VideoId FROM videos WHERE ChannelId = %s)""", (channel_id,))
                
                # Delete from videos where channel_id matches
                cursor.execute("DELETE FROM videos WHERE ChannelId = %s", (channel_id,))
                
                # Delete from playlists where channelId_of_playlist matches
                cursor.execute("DELETE FROM playlists WHERE channelId_of_playlist = %s", (channel_id,))
                
                # Finally, delete from channels where channel_id matches
                cursor.execute("DELETE FROM channels WHERE channel_id = %s", (channel_id,))

                # Commit the changes after successful deletion
                my_db.commit()
                st.success("Data deleted successfully.")
            except Exception as e:
                my_db.rollback()  # Rollback changes in case of error
                st.error(f"An error occurred: {e}")
            finally:
                cursor.close()
                my_db.close()

        elif update:
            update_channel(channel_id)
            st.success("Data updated successfully!")

    elif scrape:
        allDataOfChannel(channel_id)
        st.success("Data inserted successfully!")
        

    # Radio button for data display
    data_display = st.radio("Select the data you want to display:",
                            ("Channel Details", "Playlist Details", "Video Details", "Comment Details"))

    # SQL Connection and Querying
    my_db = connect_to_db()
    cursor = my_db.cursor()

    if data_display == "Channel Details":
        Query1 = '''SELECT * FROM channels'''
        cursor.execute(Query1)
        T1 = cursor.fetchall()
        dfT1 = pd.DataFrame(T1, columns=["Channel Title", "Channel ID", "Description", "Published Date",
                                         "Subscribers", "Video Count", "View Count", "Playlist ID"])
        st.write(dfT1)

    elif data_display == "Playlist Details":
        Query2 = '''SELECT * FROM playlists'''
        cursor.execute(Query2)
        T2 = cursor.fetchall()
        dfT2 = pd.DataFrame(T2, columns=["Playlist ID", "Title", "Channel ID", "Published Date",
                                         "Channel Name", "Video Count"])
        st.write(dfT2)

    elif data_display == "Video Details":
        Query3 = '''SELECT * FROM videos'''
        cursor.execute(Query3)
        T3 = cursor.fetchall()
        dfT3 = pd.DataFrame(T3, columns=["Channel Name", "Channel ID", "Video ID", "Title", "Tags",
                                         "Thumbnail", "Description", "Published Date", "Duration",
                                         "Views", "Likes", "Comments", "Favourite Count", "Definition", "Caption Status"])
        st.write(dfT3)

    elif data_display == "Comment Details":
        Query4 = '''SELECT * FROM comments'''
        cursor.execute(Query4)
        T4 = cursor.fetchall()
        dfT4 = pd.DataFrame(T4, columns=["Comment ID", "Video ID", "Comment Text", "Author", "Published Date"])
        st.write(dfT4)

    # Select box for queries
    Query_Questions = st.selectbox("Select the question!", ("1. Names of all the Channel and their videos.",
                                                           "2. Channels that have most number of videos and their counts.",
                                                           "3. 10 most viewed videos and their channel names.",
                                                           "4. Number of comments in each video and their video names.",
                                                           "5. Videos having highest like count and their channel names.",
                                                           "6. Number of likes of all videos and their channel name.",
                                                           "7. View counts of each channel and their channel names.",
                                                           "8. Names of the channels that have published videos in year 2022.",
                                                           "9. Average duration of all videos in each channel and the channel names.",
                                                           "10. Videos with highest comment count and their channel names."))

    # Querys
    # Question 1
    if Query_Questions == "1. Names of all the Channel and their videos.":
        Query1 = '''select title as videos, ChannelName as channelname from videos'''
        cursor.execute(Query1)
        T1 = cursor.fetchall()
        dfT1 = pd.DataFrame(T1, columns=["video title", "channel name"])
        st.write(dfT1)

    # Question 2
    elif Query_Questions == "2. Channels that have most number of videos and their counts.":
        Query2 = '''select channel_title as channelname, channel_videos as no_videos from channels
                    order by channel_videos desc'''
        cursor.execute(Query2)
        T2 = cursor.fetchall()
        dfT2 = pd.DataFrame(T2, columns=["channel name", "no of videos"])
        st.write(dfT2)

    # Question 3
    elif Query_Questions == "3. 10 most viewed videos and their channel names.":
        Query3 = '''select title as videos, Views as view_count, ChannelName as channelname from videos
                    where Views is not null order by Views desc limit 10'''
        cursor.execute(Query3)
        T3 = cursor.fetchall()
        dfT3 = pd.DataFrame(T3, columns=["video title", "view count", "channel name"])
        st.write(dfT3)

    # Question 4
    elif Query_Questions == "4. Number of comments in each video and their video names.":
        Query4 = '''select title as videos, Comments as comment_count from videos'''
        cursor.execute(Query4)
        T4 = cursor.fetchall()
        dfT4 = pd.DataFrame(T4, columns=["video title", "no of comments"])
        st.write(dfT4)

    # Question 5
    elif Query_Questions == "5. Videos having highest like count and their channel names.":
        Query5 = '''select title as videos, Likes as like_count, ChannelName as channelname from videos
                    where Likes is not null order by Likes desc'''
        cursor.execute(Query5)
        T5 = cursor.fetchall()
        dfT5 = pd.DataFrame(T5, columns=["video title", "no of likes", "channel name"])
        st.write(dfT5)

    # Question 6
    elif Query_Questions == "6. Number of likes of all videos and their channel name.":
        Query6 = '''select title as videos, Likes as like_count, ChannelName as channelname from videos'''
        cursor.execute(Query6)
        T6 = cursor.fetchall()
        dfT6 = pd.DataFrame(T6, columns=["video title", "no of likes", "channel name"])
        st.write(dfT6)

    # Question 7
    elif Query_Questions == "7. View counts of each channel and their channel names.":
        Query7 = '''select channel_title as channel, channel_views as view_count from channels'''
        cursor.execute(Query7)
        T7 = cursor.fetchall()
        dfT7 = pd.DataFrame(T7, columns=["channel name", "view count"])
        st.write(dfT7)

    # Question 8
    elif Query_Questions == "8. Names of the channels that have published videos in year 2022.":
        Query8 = '''select ChannelName as channelname, title as videos, PublishedDate as video_released_on from videos
                    where extract(year from PublishedDate) = 2022'''
        cursor.execute(Query8)
        T8 = cursor.fetchall()
        dfT8 = pd.DataFrame(T8, columns=["channel name", "video name", "release date"])
        st.write(dfT8)

    # Question 9
    elif Query_Questions == "9. Average duration of all videos in each channel and the channel names.":
        Query9 = '''select ChannelName as channelname, AVG(Duration) as average_duration_seconds from videos
                    group by ChannelName'''
        cursor.execute(Query9)
        T9 = cursor.fetchall()

        t9 = []
        for row in T9:
            channel_name = row[0]
            average_duration_seconds = row[1]

            # Convert average_duration_seconds to total seconds
            total_seconds = average_duration_seconds.total_seconds()

            # Calculate hours and minutes from total seconds
            hours = int(total_seconds // 3600)
            minutes = int((total_seconds % 3600) // 60)

            # Construct the average duration string
            avg_duration = f"{hours} hours {minutes} minutes"

            t9.append({"channelname": channel_name, "avgduration": avg_duration})

        dfT9 = pd.DataFrame(t9)
        st.write(dfT9)

    # Question 10
    elif Query_Questions == "10. Videos with highest comment count and their channel names.":
        Query10 = '''select title as videos, Comments as comment_count, ChannelName as channelname from videos
                    where Comments is not null order by Comments desc'''
        cursor.execute(Query10)
        T10 = cursor.fetchall()
        dfT10 = pd.DataFrame(T10, columns=["video title", "comment count", "channel name"])
        st.write(dfT10)

    cursor.close()
    my_db.close()

if __name__ == "__main__":
    main()

############################################################################################################################
# CHANNEL IDs - Sample #
###########################################################################################################################
#UCOEXdtfmcbGuEXS2BFKrakA - milktpapi - DONE
#UCPPiXnNKnaIP7RG9T3NIgvA - tiffany design studio - DONE
#UCZUUZFex6AaIU4QTopFudYA - growwithjo - DONE
#UCb_gdTMC2xeuO1p8lKkZHHQ - forestella - DONE
#UCBGW2LRJII6pabgBuPrOKjw - annika hinds - DONE
#UCwr-evhuzGZgDFrq_1pLt_A - error makes clever - DONE
#UCb7q-fXh21T-pq02dC9H3xg - one pact - DONE
#UCayvNWo8eZS8HJL2TSV_x7w - kennysong - DONE
#UC3eHNTyqjQLkZcrye3tXaqQ - pickles and wine - DONE
#UC1v4DD_Ma63_wzHx4jAGklA - william seng - DONE
###########################################################################################################################
