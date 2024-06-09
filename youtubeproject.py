from googleapiclient.discovery import build 
import pymongo 
import psycopg2
import pandas as pd 
import streamlit as st 
#api key connection
def api_connect():
    api_id="AIzaSyDXV9XR6ahNlW7bv1BzEAeitILIQW7x6lM"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name, api_version, developerKey=api_id)
    return youtube
youtube=api_connect()

#channel info
def channel_details_scrape(channel_id):
  request = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id)
  response = request.execute()
  #for loop for retrieval of data
  for i in response['items']:
    channel_scrape_details=dict(channel_title=i['snippet']['title'],
                               channel_id=i['id'],
                               channel_description=i['snippet']['description'],
                               channel_published_date=i['snippet']['publishedAt'],
                               channel_subscribers=i['statistics']['subscriberCount'],
                               channel_videos=i['statistics']['videoCount'],
                               channel_views=i['statistics']['viewCount'],
                               channel_playList_id=i['contentDetails']['relatedPlaylists']['uploads'])
  return channel_scrape_details

#video ids scraping
def video_ids_scraping(channel_id):
  video_ids=[]
  response=youtube.channels().list(part='contentDetails', id=channel_id).execute()
  playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  next_page_token=None
  #while loop
  while True:
    response1=youtube.playlistItems().list(part='snippet',
                                              playlistId=playlist_id,
                                              maxResults=50,
                                              pageToken=next_page_token).execute()
  #for loop
    for i in range(len(response1['items'])):
      video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
    next_page_token=response1.get('nextPageToken')
    if next_page_token is None:
      break
  return video_ids

#getting video info
def video_info(video_ids):
    video_data=[]
    
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id)
        response=request.execute()
#for loop to get video info of all video ids
        for item in response['items']:
            data=dict(ChannelName=item['snippet']['channelTitle'],
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

#comment info scraping
def comment_scraping(videoids):
    commentData=[]
    try:
        for videoid in videoids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=videoid,
                maxResults=50
            )
            response=request.execute()
                
            for item in response['items']:
                data_comment=dict(Comment_id=item['snippet']['topLevelComment']['id'],
                                Video_Id_Comment=item['snippet']['topLevelComment']['snippet']['videoId'],
                                Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                Comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                Comment_publishedAt=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                commentData.append(data_comment)
    except:
        pass
    return commentData

#Playlist info
def playlist_details_scraping(channel_id):
        all_playlist_info=[]
        next_page_token=None
        
        while True:
                request=youtube.playlists().list(
                        part="snippet,contentDetails",
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response = request.execute()
                #for loop
                for item in response['items']:
                        playlist_id_dict=dict(id_of_playlist=item['id'],
                                              title_of_playlist=item['snippet']['title'],
                                              channelId_of_playlist=item['snippet']['channelId'],
                                              publishedAt_of_playlist=item['snippet']['publishedAt'],
                                              channelName_of_playlist=item['snippet']['channelTitle'],
                                              no_of_videos=item['contentDetails']['itemCount'])
                        all_playlist_info.append(playlist_id_dict)
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return all_playlist_info

#mongoDB connection and transfering data
#ZkICk3H4cnoydxLI=password mongodb  #mongodb+srv://sai27lalitha:ZkICk3H4cnoydxLI@cluster0.mexwcxo.mongodb.net/
#postgresql: password=sailalitha, port:5432
client=pymongo.MongoClient("mongodb+srv://sai27lalitha:ZkICk3H4cnoydxLI@cluster0.mexwcxo.mongodb.net/")
db=client["YouTube_DataBase"]

#function for compiling all details of channels and inserting into mongoDb
def allDataOfChannel(channel_id):
    CH_details=channel_details_scrape(channel_id)
    PL_details=playlist_details_scraping(channel_id)
    VD_Ids_details=video_ids_scraping(channel_id)
    VD_details=video_info(VD_Ids_details)
    CM_details=comment_scraping(VD_Ids_details)

    collection1=db['Channel_Details']
    collection1.insert_one({'Channel_information':CH_details,'Playlist_information':PL_details,
                            'Videos_information':VD_details,'Comments_information':CM_details})
    
    return "UPLOADED"

#SQL connection and table creation 
# #CHANNEL TABLE

def channelsTable(singleChannelName):
    my_db=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='sailalitha',
                        database='YouTube_Data',
                        port='5432')

    cursor=my_db.cursor()

    create_query='''create table if not exists channels(channel_title varchar(100),
                                                        channel_id varchar(100) primary key,
                                                        channel_description text,
                                                        channel_published_date timestamp,
                                                        channel_subscribers bigint,
                                                        channel_videos int,
                                                        channel_views bigint,
                                                        channel_playList_id varchar(100))'''
    cursor.execute(create_query)
    my_db.commit()


    #mongodb to sql 
    #dataframe creation then pulling into sql

    CHlist=[]
    db=client['YouTube_DataBase']
    collection1=db['Channel_Details']

    for CHdata in collection1.find({"Channel_information.channel_title": singleChannelName},{"_id":0}):
        CHlist.append(CHdata["Channel_information"])

    dfSCH=pd.DataFrame(CHlist)

    for index,row in dfSCH.iterrows():
        insert_query='''insert into channels(channel_title ,
                                            channel_id,
                                            channel_description,
                                            channel_published_date,
                                            channel_subscribers,
                                            channel_videos,
                                            channel_views,
                                            channel_playList_id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s)'''
        
        values=(row['channel_title'],
                row['channel_id'],
                row['channel_description'],
                row['channel_published_date'],
                row['channel_subscribers'],
                row['channel_videos'],
                row['channel_views'],
                row['channel_playList_id'])
                
        cursor.execute(insert_query,values)
        my_db.commit()

#PLAYLIST TABLE

def playlistsTable(singleChannelName):
    my_db=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='sailalitha',
                        database='YouTube_Data',
                        port='5432')

    cursor=my_db.cursor()
    create_query='''create table if not exists playlists(id_of_playlist varchar(100) primary key,
                                                        title_of_playlist varchar(100),
                                                        channelId_of_playlist varchar(100),
                                                        publishedAt_of_playlist timestamp,
                                                        channelName_of_playlist varchar(100),
                                                        no_of_videos int)'''
    cursor.execute(create_query)
    my_db.commit()

    PLlist=[]
    db=client['YouTube_DataBase']
    collection1=db['Channel_Details']

    for PLdata in collection1.find({"Channel_information.channel_title": singleChannelName},{"_id":0}):
        PLlist.append(PLdata['Playlist_information'])

    dfSPL=pd.DataFrame(PLlist[0])

    for index,row in dfSPL.iterrows():
        insert_query='''insert into playlists(id_of_playlist ,
                                            title_of_playlist,
                                            channelId_of_playlist,
                                            publishedAt_of_playlist,
                                            channelName_of_playlist,
                                            no_of_videos)
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
        
        values=(row['id_of_playlist'],
                row['title_of_playlist'],
                row['channelId_of_playlist'],
                row['publishedAt_of_playlist'],
                row['channelName_of_playlist'],
                row['no_of_videos'])
                    
        cursor.execute(insert_query,values)
        my_db.commit()

#VIDEOS TABLE

def videosTable(singleChannelName):
    my_db=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='sailalitha',
                        database='YouTube_Data',
                        port='5432')
    cursor=my_db.cursor()
    create_query='''create table if not exists videos(ChannelName varchar(100),
                                                    ChannelId varchar(100),
                                                    VideoId varchar(100) primary key,
                                                    Title varchar(200),
                                                    Tags text,
                                                    Thumbnail varchar(150),
                                                    Description text,
                                                    PublishedDate timestamp,
                                                    Duration interval,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    FavouriteCount int,
                                                    Definition varchar(50),
                                                    CaptionStatus varchar(50))'''
    cursor.execute(create_query)
    my_db.commit()

    VDlist=[]
    db=client['YouTube_DataBase']
    collection1=db['Channel_Details']

    for VDdata in collection1.find({"Channel_information.channel_title": singleChannelName},{"_id":0}):
        VDlist.append(VDdata['Videos_information'])

    dfSVD=pd.DataFrame(VDlist[0])

    for index,row in dfSVD.iterrows():
        insert_query='''insert into videos(ChannelName,
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
                                            
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    
        values=(row['ChannelName'],
                row['ChannelId'],
                row['VideoId'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['PublishedDate'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['FavouriteCount'],
                row['Definition'],
                row['CaptionStatus'])
                 
        cursor.execute(insert_query,values)
        my_db.commit()


#COMMENTS TABLE

def commentsTable(singleChannelName):
    my_db=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='sailalitha',
                        database='YouTube_Data',
                        port='5432')

    cursor=my_db.cursor()

    create_query='''create table if not exists comments(Comment_id varchar(100) primary key,
                                                        Video_Id_Comment varchar(100),
                                                        Comment_text text,
                                                        Comment_author varchar(200),
                                                        Comment_publishedAt timestamp)'''
    cursor.execute(create_query)
    my_db.commit()

    CMlist=[]
    db=client['YouTube_DataBase']
    collection1=db['Channel_Details']

    for CMdata in collection1.find({"Channel_information.channel_title": singleChannelName},{"_id":0}):
        CMlist.append(CMdata['Comments_information'])

    dfSCM=pd.DataFrame(CMlist[0])

    for index,row in dfSCM.iterrows():
        insert_query='''insert into comments(Comment_id,
                                            Video_Id_Comment,
                                            Comment_text,
                                            Comment_author,
                                            Comment_publishedAt)
                                            
                                            values(%s,%s,%s,%s,%s)'''
        
        values=(row['Comment_id'],
                row['Video_Id_Comment'],
                row['Comment_text'],
                row['Comment_author'],
                row['Comment_publishedAt'])
                  
        cursor.execute(insert_query,values)
        my_db.commit()

#function to compile all tables
def AllTables(single_channel):
    news=channelsTable(single_channel)
    if news:
        st.write(news)
    else:
        playlistsTable(single_channel)
        videosTable(single_channel)
        commentsTable(single_channel)

    return "TABLES CREATED SUCCESSFULLY!"

#functions to create table in streamlit
#CHANNEL
def streamlit_CHtable():
    CHlist=[]
    db=client['YouTube_DataBase']
    collection1=db['Channel_Details']

    for CHdata in collection1.find({},{"_id":0, "Channel_information":1}):
        CHlist.append(CHdata['Channel_information'])

    dfCH=st.dataframe(CHlist)

    return dfCH

#PLAYLISTs

def streamlit_PLtable():
    PLlist=[]
    db=client['YouTube_DataBase']
    collection1=db['Channel_Details']

    for PLdata in collection1.find({},{"_id":0, "Playlist_information":1}):
        for i in range(len(PLdata['Playlist_information'])):
            PLlist.append(PLdata['Playlist_information'][i])

    dfPL=st.dataframe(PLlist)

    return dfPL

#VIDEOS

def streamlit_VDtable():
    VDlist=[]
    db=client['YouTube_DataBase']
    collection1=db['Channel_Details']

    for VDdata in collection1.find({},{"_id":0, "Videos_information":1}):
        for i in range(len(VDdata['Videos_information'])):
            VDlist.append(VDdata['Videos_information'][i])

    dfVD=st.dataframe(VDlist)

    return dfVD

#COMMETS

def streamlit_CMtable():
    CMlist=[]
    db=client['YouTube_DataBase']
    collection1=db['Channel_Details']

    for CMdata in collection1.find({},{"_id":0, "Comments_information":1}):
        for i in range(len(CMdata['Comments_information'])):
            CMlist.append(CMdata['Comments_information'][i])

    dfCM=st.dataframe(CMlist)

    return dfCM

#streamlit code
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("KEY SKILL TAKEAWAYS")
    st.caption("PYTHON SCRIPTING: Automate tasks, schedule data collection, and streamline the ETL process with Python scripts.")
    st.caption("DATA HARVESTING: Fetch data from YouTube API.")
    st.caption("DATA CLEANING: Clean and preprocess the data.")
    st.caption("DATA STORAGE - MongoDB: Store the data in a structured database.")
    st.caption("DATA ANALYSIS - PostgreSQL: Analyze the data using SQL and visualization tools.")
    st.caption("APPLICATION DEVELOPMENT: Create an interactive application using Streamlit.")
    st.caption("DEPLOYMENT AND MAINTENANCE: Deploy and maintain the application, ensuring scalability and security.")

channel_id=st.text_input("Enter the Channel Id of the Channel you want to get information of:")

if st.button("Collect and store the data of the given channel!"):
    CH_IdList=[]
    db=client['YouTube_DataBase']
    collection1=db['Channel_Details']

    for CH_data in collection1.find({},{"_id":0, "Channel_information":1}):
        CH_IdList.append(CH_data['Channel_information']["channel_id"])

        if channel_id in CH_IdList:
            st.success("Details of this channel already exists!")
        
        else:
            insert=allDataOfChannel(channel_id)
            st.success(insert)

#specific channel selection box
allChannels=[]
db=client['YouTube_DataBase']
collection1=db['Channel_Details']

for CHdata in collection1.find({},{"_id":0, "Channel_information":1}):
    allChannels.append(CHdata['Channel_information']['channel_title'])

select_to_migrate=st.selectbox("Select the channel you want to migrate to SQL!",allChannels)

#SQL migration button
if st.button("Migrate data to SQL!"):
    Table=AllTables(select_to_migrate)
    st.success(Table)

#table category select and view
Table_viewer_st=st.radio("SELECT THE TABLE WHICH YOU WANT TO VIEW!",("CHANNEL","PLAYLISTS","VIDEOS","COMMENTS"))

if Table_viewer_st=="CHANNEL":
    streamlit_CHtable()

elif Table_viewer_st=="PLAYLISTS":
    streamlit_PLtable()

elif Table_viewer_st=="VIDEOS":
    streamlit_VDtable()

elif Table_viewer_st=="COMMENTS":
    streamlit_CMtable()

#SQL Connection and Querying
my_db=psycopg2.connect(host='localhost',
                    user='postgres',
                    password='sailalitha',
                    database='YouTube_Data',
                    port='5432')

cursor=my_db.cursor()

Query_Questions=st.selectbox("Select the question!",("1. Names of all the Channel and their videos.",
                                                    "2. Channels that have most number of videos and their counts.",
                                                    "3. 10 most viewed videos and their channel names.",
                                                    "4. Number of comments in each video and their video names.",
                                                    "5. Videos having highest like count and their channel names.",
                                                    "6. Number of likes of all videos and their channel name.",
                                                    "7. View counts of each channel and their channel names.",
                                                    "8. Names of the channels that have published videos in year 2022.",
                                                    "9. Average duration of all videos in each channel and the channel names.",
                                                    "10. Videos with highest comment count and their channel names."))

#Querys
#Question 1
if Query_Questions=="1. Names of all the Channel and their videos.":

    Query1='''select title as videos,ChannelName as channelname from videos'''

    cursor.execute(Query1)
    my_db.commit()
    T1=cursor.fetchall()
    dfT1=pd.DataFrame(T1,columns=["video title","channel name"])

    st.write(dfT1)

#Question 2
elif Query_Questions=="2. Channels that have most number of videos and their counts.":

    Query2='''select channel_title as channelname,channel_videos as no_videos from channels
                order by channel_videos desc'''

    cursor.execute(Query2)
    my_db.commit()
    T2=cursor.fetchall()
    dfT2=pd.DataFrame(T2,columns=["channel name","no of videos"])

    st.write(dfT2)

#Question 3
elif Query_Questions=="3. 10 most viewed videos and their channel names.":

    Query3='''select title as videos,Views as view_count,ChannelName as channelname from videos
                where Views is not null order by Views desc limit 10'''

    cursor.execute(Query3)
    my_db.commit()
    T3=cursor.fetchall()
    dfT3=pd.DataFrame(T3,columns=["video title","view count","channel name"])

    st.write(dfT3)

#Question 4
elif Query_Questions=="4. Number of comments in each video and their video names.":

    Query4='''select title as videos,Comments as comment_count from videos'''

    cursor.execute(Query4)
    my_db.commit()
    T4=cursor.fetchall()
    dfT4=pd.DataFrame(T4,columns=["video title","no of comments"])

    st.write(dfT4)

#Question 5
elif Query_Questions=="5. Videos having highest like count and their channel names.":

    Query5='''select title as videos,Likes as like_count,ChannelName as channelname from videos
                where Likes is not null order by Likes desc'''

    cursor.execute(Query5)
    my_db.commit()
    T5=cursor.fetchall()
    dfT5=pd.DataFrame(T5,columns=["video title","no of likes","channel name"])

    st.write(dfT5)

#Question 6
elif Query_Questions=="6. Number of likes of all videos and their channel name.":

    Query6='''select title as videos,Likes as like_count,ChannelName as channelname from videos'''

    cursor.execute(Query6)
    my_db.commit()
    T6=cursor.fetchall()
    dfT6=pd.DataFrame(T6,columns=["video title","no of likes","channel name"])

    st.write(dfT6)

#Question 7
elif Query_Questions=="7. View counts of each channel and their channel names.":

    Query7='''select channel_title as channel,channel_views as view_count from channels'''

    cursor.execute(Query7)
    my_db.commit()
    T7=cursor.fetchall()
    dfT7=pd.DataFrame(T7,columns=["channel name","view count"])

    st.write(dfT7)

#Question 8
elif Query_Questions=="8. Names of the channels that have published videos in year 2022.":

    Query8='''select ChannelName as channelname,title as videos,PublishedDate as video_released_on from videos
                where extract(year from PublishedDate)=2022'''

    cursor.execute(Query8)
    my_db.commit()
    T8=cursor.fetchall()
    dfT8=pd.DataFrame(T8,columns=["channel name","video name","release date"])

    st.write(dfT8)

#Question 9
elif Query_Questions=="9. Average duration of all videos in each channel and the channel names.":

    Query9='''select ChannelName as channelname,AVG(Duration) as average_duration from videos
                group by ChannelName'''

    cursor.execute(Query9)
    my_db.commit()
    T9=cursor.fetchall()
    dfT9=pd.DataFrame(T9,columns=["channel name","AVG duration of videos"])
    t9=[]
    for index,row in dfT9.iterrows():
        channel_name=row["channel name"]
        average_duration=row["AVG duration of videos"]
        Avg_duration=str(average_duration)
        t9.append(dict(channelname=channel_name,avgduration=Avg_duration))
    dft9=pd.DataFrame(t9)
    st.write(dft9)

#Question 10
elif Query_Questions=="10. Videos with highest comment count and their channel names.":

    Query10='''select title as videos,Comments as comment_count,ChannelName as channelname from videos
                where Comments is not null order by Comments desc'''

    cursor.execute(Query10)
    my_db.commit()
    T10=cursor.fetchall()
    dfT10=pd.DataFrame(T10,columns=["video title","comment count","channel name"])

    st.write(dfT10)