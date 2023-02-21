#!/usr/bin/env python
# coding: utf-8

# **Файлы с сайта попстерс загружены в папку data. Файл  report_urls.csv содержит записи вида:**

# https://www.youtube.com/channel/UC4ozyeewhEmjvhH_1Q_yS4g;OK;./data/posts.xlsx;Зебра в клеточку официальный ка;Мультфильмы;UC4ozyeewhEmjvhH_1Q_yS4g;

# In[4]:


# import libs and set SQL connection string
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
import sqlalchemy
import psycopg2
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime


conn_string = os.environ['PG_DB']
#


# In[5]:


#connect to database (a lot of requests in plans)
db = create_engine(conn_string)
conn = db.connect()


# In[3]:


print("Reading yt_reels_stat table with latest date...")
reels_stat =  pd.read_sql("SELECT * FROM yt_reels_stat WHERE data_date IN (SELECT MAX(data_date) from yt_reels_stat)", conn)
print("Done.")


# In[4]:


# Load vocobulary for parser
df = pd.read_csv('./ref/vocabulary.csv',sep=";",encoding='utf-8-sig')
voc_dict = df.to_dict(orient='index')


# In[5]:


def check_items_cartoon(reel_name):
   # return list(set([value for key, value in cartoonnames.CartoonNamesSMF.items() if key in reel_name.lower()]))
    res= []
    reel_name = reel_name.lower()
    for i,j in voc_dict.items():
        if voc_dict[i]['key'] in reel_name:
            #print(voc_dict[i]['key'])
            res.append(voc_dict[i]['project'])
            reel_name=reel_name.replace(voc_dict[i]['key'],"")
    return(list(set(res)))
        
def check_len_cartoon(collection_list):
    if len(collection_list) == 0:
        return 'none'
    elif len(collection_list) == 1:
        return collection_list[0]
    else:
        return 'сборник'

def convert_collect(collection_list):
    if len(collection_list) < 2:
        return 'none'
    else:
        return ("|".join(map(str, collection_list)))


# In[6]:


def alphanum(element):
    a = ("".join(chr for  chr in element if (chr.isalnum() or chr.isspace() or chr in '[]":.,//"')))
    return("".join(e.strip('\n') for e in  a))


# In[7]:


#append new reels with description to DB 'yt_reels' table
def proc_reels_to_sql(reels_to_sql):
    
    #display(reels_to_sql.head(1))
    reels_to_sql=reels_to_sql.drop(reels_to_sql.iloc[:,1:6],axis = 1)
    reels_to_sql=reels_to_sql.drop(reels_to_sql.iloc[:,4:13],axis = 1)
   # display(reels_to_sql.head(1))
    reels_to_sql['Date'] = pd.to_datetime(reels_to_sql['Date'], format='%Y-%m-%d %H:%M:%S')
    reels_to_sql.loc[:,'Text'] = ([alphanum(x) for x in reels_to_sql['Text']])
    reels_to_sql['reel_name'] = ((reels_to_sql['Text'].str.split(']')).str[0]).str[1:]
# fill two fieils according to the  reel_name and vocabualary
#search for dict items in reel description
    reels_to_sql['cartoon_search'] = reels_to_sql['reel_name'].apply(check_items_cartoon)
#fill 'cartoon' depends on search results
    reels_to_sql['cartoon'] = reels_to_sql['cartoon_search'].apply(check_len_cartoon)
#fill 'cartoon collection' depends on search results
    reels_to_sql['cartoon_collection'] = reels_to_sql['cartoon_search'].apply(convert_collect)
# drop temporary column
    reels_to_sql.drop( columns = ['cartoon_search'] , axis=1, inplace=True)
    reels_to_sql=reels_to_sql.iloc[:,[2,4,3,5,6,0,1]]
    reels_to_sql.columns = reels_to_sql.columns.str.lower()
   # display(reels_to_sql.head(1))
#now ready to add to sql
    print("Add ", len(reels_to_sql), " records to yt_reels table")
    #now insert them in our database
    inject_dict = {    'date': sqlalchemy.types.Date(),       #date:date                                 
                   'reel_name': sqlalchemy.types.TEXT(),                                    
                   'yt_reel_id': sqlalchemy.types.TEXT(),  
                   'cartoon': sqlalchemy.types.TEXT(),         #cartoon: text                             
                   'cartoon_collection': sqlalchemy.types.TEXT(),    #cartoon_collection: text                        
                   'url': sqlalchemy.types.TEXT(),             #url:text                                                                   
                   'text': sqlalchemy.types.TEXT(),            #text: text
                   }
    start_time = time.time()
    try:
        res = reels_to_sql.to_sql('yt_reels', con=conn, if_exists='append', index=False, dtype = inject_dict)
    except:
        print("error to_sql")
    print("to_sql duration: {} seconds".format(time.time() - start_time))


# In[8]:


def proc_ch2reels_to_sql(ch2reels_to_sql, ch_id, fdate):
            to_sql = pd.DataFrame()
            print("New_reels - add them - channels 2 reels table:")
            to_sql['yt_reel_id'] = ch2reels_to_sql['yt_reel_id']
            to_sql['yt_channel_id'] = ch_id
            to_sql['yt_reel_date_add'] = fdate
            to_sql['yt_reel_date_removed'] = np.NaN
           # display(to_sql)
            inject_dict = {                                    
                   'yt_reel_id': sqlalchemy.types.TEXT(),  
                   'yt_channel_id': sqlalchemy.types.TEXT(),
                   'yt_reel_date_add':sqlalchemy.types.Date(),  
                   'yt_reel_date_removed':sqlalchemy.types.Date(),  
                    }
            start_time = time.time()
            print("Add ", len(to_sql), " records to yt_channels2reel table")
            try:
                res = to_sql.to_sql('yt_channels2reels', con=conn, if_exists='append', index=False, dtype = inject_dict)
            except:
                print("error to_sql")
            print("to_sql duration: {} seconds".format(time.time() - start_time))


# In[9]:


def proc_ch2reels_update(removed_reels, ch_id, fdate):
    #display(list(removed_reels))
    sql_list ="( '"  + "','".join(list(removed_reels)) + "' )"  
    sql_str = "UPDATE yt_channels2reels SET yt_reel_date_removed = '"+fdate+"' WHERE (yt_reel_id IN "+sql_list+") AND (yt_reel_date_removed IS NULL)"
    print("Update ", len(removed_reels), " records in yt_channels2reels table")
    try:
        rs = conn.execute(sql_str)
        print(rs.rowcount, "record(s) affected")
        print("OK  update_removed")
    except:
        print("error  update_removed")


# In[10]:


def proc_new_reels_stat(reels, ch_id, fdate):
 #   display(reels)
 #   print(fdate)
    reels = reels.reset_index()
    reels.loc[:,'data_date'] = fdate
    reels['views'] = reels['Views']
    reels['likes'] = reels['Likes']
    reels['comments'] = reels['Comments']
    reels['er'] = reels['ER']
    reels['delta_days']= 0
    reels['removed'] = 0
    reels['likes_delta'] = reels['likes']
    reels['views_delta'] = reels['views']
    reels=reels.drop(reels.iloc[:,0:9],axis = 1)
    reels=reels.drop(reels.iloc[:,-1:],axis = 1)
    
   # display(reels)
    inject_dict = {'yt_reel_id': sqlalchemy.types.TEXT(),  
                   'views': sqlalchemy.types.BIGINT(), 
                   'data_date': sqlalchemy.types.Date(),          #date_1:date                          
                   'likes': sqlalchemy.types.BIGINT(),         #likes:bigint                             
                   'comments': sqlalchemy.types.BIGINT(),        #comment:bigint                            
                   'er': DOUBLE_PRECISION(),                 #er: double precision                               
                   'likes_delta': sqlalchemy.types.BIGINT(),
                   'views_delta': sqlalchemy.types.BIGINT(),
                   'delta_days': sqlalchemy.types.BIGINT(),
                   'removed': sqlalchemy.types.BIGINT(),
                 }
    start_time = time.time()
    try:
        res = reels.to_sql('yt_reels_stat', con=conn, if_exists='append', index=False, dtype = inject_dict)
    except:
        print("error to_sql")
    print("to_sql duration: {} seconds".format(time.time() - start_time))


# In[11]:


def proc_exist_reels_stat(reels, ch_id, fdate):
    reels= reels.reset_index()
    reels['data_date'] = pd.to_datetime(reels['data_date'])
    reels['delta_days'] = (datetime.strptime(fdate,'%Y-%m-%d') - (reels['data_date'])).dt.days
    reels['data_date'] = fdate
    reels['likes_delta'] = reels['Likes'] - reels['likes']
    reels['likes'] = reels['Likes'] 
    reels['views_delta'] = reels['Views'] - reels['views']
    reels['views'] = reels['Views']
    reels['er']  = reels['ER']
    reels['comments'] = reels['Comments']
    reels=reels.drop(reels.iloc[:,0:9],axis = 1)
    reels=reels.drop(reels.iloc[:,-1:],axis = 1)
#    display(reels.head(2))

    inject_dict = {'yt_reel_id': sqlalchemy.types.TEXT(),  
                   'views': sqlalchemy.types.BIGINT(), 
                   'data_date': sqlalchemy.types.Date(),          #date_1:date                          
                   'likes': sqlalchemy.types.BIGINT(),         #likes:bigint                             
                   'comments': sqlalchemy.types.BIGINT(),        #comment:bigint                            
                   'er': DOUBLE_PRECISION(),                 #er: double precision                               
                   'likes_delta': sqlalchemy.types.BIGINT(),
                   'views_delta': sqlalchemy.types.BIGINT(),
                   'delta_days': sqlalchemy.types.BIGINT(),
                   'removed': sqlalchemy.types.BIGINT(),
                 }
    start_time = time.time()
    try:
        res = reels.to_sql('yt_reels_stat', con=conn, if_exists='append', index=False, dtype = inject_dict)
    except:
        print("error to_sql")
    print("to_sql duration: {} seconds".format(time.time() - start_time))


# In[12]:


def process_file(fpath, ch_id):
    print(fpath, ch_id)
    fdate = (datetime.fromtimestamp(os.path.getmtime(fpath)).strftime('%Y-%m-%d'))
    print(fdate)
    df=[]
    try:
        df = pd.read_excel(fpath )
    except:
        print(fpath+" doesn't exist")
    if(len(df)>0):
        ch_reels = df.drop(df.iloc[:,9:len(df.columns)],axis = 1)
        ch_reels = ch_reels.drop(["Social"],axis=1)
        #str.split('=').str[1]
        ch_reels['yt_reel_id']=ch_reels.Url.str.replace("https://www.youtube.com/watch?v=", "", regex=False).str.strip()
####### get new reels vs yt_reels and add them to yt_reels
        #get all reels list from DB
        reels_sql =  pd.read_sql('SELECT  * from yt_reels', conn)
        compare_reels = ch_reels.merge(reels_sql, on='yt_reel_id', how='outer', suffixes=['', '_'], indicator=True)
        print("New reels - add to yt_reels")
        reels_to_sql = compare_reels[compare_reels['_merge']=='left_only']
        if(len(reels_to_sql)>0):
            proc_reels_to_sql(reels_to_sql)
        else:
            print("not found")
####### check  yt_channels2reels and update if required
        
        sql_str = "SELECT * FROM yt_channels2reels WHERE yt_channel_id= '"+ch_id+"'"
        ch2reels =  pd.read_sql(sql_str, conn)
        compare = ch_reels.merge(ch2reels, on='yt_reel_id', how='outer', suffixes=['', '_'], indicator=True)
    # new reels - left_only tagged
        ch2reels_to_sql = compare[compare['_merge']=='left_only']
        print("New reels - add to channels2reels")
        if(len(ch2reels_to_sql)>0):            
            proc_ch2reels_to_sql(ch2reels_to_sql, ch_id, fdate)
        else:
            print("not found")
   # removed reels - right_only tagged
        print("Removed reels - mark them as removed")
        ch2reels_removed = compare[compare['_merge']=='right_only']
        if len(list(ch2reels_removed.yt_reel_id))>0:
            proc_ch2reels_update(ch2reels_removed.yt_reel_id, ch_id, fdate)
        else:
            print("not found")  
    ########################################
    ##  now it's time update statistics - add new reels and add stat for existing reels
    ########################################

        compare = ch_reels.merge(reels_stat, on='yt_reel_id', how='outer', suffixes=['', '_'], indicator=True)
    # new reels - left_only tagged
        new_reels = compare[compare['_merge']=='left_only']
        if (len(new_reels)>0):
            print('process new reels stat', len(new_reels))
            proc_new_reels_stat(new_reels, ch_id, fdate )
        else:
            print("New reels for stat not found") 
    # existing reeels - both tagged
        exist_reels = compare[compare['_merge']=='both']
        if (len(exist_reels)>0):
            print('process exist reels stat', len(exist_reels))
            proc_exist_reels_stat(exist_reels, ch_id, fdate )
        else:
            print("Exist reels for stat not found")
    # at last removed reels
       ### AT FIRST ! set default for all stat!!!
        #if (len(removed_reels)>0):
         #   print('process removed reels stat', len(removed_reels))
          #  display(removed_reels)
           # proc_removed_reels_stat(exist_reels, ch_id, fdate )
        #else:
         #   print("Removed reels for stat not found")


# In[9]:


# load report file, name columns and drop useless
reels = pd.read_csv('report_urls.csv', encoding='cp1251', sep=';', header = None, skiprows=1 )
reels.columns = ['urls', 'flag', 'path', 'name', 'type', 'ch_id', 'trash']
reels.ch_id = reels.ch_id.str.strip()
reels.drop(['trash'], axis =1, inplace=True)
reels = reels[reels.flag=='OK']


# In[14]:


#in an oldschool style run by list and work with files
for i in range(0,reels.shape[0]):
#for i in range(0,1):
    print('-------',i,'----------')
    process_file(reels.iloc[i,2],reels.iloc[i,5])
print("File processing finished")


# In[1]:


# Now removed reels
print("Process removed reels")


# In[26]:


query = """WITH
ids AS (SELECT  yt_reel_id as id
FROM yt_reels_stat 
GROUP BY yt_reel_id
HAVING MAX(data_date) < ( SELECT MAX(data_date) from yt_reels_stat)),
f AS (SELECT  yt_reel_id AS idd, MAX(data_date) as md
FROM yt_reels_stat 
GROUP BY yt_reel_id)
SELECT * 
FROM yt_reels_stat as y
INNER JOIN ids ON y.yt_reel_id = ids.id
INNER JOIN f ON f.idd = ids.id
WHERE y.data_date = f.md"""


# In[27]:


removed_reels =  pd.read_sql(query, conn)


# In[28]:


fdate = datetime.fromtimestamp(os.path.getmtime(reels.iloc[i,2])).strftime('%Y-%m-%d')


# In[33]:


removed_reels['likes_delta']=0
removed_reels['views_delta']=0
removed_reels['removed']=1
removed_reels['data_date'] = pd.to_datetime(removed_reels['data_date'])
removed_reels['delta_days'] = (datetime.strptime(fdate,'%Y-%m-%d') - (removed_reels['data_date'])).dt.days
removed_reels['data_date'] = fdate
removed_reels=removed_reels.drop(removed_reels.iloc[:,10:13],axis = 1)


# In[34]:


print("Removed_reels:", len(removed_reels))


# In[35]:


inject_dict = { 'yt_reel_id': sqlalchemy.types.TEXT(),  
                'data_date': sqlalchemy.types.Date(),          #date_1:date       
                'views': sqlalchemy.types.BIGINT(), 
                'likes': sqlalchemy.types.BIGINT(),         #likes:bigint                             
                'comments': sqlalchemy.types.BIGINT(),        #comment:bigint                            
                'er': DOUBLE_PRECISION(),                 #er: double precision                               
                'likes_delta': sqlalchemy.types.BIGINT(),
                'views_delta': sqlalchemy.types.BIGINT(),
                'delta_days': sqlalchemy.types.BIGINT(),
                'removed': sqlalchemy.types.BIGINT(),
                }
start_time = time.time()
try:
    res = removed_reels.to_sql('yt_reels_stat', con=conn, if_exists='append', index=False, dtype = inject_dict)
except:
    print("error to_sql")
print("to_sql duration: {} seconds".format(time.time() - start_time))


# In[ ]:




