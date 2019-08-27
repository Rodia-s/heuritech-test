import os
import json
import requests
from flask import Flask, redirect, url_for, request, render_template
from pymongo import MongoClient
import datetime
import pandas as pd
import numpy as np
#init Flask App (server) 
app = Flask(__name__)


mongo_uri="mongodb://" + "todo_db_1"

#data directory
data_directory = "./data_dir"

#'172.18.0.3'
client = MongoClient(
    mongo_uri,
    27017)
db = client.tododb
tododb = db['jsondata']
tododb = db['dailystats']


#root of server only initiates with a find on mongodb
@app.route('/')
def todo():
    _items = db.tododb.find()
    items = [item for item in _items]
    return render_template('todo.html')


#route to upload external_sample.json
@app.route('/load_json', methods=['GET'])
def upload():
    data_json =[]
    for line in open('./data_dir/external_sample.json', 'r'):
        data_json.append(json.loads(line))
        #file_data = json.load(f)
        db.jsondata.insert_one(json.loads(line))
    return render_template('success_upload.html')

# route to show the whole collection we insert
@app.route('/show_json', methods=['GET'])
def show_json():
    _data = db.jsondata.find()
    datas = [data for data in _data]
    return render_template('display_json.html', datas=datas )


# route to compute stats on the collection jsondata and insert it into dailystats collection
@app.route('/compute_daily_stats',methods=['GET'])
def compute_stats():
    #load collection data as pandas dataframe
    cursor = db.jsondata.find()
    df =  pd.DataFrame(list(cursor))
    #convert ts to datatime
    df['date'] =  pd.to_datetime(df['ts'], unit = 's')

    #convert datetime ts to pretty date
    x = df['date'].as_matrix()
    x = np.asarray(x, dtype='datetime64[D]')
    df['date'] = x

    #get all unique date in the df
    listdate = np.unique(x).tolist()

    #print(df.head())
    #print(df.info())

    #iterator on different date in the dataset upload
    dateIter = iter(listdate)

    #list all label.model.entity
    res = np.array(df['media'].values.tolist())
    entity = []
    for i in res:
        entity.append(i[0]['label'][0]["entity"])

        #entity = entity.at[i, i[0]['label'][0]["entity"]]
    entity_array_unique = np.unique(entity)
    print(entity_array_unique)
    df['entity'] = pd.Series(entity)
    print(type(res))
    print(type(res[0]))
    print(type(res[0][0]))

    print(df.head())

    #create document to be insert in mongo

    json_data = []

    #iterate on the date
    for i in listdate : 
        i = pd.Timestamp(i)
        #iterate on every unique entity 
        for j in entity_array_unique :
            df_by_query = df[df['date'] == i]
            df_by_query = df_by_query[df_by_query['entity'] == j]
            print(df_by_query.head())

            #extract json followers name author
            df_by_query['followers'] = pd.io.json.json_normalize(df_by_query['author'])
            #sort by date and tags
            df_by_query_and_comm = df_by_query[df_by_query.tags.apply(lambda lst: any(d['name']=='commercial' for d in lst))]
            #get a warning to recover later
            print(df_by_query_and_comm)

            #create df stats (compute stats on a date) to be inject on mongodb stats collection
            #shortcut : we should implement a function of every type of query on df that repeat itself
            stats_data = {}
            stats_data["date"] = str(i)
            stats_data["entity"] = str(j)
            stats_data["nbTot"] = str(df_by_query['date'].count())
            stats_data["nbComm"] = str(df_by_query_and_comm['date'].count())
            stats_data["nbNotComm"] = str( int(stats_data["nbTot"]) - int(stats_data["nbComm"]))
            if df_by_query['date'].count() !=0 :
                stats_data["percentageComm"] = str (df_by_query_and_comm['date'].count() / df_by_query['date'].count() * 100)
            else : 
                stats_data["percentageComm"] = str(0)

            stats_data["nbLikesTot"] = str (df_by_query["likes"].sum())
            stats_data["nbLikesComm"] = str (df_by_query_and_comm["likes"].sum())
            stats_data["nbLikesNotComm"] = str (int(stats_data["nbLikesTot"]) - int(stats_data["nbLikesComm"]))

            if int(stats_data["nbLikesTot"]) !=0 : 
                stats_data["percentageLikeComm"] = str ( int(stats_data["nbLikesComm"]) / int(stats_data["nbLikesTot"]) * 100) 
            else :
                stats_data["percentageLikeComm"] = str(0)
            stats_data["nbFollowersTot"] = str(int(df_by_query['followers'].sum()))
            stats_data["nbFollowersTotComm"] = str(int(df_by_query_and_comm['followers'].sum()))
            stats_data["nbFollowersTotNotComm"] = str ( int(stats_data["nbFollowersTot"]) - int(stats_data["nbFollowersTotComm"]) ) 
            #json_stats = json.dumps(stats_data)
            json_data.append(stats_data)
            db.daily_stat_df.insert_one(stats_data)
    #db.dailystats.insert_many(json_data)

    return render_template('display_stats.html', json_data=json_data )

#empty_daily_stats
@app.route('/empty_daily_stats',methods=['GET','POST'])
def empty_daily_stats():
    db.daily_stat_df.remove()
    return render_template('success_empty.html')

# route to compute stats on the collection jsondata and insert it into dailystats collection 2018-01-31 00:00:00
@app.route('/get_daily_stats',methods=['GET','POST'])
def get_daily_stats():
    if request.method == 'POST': #this block is only entered when the form is submitted
        data_send = pd.Timestamp(request.form['date'])
        _data = db.daily_stat_df.find({'date': str(data_send)})
        datas = [data for data in _data]
        return render_template('display_json.html', datas=datas)
    # elif request.method == 'GET':
    #     _data = db.daily_stat_df.find({'date': '2018-01-31 00:00:00'})
    #     datas = [data for data in _data]
    #     return render_template('display_json.html', datas=datas )
    return '''<form method="POST">
                Date: <input type="text" name="date"><br>
                <input type="submit" value="Submit"><br>
            </form>'''


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
