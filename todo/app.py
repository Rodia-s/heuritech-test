import os
import json
from flask import Flask, redirect, url_for, request, render_template
from pymongo import MongoClient

app = Flask(__name__)


client = MongoClient(
    '172.18.0.3',
    27017)
db = client.tododb
tododb = db['json_data']


@app.route('/')
def todo():
    _items = db.tododb.find()
    items = [item for item in _items]
    return render_template('todo.html', items=items )

@app.route('/new', methods=['POST'])
def new():
    item_doc={
        'name' : request.form['name'],
        'description' : request.form['description']
    }
    db.tododb.insert_one(item_doc
                         )
    return redirect(url_for('todo'))

@app.route('/load_json', methods=['GET'])
def upload():
    data_json =[]
    for line in open('external_sample.json', 'r'):
        data_json.append(json.loads(line))
        #file_data = json.load(f)
        db.json_data.insert_one(json.loads(line))
    return render_template('success_upload.html')

@app.route('/show_json', methods=['GET'])
def show_json():
    _data = db.json_data.find()
    datas = [data for data in _data]
    return render_template('display_json.html', datas=datas )



if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
