#!/usr/bin/env python

from flask import Flask
from flask import request
from flask import render_template

import datetime
import pymongo
import datetime, sys, os
import json

# Tractor API
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1, path)
del path

import tractor.api.query as tq
tq.setEngineClientParam(hostname="113.107.235.11", port=1503, user='abhishek', debug=False)

app = Flask(__name__)

@app.route('/')
@app.route('/<string:name>')
def index(name='all'):
    (current_up_tasks, current_down_tasks) = queue_data(arg='current', name=name, streams='both')
    (failed_up_tasks, failed_down_tasks) = queue_data(arg='failed', name=name, streams='both')
    (done_up_tasks, done_down_tasks) = queue_data(arg='done', name=name, streams='both')
    context = {'type' : 'Index', 'current_up_tasks' : current_up_tasks, 'current_down_tasks' : current_down_tasks, 'failed_up_tasks' : failed_up_tasks, 'failed_down_tasks' : failed_down_tasks, 'done_up_tasks' : done_up_tasks, 'done_down_tasks' : done_down_tasks}

    return render_template("index.html", **context)

@app.route('/active')
@app.route('/active/<string:name>')
def active(name='all'):
    context = {'type' : 'Active'}
    return render_template("q_mon.html", **context)

@app.route('/pending')
@app.route('/pending/<string:name>')
def pending(name='all'):
    context = {'type' : 'Pending'}
    return render_template("q_mon.html", **context)

@app.route('/failed')
@app.route('/failed/<string:name>')
def failed(name='all'):
    (up_tasks, down_tasks) = queue_data('failed')
    context = {'type' : 'Failed', 'up_tasks' : up_tasks, 'down_tasks' : down_tasks}
    return render_template("current.html", **context)

@app.route('/current')
@app.route('/current/<string:name>')
def current(name='all'):
    (up_tasks, down_tasks) = queue_data('current')
    context = {'type' : 'Current', 'up_tasks' : up_tasks, 'down_tasks' : down_tasks}
    return render_template("current.html", **context)
  
@app.route('/done')
@app.route('/done/<string:name>')
def done(name='all'):
    (done_up_tasks, done_down_tasks) = queue_data('done')
    context = {'type' : 'Done', 'up_tasks' : done_up_tasks, 'down_tasks' : done_down_tasks}
    return render_template("current.html", **context)

@app.route('/date')
def get_date_time():
    d = datetime.date.today()
    return d.stringftime("%d-%m-%Y-%H-%M-%S")

def queue_data(**kwargs):
    name = kwargs.get('name')
    arg = kwargs.get('arg')
    streams = kwargs.get('streams')

    client = pymongo.MongoClient('lic', 27017)
    collection = client.queue.tasks

    #today = datetime.date.today()
    today = datetime.datetime(2014, 12, 25, 0, 0, 0)
    d = datetime.datetime(today.year, today.month, today.day)

    up_task_docs = []
    down_task_docs = []
    if arg == 'current':
        # Ugly hack to get per person upload / download items
        if name == 'all':
            query_dict_up = { '$and' : [ { '$or' : [ { 'task_init' : { '$gt' : d } } ] }, { '$or' : [ { 'upload_status' : 'pending' }, { 'upload_status' : 'active' } ] } ] }
            query_dict_down = { '$and' : [ { '$or' : [ { 'task_init' : { '$gt' : d } } ] }, { '$or' : [ { 'download_status' : 'pending' }, { 'download_status' : 'active' } ] } ] }
            #query_dict = { '$and' : [ { 'task_init' : { '$gt' : d } }, { '$or' : [ { 'upload_status' : 'pending' }, { 'upload_status' : 'active' }, { 'download_status' : 'pending' }, { 'download_status' : 'active' }, {'spool_status' : 'pending' } ] } ] }
            #query_dict = {'task_init': {'$gt' : d}, 'upload_status' : 'pending', 'dowmload_status' : 'pending', 'spool_status' : 'pending'}
        else:
            query_dict_up = { '$and' : [ { 'task_owner' : name }, { '$or' : [ { 'task_init' : { '$gt' : d } } ] }, { '$or' : [ { 'upload_status' : 'pending' }, { 'upload_status' : 'active' } ] } ] }
            query_dict_down = { '$and' : [ { 'task_owner' : name }, { '$or' : [ { 'task_init' : { '$gt' : d } } ] }, { '$or' : [ { 'download_status' : 'pending' }, { 'download_status' : 'active' } ] } ] }


        if 'both' in streams or 'down' in streams:
            # Upload
            print >>sys.stderr, query_dict_up
            docs = collection.find(query_dict_up).sort('task_init', pymongo.ASCENDING)
            for doc in docs:
                task_doc = {}
                task_doc['task_id'] = doc['task_id']
                task_doc['task_owner'] = doc['task_owner']
                task_doc['task_init'] = doc['task_init']
                task_doc['upload_id'] = doc.get('upload_id')
                task_doc['upload_status'] = doc.get('upload_status')
                task_doc['upload_exception'] = doc.get('upload_exception')
                task_doc['spool_id'] = doc.get('spool_id')
                task_doc['spool_status'] = doc.get('spool_status')
                task_doc['spool_exception'] = doc.get('spool_exception')
                try:
                    task_doc['jid'] = json.loads(doc.get('spool_retval')).get('jid')
                except:
                    task_doc['jid'] = None
                up_task_docs.append(task_doc)

        if 'both' in streams or 'up' in streams:
            # Download
            print >>sys.stderr, query_dict_down
            docs = collection.find(query_dict_down).sort('task_init', pymongo.ASCENDING)
            for doc in docs:
                task_doc = {}
                task_doc['task_id'] = doc['task_id']
                task_doc['task_owner'] = doc['task_owner']
                task_doc['task_init'] = doc['task_init']
                try:
                    task_doc['jid'] = json.loads(doc.get('spool_retval')).get('jid')
                except:
                    task_doc['jid'] = None
                task_doc['download_id'] = doc.get('download_id')
                task_doc['download_status'] = doc.get('download_status')
                task_doc['download_exception'] = doc.get('download__exception')
                down_task_docs.append(task_doc)

    elif arg == 'failed':
        if name == 'all':
            query_dict_up = { '$and' : [ { '$or' : [ { 'task_init' : { '$gt' : d } } ] }, { '$or' : [ { 'upload_status' : 'failed' }, { 'spool_status' : 'failed' } ] } ] }
            query_dict_down = { '$and' : [ { '$or' : [ { 'task_init' : { '$gt' : d } } ] }, { '$or' : [ { 'download_status' : 'failed' } ] } ] }
            #query_dict = { '$and' : [ { 'task_init' : { '$gt' : d } }, { '$or' : [ { 'upload_status' : 'pending' }, { 'upload_status' : 'active' }, { 'download_status' : 'pending' }, { 'download_status' : 'active' }, {'spool_status' : 'pending' } ] } ] }
            #query_dict = {'task_init': {'$gt' : d}, 'upload_status' : 'pending', 'dowmload_status' : 'pending', 'spool_status' : 'pending'}
        else:
            query_dict_up = { '$and' : [ { 'task_owner' : name }, { '$or' : [ { 'task_init' : { '$gt' : d } } ] }, { '$or' : [ { 'upload_status' : 'failed' }, { 'spool_status' : 'failed' } ] } ] }
            query_dict_down = { '$and' : [ { 'task_owner' : name }, { '$or' : [ { 'task_init' : { '$gt' : d } } ] }, { '$or' : [ { 'download_status' : 'failed' } ] } ] }


        if 'both' in streams or 'down' in streams:
            # Upload
            docs = collection.find(query_dict_up).sort('task_init', pymongo.ASCENDING)
            for doc in docs:
                task_doc = {}
                task_doc['task_id'] = doc['task_id']
                task_doc['task_owner'] = doc['task_owner']
                task_doc['task_init'] = doc['task_init']
                task_doc['upload_id'] = doc.get('upload_id')
                task_doc['upload_status'] = doc.get('upload_status')
                task_doc['upload_exception'] = doc.get('upload_exception')
                task_doc['spool_id'] = doc.get('spool_id')
                task_doc['spool_status'] = doc.get('spool_status')
                task_doc['spool_exception'] = doc.get('spool_exception')
                try:
                    task_doc['jid'] = json.loads(doc.get('spool_retval')).get('jid')
                except:
                    task_doc['jid'] = None
                up_task_docs.append(task_doc)

        if 'both' in streams or 'up' in streams:
            # Download
            docs = collection.find(query_dict_down).sort('task_init', pymongo.ASCENDING)
            for doc in docs:
                task_doc = {}
                task_doc['task_id'] = doc['task_id']
                task_doc['task_owner'] = doc['task_owner']
                task_doc['task_init'] = doc['task_init']
                try:
                    task_doc['jid'] = json.loads(doc.get('spool_retval')).get('jid')
                except:
                    task_doc['jid'] = None
                task_doc['download_id'] = doc.get('download_id')
                task_doc['download_status'] = doc.get('download_status')
                task_doc['download_exception'] = doc.get('download__exception')
                down_task_docs.append(task_doc)

    elif arg == 'done':
        if name == 'all':
            query_dict_up = { '$and' : [ { '$or' : [ { 'task_init' : { '$gt' : d } } ] }, { '$and' : [ { 'upload_status' : 'done' }, { 'spool_status' : 'done' } ] } ] }
            query_dict_down = { '$and' : [ { '$or' : [ { 'task_init' : { '$gt' : d } } ] }, { '$or' : [ { 'download_status' : 'done' } ] } ] }
            #query_dict = { '$and' : [ { 'task_init' : { '$gt' : d } }, { '$or' : [ { 'upload_status' : 'pending' }, { 'upload_status' : 'active' }, { 'download_status' : 'pending' }, { 'download_status' : 'active' }, {'spool_status' : 'pending' } ] } ] }
            #query_dict = {'task_init': {'$gt' : d}, 'upload_status' : 'pending', 'dowmload_status' : 'pending', 'spool_status' : 'pending'}
        else:
            query_dict_up = { '$and' : [ { 'task_owner' : name }, { '$or' : [ { 'task_init' : { '$gt' : d } } ] }, { '$and' : [ { 'upload_status' : 'done' }, { 'spool_status' : 'done' } ] } ] }
            query_dict_down = { '$and' : [ { 'task_owner' : name }, { '$or' : [ { 'task_init' : { '$gt' : d } } ] }, { '$or' : [ { 'download_status' : 'done' } ] } ] }
                

        if 'both' in streams or 'down' in streams:
            # Upload
            docs = collection.find(query_dict_up).sort('task_init', pymongo.ASCENDING)
            for doc in docs:
                task_doc = {}
                task_doc['task_id'] = doc['task_id']
                task_doc['task_owner'] = doc['task_owner']
                task_doc['task_init'] = doc['task_init']
                task_doc['upload_id'] = doc.get('upload_id')
                task_doc['upload_status'] = doc.get('upload_status')
                task_doc['upload_exception'] = doc.get('upload_exception')
                task_doc['spool_id'] = doc.get('spool_id')
                task_doc['spool_status'] = doc.get('spool_status')
                task_doc['spool_exception'] = doc.get('spool_exception')
                try:
                    task_doc['jid'] = json.loads(doc.get('spool_retval')).get('jid')
                except:
                    task_doc['jid'] = None
                up_task_docs.append(task_doc)

        if 'both' in streams or 'up' in streams:
            # Download
            docs = collection.find(query_dict_down).sort('task_init', pymongo.ASCENDING)
            for doc in docs:
                task_doc = {}
                task_doc['task_id'] = doc['task_id']
                task_doc['task_owner'] = doc['task_owner']
                task_doc['task_init'] = doc['task_init']
                try:
                    task_doc['jid'] = json.loads(doc.get('spool_retval')).get('jid')
                except:
                    task_doc['jid'] = None
                task_doc['download_id'] = doc.get('download_id')
                task_doc['download_status'] = doc.get('download_status')
                task_doc['download_exception'] = doc.get('download__exception')
                down_task_docs.append(task_doc)

        elif arg == 'today':
            d = datetime.datetime(today.year, today.month, today.day)
        query_dict = {'task_init': {'$gt' : d } }
    else:
        query_dict = {'task_owner': arg, 'task_init': {'$gt' : d } }
        #query_dict = {'task_owner': sys.argv[1]}

    '''
    docs = collection.find(query_dict).sort('task_init', pymongo.ASCENDING)

    task_docs = []
    for doc in docs:
        task_doc = {}
        task_doc['task_id'] = doc['task_id']
        task_doc['task_owner'] = doc['task_owner']
        task_doc['task_init'] = doc['task_init']
        task_doc['upload_id'] = doc.get('upload_id')
        task_doc['upload_status'] = doc.get('upload_status')
        task_doc['spool_id'] = doc.get('spool_id')
        task_doc['spool_status'] = doc.get('spool_status')
        try:
            task_doc['jid'] = json.loads(doc.get('spool_retval')).get('jid')
        except:
            task_doc['jid'] = None
        task_doc['download_id'] = doc.get('download_id')
        task_doc['download_status'] = doc.get('download_status')

        task_docs.append(task_doc)
    '''

    return [up_task_docs, down_task_docs]

'''
@app.route('/all')
def queue_status():
   client = MongoClient('lic', 27017)
   today = get_date_time()

   coll = client.queue.tasks
   today_tasks = coll.find({'task_init' : {'gt' : today}})
   return string(coll)
'''

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=55555, debug=True)

