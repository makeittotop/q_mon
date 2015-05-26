#!/usr/bin/env python

from flask import Flask
from flask import request, redirect
from flask import render_template
from flask.ext.wtf import Form
from wtforms import StringField, BooleanField
from wtforms.validators import DataRequired

import datetime
import pymongo
import datetime, sys, os
import json

import zerorpc
import sys

# Tractor API
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1, path)
del path

import tractor.api.query as tq
tq.setEngineClientParam(hostname="113.107.235.11", port=1503, user='abhishek', debug=False)

app = Flask(__name__)

@app.route('/cancel/', methods=['POST'])
def cancel_task():
    type = request.form['task_type']
    task_id = request.form['task_id']
    state = request.form['task_state']
    print >>sys.stderr, "Type: ", type, "Task Id: ", task_id, "State: ", state

    c = zerorpc.Client()
    c.connect("tcp://16.16.16.2:4242")

    if state == 'pending':
        print c.cancel_pending_task(type, task_id)
    else:
        print c.cancel_running_task(type, task_id)

    return redirect('/')

@app.route('/')
def index():
    name = request.args.get('user')
    if not name:
        name = 'all'

    delta=7
    (current_up_tasks, current_down_tasks) = queue_data(arg='current', name=name, stream='up_down', delta=delta)
    (failed_up_tasks, failed_down_tasks) = queue_data(arg='failed', name=name, stream='up_down', delta=delta)
    (done_up_tasks, done_down_tasks) = queue_data(arg='done', name=name, stream='up_down', delta=delta)

    context = {'type' : 'Index', 'current_up_tasks' : current_up_tasks, 'current_down_tasks' : current_down_tasks, 'failed_up_tasks' : failed_up_tasks, 'failed_down_tasks' : failed_down_tasks, 'done_up_tasks' : done_up_tasks, 'done_down_tasks' : done_down_tasks}

    return render_template("index.html", **context)

@app.route('/failed')
#@app.route('/failed/<string:name>')
def failed():
    name = request.args.get('user')
    if not name:
        name = 'all'

    delta = 7
    (up_tasks, down_tasks) = queue_data(arg='failed', name=name, stream='up_down', delta=delta)

    context = {'type' : 'Failed', 'stream' : 'up_down', 'up_tasks' : up_tasks, 'down_tasks' : down_tasks, 'delta': delta}
    return render_template("current.html", **context)

@app.route('/current')
#@app.route('/current/<string:name>')
def current():
    name = request.args.get('user')
    if not name:
        name = 'all'
            
    stream = request.args.get('stream')
    if not stream:
        stream = 'up_down'

    delta = 7
    (up_tasks, down_tasks) = queue_data(arg='current', name=name, stream=stream, delta=delta)

    context = {'type' : 'Current', 'stream' : stream, 'up_tasks' : up_tasks, 'down_tasks' : down_tasks, 'delta': delta}
    return render_template("current.html", **context)
  
@app.route('/done')
#@app.route('/done/<string:name>')
def done():
    name = request.args.get('user')
    if not name:
        name = 'all'

    stream = request.args.get('stream')
    if not stream:
        stream = 'up_down'

    time_frame = request.args.get('time_frame')

    print >>sys.stderr, "Time frame: ", time_frame
    if time_frame == 'yest':
        delta = 1
    elif time_frame == 'forever':
        delta=180
    else:
        delta=7

    (done_up_tasks, done_down_tasks) = queue_data(arg='done', name=name, stream=stream, delta=delta)

    context = {'type' : 'Done', 'stream' : stream, 'up_tasks' : done_up_tasks, 'down_tasks' : done_down_tasks, 'delta': delta}
    return render_template("current.html", **context)


def queue_data(**kwargs):
    def sorter(d):
        if not d:
            return datetime.datetime(2014, 12, 25, 0, 0, 0)
        return d

    name = kwargs.get('name')
    arg = kwargs.get('arg')
    stream = kwargs.get('stream')
    delta = kwargs.get('delta')

    client = pymongo.MongoClient('lic', 27017)
    collection = client.queue.tasks

    #today = datetime.date.today()
    #today = datetime.datetime(2014, 12, 25, 0, 0, 0)
    someday = datetime.date.today() - datetime.timedelta(delta)
    print >>sys.stderr, someday

    d = datetime.datetime(someday.year, someday.month, someday.day)

    up_task_docs = []
    down_task_docs = []
    if arg == 'current':
        # Ugly hack to get per person upload / download items
        if name == 'all':
            query_dict_up = { '$or' : [ { 'upload_status' : 'pending' }, { 'upload_status' : 'active' }, { 'upload_status' : 'retry' }] }
            query_dict_down = { '$or' : [ { 'download_status' : 'pending' }, { 'download_status' : 'active' }, { 'download_status' : 'retry' } ] }
            #query_dict = { '$and' : [ { 'task_init' : { '$gt' : d } }, { '$or' : [ { 'upload_status' : 'pending' }, { 'upload_status' : 'active' }, { 'download_status' : 'pending' }, { 'download_status' : 'active' }, {'spool_status' : 'pending' } ] } ] }
            #query_dict = {'task_init': {'$gt' : d}, 'upload_status' : 'pending', 'dowmload_status' : 'pending', 'spool_status' : 'pending'}
        else:
            query_dict_up = { '$and' : [ { 'task_owner' : name }, { '$or' : [ { 'upload_status' : 'pending' }, { 'upload_status' : 'active' }, { 'upload_status' : 'retry' } ] }, ] }
            query_dict_down = { '$and' : [ { 'task_owner' : name }, { '$or' : [ { 'download_status' : 'pending' }, { 'download_status' : 'active' }, { 'download_status' : 'retry' } ] }, ] }


        if 'up_down' in stream or 'up' in stream:
            # Upload
            print >>sys.stderr, query_dict_up
            docs = collection.find(query_dict_up).sort('task_init', pymongo.ASCENDING)
            for doc in docs:
                task_doc = {}
                task_doc['task_id'] = doc['task_id']

                try:
                    task_doc['task_owner'] = doc['task_owner']
                except:
                    task_doc['task_owner'] = 'N/A'

                try:
                    task_doc['task_init'] = doc['task_init']
                except:
                    task_doc['task_init'] = None  

                try:
                    task_doc['upload_start'] = doc['upload_start']
                except:
                    task_doc['upload_start'] = None

                try:
                    task_doc['upload_stop'] = doc['upload_stop']
                except:
                    task_doc['upload_stop'] = None   
                              
                try:
                    task_doc['upload_retry_count'] = doc['upload_retry_count']
                except:
                    task_doc['upload_retry_count'] = 0
                                              
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

        if 'up_down' in stream or 'down' in stream:
            # Download
            print >>sys.stderr, query_dict_down
            docs = collection.find(query_dict_down).sort('task_init', pymongo.ASCENDING)
            for doc in docs:
                task_doc = {}
                task_doc['task_id'] = doc['task_id']

                try:
                    task_doc['task_owner'] = doc['task_owner']
                except:
                    task_doc['task_owner'] = 'N/A'

                try:
                    task_doc['task_init'] = doc['task_init']
                except:
                    task_doc['task_init'] = None  

                try:
                    task_doc['download_start'] = doc['download_start']
                except:
                    task_doc['download_start'] = None

                try:
                    task_doc['download_stop'] = doc['download_stop']
                except:
                    task_doc['download_stop'] = None   

                try:
                    task_doc['download_retry_count'] = doc['download_retry_count']
                except:
                    task_doc['download_retry_count'] = 0

                try:
                    task_doc['jid'] = json.loads(doc.get('spool_retval')).get('jid')
                except:
                    task_doc['jid'] = None
                task_doc['download_id'] = doc.get('download_id')
                task_doc['download_status'] = doc.get('download_status')
                task_doc['download_start'] = doc.get('download_start')
                task_doc['download_init'] = doc.get('download_init')
                task_doc['download_exception'] = doc.get('download__exception')
                down_task_docs.append(task_doc)

    elif arg == 'failed':
        if name == 'all':
            query_dict_up = { '$or' : [ { 'upload_status' : 'failed' }, { 'spool_status' : 'failed' } ] }
            query_dict_down = { 'download_status' : 'failed' }
         
            #query_dict_up = { '$and' : [ { '$or' : [ { 'task_init' : { '$gt' : d } } ] }, { '$or' : [ { 'upload_status' : 'failed' }, { 'spool_status' : 'failed' } ] } ] }
            #query_dict_down = { '$and' : [ { '$or' : [ { 'task_init' : { '$gt' : d } } ] }, { '$or' : [ { 'download_status' : 'failed' } ] } ] }
            #query_dict = { '$and' : [ { 'task_init' : { '$gt' : d } }, { '$or' : [ { 'upload_status' : 'pending' }, { 'upload_status' : 'active' }, { 'download_status' : 'pending' }, { 'download_status' : 'active' }, {'spool_status' : 'pending' } ] } ] }
            #query_dict = {'task_init': {'$gt' : d}, 'upload_status' : 'pending', 'dowmload_status' : 'pending', 'spool_status' : 'pending'}
        else:
            query_dict_up = { '$and' : [ { 'task_owner' : name }, { '$or' : [ { 'upload_status' : 'failed' }, { 'spool_status' : 'failed' } ] } ] }
            query_dict_down = { '$and' : [ { 'task_owner' : name }, { 'download_status' : 'failed' } ] }


        if 'up_down' in stream or 'up' in stream:
            # Upload
            docs = collection.find(query_dict_up).sort('task_init', pymongo.ASCENDING)
            for doc in docs:
                task_doc = {}
                task_doc['task_id'] = doc['task_id']

                try:
                    task_doc['task_owner'] = doc['task_owner']
                except:
                    task_doc['task_owner'] = None           

                try:
                    task_doc['task_init'] = doc['task_init']
                except:
                    task_doc['task_init'] = None  

                try:
                    task_doc['upload_start'] = doc['upload_start']
                except:
                    task_doc['upload_start'] = None

                try:
                    task_doc['upload_stop'] = doc['upload_stop']
                except:
                    task_doc['upload_stop'] = None 

                try:
                    task_doc['upload_retry_count'] = doc['upload_retry_count']
                except:
                    task_doc['upload_retry_count'] = 0

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

        if 'up_down' in stream or 'down' in stream:
            # Download
            docs = collection.find(query_dict_down).sort('task_init', pymongo.ASCENDING)
            for doc in docs:
                task_doc = {}
                task_doc['task_id'] = doc['task_id']

                try:
                    task_doc['task_owner'] = doc['task_owner']
                except:
                    task_doc['task_owner'] = None

                try:
                    task_doc['task_init'] = doc['task_init']
                except:
                    task_doc['task_init'] = None  

                try:
                    task_doc['download_start'] = doc['download_start']
                except:
                    task_doc['download_start'] = None

                try:
                    task_doc['download_stop'] = doc['download_stop']
                except:
                    task_doc['download_stop'] = None   

                try:
                    task_doc['download_retry_count'] = doc['download_retry_count']
                except:
                    task_doc['download_retry_count'] = 0

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
            #query_dict_up = { '$and' : [ { '$or' : [ { 'task_init' : { '$gt' : d } } ] }, { '$and' : [ { 'upload_status' : 'done' }, { 'spool_status' : 'done' } ] } ] }
            query_dict_up = { '$and' : [ { 'upload_stop' : { '$gt' : d } }, { 'upload_status' : 'done' }, { 'spool_status' : 'done' } ] }
            query_dict_down = { '$and' : [ { 'download_stop' : { '$gt' : d } } , { 'download_status' : 'done' } ] }
            #query_dict_down = { '$and' : [ { '$or' : [ { 'task_init' : { '$gt' : d } } ] }, { '$or' : [ { 'download_status' : 'done' } ] } ] }
            #query_dict = { '$and' : [ { 'task_init' : { '$gt' : d } }, { '$or' : [ { 'upload_status' : 'pending' }, { 'upload_status' : 'active' }, { 'download_status' : 'pending' }, { 'download_status' : 'active' }, {'spool_status' : 'pending' } ] } ] }
            #query_dict = {'task_init': {'$gt' : d}, 'upload_status' : 'pending', 'dowmload_status' : 'pending', 'spool_status' : 'pending'}
        else:
            #query_dict_up = { '$and' : [ { 'task_owner' : name }, { '$or' : [ { 'upload_stop' : { '$gt' : d } } ] }, { '$and' : [ { 'upload_status' : 'done' }, { 'spool_status' : 'done' } ] } ] }
            query_dict_up = { '$and' : [ { 'task_owner' : name }, { 'upload_stop' : { '$gt' : d } }, { 'upload_status' : 'done' }, { 'spool_status' : 'done' } ] }            
            query_dict_down = { '$and' : [ { 'task_owner' : name }, { 'download_stop' : { '$gt' : d } } , { 'download_status' : 'done' } ] }
                

        if 'up_down' in stream or 'up' in stream:
            print >>sys.stderr, query_dict_up

            # Upload
            docs = collection.find(query_dict_up).sort('task_init', pymongo.ASCENDING)
            for doc in docs:
                task_doc = {}
                task_doc['task_id'] = doc['task_id']

                try:
                    task_doc['task_owner'] = doc['task_owner']
                except:
                    task_doc['task_owner'] = None         

                try:
                    task_doc['task_init'] = doc['task_init']
                except:
                    task_doc['task_init'] = None  

                try:
                    task_doc['upload_start'] = doc['upload_start']
                except:
                    task_doc['upload_start'] = None

                try:
                    task_doc['upload_stop'] = doc['upload_stop']
                except:
                    task_doc['upload_stop'] = None 
                               
                try:
                    task_doc['upload_retry_count'] = doc['upload_retry_count']
                except:
                    task_doc['upload_retry_count'] = 0

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

                up_task_docs.sort(key=lambda x : sorter(x.get('upload_stop')))

        if 'up_down' in stream or 'down' in stream:
            print >>sys.stderr, query_dict_down

            # Download
            docs = collection.find(query_dict_down).sort('task_init', pymongo.ASCENDING)
            for doc in docs:
                task_doc = {}
                task_doc['task_id'] = doc['task_id']

                try:
                    task_doc['task_owner'] = doc['task_owner']
                except:
                    task_doc['task_owner'] = None       

                try:
                    task_doc['task_init'] = doc['task_init']
                except:
                    task_doc['task_init'] = None  

                try:
                    task_doc['download_start'] = doc['download_start']
                except:
                    task_doc['download_start'] = None

                try:
                    task_doc['download_stop'] = doc['download_stop']
                except:
                    task_doc['download_stop'] = None   

                try:
                    task_doc['download_retry_count'] = doc['download_retry_count']
                except:
                    task_doc['download_retry_count'] = 0

                try:
                    task_doc['jid'] = json.loads(doc.get('spool_retval')).get('jid')
                except:
                    task_doc['jid'] = None
                task_doc['download_id'] = doc.get('download_id')
                task_doc['download_status'] = doc.get('download_status')
                task_doc['download_exception'] = doc.get('download__exception')
                down_task_docs.append(task_doc)

                down_task_docs.sort(key=lambda x : sorter(x.get('download_stop')))

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

'''
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

@app.route('/query')
def query():
    stream=request.args.get("stream")
    if not stream:
        stream='down'

    time=request.args.get("time")    
    if not time:
        time='yest'

    return redirect('/done?stream={0}&time_frame={1}'.format(stream, time), code=307)    
'''

@app.route('/date')
def get_date_time():
    d = datetime.date.today()
    return d.strftime("%d-%m-%Y")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=55556, debug=True)

