from flask import Flask, render_template, request
from sqlalchemy import create_engine
import urllib
import time
import random
import redis

app = Flask(__name__)
params = urllib.parse.quote_plus("Driver={ODBC Driver 17 for SQL Server};Server=tcp:punitdb.database.windows.net,1433;Database=punitdb;Uid=punitbawal@punitdb;Pwd={BAW123pun};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
r = redis.StrictRedis(host='punitredis.redis.cache.windows.net', port=6380, db=0, password='kpZmbuD8UhnBWPhbwPhSeI2LsN4dvkCjBpgJ7Fm7K04=', ssl=True)

def redisconn():

    #r.flushall()
    if r.get('foo') != None:
        print("Got It")
    else:
        r.set('foo', 'bar')
        print("Inserted")

def redisQuery(query):
    if r.get(query) == None:
        rcount = engine.execute(query).fetchall()
        r.set(query, rcount[0][0])
        return None
    else:
        rcount= r.get(query)
        #print(rcount)
        return rcount
#redisconn()

@app.route('/')
def hello_world():
    return render_template('index.html')

@app.route('/queryDB', methods=['POST'])
def hi_world():
    if request.form['form'] == 'Submit' or request.form['form'] == 'SubmitCache':
        mag = request.form['mag']
        oper = request.form['oper']
        if mag != '' and oper != '':
            query = "SELECT COUNT(*) FROM earthquake where mag "+oper+mag
            if request.form['form'] == 'Submit':
                startTime = time.perf_counter()
                cnt = engine.execute(query).fetchall()
                endTime = time.perf_counter()
                print(cnt[0][0])
                print("Time Taken: ", endTime-startTime)
            elif request.form['form'] == 'SubmitCache':
                startTime = time.perf_counter()
                cnt = redisQuery(query)
                endTime = time.perf_counter()
                print(cnt)
                if cnt is None:
                    return "Not found in Cache. Inserted for next"
                print("Time Taken: ", endTime - startTime)
    elif request.form['form'] == 'Submit2' or request.form['form'] == 'SubmitCache2':
        qcount = request.form['qcount']
        qcount = int(qcount)
        smag = float(request.form['smag'])
        emag = float(request.form['emag'])
        if qcount != '' and request.form['smag'] != '' and request.form['emag'] != '':
            if request.form['form'] == 'Submit2':
                startTime = time.perf_counter()
                while qcount != 0:
                    query = "SELECT COUNT(*) FROM earthquake where mag =" + str(round(random.uniform(smag, emag), 1))
                    cnt = engine.execute(query).fetchall()
                    qcount = qcount - 1
                endTime = time.perf_counter()
                #print(cnt[0][0])
                print("Time Taken: ", endTime-startTime)
            elif request.form['form'] == 'SubmitCache2':
                startTime = time.perf_counter()
                while qcount != 0:
                    query = "SELECT COUNT(*) FROM earthquake where mag =" + str(round(random.uniform(smag, emag), 1))
                    print(query)
                    cnt = redisQuery(query)
                    qcount = qcount - 1
                endTime = time.perf_counter()
                #print(cnt[0][0])
                print("Time Taken: ", endTime-startTime)
    return 'Time Taken:'+str(endTime-startTime)