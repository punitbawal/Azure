from flask import Flask, flash, redirect, render_template, request, session, abort, url_for
import pandas as pd
from sqlalchemy import create_engine, types
import urllib
import time
import random

app = Flask(__name__)
params = urllib.parse.quote_plus("Driver={ODBC Driver 13 for SQL Server};Server=tcp:punitdb.database.windows.net,1433;Database=punitdb;Uid=punitbawal@punitdb;Pwd={BAW123pun};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

@app.route('/')
def hello_world():
    return render_template('index.html')

@app.route('/queryDB', methods=['POST'])
def hi_world():
    if request.form['form'] == 'Submit':
        mag = request.form['mag']
        oper = request.form['oper']
        if mag != '' and oper != '':
            startTime = time.perf_counter()
            cnt = engine.execute("SELECT COUNT(*) FROM earthquake where mag "+oper+mag).fetchall()
            endTime = time.perf_counter()
            print(cnt[0][0])
            print("Time Taken: ", endTime-startTime)
    elif request.form['form'] == 'Submit2':
        qcount = request.form['qcount']
        qcount = int(qcount)
        if qcount != '':
            startTime = time.perf_counter()
            while qcount != 0:
                cnt = engine.execute("SELECT COUNT(*) FROM earthquake where mag ="+str(round(random.uniform(4.5, 5.0), 1))).fetchall()
                qcount = qcount - 1
            endTime = time.perf_counter()
            #print(cnt[0][0])
            print("Time Taken: ", endTime-startTime)
    return 'Time Taken:'+str(endTime-startTime)