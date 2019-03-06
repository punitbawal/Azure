from flask import Flask, render_template, request
from sqlalchemy import create_engine
import urllib
import time
import random
import redis
import pygal
import pandas as pd
import json

app = Flask(__name__)
params = urllib.parse.quote_plus("Driver={ODBC Driver 17 for SQL Server};Server=tcp:punitdb.database.windows.net,1433;Database=punitdb;Uid=punitbawal@punitdb;Pwd={BAW123pun};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
r = redis.StrictRedis(host='punitredis.redis.cache.windows.net', port=6380, db=0, password='kpZmbuD8UhnBWPhbwPhSeI2LsN4dvkCjBpgJ7Fm7K04=', ssl=True)

line_chart = pygal.Bar()
line_chart.add('a', [1, 2])
line_chart.add('b', [1, 3])

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
        r.set(query, str(rcount))
        return None
    else:
        rcount= r.get(query)
        print(rcount)
        return rcount
#redisconn()


@app.route('/')
def hello_world():
    return render_template('index.html', chart=line_chart.render_data_uri())


@app.route('/barchart')
def bar():
    rows = engine.execute("select CAST(locationsource as varchar(max)) as locationsource,count(*) as cnt from earthquake2 group by CAST(locationsource as varchar(max));").fetchall()
    #rows = engine.execute("select top(1000) id, latitude, longitude, mag, CAST(locationsource as varchar(max)) as locationsource from earthquake2;").fetchall()
    rows = [dict(row) for row in rows]
    #return render_template('visual.html', a=rows, chartType='Scatter')
    #return render_template('visual.html', a=rows, chartType='Pie')
    return render_template('visual.html', a=rows, chartType='Bar')



@app.route('/queryDB', methods=['POST'])
def hi_world():
    qot = '\''
    #Magnitude based Operations
    if request.form['form'] == 'Submit' or request.form['form'] == 'SubmitCache':
        mag = request.form['mag']
        oper = request.form['oper']
        magfreq = int(request.form['magfreq'])
        if mag != '' and oper != '':
            query = "SELECT count(*) FROM earthquake where mag "+oper+mag
            if request.form['form'] == 'Submit':
                startTime = time.perf_counter()
                while magfreq != 0:
                    cnt = engine.execute(query).fetchall()
                    magfreq = magfreq - 1
                endTime = time.perf_counter()
                #print(cnt[0][0])
                print("Time Taken: ", endTime-startTime)
            elif request.form['form'] == 'SubmitCache':
                startTime = time.perf_counter()
                while magfreq != 0:
                    cnt = redisQuery(query)
                    magfreq = magfreq -1
                endTime = time.perf_counter()
                #print(cnt)
                if cnt is None:
                    return "Not found in Cache. Inserted for next"
                print("Time Taken: ", endTime - startTime)
    #Between Magnitude Query
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
    #LatLong & Radius based Operations
    elif request.form['form'] == 'SubmitRad' or request.form['form'] == 'SubmitRadCache':
        lat = request.form['lat']
        longi = request.form['long']
        rad = request.form['rad']
        latlongfreq = int(request.form['latlongfreq'])
        if lat != '' and longi != '' and rad != '' and request.form['latlongfreq'] != '':
            if request.form['form'] == 'SubmitRad':
                startTime = time.perf_counter()
                while latlongfreq != 0:
                    query = "SELECT count(*) FROM earthquake where ( 6371  * acos( cos( radians(" + lat + ") ) * cos( radians( latitude ) ) * cos( radians(longitude) - radians(" + longi + ")) + sin(radians(" + lat + ")) * sin( radians(latitude)))) <= " + rad
                    cnt = engine.execute(query).fetchall()
                    latlongfreq = latlongfreq - 1
                endTime = time.perf_counter()
                #print(cnt[0][0])
                print("Time Taken: ", endTime-startTime)
            elif request.form['form'] == 'SubmitRadCache':
                startTime = time.perf_counter()
                while latlongfreq != 0:
                    query = "SELECT count(*) FROM earthquake where ( 6371  * acos( cos( radians(" + lat + ") ) * cos( radians( latitude ) ) * cos( radians(longitude) - radians(" + longi + ")) + sin(radians(" + lat + ")) * sin( radians(latitude)))) <= " + rad
                    #print(query)
                    cnt = redisQuery(query)
                    latlongfreq = latlongfreq - 1
                endTime = time.perf_counter()
                #print(cnt[0][0])
                print("Time Taken: ", endTime-startTime)
    #Location based Operations
    elif request.form['form'] == 'SubmitLoc' or request.form['form'] == 'SubmitLocCache':
        place = request.form['place']
        locfreq = int(request.form['locfreq'])
        if place != '' and request.form['locfreq'] != '':
            if request.form['form'] == 'SubmitLoc':
                startTime = time.perf_counter()
                while locfreq != 0:
                    query = '''SELECT count(*) FROM earthquake where locationSource LIKE '''+qot+place+qot
                    cnt = engine.execute(query).fetchall()
                    locfreq = locfreq - 1
                endTime = time.perf_counter()
                #print(cnt[0][0])
                print("Time Taken: ", endTime-startTime)
            elif request.form['form'] == 'SubmitLocCache':
                startTime = time.perf_counter()
                while locfreq != 0:
                    query = '''SELECT count(*) FROM earthquake where locationSource LIKE '''+qot+place+qot
                    #print(query)
                    cnt = redisQuery(query)
                    locfreq = locfreq - 1
                endTime = time.perf_counter()
                #print(cnt[0][0])
                print("Time Taken: ", endTime-startTime)
    # Load Table
    elif request.form['form'] == 'LoadTable':
        startTime = time.perf_counter()
        #engine.execute("create table earthquake2(time Datetime,latitude float,longitude float,depth float,mag float,magType text,nst int,gap float,dmin float,rms float,net text,id text,updated datetime,place text,type text,horizontalError float,depthError float,magError float,magNst int,status text,locationSource text,magSource text)")
        #engine.execute("create table counties(county_name text, county_state text)")
        #engine.execute("create table population(population_state text, population_2010 int, population_2011 int, population_2012 int, population_2013 int, population_2014 int, population_2015 int, population_2016 int, population_2017 int, population_2018 int)")
        engine.execute("create table state_codes(state_codes_code text,state_codes_state text)")
        df = pd.read_csv("statecode.csv", sep=',', quotechar='"', encoding='utf8')
        df.to_sql('state_codes', con=engine, index=False, if_exists='append')
        endTime = time.perf_counter()
        # print(cnt[0][0])
        print("Time Taken: ", endTime - startTime)
    # Flush Cache
    elif request.form['form'] == 'FlushCache':
        r.flushall()
        return "Redis Cache Flushed"
    #Show Population
    elif request.form['form'] == 'ShowPopulation' or request.form['form'] == 'ShowPopulationCache':
        stateLetters = request.form['stateLetters']
        year = request.form['year']
        if year not in ['2010','2011','2012','2013','2014','2015','2016','2017','2018']:
            return "Invalid Year"
        if stateLetters != '' and year != '':
            if request.form['form'] == 'ShowPopulation':
                query = "Select population_"+year+" from population,state_codes where state_codes_code LIKE "+qot+stateLetters+qot+" and population_state LIKE state_codes_state"
                cnt = engine.execute(query).fetchall()
                if cnt is None:
                    return "No record exist"
                else:
                    return str(cnt)
            else:
                query = "Select population_" + year + " from population,state_codes where state_codes_code LIKE " + qot + stateLetters + qot + " and population_state LIKE state_codes_state"
                cnt = redisQuery(query)
                if cnt is None:
                    return "No record exist"
                else:
                    return str(cnt)
    #Show County Count
    elif request.form['form'] == 'ShowCountyCount' or request.form['form'] == 'ShowCountyCountCache':
        stateCode = request.form['stateCode']
        freq1 = int(request.form['freq1'])
        if stateCode != '':
            if request.form['form'] == 'ShowCountyCount':
                startTime = time.perf_counter()
                while freq1 != 0:
                    query = "Select CAST(state_codes_code as varchar(max)), count(CAST(county_name as varchar(max))) from counties, state_codes where county_state LIKE state_codes_state and state_codes_code LIKE "+qot+stateCode+qot+" group by CAST(state_codes_code as varchar(max))"
                    query2 = "Select CAST(county_name as varchar(max)) from counties, state_codes where county_state LIKE state_codes_state and state_codes_code LIKE " + qot + stateCode + qot
                    cnt = engine.execute(query).fetchall()
                    res = engine.execute(query2).fetchall()
                    freq1 = freq1 - 1
                endTime = time.perf_counter()
                if cnt is None:
                    return "No record exist"
                else:
                    return str(endTime - startTime)+str(cnt)+str(res)
            else:
                startTime = time.perf_counter()
                while freq1 != 0:
                    query = "Select CAST(state_codes_code as varchar(max)), count(CAST(county_name as varchar(max))) from counties, state_codes where county_state LIKE state_codes_state and state_codes_code LIKE " + qot + stateCode + qot + " group by CAST(state_codes_code as varchar(max))"
                    query2 = "Select CAST(county_name as varchar(max)) from counties, state_codes where county_state LIKE state_codes_state and state_codes_code LIKE " + qot + stateCode + qot
                    cnt = redisQuery(query)
                    res = redisQuery(query2)
                    freq1 = freq1 - 1
                endTime = time.perf_counter()
                if cnt is None:
                    return "No record exist"
                else:
                    return str(endTime - startTime) + str(cnt) + str(res)
    # Show State
    elif request.form['form'] == 'ShowState' or request.form['form'] == 'ShowStateCache':
        stateYear = request.form['stateYear']
        pop1 = request.form['statepop1']
        pop2 = request.form['statepop2']
        freq2 = int(request.form['freq2'])
        if stateYear not in ['2010','2011','2012','2013','2014','2015','2016','2017','2018']:
            return "Invalid Year"
        if stateYear != '' and pop1 != '' and pop2 != '':
            if request.form['form'] == 'ShowState':
                startTime = time.perf_counter()
                while freq2 != 0:
                    query = "select population_state from population where population_"+stateYear+" between "+pop1+" and "+pop2
                    cnt = engine.execute(query).fetchall()
                    freq2 = freq2 - 1
                endTime = time.perf_counter()
                if cnt is None:
                    return "No record exist"
                else:
                    return str(endTime-startTime)+ str(cnt)
            else:
                startTime = time.perf_counter()
                while freq2 != 0:
                    query = "select population_state from population where population_" + stateYear + " between " + pop1 + " and " + pop2
                    cnt = redisQuery(query)
                    freq2 = freq2 - 1
                endTime = time.perf_counter()
                if cnt is None:
                    return "No record exist"
                else:
                    return str(endTime - startTime) + str(cnt)
    return 'Time Taken:'+str(endTime-startTime)