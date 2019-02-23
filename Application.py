from flask import Flask
import pandas as pd
from sqlalchemy import create_engine, types
import urllib

app = Flask(__name__)
params = urllib.parse.quote_plus("Driver={ODBC Driver 13 for SQL Server};Server=tcp:punitdb.database.windows.net,1433;Database=punitdb;Uid=punitbawal@punitdb;Pwd={BAW123pun};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


@app.route('/')
def hello_world():
    return 'Hello World. Again!'

@app.route('/hi')
def hi_world():
    cnt = engine.execute("SELECT count(*) FROM earthquake").fetchall()
    print(cnt[0][0])
    return 'Hi World. Again!'+str(cnt[0][0])