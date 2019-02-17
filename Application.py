from flask import Flask

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World. Again!'

@app.route('/hi')
def hello_world():
    return 'Hi World. Again!'