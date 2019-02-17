from flask import Flask

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World. Again!'

@app.route('/hi')
def hi_world():
    return 'Hi World. Again!'