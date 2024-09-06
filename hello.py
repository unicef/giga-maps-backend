import os
import requests

from flask import Flask

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route("/metrics")
def metrics():
    response = requests.get('http://localhost:{port}/metrics'.format(port=os.environ.get('FLOWER_PORT', 6543)))
    return response.text
