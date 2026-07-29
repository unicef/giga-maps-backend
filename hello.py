import os
import requests

from flask import Flask, Response

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/metrics')
def metrics():
    port = os.environ.get('FLOWER_PORT', '6543')
    url = f'http://localhost:{port}/metrics'
    try:
        resp = requests.get(url, timeout=2)
        resp.raise_for_status()
    except requests.RequestException:
        return 'Metrics unavailable', 503

    content_type = resp.headers.get('Content-Type', 'text/plain')
    return Response(resp.text, content_type=content_type)
