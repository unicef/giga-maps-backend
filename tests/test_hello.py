import requests
from hello import app


class DummyResponse:
    def __init__(self, text='metrics', content_type='text/plain'):
        self.text = text
        self.headers = {'Content-Type': content_type}
        self.status_code = 200

    def raise_for_status(self):
        return None


def test_metrics_success(monkeypatch):
    monkeypatch.setattr('requests.get', lambda url, timeout: DummyResponse())
    client = app.test_client()
    res = client.get('/metrics')
    assert res.status_code == 200
    assert b'metrics' in res.data


def test_metrics_unavailable(monkeypatch):
    def raise_exc(url, timeout):
        raise requests.RequestException

    monkeypatch.setattr('requests.get', raise_exc)
    client = app.test_client()
    res = client.get('/metrics')
    assert res.status_code == 503
    assert b'Metrics unavailable' in res.data
