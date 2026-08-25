import os

from prometheus_client import CollectorRegistry, Counter, generate_latest, multiprocess

REQUESTS = Counter(
    'giga_test_worker_requests_total',
    'Requests handled by the multiprocess lifecycle regression app.',
)


def application(environ, start_response):
    if environ['PATH_INFO'] == '/metrics':
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        payload = generate_latest(registry)
        start_response(
            '200 OK',
            [
                ('Content-Type', 'text/plain; version=0.0.4'),
                ('Content-Length', str(len(payload))),
            ],
        )
        return [payload]

    REQUESTS.inc()
    payload = str(os.getpid()).encode('ascii')
    start_response(
        '200 OK',
        [
            ('Content-Type', 'text/plain'),
            ('Content-Length', str(len(payload))),
        ],
    )
    return [payload]
