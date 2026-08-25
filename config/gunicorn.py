"""Gunicorn hooks for ``prometheus_client`` multiprocess mode.

``PROMETHEUS_MULTIPROC_DIR`` must be prepared once, before Gunicorn starts.
Never clean it while importing this module: Gunicorn imports its configuration
again during a HUP reload, while the existing workers' metrics are still live.
"""


def child_exit(server, worker):
    """Mark a dead worker so Prometheus can clean up live gauge files.

    Called by the gunicorn arbiter whenever a worker exits (crash, reload, or a
    ``--timeout`` kill). Required by ``prometheus_client`` multiprocess mode.
    """
    from prometheus_client import multiprocess
    multiprocess.mark_process_dead(worker.pid)
