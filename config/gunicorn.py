"""
Gunicorn configuration.

Enables prometheus_client multiprocess mode so that the Django Prometheus
metrics (request counts, latency histograms, response sizes, ...) are
aggregated across every gunicorn worker into a single registry.

Without this, each sync worker keeps its own in-memory counters and a scrape
of ``/metrics`` is answered by whichever worker happens to handle it. Prometheus
then sees a different worker's counters on each scrape, the series is
non-monotonic, and ``rate()``/``increase()`` treat the jumps as counter resets
- making the numbers on the Grafana dashboard approximate. With multiprocess
mode the workers share metric files in ``PROMETHEUS_MULTIPROC_DIR`` and the
``/metrics`` endpoint aggregates them, so counters/histograms are exact.

This module is imported by the gunicorn arbiter at startup (before workers are
forked), so setting the environment variable here propagates to every worker.
"""

import os
import shutil

# Shared directory the workers write their metric files to. Respect an existing
# value if the platform already sets one; otherwise fall back to a per-container
# tmp path. It must be local to the container and cleared on every boot.
prometheus_multiproc_dir = os.environ.setdefault(
    'PROMETHEUS_MULTIPROC_DIR', '/tmp/prometheus_multiproc',
)

# Start each boot from a clean directory - stale files left over from a previous
# run would corrupt the aggregated values.
shutil.rmtree(prometheus_multiproc_dir, ignore_errors=True)
os.makedirs(prometheus_multiproc_dir, exist_ok=True)


def child_exit(server, worker):
    """Reap a dead worker's metric files so its samples aren't double counted.

    Called by the gunicorn arbiter whenever a worker exits (crash, reload, or a
    ``--timeout`` kill). Required for correct counters in multiprocess mode.
    """
    from prometheus_client import multiprocess
    multiprocess.mark_process_dead(worker.pid)
