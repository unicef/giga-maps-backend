import os
import re
import shutil
import signal
import subprocess  # noqa: S404
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch
from urllib.error import URLError
from urllib.request import urlopen

from config import gunicorn, prometheus_multiproc


class PrometheusMultiprocessPreparationTestCase(TestCase):
    directory = prometheus_multiproc.PROMETHEUS_MULTIPROC_DIR

    def setUp(self):
        self.previous_environment = os.environ.pop(
            'PROMETHEUS_MULTIPROC_DIR', None,
        )
        self._remove_test_directory()

    def tearDown(self):
        self._remove_test_directory()
        if self.previous_environment is not None:
            os.environ['PROMETHEUS_MULTIPROC_DIR'] = (
                self.previous_environment
            )
        else:
            os.environ.pop('PROMETHEUS_MULTIPROC_DIR', None)

    def _remove_test_directory(self):
        if self.directory.is_symlink():
            self.directory.unlink()
        elif self.directory.exists():
            shutil.rmtree(str(self.directory))

    def test_cleans_only_expected_metric_files(self):
        self.directory.mkdir()
        expected_files = (
            'counter_101.db',
            'histogram_102.db',
            'summary_103.db',
            'gauge_livesum_104.db',
        )
        for file_name in expected_files:
            (self.directory / file_name).touch()

        prometheus_multiproc.prepare_prometheus_multiproc_dir()

        self.assertEqual(list(self.directory.iterdir()), [])
        self.assertEqual(
            os.environ['PROMETHEUS_MULTIPROC_DIR'],
            str(self.directory),
        )

    def test_rejects_a_different_environment_path(self):
        os.environ['PROMETHEUS_MULTIPROC_DIR'] = '/'

        with self.assertRaisesRegex(RuntimeError, 'must be exactly'):
            prometheus_multiproc.prepare_prometheus_multiproc_dir()

        self.assertFalse(self.directory.exists())

    def test_rejects_unexpected_entries_before_deleting_anything(self):
        self.directory.mkdir()
        expected_file = self.directory / 'counter_101.db'
        unexpected_file = self.directory / 'application.log'
        expected_file.touch()
        unexpected_file.touch()

        with self.assertRaisesRegex(RuntimeError, 'unexpected'):
            prometheus_multiproc.prepare_prometheus_multiproc_dir()

        self.assertTrue(expected_file.exists())
        self.assertTrue(unexpected_file.exists())

    def test_rejects_a_symlink_at_the_approved_path(self):
        with tempfile.TemporaryDirectory(  # noqa: S108
            dir='/tmp',  # noqa: S108
        ) as target_directory:
            marker = Path(target_directory) / 'marker'
            marker.touch()
            self.directory.symlink_to(target_directory, target_is_directory=True)

            with self.assertRaisesRegex(RuntimeError, 'real directory'):
                prometheus_multiproc.prepare_prometheus_multiproc_dir()

            self.assertTrue(marker.exists())


class GunicornChildExitTestCase(TestCase):
    def test_child_exit_marks_the_worker_dead(self):
        worker = SimpleNamespace(pid=1234)
        with patch(
            'prometheus_client.multiprocess.mark_process_dead',
        ) as mark_process_dead:
            gunicorn.child_exit(None, worker)

        mark_process_dead.assert_called_once_with(worker.pid)


class GunicornMultiprocessLifecycleTestCase(TestCase):
    address = 'http://127.0.0.1:18043'
    metric_name = 'giga_test_worker_requests_total'

    @classmethod
    def setUpClass(cls):
        environment = os.environ.copy()
        environment.pop('PROMETHEUS_MULTIPROC_DIR', None)
        cls.server = subprocess.Popen(  # noqa: S603
            [
                sys.executable,
                '-m',
                'config.prometheus_multiproc',
                '--',
                sys.executable,
                '-m',
                'gunicorn',
                'config.tests.multiprocess_wsgi:application',
                '-c',
                'config/gunicorn.py',
                '--bind',
                '127.0.0.1:18043',
                '--workers',
                '2',
                '--log-level',
                'warning',
            ],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            cls._wait_until(cls._server_is_ready, 'Gunicorn did not start')
        except AssertionError:
            cls._stop_server()
            raise

    @classmethod
    def tearDownClass(cls):
        cls._stop_server()
        directory = prometheus_multiproc.PROMETHEUS_MULTIPROC_DIR
        if directory.exists():
            shutil.rmtree(str(directory))

    @classmethod
    def _stop_server(cls):
        if cls.server.poll() is None:
            cls.server.terminate()
            try:
                cls.server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                cls.server.kill()
                cls.server.wait(timeout=5)

    @classmethod
    def _server_output(cls):
        if cls.server.stdout is None:
            return ''
        return cls.server.stdout.read()

    @classmethod
    def _wait_until(cls, condition, message, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if cls.server.poll() is not None:
                raise AssertionError(
                    '{0}\n{1}'.format(message, cls._server_output()),
                )
            result = condition()
            if result:
                return result
            time.sleep(0.05)
        raise AssertionError(message)

    @classmethod
    def _server_is_ready(cls):
        try:
            cls._request('/metrics')
        except URLError:
            return False
        return True

    @classmethod
    def _request(cls, path):
        with urlopen(  # noqa: S310
            cls.address + path,
            timeout=2,
        ) as response:
            return response.read().decode('utf-8')

    @classmethod
    def _workers(cls):
        children = Path(
            '/proc/{0}/task/{0}/children'.format(
                cls.server.pid,
            ),
        ).read_text().split()
        return {int(pid) for pid in children}

    @classmethod
    def _metric_value(cls):
        payload = cls._request('/metrics')
        match = re.search(
            r'^{0}\s+([0-9.eE+-]+)$'.format(cls.metric_name),
            payload,
            re.MULTILINE,
        )
        if match is None:
            return 0
        return float(match.group(1))

    @classmethod
    def _send_requests(cls, count):
        worker_pids = []
        for _ in range(count):
            worker_pids.append(int(cls._request('/request')))
        return worker_pids

    def test_two_worker_metrics_survive_replacement_and_hup_reload(self):
        initial_workers = self._wait_until(
            lambda: self._workers() if len(self._workers()) == 2 else None,
            'Gunicorn did not start two workers',
        )

        request_count = 0
        observed_workers = set()
        deadline = time.monotonic() + 10
        while len(observed_workers) < 2 and time.monotonic() < deadline:
            observed_workers.update(self._send_requests(1))
            request_count += 1

        self.assertEqual(observed_workers, initial_workers)
        self.assertEqual(self._metric_value(), request_count)

        replaced_worker = min(initial_workers)
        os.kill(replaced_worker, signal.SIGTERM)
        replacement_workers = self._wait_until(
            lambda: (
                self._workers()
                if len(self._workers()) == 2
                and replaced_worker not in self._workers()
                else None
            ),
            'Gunicorn did not replace the terminated worker',
        )
        before_replacement_requests = self._metric_value()
        self.assertEqual(before_replacement_requests, request_count)
        self._send_requests(12)
        self.assertEqual(
            self._metric_value(),
            before_replacement_requests + 12,
        )

        before_hup = self._metric_value()
        os.kill(self.server.pid, signal.SIGHUP)
        self._wait_until(
            lambda: (
                self._workers()
                if len(self._workers()) == 2
                and self._workers().isdisjoint(replacement_workers)
                else None
            ),
            'Gunicorn did not complete the HUP worker reload',
        )
        self.assertEqual(self._metric_value(), before_hup)
        self._send_requests(12)
        self.assertEqual(self._metric_value(), before_hup + 12)
