import os
import re
import signal
import socket
import subprocess  # noqa: S404
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, skipUnless
from unittest.mock import patch
from urllib.error import URLError
from urllib.request import urlopen

from config import gunicorn, prometheus_multiproc


def _assert_isolated_test_directory(directory):
    production_directory = prometheus_multiproc.PROMETHEUS_MULTIPROC_DIR
    if (
        not directory.is_absolute()
        or directory == production_directory
        or production_directory in directory.parents
        or directory in production_directory.parents
    ):
        raise AssertionError(
            'Test directory must be isolated from {0}'.format(
                production_directory,
            ),
        )


def _isolated_temporary_directory():
    production_directory = prometheus_multiproc.PROMETHEUS_MULTIPROC_DIR
    temporary_directory = tempfile.TemporaryDirectory(
        prefix='giga-prometheus-test-',
        dir=str(production_directory.parent),
    )
    directory = Path(temporary_directory.name)
    try:
        _assert_isolated_test_directory(directory)
    except AssertionError:
        temporary_directory.cleanup()
        raise
    return temporary_directory, directory


class PrometheusMultiprocessPreparationTestCase(TestCase):
    def setUp(self):
        self.previous_environment = os.environ.pop(
            'PROMETHEUS_MULTIPROC_DIR', None,
        )
        self.temporary_directory, temporary_path = (
            _isolated_temporary_directory()
        )
        self.directory = temporary_path / 'prometheus_multiproc'
        _assert_isolated_test_directory(self.directory)

    def tearDown(self):
        self.temporary_directory.cleanup()
        if self.previous_environment is not None:
            os.environ['PROMETHEUS_MULTIPROC_DIR'] = (
                self.previous_environment
            )
        else:
            os.environ.pop('PROMETHEUS_MULTIPROC_DIR', None)

    def _prepare_test_directory(self):
        open_directory = prometheus_multiproc._open_directory

        def open_isolated_directory(directory):
            self.assertEqual(
                directory,
                prometheus_multiproc.PROMETHEUS_MULTIPROC_DIR,
            )
            return open_directory(self.directory)

        with patch.object(
            prometheus_multiproc,
            '_open_directory',
            side_effect=open_isolated_directory,
        ) as open_directory_mock:
            try:
                prometheus_multiproc.prepare_prometheus_multiproc_dir()
            finally:
                open_directory_mock.assert_called_once_with(
                    prometheus_multiproc.PROMETHEUS_MULTIPROC_DIR,
                )

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

        self._prepare_test_directory()

        self.assertEqual(list(self.directory.iterdir()), [])
        self.assertEqual(
            os.environ['PROMETHEUS_MULTIPROC_DIR'],
            str(prometheus_multiproc.PROMETHEUS_MULTIPROC_DIR),
        )

    def test_rejects_a_different_environment_path(self):
        os.environ['PROMETHEUS_MULTIPROC_DIR'] = '/'

        with patch.object(
            prometheus_multiproc,
            '_open_approved_directory',
        ) as open_approved_directory:
            with self.assertRaisesRegex(RuntimeError, 'must be exactly'):
                prometheus_multiproc.prepare_prometheus_multiproc_dir()

        open_approved_directory.assert_not_called()

        self.assertFalse(self.directory.exists())

    def test_production_path_remains_fixed_and_approved(self):
        self.assertEqual(
            prometheus_multiproc.PROMETHEUS_MULTIPROC_DIR,
            Path('/tmp/prometheus_multiproc'),  # noqa: S108
        )

        with patch.object(
            prometheus_multiproc,
            '_open_directory',
        ) as open_directory:
            with patch.object(
                prometheus_multiproc,
                'PROMETHEUS_MULTIPROC_DIR',
                self.directory,
            ):
                with self.assertRaisesRegex(RuntimeError, 'unapproved'):
                    prometheus_multiproc._open_approved_directory()

        open_directory.assert_not_called()

    def test_main_prepares_storage_before_execing_gunicorn(self):
        events = []
        command = ['gunicorn', 'config.wsgi:application']

        with patch.object(
            prometheus_multiproc,
            'prepare_prometheus_multiproc_dir',
            side_effect=lambda: events.append('prepare'),
        ) as prepare_directory, patch.object(
            prometheus_multiproc.os,
            'execvpe',
            side_effect=lambda *args: events.append('exec'),
        ) as execvpe:
            prometheus_multiproc.main(['--'] + command)

        self.assertEqual(events, ['prepare', 'exec'])
        prepare_directory.assert_called_once_with()
        execvpe.assert_called_once()
        self.assertEqual(execvpe.call_args.args[:2], ('gunicorn', command))
        self.assertIsInstance(execvpe.call_args.args[2], dict)

    def test_rejects_unexpected_entries_before_deleting_anything(self):
        self.directory.mkdir()
        expected_file = self.directory / 'counter_101.db'
        unexpected_file = self.directory / 'application.log'
        expected_file.touch()
        unexpected_file.touch()

        with self.assertRaisesRegex(RuntimeError, 'unexpected'):
            self._prepare_test_directory()

        self.assertTrue(expected_file.exists())
        self.assertTrue(unexpected_file.exists())

    def test_rejects_a_symlink_at_the_approved_path(self):
        target_temporary_directory, target_directory = (
            _isolated_temporary_directory()
        )
        with target_temporary_directory:
            marker = Path(target_directory) / 'marker'
            marker.touch()
            self.directory.symlink_to(target_directory, target_is_directory=True)

            with self.assertRaisesRegex(RuntimeError, 'real directory'):
                self._prepare_test_directory()

            self.assertTrue(marker.exists())


class GunicornChildExitTestCase(TestCase):
    def test_child_exit_marks_the_worker_dead(self):
        worker = SimpleNamespace(pid=1234)
        with patch(
            'prometheus_client.multiprocess.mark_process_dead',
        ) as mark_process_dead:
            gunicorn.child_exit(None, worker)

        mark_process_dead.assert_called_once_with(worker.pid)


@skipUnless(
    sys.platform.startswith('linux') and Path(
        '/proc/{0}/task/{0}/children'.format(os.getpid()),
    ).is_file(),
    'requires Linux /proc worker inspection',
)
class GunicornMultiprocessLifecycleTestCase(TestCase):
    address = None
    server = None
    temporary_directory = None
    metric_name = 'giga_test_worker_requests_total'

    @classmethod
    def setUpClass(cls):
        environment = os.environ.copy()
        cls.temporary_directory, cls.directory = (
            _isolated_temporary_directory()
        )
        cls.addClassCleanup(cls.temporary_directory.cleanup)
        _assert_isolated_test_directory(cls.directory)
        environment['PROMETHEUS_MULTIPROC_DIR'] = str(cls.directory)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(('127.0.0.1', 0))
            listener.listen()
            port = listener.getsockname()[1]
            cls.address = 'http://127.0.0.1:{0}'.format(port)

            cls.server = subprocess.Popen(  # noqa: S603
                [
                    sys.executable,
                    '-m',
                    'gunicorn',
                    'config.tests.multiprocess_wsgi:application',
                    '-c',
                    'config/gunicorn.py',
                    '--bind',
                    'fd://{0}'.format(listener.fileno()),
                    '--workers',
                    '2',
                    '--log-level',
                    'warning',
                ],
                env=environment,
                pass_fds=(listener.fileno(),),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            cls.addClassCleanup(cls._stop_server)

        cls._wait_until(cls._server_is_ready, 'Gunicorn did not start')

    @classmethod
    def _stop_server(cls):
        if cls.server is not None and cls.server.poll() is None:
            cls.server.terminate()
            try:
                cls.server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                cls.server.kill()
                cls.server.wait(timeout=5)

    @classmethod
    def _server_output(cls):
        if cls.server is None or cls.server.stdout is None:
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
        self.assertNotEqual(
            self.directory,
            prometheus_multiproc.PROMETHEUS_MULTIPROC_DIR,
        )
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
