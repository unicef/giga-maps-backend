"""Prepare Prometheus multiprocess storage once, then run a command.

Startup flow::

    web-worker.sh
      -> validate the approved container-local path
      -> remove mmap files left by the previous Gunicorn run
      -> set PROMETHEUS_MULTIPROC_DIR
      -> exec Gunicorn
      -> workers create new mmap files

The helper runs only for a full application/container startup. It does not run
for a Gunicorn HUP or worker replacement, so counters and histograms survive
those events. ``config.gunicorn.child_exit`` marks dead workers and lets
``prometheus_client`` remove their live-gauge files. A new application startup
intentionally resets counters for the new process lifecycle.

Aggregation is limited to Gunicorn workers within one App Service instance;
each scaled-out App Service instance has separate storage. With the currently
pinned django-prometheus/prometheus-client behavior, the multiprocess exporter
also omits ``process_*``, ``python_gc_*``, and ``python_info``. Review dependent
dashboards and alerts before enabling this mode in production.
"""

import argparse
import os
import re
import stat
from pathlib import Path

PROMETHEUS_MULTIPROC_DIR = Path('/tmp/prometheus_multiproc')  # noqa: S108
_EXPECTED_METRIC_FILE = re.compile(
    r'^(?:counter|histogram|summary)_[1-9][0-9]*\.db$'
    r'|^gauge_(?:all|liveall|min|livemin|max|livemax|sum|livesum'
    r'|mostrecent|livemostrecent)_[1-9][0-9]*\.db$',
)


def _validate_environment():
    configured_path = os.environ.get('PROMETHEUS_MULTIPROC_DIR')
    approved_path = str(PROMETHEUS_MULTIPROC_DIR)
    if configured_path is not None and configured_path != approved_path:
        raise RuntimeError(
            'PROMETHEUS_MULTIPROC_DIR must be exactly {0}'.format(
                approved_path,
            ),
        )


def _open_directory(directory):
    """Open ``directory`` without following a final-component symlink."""
    try:
        directory.mkdir(mode=0o700)
    except FileExistsError:
        pass

    flags = os.O_RDONLY | os.O_DIRECTORY
    if hasattr(os, 'O_NOFOLLOW'):
        flags |= os.O_NOFOLLOW
    try:
        return os.open(str(directory), flags)
    except OSError as error:
        raise RuntimeError(
            'Prometheus multiprocess path must be a real directory: {0}'.format(
                directory,
            ),
        ) from error


def _open_approved_directory():
    if (
        not PROMETHEUS_MULTIPROC_DIR.is_absolute()
        or PROMETHEUS_MULTIPROC_DIR.parent != Path('/tmp')  # noqa: S108
        or PROMETHEUS_MULTIPROC_DIR.name != 'prometheus_multiproc'
    ):
        raise RuntimeError('Refusing to use an unapproved multiprocess path')

    return _open_directory(PROMETHEUS_MULTIPROC_DIR)


def _clean_metric_files(directory_fd):
    """Remove recognized mmap files through an already-approved directory."""
    file_names = os.listdir(directory_fd)
    unexpected_entries = []
    for file_name in file_names:
        file_stat = os.stat(
            file_name,
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
        if (
            not stat.S_ISREG(file_stat.st_mode)
            or not _EXPECTED_METRIC_FILE.fullmatch(file_name)
        ):
            unexpected_entries.append(file_name)

    if unexpected_entries:
        raise RuntimeError(
            'Refusing to clean unexpected multiprocess entries: {0}'.format(
                ', '.join(sorted(unexpected_entries)),
            ),
        )

    for file_name in file_names:
        os.unlink(file_name, dir_fd=directory_fd)


def prepare_prometheus_multiproc_dir():
    """Clean only recognized metric files from the approved directory."""
    _validate_environment()
    directory_fd = _open_approved_directory()
    try:
        _clean_metric_files(directory_fd)
    finally:
        os.close(directory_fd)

    os.environ['PROMETHEUS_MULTIPROC_DIR'] = str(
        PROMETHEUS_MULTIPROC_DIR,
    )


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            'Prepare Prometheus multiprocess storage once, then run a command.'
        ),
    )
    parser.add_argument('command', nargs=argparse.REMAINDER)
    arguments = parser.parse_args(argv)
    command = arguments.command
    if command[:1] == ['--']:
        command = command[1:]
    if not command:
        parser.error('a command is required')

    prepare_prometheus_multiproc_dir()
    os.execvpe(command[0], command, os.environ.copy())  # noqa: S606


if __name__ == '__main__':
    main()
