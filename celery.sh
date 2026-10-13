#!/usr/bin/env bash
set -ex

# export environment variables to make them available in ssh session
for var in $(compgen -e); do
    echo "export $var=${!var}" >> /etc/profile
done

# Ensure log directory exists
LOG_DIR=/code/logs
mkdir -p "$LOG_DIR"

echo "Starting SSH ..."
service ssh start

export FLASK_APP=hello.py
pipenv run python -m flask run --host 0.0.0.0 --port 8000 >> "$LOG_DIR/flask.log" 2>&1 &

if $ENABLED_FLOWER_METRICS; then
    echo "Starting worker ..."
    pipenv run celery --app=proco.taskapp worker \
        --concurrency=3 \
        --time-limit=300 \
        --soft-time-limit=60 \
        --logfile="$LOG_DIR/celeryd-%n.log" \
        --loglevel=INFO \
        $* &
    echo "Starting flower ..."
    pipenv run celery --app=proco.taskapp flower >> "$LOG_DIR/flower.log" 2>&1
else
    echo "Starting worker ..."
    pipenv run celery --app=proco.taskapp worker \
        --concurrency=3 \
        --time-limit=300 \
        --soft-time-limit=60 \
        --logfile="$LOG_DIR/celeryd-%n.log" \
        --loglevel=INFO \
        $*
fi
