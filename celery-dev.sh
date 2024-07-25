#!/usr/bin/env bash
set -ex

# export environment variables to make them available in ssh session
for var in $(compgen -e); do
    echo "export $var=${!var}" >> /etc/profile
done

export FLASK_APP=hello.py
pipenv run python -m flask run --host 0.0.0.0 --port 8000 &

if $ENABLED_FLOWER_METRICS; then
    echo "Starting worker ..."
    pipenv run celery --app=proco.taskapp worker --concurrency=2 --time-limit=300 --soft-time-limit=300 $* &

    echo "Starting flower ..."
    pipenv run celery --app=proco.taskapp flower
else
    echo "Starting worker ..."
    pipenv run celery --app=proco.taskapp worker --concurrency=2 --time-limit=300 --soft-time-limit=300 $*
fi
