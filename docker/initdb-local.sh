#!/bin/sh

set -e

# Perform all actions as $POSTGRES_USER
export PGUSER="$POSTGRES_USER"

# Create gigameter database (proco is already created by POSTGRES_DB env var)
echo "Creating gigameter database if it does not exist..."

psql -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'gigameter'" | grep -q 1 || \
psql -d postgres -c "CREATE DATABASE gigameter;"

# Restore proco database from proco.bz2
PROCO_DUMP="/postgres_data/proco.bz2"
if [ -f "$PROCO_DUMP" ]; then
    echo "Restoring proco database from $PROCO_DUMP ..."
    bzcat "$PROCO_DUMP" | pg_restore -d "$POSTGRES_DB" --no-owner --no-privileges -v 2>&1 || {
        echo "pg_restore (proco) finished with warnings (this is often normal for local dev)."
    }
    echo "proco database restore complete."
else
    echo "WARNING: $PROCO_DUMP not found. Skipping proco restore."
fi

# Restore gigameter database from gigameter.bz2
GIGAMETER_DUMP="/postgres_data/gigameter.bz2"
if [ -f "$GIGAMETER_DUMP" ]; then
    echo "Restoring gigameter database from $GIGAMETER_DUMP ..."
    bzcat "$GIGAMETER_DUMP" | pg_restore -d gigameter --no-owner --no-privileges -v 2>&1 || {
        echo "pg_restore (gigameter) finished with warnings (this is often normal for local dev)."
    }
    echo "gigameter database restore complete."
else
    echo "WARNING: $GIGAMETER_DUMP not found. Skipping gigameter restore."
fi

echo "All database restores complete."
