import logging

from redbeat.schedulers import RedBeatScheduler, get_redis

logger = logging.getLogger('gigamaps.' + __name__)

# KEYS[1] - lock name, ARGV[1] - longest TTL allowed in milliseconds.
# Shortens the lock's TTL to ARGV[1] if it is longer; returns 1 if it did, otherwise 0.
LUA_CAP_TTL_SCRIPT = """
    local ttl = redis.call('pttl', KEYS[1])
    if ttl > tonumber(ARGV[1]) then
        redis.call('pexpire', KEYS[1], ARGV[1])
        return 1
    end
    return 0
"""


class SelfHealingRedBeatScheduler(RedBeatScheduler):
    """
    SelfHealingRedBeatScheduler
        RedBeatScheduler that puts back the static beat_schedule entries missing from Redis.
        redbeat stores them only when a beat process starts, so if Redis loses them (eviction under memory
        pressure, flush) the lock holder keeps running but schedules nothing until some beat restarts.
        Here every tick checks them first and stores the missing ones again.
        At start-up it also shortens a lock left with a longer TTL than its own lock timeout.

    Inherits: `RedBeatScheduler`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not kwargs.get('lazy'):
            self.cap_lock_ttl()

    def cap_lock_ttl(self):
        """
        cap_lock_ttl
            Method to shorten the TTL of the beat lock to this beat's lock timeout when it is longer, as with
            the 10 h lock of earlier releases. Runs before this beat tries to acquire the lock. A live holder
            renews the lock to its own timeout on its next tick, before sending any task; a dead holder (beat
            killed by a deploy or a VM replacement) stops blocking the other beats after lock_timeout seconds
            instead of hours.

        :return: True if the TTL was shortened
        """
        if not self.lock_key:
            return False

        client = get_redis(self.app)
        capped = client.register_script(LUA_CAP_TTL_SCRIPT)(
            keys=[self.lock_key], args=[int(self.lock_timeout * 1000)],
        )
        if capped:
            logger.warning('beat: lock %s had a TTL longer than %ss, shortened to it', self.lock_key, self.lock_timeout)
        return bool(capped)

    def tick(self, *args, **kwargs):
        # Runs before redbeat renews the lock. A beat that has just lost the lock writes the same static
        # definitions as the new holder and then stops on LockNotOwnedError, so this is harmless.
        self.restore_missing_static_entries()
        return super().tick(*args, **kwargs)

    def restore_missing_static_entries(self):
        """
        restore_missing_static_entries
            Method to store again the static beat_schedule entries whose definition or schedule slot is
            missing in Redis. A last_run_at still in Redis is kept, so a run missed meanwhile is sent on the
            next tick, as redbeat does for any overdue entry.

        :return: list of the restored entry names
        """
        conf = self.app.redbeat_conf
        names = list(conf.schedule.keys())
        if not names:
            return []

        client = get_redis(self.app)
        with client.pipeline() as pipe:
            for name in names:
                key = conf.key_prefix + name
                pipe.hexists(key, 'definition')
                pipe.zscore(conf.schedule_key, key)
            results = pipe.execute()

        missing = [
            name for index, name in enumerate(names)
            if not results[2 * index] or results[2 * index + 1] is None
        ]
        if missing:
            logger.warning(
                'beat: %d static schedule entries missing in Redis, storing them again: %s',
                len(missing), ', '.join(missing),
            )
            self.update_from_dict({name: conf.schedule[name] for name in missing})
            client.sadd(conf.statics_key, *names)

            # update_from_dict scores each entry as if it had just run; score it again from the stored meta
            for name in missing:
                entry = self.Entry.from_key(conf.key_prefix + name, app=self.app)
                client.zadd(conf.schedule_key, {entry.key: entry.score})
        return missing
