import logging

from redbeat.schedulers import RedBeatScheduler, get_redis

logger = logging.getLogger('gigamaps.' + __name__)


class SelfHealingRedBeatScheduler(RedBeatScheduler):
    """
    SelfHealingRedBeatScheduler
        RedBeatScheduler that puts back the static beat_schedule entries missing from Redis.
        redbeat stores them only when a beat process starts, so if Redis loses them (eviction under memory
        pressure, flush) the lock holder keeps running but schedules nothing until some beat restarts.
        Here every tick checks them first and stores the missing ones again.

    Inherits: `RedBeatScheduler`
    """

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
