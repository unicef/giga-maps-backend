from django.test import SimpleTestCase

from proco.connection_statistics import tasks as statistics_tasks


class WeeklyMetricTaskTimeLimitsTestCase(SimpleTestCase):
    """
    The worker default (celery.sh: soft 60 s, hard 300 s) is too short for these tasks, so each one
    sets its own limits, with time between the soft and the hard limit to stop cleanly.
    """

    def test_tasks_set_own_limits_above_worker_default(self):
        expected_limits = {
            statistics_tasks.populate_current_school_live_weekly_metrics: (10 * 60, 12 * 60),
            statistics_tasks.cleanup_old_school_live_weekly_metrics: (10 * 60, 12 * 60),
            statistics_tasks.populate_school_live_weekly_metrics: (30 * 60, 35 * 60),
            statistics_tasks.backfill_school_live_weekly_metrics_for_layer: (30 * 60, 35 * 60),
        }

        for task, (soft_limit, hard_limit) in expected_limits.items():
            with self.subTest(task=task.name):
                self.assertEqual(task.soft_time_limit, soft_limit)
                self.assertEqual(task.time_limit, hard_limit)
                self.assertLess(task.soft_time_limit, task.time_limit)
