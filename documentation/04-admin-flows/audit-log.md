# Audit log

Two independent audit mechanisms, neither of which covers everything.

---

## 1. Recent actions

`/admin/recent-actions` → `GET /api/accounts/recent_action_log/` (`LogActionViewSet`),
permission `can_view_recent_actions`.

Records administrative changes made through the console — who changed what, and when.

## 2. Model history

`django-simple-history` 3.0.0, attached via `HistoricalRecords(inherit=True)` on
`MasterDataSourceModelMixin` ([proco/core/models.py:242](../../proco/core/models.py#L242)).

Because it is on the mixin with `inherit=True`, **both** `SchoolMasterData` and
`HealthEntityMasterIntermediateData` get full row-level version history in `historical*` tables:
every field value at every revision, with the user and timestamp.

This is the detailed record of what a reviewer saw and changed during the review workflow.

## What is *not* audited

> Three significant blind spots:
>
> 1. **The raw-SQL cleanup jobs.** `cleanup_school_master_rows` and
>    `cleanup_health_entity_master_rows` issue hard `DELETE` statements outside the ORM
>    ([proco/data_sources/tasks.py:836](../../proco/data_sources/tasks.py#L836)). No historical row
>    is written. Superseded review rows vanish with no trace.
>
> 2. **Publish-time sibling deletion.** `delete_all_related_rows` hard-deletes every other staging
>    row sharing a `school_id_giga` when one is published
>    ([proco/data_sources/serializers.py:404](../../proco/data_sources/serializers.py#L404)). Also
>    unaudited.
>
> 3. **The history tables themselves are pruned.** `clean_historic_data` runs at weekends and calls
>    `data_source_additional_steps --clean_school_master_historical_rows`. So the one durable record
>    of pre-publish state has a retention limit of its own.
>
> Net effect: for anything older than the history retention window, there is **no record of what a
> master-data row looked like before it was published**. If that matters for compliance, the
> retention policy in `data_source_additional_steps` is the thing to review.

## Who-did-what on ordinary models

`BaseModel` carries `created_by` and `last_modified_by`; the publish-capable models add
`published_by` and `published_at`; `APIKey` adds `status_updated_by` and
`extension_status_updated_by`; `BackgroundTask` adds `deleted_by`.

These give the *current* attribution but not a change trail — only the last modifier is retained.

All of these FKs use `on_delete=DO_NOTHING`, consistent with users being soft-deleted. **Hard-deleting
a user would leave dangling references** that raise on access, which is a further reason never to do
so.

## Querying history

```bash
pipenv run python manage.py shell -c "
from proco.data_sources.models import SchoolMasterData
row = SchoolMasterData.objects.get(pk=12345)
for h in row.history.all():
    print(h.history_date, h.history_user, h.status)
"
```

## Related

- [school-master-review-publish.md](school-master-review-publish.md)
- [../05-background-jobs/cleanup-and-retention.md](../05-background-jobs/cleanup-and-retention.md)
