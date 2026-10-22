# Notifications and alerts

`/admin/alerts` — broadcasting messages to users.

| Endpoint | Methods | Permission |
|---|---|---|
| `/api/accounts/notifications/` | `GET`, `POST` | `can_view_notification`, `can_create_notification` |

`can_delete_notification` also exists.

---

## The `Message` model

[proco/accounts/models.py:203](../../proco/accounts/models.py#L203).

| Field | Values / purpose |
|---|---|
| `severity` | `LOW`, `MEDIUM` (default), `HIGH`, `CRITICAL` |
| `type` | `SMS`, `EMAIL`, `NOTIFICATION` |
| `sender` | Email address, phone number or user id |
| `recipient` | JSON list |
| `subject_text`, `message_text`, `template`, `description` | Content |
| `is_sent`, `is_read` | State |
| `retry_count` | Delivery attempts |

Plus `BaseModel`'s `created_by`, `last_modified_by`, timestamps and soft delete.

> **`SMS` is a declared choice with no implementation.** There is no SMS gateway configured anywhere
> in settings — no Twilio, no provider credentials. A message created with `type='SMS'` will be
> stored and never delivered. Only `EMAIL` and `NOTIFICATION` have a delivery path.

> **`retry_count` is a field, not a mechanism.** Nothing increments it and no job retries unsent
> messages. A message with `is_sent=False` stays that way. If you need delivery guarantees, this
> model does not provide them.

## Severity

Four levels, defaulting to `MEDIUM`. The frontend uses them for presentation — banner colour and
prominence. `CRITICAL` is the one users cannot miss; reserve it for outages and data issues that
affect what they are looking at.

## Related but separate notification paths

This model is **not** what sends the master-data review reminders or the API-key lifecycle emails.
Those go directly through `send_email_over_mailjet_service`. Three distinct notification systems
coexist:

| System | Purpose | Code |
|---|---|---|
| `Message` / `/admin/alerts` | Broadcasts an admin composes | `proco/accounts/` |
| Direct Mailjet sends | Workflow emails (review reminders, key approvals) | `proco/data_sources/`, `proco/accounts/` |
| Slack webhook | Data-change alerts to a channel | [proco/utils/slack_notification_service.py](../../proco/utils/slack_notification_service.py) |

Only the first is manageable through this screen.

## Slack data-change alerts

Not configurable in the admin console — driven entirely by
`SCHOOL_MASTER_DATA_CHANGES_SLACK_WEBHOOK_URL`, which is **not in `.env_example`**. Messages are
labelled by how the publish was triggered (`[Pre-Review]`, `[Auto-Publish]`, `[Admin Portal]`,
`[CLI]`) and include `APP_ENVIRONMENT`, the country, and a per-field change breakdown.

One task is dispatched **per changed country per ingestion run**, so a large upstream release
produces a burst. Details:
[../06-integrations/email-and-slack.md](../06-integrations/email-and-slack.md#slack).

## Email kill switches

Before assuming a notification is broken, check:

| Flag | Default |
|---|---|
| `ENABLED_API_KEY_EMAILS` | `True` |
| `ENABLED_DATA_LAYER_EMAILS` | — |
| `ENABLED_DATA_SOURCES_EMAILS` | — |

And remember `ANYMAIL['IGNORE_RECIPIENT_STATUS'] = True` suppresses bounce errors — a notification
to a dead address reports success.

## Related

- [../06-integrations/email-and-slack.md](../06-integrations/email-and-slack.md)
- [auth-and-rbac.md](auth-and-rbac.md)
