# Email and Slack notifications

## Email — Mailjet via Anymail

### Configuration

[config/settings/base.py:274](../../config/settings/base.py#L274):

```python
ANYMAIL = {
    'MAILJET_API_URL': env('MAILJET_API_URL', default='https://api.mailjet.com/v3'),
    'MAILJET_API_KEY': env('MAILJET_API_KEY', default=None),
    'MAILJET_SECRET_KEY': env('MAILJET_SECRET_KEY', default=None),
    'IGNORE_RECIPIENT_STATUS': True,
    'IGNORE_UNSUPPORTED_FEATURES': True,
    'REQUESTS_TIMEOUT': 60.0,
}
```

> **`IGNORE_RECIPIENT_STATUS: True` means a rejected or bounced recipient does not raise.** A
> notification to a stale address fails silently. Combined with the kill switches below, "the email
> never arrived" has several indistinguishable causes.

`django-anymail` 7.2 is declared in **`[dev-packages]`** despite being used by production mailing
code — same situation as `delta-sharing`.

### Addresses

| Variable | Default | Purpose |
|---|---|---|
| `DEFAULT_FROM_EMAIL` | — | Envelope sender |
| `SERVER_EMAIL`, `SERVER_EMAIL_SIGNATURE` | — | Error mail and signature |
| `NO_REPLY_EMAIL_ID_OPTIONS` | `DEFAULT_FROM_EMAIL` | Alternate no-reply senders |
| `SUPPORT_EMAIL_ID` | `DEFAULT_FROM_EMAIL` | Shown to users |
| `SUPPORT_PHONE_NUMBER` | `'Test Phone No for Support'` | Shown to users |
| `CONTACT_MANAGERS` | `['test@test.test']` | Contact-form recipients |
| `ADMINS` | — | Django error mail |

> Two defaults are placeholders that will reach users if unset: `SUPPORT_PHONE_NUMBER` renders as
> the literal string **"Test Phone No for Support"**, and `CONTACT_MANAGERS` defaults to
> **`test@test.test`** — so contact-form submissions go nowhere. Neither is in `.env_example`.

`CONTACT_EMAIL` is additionally exposed through **django-constance**
([config/settings/base.py:370](../../config/settings/base.py#L370)), so it can be changed at runtime
without a deploy. Constance stores its values in Redis (`CONSTANCE_REDIS_CONNECTION`).

### Kill switches

| Flag | Default | Gates |
|---|---|---|
| `ENABLED_API_KEY_EMAILS` | `True` | API key lifecycle notifications |
| `ENABLED_DATA_LAYER_EMAILS` | — | Data layer publish notifications |
| `ENABLED_DATA_SOURCES_EMAILS` | — | Master-data review reminders |
| `MAILING_USE_CELERY` | — | Send via the `send_email` task instead of inline |

> **Turn these off in any environment that shares a mail provider with production.** A staging
> environment pointed at the real Mailjet account will email real reviewers about test data.

### What sends email

| Trigger | Recipients | Code |
|---|---|---|
| Master data changed upstream | Holders of `CAN_UPDATE_SCHOOL_MASTER_DATA` / `CAN_PUBLISH_SCHOOL_MASTER_DATA` | [proco/data_sources/tasks.py:196](../../proco/data_sources/tasks.py#L196) |
| Rows waiting > 48 h | Targeted editors/publishers | [proco/data_sources/tasks.py:659](../../proco/data_sources/tasks.py#L659) |
| API key requested / approved / declined / extended | The requester, and approvers | `proco/accounts/` |
| Data layer published | Configured recipients | `proco/accounts/` |
| Contact form submitted | `CONTACT_MANAGERS` / constance `CONTACT_EMAIL` | `proco/contact/` |

The reminder task guards itself explicitly and writes the reason to `BackgroundTask.log`:

```python
if not settings.ENABLED_DATA_SOURCES_EMAILS:
    task_instance.info('ERROR: School Master data source email notification is disabled.')
elif is_blank_string(settings.ANYMAIL.get('MAILJET_API_KEY')) or \
     is_blank_string(settings.ANYMAIL.get('MAILJET_SECRET_KEY')):
    task_instance.info('ERROR: MailJet creds are not configured ...')
```

So for that one job, the `BackgroundTask` log tells you why nothing was sent. Most others do not.

### Email templates and footers

`django-templated-email` renders the messages. Footers are assembled from:

`GIGA_LOGO_URL` · `GIGA_WEBSITE_URL` · `GIGA_LINKEDIN_URL` · `GIGA_X_URL` ·
`GIGA_INSTAGRAM_URL` · `GIGA_YOUTUBE_URL` · `GIGA_NEWS_LETTER_URL`

Deep links into the admin console come from `SCHOOL_MASTER_DASHBOARD_URL`,
`DATA_LAYER_DASHBOARD_URL` and `API_KEY_ADMIN_DASHBOARD_URL`. All default to `None`, in which case
the link line is omitted rather than rendered broken.

### The `send_email` task

[proco/mailing/tasks.py:5](../../proco/mailing/tasks.py#L5) — the entire `mailing` app is 21 lines:

```python
@app.task(ignore_result=True)
def send_email(backend, *args, **kwargs):
    ...
```

No time-limit arguments, so it inherits the worker's 60-second soft limit. Fine per message.

---

## Slack

### Configuration

A single incoming webhook: `SCHOOL_MASTER_DATA_CHANGES_SLACK_WEBHOOK_URL`
([config/settings/base.py:603](../../config/settings/base.py#L603)), default `None`. **Not in
`.env_example`.**

### Behaviour

`SlackNotificationService`
([proco/utils/slack_notification_service.py:8](../../proco/utils/slack_notification_service.py#L8)):

```python
if not self.school_master_data_changes_webhook_url:
    logger.info("Slack notifications webhook url not provided.")
    return
```

Unset → logged at INFO and skipped. Unlike the email path, a send failure is **logged and
re-raised**:

```python
except Exception as e:
    logger.error(f"Failed to send Slack notification: {str(e)}. Nofitication: {message}")
    raise e
```

So a Slack outage propagates into the calling task. Since notifications are dispatched with
`.delay()` as their own task, this fails the notification task rather than the ingestion — but it
will show up in Sentry.

### Message headers by source

`_get_notification_header` labels the message by how the publish was triggered:

| `publish_source` | Header |
|---|---|
| `pre_review` | `[Pre-Review] School Master Static Data Changed` |
| `auto_publish` | `[Auto-Publish] School Records Published Automatically` |
| `admin_portal_selected` | `[Admin Portal] Selected School Records Published` |
| `admin_portal_country` | `[Admin Portal] Full Country School Data Published` |
| `cli` | `[CLI] School Data Published via Command-Line` |

Unrecognised values fall back to `pre_review`.

### Message contents

`_format_master_release_message` includes `APP_ENVIRONMENT`, the country, new-row counts and a
per-field change breakdown across three model groups (`school_model_changes`,
`school_weekly_changes`, `rt_registration_changes`), splitting into multiple blocks when it exceeds
Slack's block size limit.

> Because `APP_ENVIRONMENT` is included in every message, a staging environment configured with the
> production webhook is identifiable — but it will still post. Give non-production environments
> their own webhook or leave it unset.

### Where it is triggered

```python
for country_iso3_format, _ in changes_for_countries.items():
    send_school_master_data_change_slack_notification.delay(country_iso3_format)
```

[proco/data_sources/tasks.py:247](../../proco/data_sources/tasks.py#L247) — one task per changed
country, per ingestion run. With many countries changing in one pull, expect a burst.

## Troubleshooting

| Symptom | Check |
|---|---|
| No emails at all | `MAILJET_API_KEY` / `MAILJET_SECRET_KEY`, and the `ENABLED_*_EMAILS` flags |
| Some recipients miss emails | `IGNORE_RECIPIENT_STATUS: True` hides bounces — check Mailjet's own logs |
| Reminder emails stopped | `BackgroundTask.log` for `email_reminder_…` — it records the reason |
| Reminder task killed mid-send | It inherits the 60 s soft limit; some recipients notified, others not |
| Contact form goes nowhere | `CONTACT_MANAGERS` default is `test@test.test`; also check constance `CONTACT_EMAIL` |
| No Slack messages | Webhook unset — `Slack notifications webhook url not provided.` at INFO |
| Slack errors in Sentry | Send failures are re-raised, unlike email |
