# NocoDB

A lightweight no-code database used by the Giga team, integrated for a narrow purpose.

## Configuration

[config/settings/base.py:378](../../config/settings/base.py#L378):

| Variable | Default | Purpose |
|---|---|---|
| `NOCODB_API_URL` | `None` | API base, e.g. `https://nocodb.giga.global/api/v2` |
| `NOCODB_API_TOKEN` | `None` | Auth token |
| `NOCODB_TABLE_ID` | `None` | Target table |
| `NOCODB_TABLE_ID_PRODUCTION` | `None` | Production table |

Two table ids are configured, so the integration writes to a different table depending on
environment. Which one is chosen is decided in the calling code — check
`proco/utils/cms/` and `proco/accounts/` before assuming.

> `NOCODB_API_TOKEN` grants write access to a shared team workspace outside this application's
> control. Treat it with the same care as a production credential, and prefer separate tokens per
> environment.

All four default to `None`, so an unconfigured environment silently skips the integration.

## Usage

The integration is narrow and lives in `proco/utils/cms/`. Grep for it before changing anything:

```bash
grep -rn 'NOCODB' proco --include='*.py'
```

> **Unverified**: the precise records written and their schema are determined by the NocoDB table
> definition, which lives outside this repository. Confirm against the table before relying on the
> shape of what is pushed.

## Troubleshooting

| Symptom | Check |
|---|---|
| Nothing appears in NocoDB | All four variables set? Any is `None` → skipped |
| Writes land in the wrong table | `NOCODB_TABLE_ID` vs `NOCODB_TABLE_ID_PRODUCTION` selection logic |
| 401 from NocoDB | Token revoked or scoped to a different workspace |
