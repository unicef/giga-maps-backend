# Admin flows

The admin console is part of the **frontend** application, served under `/admin/*`, not Django's
built-in admin (`path('admin/', admin.site.urls)` is commented out in
[config/urls.py:32](../../config/urls.py#L32)). Everything an administrator does goes through the
same DRF API as the public map, gated by the RBAC system.

**21 admin routes**, all declared in
[src/core/routes.ts](https://github.com/unicef/giga-maps-frontend/blob/staging/src/core/routes.ts).

Start with [auth-and-rbac.md](auth-and-rbac.md) — every flow below is gated by it.

---

## Route → flow → endpoint map

| Frontend route | Flow | Primary endpoints | Permission |
|---|---|---|---|
| `/admin` | Dashboard | — | any tab permission |
| `/admin/users` | [Users](user-and-role-management.md) | `auth/users/` | `can_access_users_tab`, `can_view_user` |
| `/admin/user/:userId` | [User detail](user-and-role-management.md) | `auth/users/{id}/` | `can_update_user` |
| `/admin/roles`, `/admin/roles/create`, `/admin/role/:id` | [Roles](user-and-role-management.md) | `auth/roles/`, `auth/roles/{id}/` | `can_view_all_roles`, `can_create_role_configurations` |
| `/admin/user-permission` | [Permissions](user-and-role-management.md) | `auth/roles/` | `can_update_user_role` |
| `/admin/api-keys` | [API key approval](api-key-approval.md) | `accounts/api_keys/`, `accounts/api_categories/` | `can_approve_reject_api_key`, `can_delete_api_key` |
| `/admin/giga-layer` + create/view/edit | [Data layers](data-layers.md) | `accounts/layers/`, `accounts/data_sources/` | `can_view_data_layer` … `can_publish_data_layer` |
| `/admin/filter` + create/edit | [Advanced filters](advanced-filters.md) | `accounts/adv_filters/`, `accounts/column_configurations/` | `can_view_advance_filter` … `can_publish_advance_filter` |
| `/admin/data-source`, `/admin/data-source/edit/:id` | [Master-data review](school-master-review-publish.md) | `sources/school_master/*` | `can_view_school_master_data` … `can_publish_school_master_data` |
| `/admin/background-task`, `/admin/background-task/view/:id` | [Background tasks](background-task-console.md) | `background/backgroundtask/` | `can_view_background_task` |
| `/admin/contact-message`, `/admin/contact-message/view/:id` | [Contact messages](content-management.md) | `contact/contactmessage/` | `can_view_contact_message` |
| `/admin/recent-actions` | [Audit log](audit-log.md) | `accounts/recent_action_log/` | `can_view_recent_actions` |
| `/admin/alerts` | [Notifications](notifications-and-alerts.md) | `accounts/notifications/` | `can_view_notification`, `can_create_notification` |
| `/admin/country` + add/edit variants | [Country data](country-and-school-data.md) | `locations/country/`, `statistics/countryweeklystatus/`, `statistics/countrydailystatus/` | `can_view_country` … `can_delete_country` |
| `/admin/schools` + add/edit variants | [School data](country-and-school-data.md) | `locations/schools/school/`, `statistics/schoolweeklystatus/`, `statistics/schooldailystatus/`, `locations/schools/fileimport/` | `can_view_school` … `can_delete_school`, `can_import_csv` |
| `/admin/about-us` | [Content](content-management.md) | `about_us/about_us/`, `about_us/slide_image/` | — |

The endpoint column is taken from the effector effects in
`src/@/admin/effects/*.ts`, which is where every admin HTTP call lives.

---

## Flows by theme

### Access control
- [auth-and-rbac.md](auth-and-rbac.md) — JWT, roles, the 47 permissions, and how permission classes compose
- [user-and-role-management.md](user-and-role-management.md) — creating users, assigning roles, building custom roles

### Data stewardship
- [school-master-review-publish.md](school-master-review-publish.md) — the eight-state editorial workflow
- [country-and-school-data.md](country-and-school-data.md) — direct CRUD on countries, schools and their statistics; CSV import
- [data-layers.md](data-layers.md) — defining what the map draws
- [advanced-filters.md](advanced-filters.md) — defining what users can filter by

### External access
- [api-key-approval.md](api-key-approval.md) — approving, rejecting, extending and revoking public API keys

### Operations
- [background-task-console.md](background-task-console.md) — watching jobs, clearing stuck rows
- [audit-log.md](audit-log.md) — who changed what
- [cache-invalidation.md](cache-invalidation.md) — forcing a cache refresh
- [notifications-and-alerts.md](notifications-and-alerts.md) — broadcasting messages

### Content
- [content-management.md](content-management.md) — About Us page, slider images, contact messages

---

## Cross-cutting conventions

### The `?expand=` parameter

Admin list endpoints lean heavily on `drf-flex-fields`. The console requests exactly the nested
objects it needs:

```
accounts/layers/?expand=created_by,last_modified_by,published_by&page_size=20&page=1&ordering=-last_modified_at,name
accounts/api_keys/?expand=user,api&ordering=-status
sources/school_master/?expand=school,modified_by,published_by,country
```

Each viewset declares what is expandable via `permit_list_expands`. Requesting a field not on that
list is ignored rather than erroring.

### Publish endpoints are a separate action

Data layers, advanced filters and master-data rows all follow the same shape: the resource has a
`PUT .../{id}/publish/` sub-route, guarded by a dedicated `can_publish_*` permission, distinct from
`can_update_*`. Editing and approving are always different privileges.

### Draft → published lifecycle

Three unrelated models implement the same idea with different vocabularies:

| Model | States |
|---|---|
| `DataLayer` | `DRAFT` → `READY_TO_PUBLISH` → `PUBLISHED` / `DISABLED` |
| `AdvanceFilter` | same shape |
| `SchoolMasterData` | the eight-state machine |
| `APIKey` | `INITIATED` → `APPROVED` / `DECLINED` |

Only master data has a two-role handoff.

### Soft delete everywhere

`DELETE` sets `deleted = now()`. Rows stay in the table and remain visible to queries that do not
filter on `deleted__isnull=True`. The `BaseManager` handles this for most models; raw SQL and the
cleanup jobs do not.

### Audit trail

Two independent mechanisms:

1. **`django-simple-history`** on master-data models — full row versions in `historical*` tables.
2. **Recent action log** — `accounts/recent_action_log/`, surfaced at `/admin/recent-actions`.

Neither covers the raw-SQL cleanup jobs, which delete rows outside the ORM.
