# Authentication and RBAC

The project does **not** use Django's built-in `auth.Permission` / `auth.Group`. It has its own
role and permission system with **47 permission slugs** across 13 functional groups.

---

## Authentication

### The user model

`ApplicationUser` ([proco/custom_auth/models.py:13](../../proco/custom_auth/models.py#L13)) extends
`AbstractBaseUser` — not `AbstractUser`. Key fields:

| Field | Note |
|---|---|
| `email` | Unique, and the login identifier |
| `username` | Present, but email is what authenticates |
| `is_active`, `is_staff`, `is_superuser` | Standard flags |
| `countries_available` | M2M — scopes a user to specific countries |
| `deleted` | Soft delete |

`has_perm()` and `has_module_perms()` are overridden to **return `is_superuser` only**:

```python
def has_perm(self, perm, obj=None):
    return self.is_superuser
```

So Django's permission framework is effectively bypassed. Anything checking `user.has_perm(...)`
gets an answer about superuser status, not about the project's real permissions. Use
`user.permissions` instead.

### JWT via Azure AD B2C

Configured at [config/settings/base.py:388](../../config/settings/base.py#L388):

| Setting | Value |
|---|---|
| Algorithm | `RS256` (asymmetric — the backend verifies, B2C signs) |
| Header prefix | `Bearer` |
| Expiry | 1 day |
| Verification | On, including expiration |
| Username handler | `proco.custom_auth.utils.jwt_get_username_from_payload_handler` |
| Decode handler | `proco.custom_auth.utils.jwt_decode_handler` |

B2C policies come from `AD_B2C_SIGNUP_SIGNIN_POLICY`, `AD_B2C_FORGOT_PASSWORD_POLICY` and
`AD_B2C_EDIT_PROFILE_POLICY` — sign-up, password reset and profile editing all happen on Microsoft's
side. The backend never sees a password for B2C users.

DRF's authentication classes, in order
([config/settings/base.py:135](../../config/settings/base.py#L135)):

1. `proco.custom_auth.authentication.JSONWebTokenAuthentication`
2. `rest_framework.authentication.SessionAuthentication`

`AUTHENTICATION_BACKENDS` additionally lists `RemoteAndModelBackend`
([proco/custom_auth/backends.py:9](../../proco/custom_auth/backends.py#L9)), a thin `ModelBackend`
subclass that keeps the constant-time password comparison for non-existent users. It exists for
local/admin password login.

### Inactivity expiry

`IsUserAuthenticated` ([proco/core/permissions.py:65](../../proco/core/permissions.py#L65)) rejects
a request when:

- the user is anonymous, soft-deleted or inactive, **or**
- `last_login` is more than **5 days** old.

```python
login_expiration_date = datetime.now() - timedelta(days=5)
if login_date < login_expiration_date:
    raise rest_exceptions.AuthenticationFailed()
```

This is a second, independent expiry on top of the 1-day JWT lifetime, and it is keyed on
`last_login` rather than token issuance. A user who has not signed in for five days is locked out
even with a freshly issued token. Both failures surface as a generic `AuthenticationFailed`, so
"my token is valid but I get 401" almost always means this check.

---

## Roles

`Role` ([proco/custom_auth/models.py:135](../../proco/custom_auth/models.py#L135)) has two
categories:

| Category | Meaning |
|---|---|
| `system` | Built-in. Two names are reserved: `Admin` and `Read Only` |
| `custom` | Created by administrators, arbitrary permission sets |

Users attach to roles through `UserRoleRelationship`. The relationship is many-to-many in the
schema, but **most of the code assumes one role per user**:

```python
def get_roles(self):
    first_role = self.roles.first()
    if first_role:
        return first_role.role
```

`role()`, `role_verbose_name` and `ProcoBasePermission._is_super_allowed` all read only the *first*
role. Assigning a second role will contribute its permissions (see below) but will not affect
role-name checks. Treat one-role-per-user as the supported configuration.

### How permissions are computed

```python
def calculate_user_permissions(self, user):
    user_permissions = {}
    for user_role_relations in user.roles.all():
        user_permissions.update(user_role_relations.perm_dict())
    return user_permissions
```

The union across all roles, as a `{slug: True}` dict.

> **`user.permissions` is a property with no caching.** Every access re-runs
> `calculate_user_permissions`, which queries `user.roles.all()` and then, for each role,
> `role.permissions.values_list('slug', flat=True)`. A view with four stacked permission classes
> triggers this repeatedly within a single request, and several serializers read it again. This is
> a straightforward N+1 on every authenticated request. Caching it per-request would be a
> low-risk, measurable win — see
> [../08-operations/caching-and-performance.md](../08-operations/caching-and-performance.md).

---

## The permission classes

All derive from `ProcoBasePermission`
([proco/core/permissions.py:10](../../proco/core/permissions.py#L10)). The important part:

```python
def has_permission(self, request, view):
    if request.method != self.method:
        return True                      # ← not my method: abstain
    if request.user.is_anonymous:
        return False
    if request.user.is_staff or request.user.is_superuser:
        return True
    if self._is_super_allowed(request, view):   # role name in (Admin,)
        return True
    return self.check_permission(request, view) # slug lookup
```

**Each class guards exactly one HTTP method and abstains on all others.** That is why views stack
several of them:

```python
permission_classes = (
    core_permissions.IsUserAuthenticated,
    core_permissions.CanViewSchoolMasterData,      # guards GET
    core_permissions.CanUpdateSchoolMasterData,    # guards PUT/PATCH
    core_permissions.CanDeleteSchoolMasterData,    # guards DELETE
)
```

> **This is the main footgun in the authorization layer.** DRF requires *all* permission classes to
> pass, so abstention is the only way to compose per-method rules — but it means a method with no
> matching class is **completely unguarded** beyond `IsUserAuthenticated`. When adding a new method
> to an existing viewset, you must also add its permission class. Nothing will fail loudly if you
> forget.

Two escape hatches sit above the slug check:

- `is_staff` **or** `is_superuser` → allowed, unconditionally.
- Role name in `get_allowed_super_roles()`, which defaults to `(Role.SYSTEM_ROLE_NAME_ADMIN,)` →
  allowed. Subclasses can widen this.

So the `Admin` system role bypasses every individual slug. Removing a permission from the Admin
role has no effect.

---

## The 47 permissions

### User management (10)

| Slug | Grants |
|---|---|
| `can_access_users_tab` | Access the User Management tab at all |
| `can_view_user` / `can_add_user` / `can_update_user` / `can_delete_user` | User CRUD |
| `can_view_all_roles` | See the role list |
| `can_update_user_role` | Reassign a user's role |
| `can_create_role_configurations` / `can_update_role_configurations` / `can_delete_role_configurations` | Role CRUD |

### Data layers (5)

`can_view_data_layer` · `can_add_data_layer` · `can_update_data_layer` ·
`can_publish_data_layer` · `can_preview_data_layer`

`preview` is separate from `view`: previewing renders an unpublished layer against live data, which
is expensive. See [data-layers.md](data-layers.md).

### Advanced filters (4)

`can_view_advance_filter` · `can_add_advance_filter` · `can_update_advance_filter` ·
`can_publish_advance_filter`

### Column configurations (1)

`can_view_column_configurations` — read-only. Column configurations are seeded by management
command, not edited in the UI.

### Master data (3)

`can_view_school_master_data` · `can_update_school_master_data` (Editor) ·
`can_publish_school_master_data` (Publisher)

These three drive the entire editorial workflow —
[school-master-review-publish.md](school-master-review-publish.md). **They also govern health
entities**, which have no permissions of their own.

### API keys (2)

`can_delete_api_key` · `can_approve_reject_api_key`

There is no `can_view_api_key` or `can_create_api_key`: viewing is implied by tab access, and
creation is a *user* action, not an admin one.

### Countries (4) · Schools (4)

`can_view_*` · `can_add_*` · `can_update_*` · `can_delete_*` for each.

### CSV import (3)

`can_view_uploaded_csv` · `can_import_csv` · `can_delete_csv`

### Background tasks (4)

`can_view_background_task` · `can_add_background_task` · `can_update_background_task` ·
`can_delete_background_task`

`can_delete_background_task` is what lets an operator clear a stuck `running` row — see
[background-task-console.md](background-task-console.md).

### Contact messages (3)

`can_view_contact_message` · `can_update_contact_message` · `can_delete_contact_message`

### Recent actions (1)

`can_view_recent_actions` — the audit log.

### Notifications (3)

`can_view_notification` · `can_create_notification` · `can_delete_notification`

---

## Special-purpose permission classes

Beyond the slug-backed classes, `proco/core/permissions.py` defines:

| Class | Checks |
|---|---|
| `IsUserAuthenticated` | Active, not deleted, logged in within 5 days |
| `IsUserEnabled` | Account is enabled |
| `IsSuperUserEnabledAuthenticated` | Superuser-only endpoints |
| `IsRequestUserSameAsQueryParam` | `user_id` in the URL matches the caller — self-service endpoints |
| `CanCleanCache` | Guards the cache-invalidation endpoints |

## Seeding roles

```bash
pipenv run python manage.py update_system_role_permissions
```

Creates/refreshes the `Admin` and `Read Only` system roles and their permission sets. Run it after
**any** change to the permission constants — new slugs are not attached to existing roles
automatically.

For non-production environments:

```bash
pipenv run python manage.py non_prod_setup_roles_and_users
```

## Creating an admin

```bash
pipenv run python manage.py create_admin_user \
  -email='you@example.com' -first_name='You' -last_name='Dev'
```

## Related

- [user-and-role-management.md](user-and-role-management.md) — the admin UI for this
- [../03-user-flows/api-key-request.md](../03-user-flows/api-key-request.md) — the *other* auth path
- [../07-api-reference/conventions.md](../07-api-reference/conventions.md) — how auth appears on the wire
