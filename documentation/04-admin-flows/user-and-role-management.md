# User and role management

`/admin/users`, `/admin/user/:userId`, `/admin/roles`, `/admin/roles/create`, `/admin/role/:id`,
`/admin/user-permission`.

Read [auth-and-rbac.md](auth-and-rbac.md) first — it explains the model these screens edit.

| Endpoint | Methods | View |
|---|---|---|
| `/api/auth/users/` | `GET`, `POST` | `UserViewSet` |
| `/api/auth/users/{pk}/` | `GET`, `PUT` | `UserViewSet` |
| `/api/auth/user_details/` | `GET`, `POST`, `PUT` | `UserDetailsViewSet` — self-service |
| `/api/auth/roles/` | `GET`, `POST` | `RoleViewSet` |
| `/api/auth/roles/{pk}/` | `GET`, `PUT`, `DELETE` | `RoleViewSet` |

---

## Users

`ApplicationUser` ([proco/custom_auth/models.py:13](../../proco/custom_auth/models.py#L13)):

| Field | Note |
|---|---|
| `email` | Unique, and the login identifier |
| `first_name`, `last_name` | |
| `is_active` | Disabling a user without deleting them |
| `is_staff`, `is_superuser` | **Bypass every permission check** |
| `countries_available` | M2M — scopes a user to specific countries |
| `deleted` | Soft delete |

Permissions: `can_access_users_tab` (to see the tab at all), then `can_view_user`, `can_add_user`,
`can_update_user`, `can_delete_user`.

> **`is_staff` and `is_superuser` short-circuit `ProcoBasePermission.has_permission`** before any
> slug is consulted ([proco/core/permissions.py:33](../../proco/core/permissions.py#L33)). Granting
> either gives unrestricted access regardless of role. They are not "admin console access" flags —
> treat them as root.

### Self-service

`/api/auth/user_details/` lets a user read and update their own record.
`IsRequestUserSameAsQueryParam` ([proco/core/permissions.py:44](../../proco/core/permissions.py#L44))
enforces that a `user_id` in the URL matches the caller.

### Deleting a user

Always soft-delete. User FKs across the schema use `on_delete=DO_NOTHING` — `created_by`,
`modified_by`, `published_by`, `status_updated_by`, `deleted_by` — so a **hard delete leaves dangling
references** that raise on access.

## Roles

`Role` ([proco/custom_auth/models.py:135](../../proco/custom_auth/models.py#L135)):

| Category | Meaning |
|---|---|
| `system` | Built-in. Reserved names: `Admin`, `Read Only` |
| `custom` | Administrator-defined permission sets |

Permissions: `can_view_all_roles`, `can_create_role_configurations`,
`can_update_role_configurations`, `can_delete_role_configurations`, `can_update_user_role`.

> **The `Admin` system role bypasses individual permission slugs.**
> `ProcoBasePermission.get_allowed_super_roles()` returns `(Role.SYSTEM_ROLE_NAME_ADMIN,)`, and a
> user whose first role matches is allowed through before the slug check. Removing a permission from
> the `Admin` role has no effect.

## One role per user, in practice

The schema allows many roles per user via `UserRoleRelationship`, and
`calculate_user_permissions` does union them:

```python
for user_role_relations in user.roles.all():
    user_permissions.update(user_role_relations.perm_dict())
```

But `get_roles()`, `role()`, `role_verbose_name` and the `Admin`-role bypass all read
`self.roles.first()` only.

> So a second role contributes its **permission slugs** but is invisible to any **role-name** check.
> A user whose first role is `Read Only` and second is `Admin` gets Admin's slugs but not Admin's
> bypass. Assign one role per user.

## Building a custom role

1. `/admin/roles/create`, category `custom`.
2. Select permissions. The full list with grouping is in
   [auth-and-rbac.md](auth-and-rbac.md#the-47-permissions); the frontend gets the same list from
   `GET /api/accounts/app_configs/`, which returns `PERMISSION_CHOICES` among other enumerations.
3. Assign it to users at `/admin/user/:userId`.

Two pairings worth getting right:

- **`can_update_school_master_data` vs `can_publish_school_master_data`.** These are Editor and
  Publisher. Granting both makes the user a Publisher **and removes the Editor-only transitions** —
  `is_editor_only` is defined as *not* having publish. See
  [school-master-review-publish.md](school-master-review-publish.md#roles).
- **`can_preview_data_layer`.** Preview runs an unpublished layer's full query against live data,
  uncached. Grant it sparingly on production.

## Seeding

```bash
# Create/refresh the Admin and Read Only system roles
pipenv run python manage.py update_system_role_permissions

# Non-production: roles plus representative users
pipenv run python manage.py non_prod_setup_roles_and_users

# A superuser
pipenv run python manage.py create_admin_user \
  -email='you@example.com' -first_name='You' -last_name='Dev'
```

> **Run `update_system_role_permissions` after every change to the permission constants.** New slugs
> are not attached to existing roles automatically — a newly added permission simply does not exist
> for anyone until this runs.

## Performance note

`user.permissions` is a property with **no caching**; every access re-queries roles and their
permissions. Stacked permission classes call it repeatedly per request. See
[../08-operations/caching-and-performance.md](../08-operations/caching-and-performance.md#1-userpermissions-is-recomputed-on-every-access).

## Related

- [auth-and-rbac.md](auth-and-rbac.md)
- [school-master-review-publish.md](school-master-review-publish.md)
