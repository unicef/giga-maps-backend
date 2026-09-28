# Azure AD B2C

Identity for the frontend and admin console. The backend **verifies** tokens; it never issues them
and never sees a B2C user's password.

## Configuration

[config/settings/base.py:403](../../config/settings/base.py#L403):

| Variable | Purpose |
|---|---|
| `AD_B2C_TENANT_ID` | Tenant |
| `AD_B2C_CLIENT_ID` | App registration |
| `AD_B2C_BASE_URL` | B2C endpoint |
| `AD_B2C_DOMAIN` | B2C domain |
| `AD_B2C_SIGNUP_SIGNIN_POLICY` | e.g. `B2C_1_UNICEF_SOCIAL_signup_signin` |
| `AD_B2C_FORGOT_PASSWORD_POLICY` | e.g. `B2C_1_PasswordResetPolicy` |
| `AD_B2C_EDIT_PROFILE_POLICY` | e.g. `B2C_1_ProfileEditingPolicy` |

Sign-up, sign-in, password reset and profile editing are all **B2C user journeys**, driven from the
frontend. The backend is not involved.

## JWT verification

[config/settings/base.py:388](../../config/settings/base.py#L388):

| Setting | Value |
|---|---|
| Algorithm | `RS256` — asymmetric; B2C signs, the backend verifies |
| Header prefix | `Bearer` |
| Expiry | 1 day |
| `JWT_VERIFY` / `JWT_VERIFY_EXPIRATION` | `True` |
| `JWT_LEEWAY` | `0` |
| `JWT_PUBLIC_KEY` | `None` in settings — resolved at runtime |
| Username handler | `proco.custom_auth.utils.jwt_get_username_from_payload_handler` |
| Decode handler | `proco.custom_auth.utils.jwt_decode_handler` |

`JWT_LEEWAY = 0` means **no clock-skew tolerance**. A backend host whose clock drifts ahead of B2C
will reject tokens that are still valid. Keep NTP healthy.

Library: `djangorestframework-jwt` 1.11.0 — **unmaintained**. It is a constraint on any Django
upgrade.

## Two expiries, not one

Beyond the 1-day token lifetime, `IsUserAuthenticated`
([proco/core/permissions.py:65](../../proco/core/permissions.py#L65)) rejects any user whose
`last_login` is more than **5 days** old:

```python
login_expiration_date = datetime.now() - timedelta(days=5)
if login_date < login_expiration_date:
    raise rest_exceptions.AuthenticationFailed()
```

> This is keyed on `last_login`, not on token issuance, and it produces the same generic
> `AuthenticationFailed` as an invalid token. **"My token is valid but I get 401" is almost always
> this check.** It also means a service account driving the API with a refreshed token will be
> locked out after five days unless something updates `last_login`.

## User provisioning

`ApplicationUser` rows are created locally. `RemoteAndModelBackend`
([proco/custom_auth/backends.py:9](../../proco/custom_auth/backends.py#L9)) is a `ModelBackend`
subclass retained for local/admin password login; B2C users authenticate through the JWT class
instead.

Creating an administrator:

```bash
pipenv run python manage.py create_admin_user \
  -email='you@example.com' -first_name='You' -last_name='Dev'
```

Roles and permissions are entirely local — B2C carries identity, not authorization. See
[../04-admin-flows/auth-and-rbac.md](../04-admin-flows/auth-and-rbac.md).

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| All authenticated requests 401 | B2C misconfigured, or public key unresolvable |
| A specific user 401s, others fine | `last_login` older than 5 days, or `is_active=False`/`deleted` set |
| Intermittent 401s | Clock skew — `JWT_LEEWAY` is `0` |
| Authenticated but 403 | Authorization, not authentication — check the permission slug |
