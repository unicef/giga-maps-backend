# Content management

The CMS-lite surfaces: the About page, its image slider, and the contact-message queue.

---

## About Us

`/admin/about-us`

| Endpoint | Methods | View |
|---|---|---|
| `/api/about_us/about_us/` | `GET`, `POST`, `PUT`, `DELETE` | `AboutUsAPIView` |
| `/api/about_us/about_us/active_data/` | `GET` | `AboutUsAPIView` |
| `/api/about_us/slide_image/` | `GET`, `POST`, `DELETE` | `SlideImageAPIView` |
| `/api/about_us/slide_image/{pk}/` | `GET`, `PUT` | `SlideImageAPIView` |

Models: `AboutUs` and `SliderImage`
([proco/about_us/models.py:9](../../proco/about_us/models.py#L9)). The whole app is 575 lines.

`active_data/` is the public read endpoint the frontend calls at `/about`; the unsuffixed route is
the editing surface. Only one About Us record is "active" at a time, so editing is
create-a-new-version rather than in-place mutation.

> **About Us has no permission slug of its own.** Unlike every other admin area, there is no
> `can_*_about_us` in the 47 permissions. Access is governed only by `IsUserAuthenticated` plus
> whatever the viewset declares — check [proco/about_us/api.py](../../proco/about_us/api.py) before
> assuming a role restricts it.

Seeding:

```bash
pipenv run python manage.py load_about_us_content --load_about_us_content
```

### Slider images

`SliderImage` records feed the About page carousel. Uploads go to Azure Blob Storage via
`django-storages[azure]` (`AZURE_ACCOUNT_NAME`, `AZURE_ACCOUNT_KEY`, `AZURE_CONTAINER`).

Images are written under `IMAGES_PATH = 'images'` with randomised filenames
(`get_random_name_image`), the same helper used for `Country.flag` and `Country.map_preview`.

> Deleting a `SliderImage` row does not delete the blob. Orphaned images accumulate in the
> container.

## Contact messages

`/admin/contact-message`, `/admin/contact-message/view/:id`

| Endpoint | Methods |
|---|---|
| `/api/contact/contactmessage/` | `GET`, `DELETE` |
| `/api/contact/contactmessage/{pk}/` | `GET` |

| Permission | Grants |
|---|---|
| `can_view_contact_message` | Read the queue |
| `can_update_contact_message` | Mark handled |
| `can_delete_contact_message` | Remove (soft delete) |

Messages carry a category (`ContactMessage.CATEGORY_CHOICES`, exposed to the frontend through
`/api/accounts/app_configs/`).

> Submissions also trigger an email to `CONTACT_MANAGERS` / the constance `CONTACT_EMAIL`. **Both
> default to non-working values** (`test@test.test` and `[]` respectively), and Anymail is
> configured with `IGNORE_RECIPIENT_STATUS: True`, so a misconfiguration is completely silent. If
> this queue is the only place anyone sees enquiries, that is the likely reason. See
> [../03-user-flows/contact.md](../03-user-flows/contact.md).

There is **no throttling** on the public submission endpoint, so the queue is spammable.

## Other admin-editable content

| Content | Where |
|---|---|
| Country description, disclaimer, data-source attribution | `/admin/country` — `Country.description`, `country_disclaimer`, `data_source`, `data_source_description`, `health_data_source` |
| User-facing alerts | `/admin/alerts` — see [notifications-and-alerts.md](notifications-and-alerts.md) |
| Data layer names, descriptions, legends | `/admin/giga-layer` |
| Filter labels | `/admin/filter`, and `ColumnConfiguration.label` (seeded, not editable) |
| `CONTACT_EMAIL` | django-constance, runtime-editable, stored in Redis |

`Country.health_data_source` is a separate attribution field for entity data — set it when
onboarding health facilities for a country, or the About/country panel will credit the school data
source for both.

## Related

- [../03-user-flows/contact.md](../03-user-flows/contact.md)
- [country-and-school-data.md](country-and-school-data.md)
