import traceback
import logging

from django.contrib.admin.models import LogEntry, ADDITION, CHANGE, DELETION
from django.contrib.contenttypes.models import ContentType

logger = logging.getLogger('gigamaps.' + __name__)

def action_log(request, queryset, checked, change_message, model, field_name):
    action_flag = ADDITION
    if checked == 1:
        change_message = [{"added": {}}]
    elif checked == 2:
        action_flag = CHANGE
        if len(change_message) > 0:
            change_message = [{"changed": {"fields": change_message}}]
        else:
            change_message = [{"changed": "No fields changed."}]
    elif checked == 3:
        action_flag = DELETION
        change_message = ''

    ct = ContentType.objects.get_for_model(model)  # for_model --> get_for_model
    for obj in queryset:
        try:
            object_id = obj.pk
            object_repr = str(obj)
        except Exception:
            object_id = obj['id']
            object_repr = str(obj[field_name])

        LogEntry.objects.log_action(  # log_entry --> log_action
            user_id=request.user.id,
            content_type_id=ct.pk,
            object_id=object_id,
            object_repr=object_repr,
            action_flag=action_flag,  # actions_flag --> action_flag
            change_message=change_message)


def changed_fields(instance, validated_data, changed_data=None):
    if not changed_data:
        changed_data = []

    model_instance = ['country', 'school', 'last_weekly_status', 'location']
    try:
        for field, value in validated_data.items():
            if not isinstance(value, dict):
                if field in model_instance and int(value) != int(getattr(instance, field, None).id):
                    changed_data.append(field)
                elif 'date' in field:
                    if (
                        (
                            getattr(instance, field, None) is not None and
                            str(getattr(instance, field, None).strftime("%d-%m-%Y")) != value
                        ) or getattr(instance, field, None) is None and value != ''
                    ):
                        changed_data.append(field)
                    else:
                        try:
                            changed_data.remove(field)
                        except:
                            pass
                elif 'date' not in field and field not in model_instance and value != getattr(instance, field, None):
                    if (
                        value == "" and
                        (getattr(instance, field, None) is None or getattr(instance, field, None) == '') or
                        (field in ['schools_with_data_percentage'] and getattr(instance, field, None) == float(value))
                    ):
                        pass
                    elif (
                        (getattr(instance, field, None) != '' and (value != "" or value == "")) or
                        (getattr(instance, field, None) is None and value != "") or
                        (getattr(instance, field, None) == '' and value != "")
                    ):
                        changed_data.append(field)
            else:
                changed_fields(getattr(instance, field), validated_data[field], changed_data)
    except:
        logger.error(traceback.format_exc())

    changed_data = list(set(changed_data))
    remove_item = ["created", "modified"]
    for field in remove_item:
        if field in changed_data:
            changed_data.remove(field)
    return changed_data


def changed_about_us_content_fields(instance, validated_data, changed_data=None):
    if not changed_data:
        changed_data = []

    try:
        for field, value in validated_data.items():
            if not isinstance(value, dict) and not isinstance(value, list):
                if value != getattr(instance, field, None):
                    if (
                        (getattr(instance, field, None) != '' and (value != "" or value == "")) or
                        (getattr(instance, field, None) is None and value != "") or
                        (getattr(instance, field, None) == '' and value != "")
                    ):
                        changed_data.append(field)
            elif isinstance(value, dict):
                changed_about_us_content_fields(getattr(instance, field), validated_data[field], changed_data)
            elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], str):
                try:
                    if set(instance[field]) != set(value):
                        changed_data.append(field)
                except:
                    if set(getattr(instance, field)) != set(value):
                        changed_data.append(field)

            elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                i = 0
                for item in value:
                    for dict_field, dict_value in item.items():
                        if isinstance(dict_value, dict):
                            changed_about_us_content_fields(getattr(instance, field)[i][dict_field], item[dict_field],
                                                            changed_data)
                        elif isinstance(dict_value, list):
                            if set(dict_value) != set(getattr(instance, field)[i][dict_field]):
                                changed_data.append(field + '_' + dict_field)
                        else:
                            if dict_value != getattr(instance, field)[i][dict_field]:
                                changed_data.append(field + '_' + dict_field)
                    i += 1
    except:
        logger.error(traceback.format_exc())
    return changed_data
