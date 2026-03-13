import pandas as pd
from datetime import datetime


def clean_null(input):
    if isinstance(input, list):
        return [clean_null(i) for i in input]
    elif isinstance(input, dict):
        output = {}
        for k, v in input.items():
            if v is not None:
                output[k] = clean_null(v)
        return output
    return input


def clean_payload(item):
    item = clean_dict_items(item)
    output = {}
    for k, v in item.items():
        if isinstance(v, datetime):
            dt_str = v.strftime("%Y-%m-%dT%H:%M:%SZ")
            if len(dt_str) > 20:
                output[k] = f"{dt_str[:-2]}:{dt_str[-2:]}"
            else:
                output[k] = dt_str
        elif isinstance(v, dict):
            output[k] = clean_payload(v)
        else:
            output[k] = v
    return output


def clean_dict_items(dict, value=None):
    return {k: v for k, v in dict.items() if v is not None}


def get_row_value(row, map, name):
    if map.get(name) is None:
        return None
    if map[name] not in row.index:
        return None
    value = row[map[name]]
    if pd.isnull(value):
        return None
    return value


def str_to_datetime(str):
    new_date = datetime.strptime(str, '%Y-%m-%dT%H:%M:%S')
    return new_date
