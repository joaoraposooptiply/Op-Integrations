import os

import pandas as pd


def get_snapshot(stream, snapshot_dir, converters={}):
    if os.path.isfile(f"{snapshot_dir}/{stream}.snapshot.csv"):
        snapshot = pd.read_csv(f"{snapshot_dir}/{stream}.snapshot.csv", converters=converters, dtype="object")
    else:
        snapshot = None
    return snapshot


def delete_from_snapshot(delete_items, stream, snapshot_dir, pk="id"):
    if os.path.isfile(f"{snapshot_dir}/{stream}.snapshot.csv"):
        snapshot = pd.read_csv(f"{snapshot_dir}/{stream}.snapshot.csv", dtype="object")
        snapshot[pk] = snapshot[pk].astype(str)
    else:
        return None
    delete_items[pk] = delete_items[pk].astype(str)
    delete_items = pd.concat([snapshot, delete_items])
    delete_items = delete_items.drop_duplicates(pk, keep=False)
    delete_items.to_csv(f"{snapshot_dir}/{stream}.snapshot.csv", index=False)
    return delete_items


def snapshot_records(stream_data, stream, snapshot_dir, pk="id", return_full=False):
    if stream_data is None:
        return None

    return_data = None

    if os.path.isfile(f"{snapshot_dir}/{stream}.snapshot.csv"):
        snapshot = pd.read_csv(f"{snapshot_dir}/{stream}.snapshot.csv", dtype="object")
        snapshot[pk] = snapshot[pk].astype(str)
    else:
        snapshot = None

    if snapshot is not None:
        stream_data[pk] = stream_data[pk].astype(str)
        stream_data = pd.concat([snapshot, stream_data])
        return_data = stream_data.drop_duplicates(pk, keep=False)
        stream_data = stream_data.drop_duplicates(pk, keep="last")
        stream_data.to_csv(f"{snapshot_dir}/{stream}.snapshot.csv", index=False)
        if return_full:
            return stream_data
    else:
        stream_data.to_csv(f"{snapshot_dir}/{stream}.snapshot.csv", index=False)
        return_data = stream_data

    return return_data


def extract_remoteId(obj):
    remoteId = obj.get("remoteIdMap")
    key = list(remoteId.keys())[0]
    return remoteId[key]


def handle_invalid_dates(date_str, date_format='%Y-%m-%dT%H:%M:%S'):
    try:
        dt = pd.to_datetime(date_str, format=date_format)
        return dt
    except ValueError:
        return pd.NaT


def concat_columns(df, columns, sep='|'):
    selected = df[columns].astype(str)
    concatenated = selected.apply(lambda x: sep.join(x), axis=1)
    return concatenated


def round_to_2(value):
    if pd.isna(value) or not value or not str(value).replace(".", "").replace("-", "").isnumeric():
        return 0
    elif float(value) > 9999999.99:
        return 0
    else:
        return round(float(value), 2)


def round_to_0(value):
    if pd.isna(value) or not value or not str(value).replace(".", "").replace("-", "").isnumeric():
        return 0
    elif float(value) > 9999999.99:
        return 0
    else:
        value = round(float(value), 2)
        return int(value)


def nan_to_none(value):
    return None if pd.isna(value) else value


def validate_attribute(value):
    if not pd.isna(value) and not pd.isnull(value) and len(str(value)) > 0:
        return str(value)
    else:
        return None


def convert_to_bool(val):
    if isinstance(val, bool):
        return val
    elif isinstance(val, str):
        if val.lower() == 'true':
            return True
        elif val.lower() == 'false':
            return False
    return val


def round_numeric_to_0(value):
    if pd.isna(value):
        return value
    if not value or not str(value).replace(".", "").replace("-", "").isnumeric():
        return 0
    elif float(value) > 9999999.99:
        return 0
    else:
        value = round(float(value), 2)
        return int(value)


def round_numeric_to_2(value):
    if pd.isna(value):
        return value
    if not value or not str(value).replace(".", "").replace("-", "").isnumeric():
        return 0
    elif float(value) > 9999999.99:
        return 0
    else:
        return round(float(value), 2)
