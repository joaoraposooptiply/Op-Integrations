import json
import os

optiply_base_url = os.environ.get('optiply_base_url', 'https://api.optiply.com/v1')


def post_optiply(api_creds, auth, payload, entity):
    url = f"{optiply_base_url}/{entity}?accountId={api_creds['account_id']}&couplingId={api_creds['couplingId']}"

    payload = json.dumps({
        "data": {
            "type": entity,
            "attributes": payload
        }
    })

    response = auth._request("POST", url=url, data=payload)
    return response


def patch_optiply(api_creds, auth, payload, optiply_id, entity):
    url = f"{optiply_base_url}/{entity}/{optiply_id}?accountId={api_creds['account_id']}&couplingId={api_creds['couplingId']}"

    payload = json.dumps({
        "data": {
            "type": entity,
            "attributes": payload
        }
    })

    response = auth._request("PATCH", url=url, data=payload)
    return response


def delete_optiply(api_creds, auth, optiply_id, entity):
    url = f"{optiply_base_url}/{entity}/{optiply_id}?accountId={api_creds['account_id']}&couplingId={api_creds['couplingId']}"
    response = auth._request("DELETE", url=url)
    return response


def get_optiply(api_creds, auth, url):
    response = auth._request("GET", url=url)
    return response
