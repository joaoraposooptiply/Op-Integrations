import os
import requests
import datetime
import backoff
import json
import pandas as pd
import random

optiply_base_url = os.environ.get('optiply_base_url', 'https://api.optiply.com/v1')
optiply_dashboard_url = os.environ.get('optiply_dashboard_url', 'https://dashboard.optiply.nl/api')


class OptiplyAuthenticator:
    def __init__(self, credentials, dir_path, output_dir) -> None:
        self.config = credentials
        self.config_path = dir_path
        self.last_refreshed = None
        self.output_dir = output_dir
        self.test = bool(
            credentials.get("hotglue_test", os.environ.get("IS_TEST", False))
        )
        self.requests_table = None
        if self.test:
            self.prepare_testing()

    def prepare_testing(self) -> None:
        self.requests_table = pd.DataFrame(columns=["uri", "method", "payload"])

    def is_token_valid(self):
        if self.test:
            return True
        if not self.config.get("access_token"):
            return False
        if self.check_access():
            return True
        return False

    def get_access(self):
        url = f"{optiply_dashboard_url}/auth/oauth/token?grant_type=password"
        data = {
            "username": self.config.get("username"),
            "password": self.config.get("password"),
        }
        client_id = self.config.get("client_id")
        client_secret = self.config.get("client_secret")

        if client_id and client_secret:
            resp = self._request(
                "POST", dummy=True, url=url, data=data, auth=(client_id, client_secret)
            )
        else:
            raise Exception("MISSING CONFIG -- client_id or client_secret")

        self.validate_and_update(resp)

    def validate_and_update(self, resp):
        if 500 <= resp.status_code < 600:
            raise Exception(resp.text)
        elif 400 <= resp.status_code < 500:
            raise Exception(resp.text)

        resp = resp.json()
        self.last_refreshed = datetime.datetime.utcnow()
        self.config["access_token"] = resp["access_token"]
        self.config["refresh_token"] = resp["refresh_token"]
        self.update_tenant_config(self.config["access_token"])

    def access_token(self):
        if self.is_token_valid():
            return self.config["access_token"]
        self.get_access()
        return self.config["access_token"]

    @backoff.on_exception(backoff.constant, Exception, max_tries=10, interval=20)
    def _request(self, method, dummy=False, **kwargs):
        print(f"{method} URL=[{kwargs.get('url')}] Payload=[{kwargs.get('data')}]")

        if self.test and method != "GET":
            response = FakeResponse()
            response.status_code = 200

            payload = kwargs.get("data", None)
            if payload is not None:
                payload = json.loads(payload)
                payload["data"]["attributes"].pop("remoteDataSyncedToDate", None)

            self.requests_table = pd.concat(
                [
                    self.requests_table,
                    pd.DataFrame.from_dict(
                        {
                            "uri": [kwargs.get("url")],
                            "method": [method],
                            "payload": [payload],
                        }
                    ),
                ]
            )
            test_dir = f"{self.output_dir}/test_output.csv"
            self.requests_table.to_csv(test_dir, index=False)
            return response

        if not dummy and self.config.get("access_token", None):
            kwargs.update(
                {
                    "headers": {
                        "Content-Type": "application/vnd.api+json",
                        "Authorization": f'Bearer {self.config["access_token"]}',
                        "User-Agent": "hotglue_py_agent"
                    }
                }
            )

        response = requests.request(method, **kwargs)
        print(f"STATUS CODE {response.status_code}")

        if response.status_code == 401:
            if "UserDetailsService returned null" in response.text:
                raise Exception("CREDENTIALS ARE WRONG -- update client_secret and client_id")
            else:
                self.get_access()

        if (response.status_code > 400 or response.status_code < 200) and response.status_code not in [404, 409]:
            response.raise_for_status()

        if (
            response.status_code not in [200, 201, 204, 409]
            and not (response.status_code == 400 and "is not a valid address" in response.text)
            and not (response.status_code == 404 and method == 'DELETE')
            and not (response.status_code == 404 and method == 'POST' and ('receiptLines' in kwargs.get("url", "")))
            and not (response.status_code == 404 and method == 'PATCH' and ('supplierProducts' in kwargs.get("url", "")))
        ):
            raise Exception(response.text)
        return response

    def update_tenant_config(self, access_token):
        json_f = open(self.config_path, "r")
        data = json.load(json_f)
        data["apiCredentials"]["access_token"] = access_token
        json_f = open(self.config_path, "w")
        json.dump(data, json_f)

    def check_access(self):
        url = f"{optiply_base_url}/products?page[limit]=1&page[offset]=0"
        response = self._request("GET", url=url)
        if response.status_code == 200:
            return True
        return False

    def get_data(self, url):
        parameters = {
            "page[limit]": 100,
            "page[offset]": 0,
            "sort": "id"
        }
        data = []
        paginate = 0
        while paginate is not None:
            response = self._request("GET", url=url, params=parameters)
            paginate += 1
            if response.status_code == 200:
                resp = response.json()
                data += resp['data']
                if len(response.json()['data']) == 0:
                    paginate = None
                else:
                    parameters.update({"page[offset]": paginate * parameters['page[limit]']})
            else:
                raise Exception(response.text)
        return data


class FakeResponse(requests.Response):
    def json(self, **kwargs):
        return {"data": {"id": random.randint(0, int(1e10))}}
