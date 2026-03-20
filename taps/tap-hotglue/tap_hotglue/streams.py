"""Stream type classes for tap-hotglue."""

import json
from typing import Any, Dict, Iterable, Optional

import requests
from singer_sdk import typing as th
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_hotglue.client import HotglueStream


def _stringify(row: dict, keys: list) -> dict:
    """Serialize non-string values as JSON strings for target-csv compatibility."""
    for k in keys:
        v = row.get(k)
        if v is not None and not isinstance(v, str):
            row[k] = json.dumps(v)
    return row


# ---------------------------------------------------------------------------
# Standalone streams
# ---------------------------------------------------------------------------


class TenantsStream(HotglueStream):
    """All tenants in the environment."""

    name = "tenants"
    primary_keys = ["tenant_id"]
    replication_key = None
    records_jsonpath = "$[*]"

    schema = th.PropertiesList(
        th.Property("tenant_id", th.StringType),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/tenants/{self.env_id}"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """API returns a flat list of tenant ID strings — wrap into dicts."""
        for tenant_id in response.json():
            yield {"tenant_id": str(tenant_id)}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Pass tenant_id to child streams."""
        return {"tenant_id": record["tenant_id"]}


class FlowsStream(HotglueStream):
    """All supported flows (connectors) in the environment."""

    name = "flows"
    primary_keys = ["flow_id"]
    replication_key = None
    records_jsonpath = "$[*]"

    schema = th.PropertiesList(
        th.Property("flow_id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("type", th.StringType),
        th.Property("taps", th.StringType),
        th.Property("targets", th.StringType),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/{self.env_id}/flows/supported"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if "id" in row:
            row["flow_id"] = row.pop("id")
        return _stringify(row, ["type", "taps", "targets"])


# ---------------------------------------------------------------------------
# Per-tenant streams (children of TenantsStream)
# ---------------------------------------------------------------------------


class TenantConfigStream(HotglueStream):
    """Tenant configuration — credentials, metadata, account info."""

    name = "tenant_config"
    primary_keys = ["tenant_id"]
    replication_key = None
    records_jsonpath = "$"
    parent_stream_type = TenantsStream

    schema = th.PropertiesList(
        th.Property("tenant_id", th.StringType),
        th.Property("apiCredentials", th.StringType),
        th.Property("hotglue_metadata", th.StringType),
        th.Property("importCredentials", th.StringType),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/tenant/{self.env_id}/{{tenant_id}}/config"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["tenant_id"] = context["tenant_id"]
        return _stringify(row, ["apiCredentials", "hotglue_metadata", "importCredentials"])


class TenantMappingStream(HotglueStream):
    """Field mapping configuration per tenant."""

    name = "tenant_mapping"
    primary_keys = ["tenant_id"]
    replication_key = None
    records_jsonpath = "$"
    parent_stream_type = TenantsStream

    schema = th.PropertiesList(
        th.Property("tenant_id", th.StringType),
        th.Property("mapping", th.StringType),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/tenant/{self.env_id}/{{tenant_id}}/mapping"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Wrap the mapping response into a record."""
        if response.status_code in (400, 404):
            return

        try:
            data = response.json()
        except Exception:
            return

        yield {"mapping": data}

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["tenant_id"] = context["tenant_id"]
        return _stringify(row, ["mapping"])


class LinkedFlowsStream(HotglueStream):
    """Flows linked to each tenant — bridges tenants to flow-level data."""

    name = "linked_flows"
    primary_keys = ["tenant_id", "flow_id"]
    replication_key = None
    records_jsonpath = "$[*]"
    parent_stream_type = TenantsStream

    schema = th.PropertiesList(
        th.Property("tenant_id", th.StringType),
        th.Property("flow_id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("type", th.StringType),
        th.Property("taps", th.StringType),
        th.Property("targets", th.StringType),
        th.Property("version", th.StringType),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/{self.env_id}/flows/linked"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = {}
        if context:
            params["tenant_id"] = context["tenant_id"]
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["tenant_id"] = context["tenant_id"]
        if "id" in row:
            row["flow_id"] = row.pop("id")
        return _stringify(row, ["type", "taps", "targets", "version"])

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Pass tenant_id + flow_id to grandchild streams."""
        return {
            "tenant_id": record.get("tenant_id") or (context or {}).get("tenant_id"),
            "flow_id": record.get("flow_id") or record.get("id"),
        }


# ---------------------------------------------------------------------------
# Per-tenant-flow streams (children of LinkedFlowsStream)
# ---------------------------------------------------------------------------


class LinkedSourcesStream(HotglueStream):
    """Source configuration per tenant/flow."""

    name = "linked_sources"
    primary_keys = ["tenant_id", "flow_id", "tap"]
    replication_key = None
    records_jsonpath = "$[*]"
    parent_stream_type = LinkedFlowsStream

    schema = th.PropertiesList(
        th.Property("tenant_id", th.StringType),
        th.Property("flow_id", th.StringType),
        th.Property("tap", th.StringType),
        th.Property("domain", th.StringType),
        th.Property("label", th.StringType),
        th.Property("type", th.StringType),
        th.Property("config", th.StringType),
        th.Property("fieldMap", th.StringType),
        th.Property("connect_ui_params", th.StringType),
        th.Property("connector_props", th.StringType),
        th.Property("category", th.StringType),
        th.Property("isForked", th.StringType),
        th.Property("forkedFieldMap", th.StringType),
        th.Property("config_fetched", th.StringType),
        th.Property("isReconnecting", th.StringType),
        th.Property("connection_name", th.StringType),
        th.Property("default_import_scheduler", th.StringType),
        th.Property("default_export_scheduler", th.StringType),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/{self.env_id}/{{flow_id}}/{{tenant_id}}/linkedSources"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        return {"config": "true"}

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["tenant_id"] = context["tenant_id"]
            row["flow_id"] = context["flow_id"]
        return _stringify(row, [
            "config", "fieldMap", "connect_ui_params", "connector_props",
            "isForked", "forkedFieldMap", "config_fetched", "isReconnecting",
        ])


class LinkedTargetsStream(HotglueStream):
    """Target configuration per tenant/flow."""

    name = "linked_targets"
    primary_keys = ["tenant_id", "flow_id", "target"]
    replication_key = None
    records_jsonpath = "$[*]"
    parent_stream_type = LinkedFlowsStream

    schema = th.PropertiesList(
        th.Property("tenant_id", th.StringType),
        th.Property("flow_id", th.StringType),
        th.Property("target", th.StringType),
        th.Property("domain", th.StringType),
        th.Property("label", th.StringType),
        th.Property("type", th.StringType),
        th.Property("config", th.StringType),
        th.Property("fieldMap", th.StringType),
        th.Property("connector_props", th.StringType),
        th.Property("category", th.StringType),
        th.Property("isForked", th.StringType),
        th.Property("connection_name", th.StringType),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/{self.env_id}/{{flow_id}}/{{tenant_id}}/linkedTargets"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["tenant_id"] = context["tenant_id"]
            row["flow_id"] = context["flow_id"]
        return _stringify(row, [
            "config", "fieldMap", "connector_props", "isForked",
        ])


class SourceStateStream(HotglueStream):
    """Source bookmark/state for incremental sync tracking per tenant/flow."""

    name = "source_state"
    primary_keys = ["tenant_id", "flow_id"]
    replication_key = None
    records_jsonpath = "$"
    parent_stream_type = LinkedFlowsStream

    schema = th.PropertiesList(
        th.Property("tenant_id", th.StringType),
        th.Property("flow_id", th.StringType),
        th.Property("state", th.StringType),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/{self.env_id}/{{flow_id}}/{{tenant_id}}/linkedSources/state"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        if response.status_code in (400, 404):
            return

        try:
            data = response.json()
        except Exception:
            return

        yield {"state": data}

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["tenant_id"] = context["tenant_id"]
            row["flow_id"] = context["flow_id"]
        return _stringify(row, ["state"])


class JobsStream(HotglueStream):
    """Job history per tenant/flow."""

    name = "jobs"
    primary_keys = ["tenant_id", "flow_id", "job_id"]
    replication_key = "start_time"
    records_jsonpath = "$[*]"
    parent_stream_type = LinkedFlowsStream

    schema = th.PropertiesList(
        th.Property("tenant_id", th.StringType),
        th.Property("flow_id", th.StringType),
        th.Property("job_id", th.StringType),
        th.Property("job_name", th.StringType),
        th.Property("env_id", th.StringType),
        th.Property("s3_root", th.StringType),
        th.Property("tap", th.StringType),
        th.Property("target", th.StringType),
        th.Property("status", th.StringType),
        th.Property("start_time", th.DateTimeType),
        th.Property("last_updated", th.DateTimeType),
        th.Property("scheduled_job", th.StringType),
        th.Property("sync_type", th.StringType),
        th.Property("streaming_job", th.StringType),
        th.Property("message", th.StringType),
        th.Property("error", th.StringType),
        th.Property("duration", th.StringType),
        th.Property("task_type", th.StringType),
        th.Property("task_region", th.StringType),
        th.Property("launch_type", th.StringType),
        th.Property("task_definition", th.StringType),
        th.Property("resources_usage", th.StringType),
        th.Property("status_timestamp", th.StringType),
        th.Property("data_sizes", th.StringType),
        th.Property("metrics", th.StringType),
        th.Property("tap_install_uri", th.StringType),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/{self.env_id}/{{flow_id}}/{{tenant_id}}/jobs"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["tenant_id"] = context["tenant_id"]
            row["flow_id"] = context["flow_id"]
        if "tenant" in row and "tenant_id" not in row:
            row["tenant_id"] = row.pop("tenant")
        return _stringify(row, [
            "scheduled_job", "streaming_job", "duration", "error",
            "task_definition", "resources_usage", "status_timestamp",
            "data_sizes", "metrics",
        ])
