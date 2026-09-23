"""Tests for REST custom field value parsing."""

import json
from unittest.mock import MagicMock

import pytest

from target_netsuite_v2.sinks import netsuiteV2Sink

CURRENCY_LIST_REPR = (
    "{'items': [{'currency': {'id': '1'}}, {'currency': {'id': '2'}}]}"
)


def _sink():
    sink = netsuiteV2Sink(
        target=MagicMock(),
        stream_name="Customer",
        schema={"type": "object", "properties": {}},
        key_properties=[],
    )
    sink.logger = MagicMock()
    return sink


def test_apply_custom_fields_decodes_stringified_objects():
    sink = _sink()
    payload = {}
    sink._apply_custom_fields_to_payload(
        payload,
        [
            {"name": "custentity1", "value": "Aug 2026"},
            {"name": "currencyList", "value": CURRENCY_LIST_REPR},
        ],
    )
    assert payload["custentity1"] == "Aug 2026"
    assert payload["currencyList"] == {
        "items": [{"currency": {"id": "1"}}, {"currency": {"id": "2"}}],
    }


def test_apply_custom_fields_parses_json_encoded_list():
    sink = _sink()
    payload = {}
    encoded = json.dumps([{"name": "currencyList", "value": '{"items": []}'}])
    sink._apply_custom_fields_to_payload(payload, encoded)
    assert payload["currencyList"] == {"items": []}


def test_apply_custom_fields_rejects_non_list():
    sink = _sink()
    with pytest.raises(Exception, match="Invalid customFields"):
        sink._apply_custom_fields_to_payload({}, {"not": "a list"})
