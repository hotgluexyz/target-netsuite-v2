import abc
import json
import hashlib

from datetime import datetime
from singer_sdk.plugin_base import PluginBase
from singer_sdk.sinks import BatchSink
from target_hotglue.client import HotglueBaseSink
from target_hotglue.common import HGJSONEncoder
from typing import Dict, List, Optional
from target_netsuite_v2.mapper.base_mapper import InvalidReferenceError

class NetSuiteBaseSink(HotglueBaseSink):
    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self.suite_talk_client = self._target.suite_talk_client

    def record_exists(self, record: dict) -> bool:
        return bool(record.get("internalId"))

    def build_record_hash(self, record: dict):
        return hashlib.sha256(json.dumps(record, cls=HGJSONEncoder).encode()).hexdigest()

    def get_existing_state(self, hash: str):
        states = self.latest_state["bookmarks"][self.name]

        existing_state = next((s for s in states if hash==s.get("hash") and s.get("success")), None)

        if existing_state:
            self.latest_state["summary"][self.name]["existing"] += 1

        return existing_state

class NetSuiteBatchSink(NetSuiteBaseSink, BatchSink):
    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.

        The `context["records"]` list will contain all records from the given batch
        context.

        Args:
            context: Stream partition or context dictionary.
        """
        if not self.latest_state:
            self.init_state()

        batch_records = context["records"]

        reference_data = self.get_batch_reference_data(context)

        for record in batch_records:
            self.process_batch_record(record, reference_data)

    def get_batch_reference_data(self, context: dict) -> dict:
        """Get the reference data for a batch

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dict containing batch specific reference data.
        """
        return self._target.reference_data

    def process_batch_record(self, record: dict, reference_data: dict):
        """Process a record in the batch

        Preprocess the record to map it to the desired payload.
        Capture state updates, and upsert the record to the target

        Args:
            record: Individual raw record in the stream.
            reference_data: A dictionary containing all reference_data necessary for a batch.
        """
        hash = self.build_record_hash(record)
        existing_state = self.get_existing_state(hash)
        try:
            preprocessed = self.preprocess_batch_record(record, reference_data)
        except InvalidReferenceError as e:
            state = {}
            state["error"] = str(e)
            external_id = record.get("externalId")
            if external_id:
                state["externalId"] = external_id
            id = record.get("id")
            if id:
                state["id"] = id
            self.update_state(state)
            return

        external_id = preprocessed.get("externalId")

        if existing_state:
            self.update_state(existing_state, is_duplicate=True)
            return

        id, success, state = self.upsert_record(preprocessed, reference_data)

        if success:
            self.logger.info(f"{self.name} processed id: {id}")

        state["success"] = success

        if id:
            state["id"] = id

        if external_id:
            state["externalId"] = external_id

        self.update_state(state)

    @abc.abstractmethod
    def preprocess_batch_record(self, record: dict) -> dict:
        """Preprocess a batch with the given batch context.

        This method must be overridden.
        Map the raw record to a dictionary for use in the API payload.

        Args:
            record: Individual raw record in the stream.
        """
        pass

    def upsert_record(self, record: dict, reference_data: dict):
        state = {}

        did_update = False
        if self.record_exists(record):
            id, success, error_message = self.suite_talk_client.update_record(self.record_type, record['internalId'], record)
            did_update = True
        else:
            id, success, error_message = self.suite_talk_client.create_record(self.record_type, record)

        if error_message:
            state["error"] = error_message
        elif did_update:
            state["is_updated"] = True

        return id, success, state

    def _are_dates_equivalent(self, netsuite_date, unified_date) -> bool:
        """Compares two date strings and returns True if they have the same month, day, and year."""
        if netsuite_date is None and unified_date is None:
            return True
        if netsuite_date is None or unified_date is None:
            return False
        try:
            dt1 = datetime.strptime(unified_date[:10], "%Y-%m-%d")
            dt2 = datetime.strptime(netsuite_date, "%m/%d/%Y")
            return (dt1.year, dt1.month, dt1.day) == (dt2.year, dt2.month, dt2.day)
        except ValueError:
            return False

    def _omit_key(self, d, key):
        return {k: v for k, v in d.items() if k != key}


