from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.account_schema_mapper import AccountSchemaMapper

class AccountSink(NetSuiteBatchSink):
    name = "Accounts"
    record_type = "account"

    def preprocess_batch_record(self, record: dict) -> dict:
        return AccountSchemaMapper(record, self._target.reference_data).to_netsuite()
