from target_netsuite_v2.client import NetSuiteBatchSink
from target_netsuite_v2.mapper.account_schema_mapper import AccountSchemaMapper

class AccountSink(NetSuiteBatchSink):
    name = "Accounts"
    endpoint = "/account"

    def process_batch_record(self, record: dict, index: int) -> dict:
        return AccountSchemaMapper(record, self._target.reference_data).to_netsuite()
