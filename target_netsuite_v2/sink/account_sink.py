from hotglue_models_accounting.accounting import Account
from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.account_schema_mapper import AccountSchemaMapper

class AccountSink(NetSuiteBatchSink):
    name = "Accounts"
    record_type = "account"
    unified_schema = Account
    auto_validate_unified_schema = True

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return AccountSchemaMapper(record, self.name, reference_data).to_netsuite()
