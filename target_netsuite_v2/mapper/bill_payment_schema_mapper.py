from target_netsuite_v2.mapper.base_mapper import BaseMapper

class BillPaymentSchemaMapper(BaseMapper):
    def __init__(
            self,
            record,
            entity,
            vendor_bill_id,
            reference_data
    ) -> None:
        self.record = record
        self.entity = entity
        self.vendor_bill_id = vendor_bill_id
        self.reference_data = reference_data

    def to_netsuite(self) -> dict:
        payload = {
            "entity": self.entity,
            **self._map_currency(),
            **self._map_subrecord("Accounts", "accountId", "accountName", "account"),
            **self._map_apply()
        }

        field_mappings = {
            "externalId": "externalId",
            "externalId": "tranId",
            "exchangeRate": "exchangeRate",
            "paymentDate": "tranDate"
        }

        for record_key, payload_key in field_mappings.items():
            if record_key in self.record:
                payload[payload_key] = self.record.get(record_key)

        return payload

    def _map_apply(self):
        return {
            "apply": {
                "items": [
                    {
                        "doc": {
                            "id": self.vendor_bill_id
                        },
                        "apply": True,
                        "amount": self.record.get("amount")
                    }
                ]
            }
        }

