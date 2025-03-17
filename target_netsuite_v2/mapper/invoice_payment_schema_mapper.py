from target_netsuite_v2.mapper.base_mapper import BaseMapper

class InvoicePaymentSchemaMapper(BaseMapper):
    field_mappings = {
        "externalId": "externalId",
        "exchangeRate": "exchangeRate",
        "paymentDate": "tranDate"
    }

    def __init__(
            self,
            record,
            entity,
            invoice_id,
            reference_data
    ) -> None:
        self.record = record
        self.entity = entity
        self.invoice_id = invoice_id
        self.reference_data = reference_data

    def to_netsuite(self) -> dict:
        payload = {
            "customer": self.entity,
            **self._map_currency(),
            **self._map_subrecord("Accounts", "accountId", "accountName", "account"),
            **self._map_apply()
        }

        self._map_fields(payload)

        return payload

    def _map_apply(self):
        return {
            "apply": {
                "items": [
                    {
                        "doc": {
                            "id": self.invoice_id
                        },
                        "apply": True,
                        "amount": self.record.get("amount")
                    }
                ]
            }
        }

