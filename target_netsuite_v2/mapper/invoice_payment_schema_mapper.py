from target_netsuite_v2.mapper.base_mapper import BaseMapper, InvalidInputError

class InvoicePaymentSchemaMapper(BaseMapper):
    field_mappings = {
        "externalId": "externalId",
        "exchangeRate": "exchangeRate",
        "paymentDate": "tranDate"
    }

    def __init__(
            self,
            record,
            sink_name,
            entity,
            invoice_id,
            reference_data
    ) -> None:
        self.record = record
        self.entity = entity
        self.invoice_id = invoice_id
        self.reference_data = reference_data
        self.existing_record = None
        
        # if there is invoice_id it means we are creating the payment through the invoice relatedPayments
        # in that case we don`t look for existing payments in the mapper
        if not self.invoice_id:
            self.existing_record = self._find_existing_record(self.reference_data[sink_name])

    def to_netsuite(self) -> dict:
        payload = {
            **self._map_internal_id(),
            **self._map_entity(),
            **self._map_currency(),
            **self._map_subrecord("Accounts", "accountId", "accountName", "account"),
            **self._map_apply(),
            **self._map_entity(), # we do this again to make sure the entity is set
        }

        self._map_fields(payload)

        return payload

    def _map_invoice(self):
        if self.invoice_id:
            return { "id": self.invoice_id }
        
        reference = self._find_reference_by_id_or_ref(
            self.reference_data["Invoices"],
            "invoiceId",
            None,
            external_id_field="invoiceExternalId"
        )

        if reference:
            if not self.entity:
                self.entity = reference["entityid"]
            elif self.entity != reference["entityid"]:
                raise InvalidInputError(f"The Customer supplied must be the same Invoice Customer")

            return { "id": reference["internalId"] }

        return {}

    def _map_entity(self):
        if self.entity:
            return {"customer": self.entity}
        
        reference = self._find_reference_by_id_or_ref(
            self.reference_data["Customers"],
            "customerId",
            "customerName",
            external_id_field="customerExternalId"
        )

        if reference:
            self.entity = reference["internalId"]
            return { "customer": { "id": reference["internalId"] } }

        return {}

    def _map_apply(self):
        return {
            "apply": {
                "items": [
                    {
                        "doc": { **self._map_invoice() },
                        "apply": True,
                        "amount": self.record.get("amount")
                    }
                ]
            }
        }

