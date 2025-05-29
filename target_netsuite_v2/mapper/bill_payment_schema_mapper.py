from target_netsuite_v2.mapper.base_mapper import BaseMapper, InvalidInputError

class BillPaymentSchemaMapper(BaseMapper):
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
            vendor_bill_id,
            reference_data
    ) -> None:
        self.record = record
        self.entity = entity
        self.vendor_bill_id = vendor_bill_id
        self.reference_data = reference_data
        self.existing_record = None
        
        # if there is vendor_bill_id it means we are creating the payment through the bill relatedPayments
        # in that case we don`t look for existing payments in the mapper
        if not self.vendor_bill_id:
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

    def _map_bill(self):
        if self.vendor_bill_id:
            return { "id": self.vendor_bill_id }
        
        reference = self._find_reference_by_id_or_ref(
            self.reference_data["Bills"],
            "billId",
            None,
            external_id_field="billExternalId"
        )

        if reference:
            # If the entity was not provided, fall back to the one from the Bill
            if not self.entity:
                self.entity = reference["entityid"]
            elif self.entity != reference["entityid"]:
                raise InvalidInputError(f"The Vendor supplied must be the same Bill Vendor")

            return { "id": reference["internalId"] }

        return {}

    def _map_entity(self):
        if self.entity:
            return {"entity": self.entity}
        
        reference = self._find_reference_by_id_or_ref(
            self.reference_data["Vendors"],
            "vendorId",
            "vendorName",
            external_id_field="vendorExternalId"
        )

        if reference:
            self.entity = reference["internalId"]
            return { "entity": { "id": reference["internalId"] } }

        return {}

    def _map_apply(self):
        return {
            "apply": {
                "items": [
                    {
                        "doc": { **self._map_bill() },
                        "apply": True,
                        "amount": self.record.get("amount")
                    }
                ]
            }
        }

