"""netsuite-v2 target sink class, which handles writing streams."""

from singer_sdk.sinks import BatchSink
from target_netsuite_v2.netsuite import NetSuite

from netsuitesdk.internal.exceptions import NetSuiteRequestError
import json
import os

from difflib import SequenceMatcher
from heapq import nlargest as _nlargest
from pendulum import parse
from datetime import datetime

class netsuiteSoapV2Sink(BatchSink):
    """netsuite-v2 target sink class."""

    def get_close_matches(self, word, possibilities, n=20, cutoff=0.7):
        if not n >  0:
            raise ValueError("n must be > 0: %r" % (n,))
        if not 0.0 <= cutoff <= 1.0:
            raise ValueError("cutoff must be in [0.0, 1.0]: %r" % (cutoff,))
        result = []
        s = SequenceMatcher()
        s.set_seq2(word)
        for x in possibilities:
            s.set_seq1(x)
            if s.real_quick_ratio() >= cutoff and \
            s.quick_ratio() >= cutoff and \
            s.ratio() >= cutoff:
                result.append((s.ratio(), x))
        result = _nlargest(n, result)

        return {v: k for (k, v) in result}

    def get_ns_client(self):
        ns_account = self.config.get("ns_account")
        ns_consumer_key = self.config.get("ns_consumer_key")
        ns_consumer_secret = self.config.get("ns_consumer_secret")
        ns_token_key = self.config.get("ns_token_key")
        ns_token_secret = self.config.get("ns_token_secret")
        is_sandbox = self.config.get("is_sandbox")

        self.logger.info(f"Starting netsuite connection")
        ns = NetSuite(
            ns_account=ns_account,
            ns_consumer_key=ns_consumer_key,
            ns_consumer_secret=ns_consumer_secret,
            ns_token_key=ns_token_key,
            ns_token_secret=ns_token_secret,
            is_sandbox=is_sandbox,
        )

        ns.connect_tba(caching=False)
        self.ns_client = ns.ns_client
        self.logger.info(f"Successfully created netsuite connection..")

    def get_reference_data(self):
        if self.config.get("snapshot_hours"):
            try:
                with open(f'{self.config.get("snapshot_dir", "snapshots")}/reference_data.json') as json_file:
                    reference_data = json.load(json_file)
                    if reference_data.get("write_date"):
                        last_run = parse(reference_data["write_date"])
                        last_run = last_run.replace(tzinfo=None)
                        if (datetime.utcnow()-last_run).total_hours()<int(self.config.get("snapshot_hours")):
                            return reference_data
            except:
                self.logger.info(f"Snapshot not found or not readable.")

        self.logger.info(f"Reading data from API...")
        reference_data = {}
        reference_data["Vendors"] = self.ns_client.entities["Vendors"].get_all(["entityId", "companyName"])
        reference_data["Subsidiaries"] = self.ns_client.entities["Subsidiaries"].get_all(["name"])
        reference_data["Classifications"] = self.ns_client.entities["Classifications"].get_all(["name"])
        reference_data["Items"] = self.ns_client.entities["Items"].get_all(["itemId"])
        reference_data["Currencies"] = self.ns_client.entities["Currencies"].get_all()
        reference_data["Departments"] = self.ns_client.entities["Departments"].get_all(["name"])
        reference_data["Customer"] = self.ns_client.entities["Customer"].get_all(["name", "companyName", "entityId"])
        try:
            reference_data["Locations"] = self.ns_client.entities["Locations"].get_all(["name"])
        except NetSuiteRequestError as e:
            message = e.message.replace("error", "failure").replace("Error", "")
            self.logger.warning(f"It was not possible to retrieve Locations data: {message}")
        reference_data["Accounts"] = self.ns_client.entities["Accounts"](self.ns_client.ns_client).get_all(["acctName", "acctNumber", "subsidiaryList"])

        if self.config.get("snapshot_hours"):
            reference_data["write_date"] = datetime.utcnow().isoformat()
            os.makedirs("snapshots", exist_ok=True)
            with open('snapshots/reference_data.json', 'w') as outfile:
                json.dump(reference_data, outfile)

        return reference_data

    def process_journal_entry(self, context, record):
        subsidiaries = {}
        line_items = []
        for line in record.get("journalLines", record.get("lines", [])):
            journal_entry_line = dict()

            if context["reference_data"].get("Accounts"):
                acct_data = None
                if line.get("accountId"):
                    acct_data = [a for a in context["reference_data"]["Accounts"] if a["internalId"] == line["accountId"]]
                
                elif line.get("accountNumber") and not line.get("accountId"):
                    acct_num = str(line["accountNumber"])
                    acct_data = [a for a in context["reference_data"]["Accounts"] if a["acctNumber"] == acct_num]
                
                if not acct_data:
                    raise Exception(f"AccountId '{line.get('accountId')}' and/or accountNumber {line.get('accountNumber')} were not provided or not valid.")

                acct_data = acct_data[0]
                ref_acct = {
                    "name": acct_data.get("acctName"),
                    "externalId": acct_data.get("externalId"),
                    "internalId": acct_data.get("internalId"),
                }
                journal_entry_line = {"account": ref_acct}

                # Extract the subsidiaries from Account
                if line.get("subsidiary"):
                    subsidiary = dict(name=None, internalId=line.get("subsidiary"), externalId=None, type=None)
                else:
                    subsidiary = acct_data['subsidiaryList']
                    if subsidiary:
                        subsidiary = subsidiary[0]
                if subsidiary:
                    if line["postingType"].lower() == "credit":
                        subsidiaries["toSubsidiary"] = subsidiary
                    elif line["postingType"].lower() == "debit":
                        subsidiaries["subsidiary"] = subsidiary
                    else:
                        raise('Posting Type must be "credit" or "debit"')
            else:
                raise Exception("We failed to fetch Accounts from NetSuite. Please validate permissions.")

            # Get the NetSuite Class Ref
            if context["reference_data"].get("Classifications") and line.get("className"):
                class_names = [c["name"] for c in context["reference_data"]["Classifications"]]
                class_name = self.get_close_matches(line["className"], class_names)
                if class_name:
                    class_name = max(class_name, key=class_name.get)
                    class_data = [c for c in context["reference_data"]["Classifications"] if c["name"]==class_name]
                    if class_data:
                        class_data = class_data[0]
                        journal_entry_line["class"] = {
                            "name": class_data.get("name"),
                            "externalId": class_data.get("externalId"),
                            "internalId": class_data.get("internalId"),
                        }

            # Get the NetSuite Department Ref
            if context["reference_data"].get("Departments") and line.get("department"):
                dept_names = [d["name"] for d in context["reference_data"]["Departments"]]
                dept_name = self.get_close_matches(line["department"], dept_names)
                if dept_name:
                    dept_name = max(dept_name, key=dept_name.get)
                    dept_data = [d for d in context["reference_data"]["Departments"] if d["name"] == dept_name]
                    if dept_data:
                        dept_data = dept_data[0]
                        journal_entry_line["department"] = {
                            "name": dept_data.get("name"),
                            "externalId": dept_data.get("externalId"),
                            "internalId": dept_data.get("internalId"),
                        }

            # Get the NetSuite Location Ref
            if line.get("locationId"):
                journal_entry_line["location"] = {"internalId": line.get("locationId")}
            elif context["reference_data"].get("Locations") and line.get("location"):
                loc_data = [l for l in context["reference_data"]["Locations"] if l["name"] == line["location"]]
                if loc_data:
                    loc_data = loc_data[0]
                    journal_entry_line["location"] = {
                        "name": loc_data.get("name"),
                        "externalId": loc_data.get("externalId"),
                        "internalId": loc_data.get("internalId"),
                    }

            # Get the NetSuite Customer Ref
            if context["reference_data"].get("Customer"):
                customer_data = []
                if line.get("customerId"):
                    customer_data = [c for c in context["reference_data"]["Customer"] if c["internalId"] == line["customerId"]]
                if line.get("customerName") and not customer_data:
                    # look customer by entityId
                    customer_data = [c for c in context["reference_data"]["Customer"] if c["entityId"] == line["customerName"]]
                    # look for equal or similar customer name
                    if not customer_data:
                        customer_names = []
                        for c in context["reference_data"]["Customer"]:
                            if "name" in c.keys():
                                if c["name"]:
                                    customer_names.append(c["name"])
                            else:
                                if c["companyName"]:
                                    customer_names.append(c["companyName"])
                        customer_name = self.get_close_matches(line["customerName"], customer_names, n=2, cutoff=0.95)
                        if customer_name:
                            customer_name = max(customer_name, key=customer_name.get)
                            customer_data = []
                            for c in context["reference_data"]["Customer"]:
                                if "name" in c.keys():
                                    if c["name"] == customer_name:
                                        customer_data.append(c)
                                else:
                                    if c["companyName"] == customer_name:
                                        customer_data.append(c)
                    
                if customer_data:
                    customer_data = customer_data[0]
                    journal_entry_line["entity"] = {
                        "externalId": customer_data.get("externalId"),
                        "internalId": customer_data.get("internalId"),
                    }

            # Check the Posting Type and insert the Amount
            amount = 0 if not line["amount"] else abs(round(line["amount"], 2))
            if line["postingType"].lower() == "credit":
                journal_entry_line["credit"] = amount
            elif line["postingType"].lower() == "debit":
                journal_entry_line["debit"] = amount

            # Insert the Journal Entry to the memo field
            if "description" in line.keys():
                journal_entry_line["memo"] = line["description"]

            # Add support for custom values
            custom_field_values = []

            if line.get("asset"):
                custom_field_values.append({"type": "Select", "scriptId": "custcol_far_trn_relatedasset", "value": line["asset"]})

            # Support dynamic custom fields
            custom_fields = line.get("customFields") or []

            if type(custom_fields) == str:
                custom_fields = json.loads(custom_fields)

            if type(custom_fields) != list:
                raise Exception(f"Invalid customFields. Expecting a list of name/value pairs. Received: {custom_fields}")

            for entry in custom_fields:
                value = entry.get("value")
                ns_id = entry.get("name")
                if value:
                    custom_field_values.append({"type": "Select", "scriptId": ns_id, "value": value})

            if custom_field_values:
                journal_entry_line["customFieldList"] = custom_field_values
            
            line_items.append(journal_entry_line)

        # Get the currency ID
        if record.get("currency") and not context["reference_data"].get("Currencies"):
            raise Exception("A currency was provided in the payload, but we failed to fetch Currencies from NetSuite. Please validate permissions.")

        if context["reference_data"].get("Currencies") and record.get("currency"):
            currency_data = [
                c for c in context["reference_data"]["Currencies"] if c["symbol"] == record["currency"]
                ]
            if currency_data:
                currency_data = currency_data[0]
                currency_ref = {
                    "name": currency_data.get("symbol"),
                    "externalId": currency_data.get("externalId"),
                    "internalId": currency_data.get("internalId"),
                }
        else:
            currency_ref = None

        # Check if subsidiary is duplicated and delete toSubsidiary if true
        subsidiary = None
        if record.get("subsidiary"):
            subsidiary = {"internalId": record["subsidiary"]}
        elif len(subsidiaries)>1:
            if subsidiaries['subsidiary'] == subsidiaries['toSubsidiary']:
                subsidiary = subsidiaries['subsidiary']

        if "transactionDate" in record.keys():
            created_date = parse(record["transactionDate"])
        else:
            created_date = None

        # Create the journal entry
        journal_entry = {
            "createdDate": created_date,
            "tranDate": created_date,
            "lineList": line_items,
            "currency": currency_ref,
            "subsidiary": subsidiary
        }

        if record.get("id"):
            journal_entry["externalId"] = record["id"]
        else:
            raise Exception(f"Invalid Journal Entry: id is a required field. {record}")

        if "journalDesc" in record.keys():
            journal_entry["memo"] = "" if not record["journalDesc"] else record["journalDesc"]
        
        # Support dynamic custom fields
        record_custom_fields = []
        custom_fields = record.get("customFields") or []

        if type(custom_fields) == str:
            custom_fields = json.loads(custom_fields)

        if type(custom_fields) != list:
            raise Exception(f"Invalid customFields. Expecting a list of name/value pairs. Received: {custom_fields}")

        for entry in custom_fields:
            value = entry.get("value")
            ns_id = entry.get("name")
            if value:
                record_custom_fields.append({"type": "Select", "scriptId": ns_id, "value": value})

        if record_custom_fields:
            journal_entry["customFieldList"] = record_custom_fields

        return journal_entry

    def process_customer_payment(self, context, record):
        # Get the currency ID
        if context["reference_data"].get("Currencies") and record.get("currency"):
            currency_data = [
                c for c in context["reference_data"]["Currencies"] if c["symbol"] == record["currency"]
                ]
            if currency_data:
                currency_data = currency_data[0]
                currency_ref = {
                    "name": currency_data.get("symbol"),
                    "externalId": currency_data.get("externalId"),
                    "internalId": currency_data.get("internalId"),
                }
        else:
            currency_ref = None

        if "transactionDate" in record.keys():
            created_date = parse(record["transactionDate"])
        else:
            created_date = None

        # Create the journal entry
        journal_entry = {
            "createdDate": created_date,
            "tranDate": created_date,
            "currency": currency_ref
        }

        if record.get("id"):
            journal_entry["externalId"] = record["id"]

        return journal_entry


    def process_inbound_shipment(self, context, record):
        inbound_shipment = record
        inbound_shipment["internalId"] = record["id"]

        return inbound_shipment