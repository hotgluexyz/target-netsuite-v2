"""netsuite-v2 target sink class, which handles writing streams."""

from dateutil.parser import parse
from singer_sdk.sinks import BatchSink
from target_netsuite_v2.netsuite import NetSuite

from netsuitesdk.internal.exceptions import NetSuiteRequestError

from difflib import SequenceMatcher
from heapq import nlargest as _nlargest
import json


class netsuiteV2Sink(BatchSink):
    """netsuite-v2 target sink class."""

    max_size = 100  # Max records to write in one batch

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
        self.logger.info(f"Readding data from API...")
        reference_data = {}
        reference_data["Classifications"] = self.ns_client.entities["Classifications"].get_all(["name"])
        reference_data["Currencies"] = self.ns_client.entities["Currencies"].get_all()
        reference_data["Departments"] = self.ns_client.entities["Departments"].get_all(["name"])
        reference_data["Customer"] = self.ns_client.entities["Customer"].get_all(["name", "companyName"])
        try:
            reference_data["Locations"] = self.ns_client.entities["Locations"].get_all(["name"])
        except NetSuiteRequestError as e:
            message = e.message.replace("error", "failure").replace("Error", "")
            self.logger.warning(f"It was not possible to retrieve Locations data: {message}")
        reference_data["Accounts"] = self.ns_client.entities["Accounts"].get_all(["acctName", "acctNumber", "subsidiaryList"])

        return reference_data

    def start_batch(self, context: dict) -> None:
        """Start a batch."""

        self.get_ns_client()
        context["reference_data"] = self.get_reference_data()
        context["records"] = []

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        subsidiaries = {}
        line_items = []
        for line in record.get("lines"):
            if context["reference_data"].get("Accounts") and line.get("accountNumber"):
                acct_num = str(line["accountNumber"])
                acct_data = [a for a in context["reference_data"]["Accounts"] if a["acctNumber"] == acct_num]
                if not acct_data:
                    self.logger.warning(f"{acct_num} is not valid for this netsuite account, skipping line")
                    continue
                acct_data = acct_data[0]
                ref_acct = {
                    "name": acct_data.get("acctName"),
                    "externalId": acct_data.get("externalId"),
                    "internalId": acct_data.get("internalId"),
                }
                journal_entry_line = {"account": ref_acct}

                # Extract the subsidiaries from Account
                if line.get("Subsidiary"):
                    subsidiary = dict(name=None, internalId=line.get("subsidiary"), externalId=None, type=None)
                else:
                    subsidiary = acct_data['subsidiaryList']
                if subsidiary:
                    subsidiary = subsidiary[0]
                    if line["postingType"].lower() == "credit":
                        subsidiaries["toSubsidiary"] = subsidiary
                    elif line["postingType"].lower() == "debit":
                        subsidiaries["subsidiary"] = subsidiary
                    else:
                        raise('Posting Type must be "credit" or "debit"')

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
            if context["reference_data"].get("Locations") and line.get("location"):
                loc_data = [l for l in context["reference_data"]["Locations"] if l["name"] == line["location"]]
                if loc_data:
                    loc_data = loc_data[0]
                    journal_entry_line["location"] = {
                        "name": loc_data.get("name"),
                        "externalId": loc_data.get("externalId"),
                        "internalId": loc_data.get("internalId"),
                    }

            # Get the NetSuite Location Ref
            if context["reference_data"].get("Customer") and line.get("customerName"):
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
            if "Description" in line.keys():
                journal_entry_line["memo"] = line["Description"]
            
            line_items.append(journal_entry_line)

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

        # Check if subsidiary is duplicated and delete toSubsidiary if true
        if len(subsidiaries)>1:
            if subsidiaries['subsidiary'] == subsidiaries['toSubsidiary']:
                del subsidiaries['toSubsidiary']

        if "transactionDate" in record.keys():
            created_date = parse(record["transactionDate"])
        else:
            created_date = None

        # Create the journal entry
        journal_entry = {
            "createdDate": created_date,
            "tranDate": created_date,
            "externalId": record["id"],
            "lineList": line_items,
            "currency": currency_ref
        }

        if "journalDesc" in record.keys():
            journal_entry["memo"] = "" if not record["JournalDesc"] else record["JournalDesc"]
        
        # Update the entry with subsidiaries
        journal_entry.update(subsidiaries)

        context["records"].append(journal_entry)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        entity = "JournalEntry"
        self.logger.info(f"Posting data for entity {entity}")
        for journal in context["records"]:
            response = self.ns_client.entities[entity].post(journal)
            self.logger.info(response)
