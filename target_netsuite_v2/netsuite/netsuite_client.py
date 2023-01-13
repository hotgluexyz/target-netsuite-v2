from netsuitesdk.internal.client import NetSuiteClient


class ExtendedNetSuiteClient(NetSuiteClient):
    def __init__(self, account=None, caching=True, caching_timeout=2592000):
        NetSuiteClient.__init__(self, account, caching, caching_timeout)
        self._search_preferences = self.SearchPreferences(
            bodyFieldsOnly=True,
            pageSize=1000,
            returnSearchColumns=True
        )

    def update(self, record):

        response = self.request('update', record=record)
        response = response.body.writeResponse
        status = response.status
        if status.isSuccess:
            record_ref = response['baseRef']
            self.logger.debug(
                'Successfully updated record of internalId: {internalId}, externalId: {externalId}, response: {recordRef}'.format(
                     internalId=record_ref['internalId'], externalId=record_ref['externalId'], recordRef=record_ref))
            return record_ref
        else:
            exc = self._request_error('update', detail=status['statusDetail'][0])
            raise exc
