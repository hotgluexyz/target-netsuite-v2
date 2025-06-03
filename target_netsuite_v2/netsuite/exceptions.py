# pylint: disable=super-init-not-called

class TapNetSuiteException(Exception):
    pass

class TapNetSuiteQuotaExceededException(TapNetSuiteException):
    pass

class AccountDocumentPermissionError(Exception):
    pass