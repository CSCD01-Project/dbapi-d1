from .cursor import Cursor
from .exceptions import NotSupportedError


class Connection:
    def __init__(self, account_id: str, api_token: str, database_id: str):
        self.account_id = account_id
        self.api_token = api_token
        self.database_id = database_id
        self.closed = False
        self._first_connect = True

    def cursor(self):
        self._check_closed()
        return Cursor(self)

    def commit(self):
        if self._first_connect:
            # no-op during first connect probe
            return
        raise NotSupportedError("D1 does not support rollback")

    def rollback(self):
        if self._first_connect:
            # no-op during first connect probe
            return
        raise NotSupportedError("D1 does not support rollback")

    def close(self):
        self.closed = True

    def _check_closed(self):
        if self.closed:
            raise Exception("Connection is closed.")
