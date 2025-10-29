from .cursor import Cursor
from .exceptions import NotSupportedError


class Connection:
    def __init__(self, dsn, **kwargs):
        self.dsn = dsn
        self.closed = False
        # TODO: initialize Cloudflare D1 client

    def cursor(self):
        return Cursor(self)

    def close(self):
        self.closed = True

    def commit(self):
        raise NotSupportedError("D1 does not support commit()")

    def rollback(self):
        raise NotSupportedError("D1 does not support rollback()")


def connect(dsn, **kwargs):
    """
    Factory function to create a new D1 connection
    """
    return Connection(dsn, **kwargs)
