"""DB-API driver for Cloudflare D1"""

from .connection import Connection
from .exceptions import (
    Error,
    DatabaseError,
    ProgrammingError,
    InterfaceError,
    NotSupportedError,
)

__all__ = [
    "Connection",
    "Error",
    "DatabaseError",
    "ProgrammingError",
    "InterfaceError",
    "NotSupportedError",
]


def connect(account_id, api_token, database_id):
    return Connection(account_id, api_token, database_id)


apilevel = "2.0"
threadsafety = 1  # Threads may share the module, but not connections
paramstyle = "qmark"  # Use ? placeholders in SQL
