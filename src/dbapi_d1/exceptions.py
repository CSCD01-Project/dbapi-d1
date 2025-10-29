class DatabaseError(Exception):
    pass


class OperationalError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass
