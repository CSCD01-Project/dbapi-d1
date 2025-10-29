class Error(Exception):
    pass


class DatabaseError(Error):
    pass


class OperationalError(Error):
    pass


class ProgrammingError(Error):
    pass


class InterfaceError(Error):
    pass


class NotSupportedError(Error):
    pass
