from .exceptions import DatabaseError


class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.results = []

    def execute(self, sql, params=None):
        # TODO: implement actual query execution using D1 client
        print(f"Executing SQL: {sql} with params {params}")
        self.results = [{"example": 1}]  # dummy result

    def fetchone(self):
        if not self.results:
            return None
        return self.results.pop(0)

    def fetchall(self):
        results = self.results[:]
        self.results = []
        return results

    def close(self):
        pass
