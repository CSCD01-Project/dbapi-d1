import requests
from .exceptions import DatabaseError, OperationalError


class Cursor:
    """
    Fully PEP 249-compliant cursor for Cloudflare D1.
    """

    def __init__(self, connection):
        self.connection = connection
        self._last_result = []
        self.description = None
        self._row_index = 0
        self.closed = False

    def execute(self, sql: str, params=None):
        self._check_closed()
        payload = {"sql": sql, "params": params or []}
        url = f"https://api.cloudflare.com/client/v4/accounts/{self.connection.account_id}/d1/database/{self.connection.database_id}/query"
        headers = {
            "Authorization": f"Bearer {self.connection.api_token}",
            "Content-Type": "application/json",
        }

        try:
            resp = requests.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            api_result = resp.json()
        except requests.exceptions.HTTPError as e:
            # Attempt to parse the API response JSON for detailed errors
            try:
                api_result = resp.json()
                if not api_result.get("success", True):
                    errors = api_result.get("errors", [])
                    error_messages = []
                    for err in errors:
                        msg = err.get("message", "Unknown error")
                        pointer = err.get("source", {}).get("pointer", "")
                        code = err.get("code")
                        error_messages.append(
                            f"{msg} {f'(pointer: {pointer}) ' if pointer else ''}{f'(code: {code})' if code else ''}".strip()
                        )
                    raise DatabaseError(
                        f"(HTTP {resp.status_code}) D1 API error:\n"
                        + "\n| ".join(error_messages)
                    )
            except (ValueError, UnboundLocalError):
                raise DatabaseError(f"D1 HTTP error: {str(e)}") from e
        except requests.exceptions.RequestException as e:
            raise DatabaseError(f"Request failed: {str(e)}") from e

        # Extract rows
        self._last_result = (
            api_result["result"][0]["results"] if api_result["result"] else []
        )

        # Build description for PEP 249 compliance: tuple of 7 elements
        if self._last_result:
            self.description = [
                (col, None, None, None, None, None, None)
                for col in self._last_result[0].keys()
            ]
        else:
            self.description = []

        # Convert dict rows to tuples
        self._last_result = [tuple(row.values()) for row in self._last_result]
        self._row_index = 0

    def fetchone(self):
        self._check_closed()
        if self._row_index < len(self._last_result):
            row = self._last_result[self._row_index]
            self._row_index += 1
            return row
        return None

    def fetchall(self):
        self._check_closed()
        remaining = self._last_result[self._row_index :]
        self._row_index = len(self._last_result)
        return remaining

    def fetchmany(self, size=1):
        self._check_closed()
        start = self._row_index
        end = min(self._row_index + size, len(self._last_result))
        self._row_index = end
        return self._last_result[start:end]

    def close(self):
        self.closed = True
        self._last_result = []
        self.description = None

    def _check_closed(self):
        if self.closed:
            raise Exception("Cursor is closed.")
