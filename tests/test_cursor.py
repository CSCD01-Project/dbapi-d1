import unittest
from unittest.mock import patch

import requests

import dbapi_d1
import dbapi_d1.cursor as cursor_mod
from dbapi_d1 import DatabaseError

class DummySuccessResponse:
    status_code = 200

    def raise_for_status(self):
        pass

    def json(self):
        return {
            "success": True,
            "result": [
                {
                    "results": [
                        {"id": 1, "name": "Alice", "value": 42},
                        {"id": 2, "name": "Bob", "value": 55},
                    ]
                }
            ],
        }
    
class DummyEmptyResponse:
    status_code = 200

    def raise_for_status(self):
        pass

    def json(self):
        return {
            "success": True,
            "result": [],
        }
    
class DummyErrorJSONResponse:
    status_code = 400

    def raise_for_status(self):
        raise requests.exceptions.HTTPError("Bad Request")

    def json(self):
        return {
            "success": False,
            "errors": [
                {
                    "message": "Syntax error near 'FROM'",
                    "source": {"pointer": "/sql"},
                    "code": 1001,
                }
            ],
        }
    
def make_conn():
    return dbapi_d1.Connection("account123", "token_abc", "db_xyz")

class CursorTestSuite(unittest.TestCase):
    
    @patch("dbapi_d1.cursor.requests.post")
    def test_execute_and_fetchall(self, post_mock):
        post_mock.return_value = DummySuccessResponse()

        conn = make_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, name, value FROM test_table WHERE value > ?", [10])
        result = cur.fetchall()

        expected = [
            (1, "Alice", 42),
            (2, "Bob", 55),
        ]
        self.assertEqual(result, expected)

        col_names = [d[0] for d in cur.description]
        self.assertEqual(col_names, ["id", "name", "value"])
        self.assertTrue(all(len(d) == 7 for d in cur.description))

    @patch("dbapi_d1.cursor.requests.post")
    def test_execute_empty_result(self, post_mock):
        post_mock.return_value = DummyEmptyResponse()

        conn = make_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM empty_table")
        result = cur.fetchall()
        expected=[]

        self.assertEqual(result, expected)
        self.assertEqual(cur.description, [])
    
    @patch("dbapi_d1.cursor.requests.post")
    def test_fetchone_and_fetchmany(self, post_mock):
        post_mock.return_value = DummySuccessResponse()

        conn = make_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, name, value FROM test_table", [])

        first = cur.fetchone()
        self.assertEqual(first, (1, "Alice", 42))

        next_batch = cur.fetchmany(1)
        self.assertEqual(next_batch, [(2, "Bob", 55)])

        self.assertIsNone(cur.fetchone())
        self.assertEqual(cur.fetchall(), [])
    
    @patch("dbapi_d1.cursor.requests.post")
    def test_execute_builds_correct_http_request(self, post_mock):
        post_mock.return_value = DummyEmptyResponse()
        captured = {}

        def fake_post(url, json, headers):
            captured["url"] = url
            captured["json"] = json
            captured["headers"] = headers
            return DummyEmptyResponse()

        post_mock.side_effect = fake_post

        conn = dbapi_d1.Connection("acct_999", "token_secret", "db_123")
        cur = conn.cursor()

        cur.execute("SELECT * FROM test_table WHERE id = ?", [123])

        expected_url = (
            "https://api.cloudflare.com/client/v4/accounts/"
            "acct_999/d1/database/db_123/query"
        )
        self.assertEqual(captured["url"], expected_url)
        self.assertEqual(
            captured["json"],
            {"sql": "SELECT * FROM test_table WHERE id = ?", "params": [123]},
        )
        self.assertEqual(captured["headers"]["Authorization"], "Bearer token_secret")
        self.assertEqual(captured["headers"]["Content-Type"], "application/json")
    
    @patch("dbapi_d1.cursor.requests.post")
    def test_http_error_with_json_errors(self, post_mock):
        post_mock.return_value = DummyErrorJSONResponse()

        conn = make_conn()
        cur = conn.cursor()

        with self.assertRaises(DatabaseError) as cm:
            cur.execute("SELECT * FROM broken table")

        msg = str(cm.exception)
        self.assertIn("(HTTP 400)", msg)
        self.assertIn("Syntax error near 'FROM'", msg)
        self.assertIn("pointer: /sql", msg)
        self.assertIn("code: 1001", msg)

    @patch("dbapi_d1.cursor.requests.post")
    def test_network_error(self, post_mock):
        def fail(*args, **kwargs):
            raise requests.exceptions.RequestException("network down")

        post_mock.side_effect = fail

        conn = make_conn()
        cur = conn.cursor()

        with self.assertRaises(DatabaseError) as cm:
            cur.execute("SELECT 1")

        self.assertIn("Request failed: network down", str(cm.exception))

if __name__ == "__main__":
    unittest.main()
