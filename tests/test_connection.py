import unittest

import dbapi_d1
from dbapi_d1 import Connection

class ConnectionTestSuite(unittest.TestCase):
    def test_cursor_returns_new_cursor(self):
        conn = Connection("acct", "token", "db")
        cur = conn.cursor()

        self.assertIs(cur.connection, conn)

    def test_cursor_raises_if_connection_closed(self):
        conn = Connection("acct", "token", "db")
        conn.close()

        with self.assertRaises(Exception) as cm:
            conn.cursor()

        self.assertIn("Connection is closed", str(cm.exception))

    def test_close_marks_connection_closed(self):
        conn = Connection("acct", "token", "db")
        self.assertFalse(conn.closed)
        conn.close()
        self.assertTrue(conn.closed)

    def test_module_level_connect_helper(self):
        conn = dbapi_d1.connect("acctX", "tokenY", "dbZ")
        self.assertIsInstance(conn, Connection)
        self.assertEqual(conn.account_id, "acctX")
        self.assertEqual(conn.api_token, "tokenY")
        self.assertEqual(conn.database_id, "dbZ")


if __name__ == "__main__":
    unittest.main()