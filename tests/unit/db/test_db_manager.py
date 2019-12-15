from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch
from pandabox.db.dbmanager import DbManager
from pandabox.exceptions import ConnectionNotExist

test_env_path = Path(__file__).parent.parent / "test.env"


class TestDbManagerDefault(TestCase):

    def setUp(self) -> None:
        self.dbm = DbManager(env_path=test_env_path)

    def test_conn_with_prefix_exists(self):
        expected_key = "conn_with_prefix"
        found_key = None

        for key, value in self.dbm.connection_strings.items():
            print(key)
            if key == expected_key:
                found_key = key
        self.assertEqual(expected_key, found_key)

    def test_conn_without_prefix_raises_exception(self):
        self.assertRaises(ConnectionNotExist, self.dbm.get_connection, "conn_without_prefix")

    def test_conn_with_custom_prefix_raises_exception(self):
        self.assertRaises(ConnectionNotExist, self.dbm.get_connection, "conn_with_cust_prefix")

    def test_connection_string_value(self):
        conn_str = self.dbm.get_connection_string("conn_with_prefix")
        self.assertEqual(conn_str, "this-is-my-connection-string")
        self.assertFalse(conn_str.startswith("pandabox+"))

    @patch("pandabox.db.dbmanager.psycopg2")
    def test_connection_called(self, psy2_mock):
        _ = self.dbm.get_connection("conn_with_prefix")
        psy2_mock.connect.assert_called_once()
        psy2_mock.connect.assert_called_with("this-is-my-connection-string")

    @patch("pandabox.db.dbmanager.create_engine")
    def test_alchemy_engine_called(self, create_engine_mock):
        _ = self.dbm.get_alchemy_engine("conn_with_prefix")
        create_engine_mock.assert_called_once()
        create_engine_mock.assert_called_with("this-is-my-connection-string")


class TestDbManagerNoPrefix(TestCase):
    """Test No DbManager Connection String Prefix"""
    def setUp(self) -> None:
        self.dbm = DbManager(env_path=test_env_path,
                             use_prefix=False)

    def test_all_keys_exist(self):
        # when no prefix is specified, then all keys in .env should be loaded regardless of form
        expected_keys = ["conn_with_prefix", "conn_without_prefix", "conn_with_cust_prefix"]
        actual_keys = list(self.dbm.connection_strings.keys())

        expected_keys.sort()
        actual_keys.sort()

        self.assertEqual(expected_keys, actual_keys)


class TestDbManagerCustomPrefix(TestCase):
    """Test Custom DbManager Connection String Prefix"""
    def setUp(self) -> None:
        self.dbm = DbManager(env_path=test_env_path,
                             use_prefix=True,
                             prefix="foobar+")

    def test_custom_key_exists(self):
        expected_key = "conn_with_cust_prefix"
        found_key = None

        for key, value in self.dbm.connection_strings.items():
            print(key)
            if key == expected_key:
                found_key = key
        self.assertEqual(expected_key, found_key)

    def test_other_keys_do_not_exist(self):
        con_keys = list(self.dbm.connection_strings.keys())
        self.assertEqual(1, len(con_keys))
