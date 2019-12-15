from unittest import TestCase
from unittest.mock import MagicMock
from pandabox.db.queries import DbQueries
from pandabox.tools import PandaBox, Panda
from pandas import DataFrame
from pathlib import Path
from typing import Callable


class TestDbQueries(TestCase):
    """
    Unit Tests for the DbQueries Class
    """
    def setUp(self) -> None:
        self.mock_db_conn = MagicMock()
        self.query_path = Path(__file__).parent / "test_queries"
        self.db_queries = DbQueries(self.query_path, self.mock_db_conn)

    def test_methods_exist(self):
        fetch_table = getattr(self.db_queries, "fetch_tables", None)
        fetch_schema = getattr(self.db_queries, "fetch_schemas", None)
        self.assertIsInstance(fetch_table, Callable)
        self.assertIsInstance(fetch_schema, Callable)

    def test_fetch_one(self):
        panda = self.db_queries.fetch_one("tables")
        self.assertIsInstance(panda, Panda)
        self.assertIsInstance(panda.name, str)
        self.assertIsInstance(panda.data, DataFrame)
        self.assertEqual(panda.name, "tables")

    def test_raise_att_error_for_fetch_one(self):
        self.assertRaises(AttributeError, self.db_queries.fetch_one, "foo")

    def test_fetch_all(self):
        panda_box = self.db_queries.fetch_all()
        self.assertIsInstance(panda_box, PandaBox)
        self.assertEqual(2, len(panda_box))

    def test_fetch_many(self):
        panda_box = self.db_queries.fetch_many("tables", "schemas")
        self.assertIsInstance(panda_box, PandaBox)
        self.assertEqual(2, len(panda_box))

    def test_raise_att_error_for_fetch_many(self):
        self.assertRaises(AttributeError, self.db_queries.fetch_many, "tables", "foo")
