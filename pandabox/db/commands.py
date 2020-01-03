from typing import Callable
from psycopg2.extensions import connection
from pathlib import Path
from jinja2 import Template
from threading import Thread


class DbCommand:
    """

    """

    def __init__(self, query_path: Path, db_connection: connection, **params):

        self.params = params
        self.query_path = query_path
        self.db_connection = db_connection
        self.query_names = []

        queries = [q for q in query_path.absolute().glob('*.sql')]

        for q in queries:
            name = q.name.split('.')[0]
            templated_query = self._render_sql_template(q.read_text())
            method = self._get_data_method_factory(query=templated_query)
            setattr(self, f"exec_{name}", method)
            setattr(self, f"{name}_query", templated_query)
            self.query_names.append(name)

    def _render_sql_template(self, query: str) -> str:
        template = Template(source=query,
                            line_comment_prefix='--',
                            autoescape=True)
        return template.render(params=self.params)

    def _get_data_method_factory(self, query: str) -> Callable:
        def exe_query():
            cursor = self.db_connection.cursor()
            try:
                cursor.execute(query)
                self.db_connection.commit()
            except Exception:
                self.db_connection.rollback()
                raise
        return exe_query

    @staticmethod
    def _thread_wrapper(method: Callable) -> None:
        method()

    def execute_one(self, name: str):
        name = f"exec_{name}"
        method = getattr(self, name)
        method()

    def execute_many(self, *names):
        """

        """
        methods = [getattr(self, f"exec_{name}") for name in names]
        threads = []

        for method in methods:
            thread = Thread(target=self._thread_wrapper, args=(method,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    def execute_all(self):
        self.execute_many(*self.query_names)

    def print_query(self, name: str) -> None:
        for query_name in self.query_names:
            if query_name == name:
                query = getattr(self, f"{query_name}_query")
                print(query)
                break

    def get_query(self, name: str) -> str:
        for query_name in self.query_names:
            if query_name == name:
                return getattr(self, f"{query_name}_query")
