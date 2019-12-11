import pandas as pd
from typing import Callable, List
from psycopg2.extensions import connection
from pathlib import Path
from threading import Thread
from queue import Queue


class Data:

    def __init__(self, name: str, data: pd.DataFrame):
        self.name = name
        self.data = data


class DbQueries:

    def __init__(self, query_path: Path, db_connection: connection):
        self.query_path = query_path
        self.db_connection = db_connection
        self.query_names = []

        queries = [q for q in query_path.absolute().glob('*.sql')]
        for q in queries:
            name = q.name.split('.')[0]
            method = self._get_data_method_factory(name=name, query=q.read_text())
            setattr(self, f"fetch_{name}", method)
            self.query_names.append(name)

    def _get_data_method_factory(self, name: str, query: str) -> Callable:
        def get_data():
            data = pd.read_sql(query, self.db_connection, parse_dates=True)
            return Data(name=name, data=data)

        return get_data

    def fetch_one(self, name: str) -> Data:
        name = f"fetch_{name}"
        method = getattr(self, name, None)
        return method()

    @staticmethod
    def _thread_wrapper(method: Callable, queue: Queue) -> None:
        data = method()
        queue.put(data)

    def fetch_many(self, *query_names) -> List[Data]:
        queue = Queue()
        methods = [getattr(self, f"fetch_{name}") for name in query_names]
        threads = []
        results = []

        for method in methods:
            thread = Thread(target=self._thread_wrapper, args=(method, queue))
            thread.start()
            threads.append(thread)

        while not queue.empty():
            result = queue.get()
            results.append(result)

        queue.join()
        for thread in threads:
            thread.join()
        return results

    def fetch_all(self):
        return self.fetch_many(*self.query_names)
