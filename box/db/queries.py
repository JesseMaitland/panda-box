import pandas as pd
from typing import Callable, List
from psycopg2.extensions import connection
from pathlib import Path
from threading import Thread
from queue import Queue
from box.tools import Panda, PandaBox
from time import sleep

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
            return Panda(name=name, data=data)
        return get_data

    def fetch_one(self, name: str) -> Panda:
        name = f"fetch_{name}"
        method = getattr(self, name, None)
        return method()

    @staticmethod
    def _thread_wrapper(method: Callable, queue: Queue) -> None:
        data = method()
        queue.put(data)

    def fetch_many(self, *names) -> PandaBox:
        queue = Queue()
        methods = [getattr(self, f"fetch_{name}") for name in names]
        threads = []
        results = []

        for method in methods:
            thread = Thread(target=self._thread_wrapper, args=(method, queue))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        results = [queue.get() for thread in threads]
        return PandaBox(*results)

    def fetch_all(self):
        return self.fetch_many(*self.query_names)
