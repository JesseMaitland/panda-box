import psycopg2
from sqlalchemy import create_engine
from typing import Dict
from pathlib import Path
from dotenv import dotenv_values
from pandabox.exceptions import ConnectionNotExist


class DbManager:

    def __init__(self,
                 env_path: Path = None,
                 connection_strings: Dict = None,
                 init_env: bool = True,
                 use_prefix: bool = True,
                 prefix: str = None):

        if connection_strings is None:
            connection_strings = {}

        if env_path is None:
            env_path = Path.cwd() / ".env"

        # only use the default prefix is none is provided
        if use_prefix:
            if prefix is None:
                prefix = "pandabox+"
        else:
            prefix = ""

        self.env_path = env_path
        self.connection_strings = connection_strings
        self.prefix = prefix

        self._ex_msg = "database {} connection string is not registered in the connection manager"

        if init_env:
            self.init_env()

    def init_env(self):
        for key, value in dotenv_values(dotenv_path=self.env_path).items():
            if value.startswith(self.prefix):
                self.add_connection_string(key.lower(), value.lstrip(self.prefix))

    def get_connection_string(self, name: str):
        for key, value in self.connection_strings.items():
            if name == key:
                return value
        raise ConnectionNotExist(self._ex_msg.format(name))

    def get_connection(self, name: str):
        for key, value in self.connection_strings.items():
            if key == name:
                return psycopg2.connect(value)
        raise ConnectionNotExist(self._ex_msg.format(name))

    def get_alchemy_engine(self, name: str):
        for key, value in self.connection_strings.items():
            if key == name:
                return create_engine(value)
        raise ConnectionNotExist(self._ex_msg.format(name))

    def add_connection_string(self, name: str, value: str):
        self.connection_strings[name] = value
