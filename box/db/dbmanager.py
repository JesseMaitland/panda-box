import psycopg2
from sqlalchemy import create_engine
from typing import Dict
from pathlib import Path
from dotenv import load_dotenv, dotenv_values
from box.exceptions import ConnectionNotExist


class DbManager:

    def __init__(self, env_path: Path = None, connection_strings: Dict = None, init_env: bool = True):
        if connection_strings is None:
            connection_strings = {}

        if env_path is None:
            env_path = Path.cwd() / ".env"

        self.env_path = env_path
        self.connection_strings = connection_strings

        if init_env:
            self.init_env()

    def init_env(self):
        load_dotenv(self.env_path)
        for key, value in dotenv_values().items():
            self.add_connection_string(key.lower(), value)

    def get_connection_string(self, name: str):
        for key, value in self.connection_strings.items():
            if name == key:
                return value

    def get_connection(self, name: str):
        for key, value in self.connection_strings.items():
            if key == name:
                return psycopg2.connect(value)
        raise ConnectionNotExist(f"database {name} connection string is not registered in the connection manager")

    def get_alchemy_engine(self, name: str):
        for key, value in self.connection_strings.items():
            if key == name:
                return create_engine(value)
        raise ConnectionNotExist(f"database {name} connection string is not registered in the connection manager")

    def add_connection_string(self, name: str, value: str):
        self.connection_strings[name] = value
