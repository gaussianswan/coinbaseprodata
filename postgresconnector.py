# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 19:09:50 2022

@author: Stephon Henry-Rerrie
"""

from sqlalchemy import create_engine
import pandas as pd

class PostgresConnection:

    def __init__(self, username, password, host, database, port: int = 5432) -> None:
        self.username = username
        self.__password = password
        self.database = database
        self.host = host
        self.port = port

        self.connection = create_engine(self.create_connection_string())

    def create_connection_string(self):
        string = f'postgresql://{self.username}:{self.__password}@{self.host}:{self.port}/{self.database}'

        return string

    def run_query_to_df(self, query: str) -> pd.DataFrame:

        df = pd.read_sql(sql = query, con = self.connection)
        return df

    def update_table_with_df(self, df: pd.DataFrame, table_name: str, if_exists: str, **kwargs):

        df.to_sql(name = table_name, con = self.connection, if_exists=if_exists, **kwargs)
