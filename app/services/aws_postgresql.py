import psycopg2
from psycopg2 import pool
import configparser
import logging

import pandas as pd

# Load the configuration file
config = configparser.ConfigParser()
config.read('/Users/HuJ/Documents/Mail2Ledger/.env')

class AWSPostgresGateway:
    def __init__(self):
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            1, 20,  # minimum and maximum connections in the pool
            host=config['AWS_DB']['HOST'],
            port=config['AWS_DB']['PORT'],
            database=config['AWS_DB']['DATABASE'],
            user=config['AWS_DB']['USER'],
            password=config['AWS_DB']['PASSWORD'],
            sslmode=config.get('AWS_DB', 'SSL_MODE', fallback='disable')
        )
        self.conn = self.connection_pool.getconn()
        self.cursor = self.conn.cursor()

    def execute_query(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            self.conn.commit()
            return self.cursor.fetchall()
        except Exception as e:
            logging.error(f"??????????Error executing query: {query} with params: {params} - {e}")
            return None

    def execute_update(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error executing update: {query} with params: {params} - {e}")
    
    def query_df(self, sql: str, params=None) -> pd.DataFrame:
        try:
            self.cursor.execute(sql, params)
            cols = [desc[0] for desc in self.cursor.description]   # 取得欄位名
            data = self.cursor.fetchall()
            return pd.DataFrame(data, columns=cols)
        except Exception as e:
            logging.error(f"Error executing query_df: {sql} - {e}")
            return pd.DataFrame()      # 回傳空 DF 讓後續程式不中斷

    def close_connection(self):
        """
        Close the database connection and cursor.

        This method is typically called when the object is no longer needed,
        to free up the database connection and cursor.
        """

        self.cursor.close()
        self.conn.close()
