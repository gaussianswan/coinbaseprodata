# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 19:12:11 2022

@author: Stephon Henry-Rerrie

Here, we are loading in the BTC-USD parquet file and then dumping this information into a database table
"""

from postgresconnector import PostgresConnection
from dotenv import load_dotenv
import os 
import pandas as pd 

load_dotenv() 

filename = 'BTC-USD_minute_data.parquet'
btc_data = pd.read_parquet(filename)
btc_data['exchange'] = 'coinbase' 
btc_data['product'] = 'BTC-USD'

host = os.environ['DB_HOST']
password = os.environ['DB_PASSWORD']
username = os.environ['DB_USERNAME']
port = os.environ['DB_PORT']
database = os.environ['DB_DATABASE']
connection = PostgresConnection(username, password, host, database, port)

connection.update_table_with_df(btc_data, 'coinbaseminutepricingdata', if_exists = 'replace')





