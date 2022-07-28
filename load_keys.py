# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 19:22:10 2022

@author: Administrator
"""
from dotenv import load_dotenv
from collections import namedtuple
import os


def load_cbpro_api_keys(portfolio_name: str, nickname: str) -> namedtuple:
    """Loads the portfolio api keys as a named tuple

    Args:
        portfolio_name (str): Portfolio name from coinbase pro
        nickname (str): Nickname for the api key

    Returns:
        namedtuple: Named tuple holding all the api keys
    """
    load_dotenv()
    api_key = os.environ[f'{portfolio_name}_{nickname}_API_KEY']
    api_secret = os.environ[f'{portfolio_name}_{nickname}_API_SECRET']
    passphrase = os.environ[f'{portfolio_name}_{nickname}_PASSPHRASE']
    api_keys = namedtuple(typename='APIKey', field_names=['api_key', 'api_secret', 'passphrase'])

    keys = api_keys(api_key=api_key , api_secret=api_secret, passphrase=passphrase)

    return keys

def load_db_keys() -> namedtuple:
    """Loads the db credentials

    Returns:
        namedtuple: Named tuple holding all the postgres credentials for AWS from your environment
    """
    load_dotenv()
    dbcredentials = namedtuple('DBCredentials', field_names=['username', 'password', 'host', 'database', 'port'])
    host = os.environ['DB_HOST']
    password = os.environ['DB_PASSWORD']
    username = os.environ['DB_USERNAME']
    port = os.environ['DB_PORT']
    database = os.environ['DB_DATABASE']

    credentials = dbcredentials(username, password, host, database, port)

    return credentials

def load_aws_keys() -> namedtuple:

    awskeys = namedtuple("AWSKeys", field_names = ['access_key_id', 'secret_access_key'])
    load_dotenv()

    access_key_id = os.environ['AWS_ACCESS_KEYID']
    secret_access_key = os.environ['AWS_SECRET_ACCESSKEY']

    keys = awskeys(access_key_id, secret_access_key)
    return keys


