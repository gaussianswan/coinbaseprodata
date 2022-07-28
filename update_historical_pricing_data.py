from load_keys import load_aws_keys, load_db_keys
from postgresconnector import PostgresConnection
from historicaldatahelper import CBProHistDataRetriever

import pandas as pd
import datetime

dbkeys = load_db_keys()

connection = PostgresConnection(*dbkeys)
data_helper = CBProHistDataRetriever()

query = "SELECT product, MAX(time) last_updated FROM coinbasepricingdata GROUP BY product"
last_updated = connection.run_query_to_df(query)
dataframes = {}

for row in last_updated.itertuples():
    product = row.product
    start_time = row.last_updated
    end = pd.Timestamp(datetime.datetime.now())

    df = data_helper.get_historical_prices(product=product, start_date=start_time, end_date=end, granularity=60)
    df = df.iloc[1:]
    df['exchange'] = 'coinbase'
    df['product'] = product
    dataframes[product] = df

# Then we can go through and update the table with this data
for dataframe in dataframes.values():
    connection.update_table_with_df(dataframe, 'coinbasepricingdata', if_exists='append', index = False)