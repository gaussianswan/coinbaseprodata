import aiohttp
import asyncio
import pandas as pd
import datetime

from datafuncs import process_cbpro_hist_data
from functools import reduce
from postgresconnector import PostgresConnection
from load_keys import load_db_keys
from aiolimiter import AsyncLimiter

dbkeys = load_db_keys()
connection = PostgresConnection(*dbkeys)

num_candles = 300

# These are the parameters that we care about change
granularity = 60
product = 'ATOM-USD'
start = pd.Timestamp(year = 2021, month = 1, day = 1)
end = pd.Timestamp(datetime.datetime.now())

date_range = list(pd.date_range(start, end, freq=pd.tseries.offsets.DateOffset(seconds = num_candles * granularity)))

if end not in date_range:
    date_range.append(end)

date_isoformat = []
for date in date_range:
    date_isoformat.append(date.isoformat())

urls = []
for i in range(len(date_isoformat)-1):
    url = f'https://api.exchange.coinbase.com/products/{product}/candles?start={date_isoformat[i]}&end={date_isoformat[i+1]}&granularity={granularity}'
    urls.append(url)

async def dataworker(limiter: AsyncLimiter, request_url: str):
    async with limiter:
        async with aiohttp.ClientSession() as session:
            async with session.request(method = 'GET', url = request_url) as response:
                data = await response.json()
                await asyncio.sleep(0.2)
    return data

async def main(urls):
    limiter = AsyncLimiter(10, 1) # We can only make 10 requests per second

    tasks = []
    for url in urls:
        tasks.append(dataworker(limiter, url))

    results = await asyncio.gather(*tasks)
    return results

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
loop = asyncio.get_event_loop()
data = loop.run_until_complete(main(urls=urls))

final_df = pd.DataFrame()
dataframes = []
for d in data:
    if len(d) != 0:
        df = process_cbpro_hist_data(d)
        if not df.empty:
            dataframes.append(df)

all_historical_data = reduce(lambda x, y: pd.concat([x, y]), dataframes)
all_historical_data.drop_duplicates(inplace=True)
all_historical_data.sort_values(by = 'time', inplace=True)
all_historical_data.reset_index(drop = True, inplace = True)
all_historical_data['exchange'] = 'coinbase'
all_historical_data['product'] = product

connection.update_table_with_df(df = all_historical_data, table_name='coinbasepricingdata', if_exists='append', index = False)