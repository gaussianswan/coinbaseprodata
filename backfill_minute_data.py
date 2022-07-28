from historicaldatahelper import CBProHistDataRetriever
import datetime
import pandas as pd
import telegram_send as t

end = pd.Timestamp(datetime.datetime.now())
start = end - pd.Timedelta(weeks = 3 * 52)
assets = ['ETH-USD', 'LTC-USD']

if __name__ == '__main__':

    data_retriever = CBProHistDataRetriever()
    granularity = 60

    for asset in assets:
        filename = f'{asset}_minute_data.parquet'
        t.send(messages = [f"Retrieving data for {asset}....."])
        historical_data = data_retriever.get_historical_prices(
            product=asset,
            start_date=start,
            end_date=end,
            granularity=granularity
        )
        # Then we are going to save this to some parquet file
        historical_data.to_parquet(filename)
        t.send(messages = ["Done saving data to {}".format(filename)])





