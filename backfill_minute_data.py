from historicaldatahelper import CBProHistDataRetriever
import datetime
import pandas as pd

end = pd.Timestamp(datetime.datetime.now())
start = end - pd.Timedelta(weeks = 3 * 52)
assets = ['BTC-USD', 'ETH-USD', 'LTC-USD']

if __name__ == '__main__':

    data_retriever = CBProHistDataRetriever()
    granularity = 60

    for asset in assets:
        filename = f'asset_minute_data.parquet'
        print(f"Retrieving data for {asset}.....")
        historical_data = data_retriever.get_historical_prices(
            product=asset,
            start_date=start,
            end_date=end,
            granularity=granularity
        )
        print("Done getting data")
        # Then we are going to save this to some parquet file
        historical_data.to_parquet(filename)
        print("Done saving data to {}".format(filename))



