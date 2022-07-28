from cbpro import PublicClient
from pandas.tseries.offsets import DateOffset
from datafuncs import process_cbpro_hist_data

import pandas as pd
import time
class CBProHistDataRetriever:

    def __init__(self):
        self.client = PublicClient()
        self.candles_per_request = 300

    def get_historical_prices(self, product: str, start_date: pd.Timestamp, end_date: pd.Timestamp, granularity: int)-> pd.DataFrame:
        offset = DateOffset(seconds = self.candles_per_request * granularity)
        date_range = list(pd.date_range(start=start_date, end=end_date, freq=offset))

        if end_date not in date_range:
            date_range.append(end_date)
        date_range_iso = []
        for date in date_range:
            date_range_iso.append(date.isoformat())

        historical_data = []
        for i in range(len(date_range_iso)-1):
            start_date = date_range_iso[i]
            end_date = date_range_iso[i+1]
            data = self.client.get_product_historic_rates(
                product_id=product,
                start=start_date,
                end=end_date,
                granularity=granularity
            )

            data.reverse() # The data comes in the wrong order.

            historical_data.extend(data)
            time.sleep(0.1) # Going to sleep so that we

        historical_data_df = process_cbpro_hist_data(historical_data)
        historical_data_df.drop_duplicates(inplace = True)
        return historical_data_df


