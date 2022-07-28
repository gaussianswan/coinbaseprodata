import pandas as pd

def process_cbpro_hist_data(data):
    df = pd.DataFrame(data, columns=['time', 'low', 'high', 'open', 'close', 'volume'])
    df['time'] = pd.to_datetime(df['time'], unit = 's')
    return df