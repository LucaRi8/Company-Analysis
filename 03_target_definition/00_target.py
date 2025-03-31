import pandas as pd
import numpy as np

#%%
# load financial stock price data
stock_price_df = pd.read_csv('data/historical_market_cap.csv')
# 
stock_price_df = stock_price_df.sort_values(by=['symbol', 'date'])
stock_price_df['rolling_mk'] = (
    stock_price_df
    .groupby('symbol')['marketCap']
    .transform(lambda x: x.rolling(window=50, min_periods=1).mean())
)
stock_price_df = (
    stock_price_df
    .assign(
        # create quartely reference date and scale the date 6 month back
        date_q=lambda df: pd.to_datetime(df['date']) + pd.offsets.QuarterEnd(0) - pd.offsets.QuarterEnd(2)
    )
)
stock_price_df['row_number'] = stock_price_df.groupby(['symbol', 'date_q'])['date'].rank(method='first').astype(int)
stock_price_df['max_row_number'] = stock_price_df.groupby(['symbol', 'date_q'])['row_number'].transform('max')
stock_price_df = stock_price_df[stock_price_df['row_number'] == stock_price_df['max_row_number']]