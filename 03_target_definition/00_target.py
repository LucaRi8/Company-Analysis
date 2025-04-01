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
        # create year reference date and scale the date 1 year back
        date_y=lambda df: pd.to_datetime(df['date']) + pd.offsets.YearEnd(0) - pd.offsets.YearEnd(1)
    )
)
stock_price_df['row_number'] = stock_price_df.groupby(['symbol', 'date_y'])['date'].rank(method='first').astype(int)
stock_price_df['max_row_number'] = stock_price_df.groupby(['symbol', 'date_y'])['row_number'].transform('max')
stock_price_df = stock_price_df[stock_price_df['row_number'] == stock_price_df['max_row_number']]
stock_price_df = (
    stock_price_df
    .drop(columns=['row_number', 'max_row_number', 'date'])
    .rename(columns={'symbol' : "ticker", 'date_y' : "reference_date_y"})
)
stock_price_df.to_csv('target_data/target.csv')