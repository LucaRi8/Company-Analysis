import pandas as pd
import numpy as np

#%%
# load financial stock price data
stock_price_df = pd.read_csv('data/stocks_daily_sampled.csv')
# 
ticker = ['date'] + stock_price_df.iloc[0, 1:].to_list()
stock_price_df = stock_price_df.iloc[2:, :]
stock_price_df.columns = ticker

stock_price_df = (
    stock_price_df
    .melt(id_vars=["date"], var_name="ticker", value_name="price")
    .assign(
        # create quartely reference date and scale the date 3 month back
        date_q=lambda df: pd.to_datetime(df['date']) + pd.offsets.QuarterEnd(0) - pd.offsets.QuarterEnd(1),
        price=lambda df: df['price'].astype(float)
    )
    .groupby(['date_q', 'ticker'])
    .agg({'price': 'mean'})
    .reset_index()
    .dropna()
)

stock_price_df.to_csv('target_data/target.csv', index=False)