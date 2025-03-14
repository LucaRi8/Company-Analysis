#%%
import pandas as pd
import numpy as np

# load target data
target_df = pd.read_csv('target_data/target.csv')
fin_df = pd.read_csv('preprocessed_data/basic_financials_preproc.csv')
seniment_df = pd.read_csv('preprocessed_data/sentiment.csv')

target_df = (
    target_df
    .assign(date_q=lambda df: pd.to_datetime(df['date_q']))
    .merge(
        fin_df
        .drop('date_y', axis=1)
        .assign(date_q=lambda df: pd.to_datetime(df['date_q'])), 
        on=['date_q', 'ticker'], how='left'
    )
    .merge(
        seniment_df[['date_q', 'ticker', 'sentiment_avg_q', 'sentiment_sd_q', 'sentiment_num_q']]
        .assign(date_q=lambda df: pd.to_datetime(df['date_q']).dt.normalize()), 
        on=['date_q', 'ticker'], how='left'
    )
)

target_df.to_csv('target_data/target_with_feats.csv', index=False)
