
import pandas as pd
import numpy as np

# load target data
target_df = pd.read_csv('target_data/target.csv')
fin_df = pd.read_csv('preprocessed_data/basic_financials_preproc.csv')
seniment_df = pd.read_csv('preprocessed_data/sentiment.csv')

target_df = (
    target_df
    .assign(date_q=lambda df: pd.to_datetime(df['date_q']).dt.date)
    .merge(
        fin_df
        .drop('date_y', axis=1)
        .assign(date_q=lambda df: pd.to_datetime(df['date_q']).dt.date), 
        on=['date_q', 'ticker'], how='left'
    )
    .merge(
        seniment_df[['date_q', 'ticker', 'sentiment_avg_q', 'sentiment_sd_q', 'sentiment_num_q']]
        .assign(date_q=lambda df: pd.to_datetime(df['date_q']).dt.date), 
        on=['date_q', 'ticker'], how='left'
    )
)

date_split = pd.to_datetime('2024-01-01')
train_df = target_df[pd.to_datetime(target_df['date_q']) < date_split]
test_df = target_df[pd.to_datetime(target_df['date_q']) >= date_split]

target_df.to_csv('target_data/target_with_feats.csv', index=False)
train_df.to_csv('target_data/train_with_feats.csv', index=False)
test_df.to_csv('target_data/test_with_feats.csv', index=False)
