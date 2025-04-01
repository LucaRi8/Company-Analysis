 #%%
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
import numpy as np
from utils.const import fin_col_to_rename, fin_cols_to_manipulate, fc_parameters
from tsfresh.feature_extraction import extract_features
from tsfresh.utilities.dataframe_functions import roll_time_series
from tsfresh.utilities.distribution import MultiprocessingDistributor


# Load the data
income_df = pd.read_csv('data/annual_income_statement.csv')
bal_df = pd.read_csv('data/annual_balance_sheet_statement.csv')

income_df = income_df[income_df['reportedCurrency'] == 'USD']
income_df = (
    income_df
    .assign(
        date_y=lambda df: pd.to_datetime(df['date']) + pd.offsets.YearEnd(0)
    )
    .drop(columns=['reportedCurrency', 'date', 'cik', 'fillingDate', 'acceptedDate', 'period', 'link', 'finalLink', 'calendarYear'])
    .rename(columns={'symbol': 'ticker'})
)

bal_df = bal_df[bal_df['reportedCurrency'] == 'USD']
bal_df = (
    bal_df
    .assign(
        date_y=lambda df: pd.to_datetime(df['date']) + pd.offsets.YearEnd(0)
    )
    .drop(columns=['reportedCurrency', 'date', 'cik', 'fillingDate', 'acceptedDate', 'period', 'link', 'finalLink', 'calendarYear'])
    .rename(columns={'symbol': 'ticker'})
)

statment_df = (
    income_df
    .merge(
        bal_df,
        on = ['ticker', 'date_y'],
        how = 'inner'
    )
    .rename(columns={'date_y': 'reference_date_y'})
)

# # exploratory o the columns
# ncol = fin_df.shape[1]
# nrow = fin_df.shape[0]
# def summary(df):
#     summary_df = pd.DataFrame({
#         'Column': df.columns,
#         'Non-Null Count': [df[[col]].count().values/nrow for col in df.columns],
#         'Unique Count': [df[[col]].nunique().values/nrow for col in df.columns],
#         'Mean':[df[[col]].mean(numeric_only=True).values for col in df.columns],
#         'Std': [df[[col]].std(numeric_only=True).values for col in df.columns],
#         'Min': [df[[col]].min(numeric_only=True).values for col in df.columns],
#         '25%': [df[[col]].quantile(0.25, numeric_only=True).values for col in df.columns],
#         '50%': [df[[col]].median(numeric_only=True).values for col in df.columns],
#         '75%': [df[[col]].quantile(0.75, numeric_only=True).values  for col in df.columns],
#         'Max': [df[[col]].max(numeric_only=True).values  for col in df.columns]
#     })
#     return summary_df

# fin_df_summary = summary(fin_df)
# print(fin_df_summary)


# time series manipulations, calclation of basic features based on time series of original features
# like linear model slope, weigthed mean of variations, etc.
 
 #%%
statment_df_rolled = roll_time_series(
    statment_df[fin_cols_to_manipulate + ['ticker', 'reference_date_y']].dropna(axis=0), 
    column_id="ticker", column_sort="reference_date_y", max_timeshift = 4
)
statement_ts_fs_df = (
    extract_features(
        statment_df_rolled.dropna(axis=0).drop(columns='ticker'), 
        column_id='id', column_sort='reference_date_y', 
        default_fc_parameters=fc_parameters)
        .reset_index()
        .rename(columns={'level_0': 'ticker', 'level_1': 'reference_date_y'})
)

statment_df = (
    statment_df
    .merge(
        statement_ts_fs_df,
        on = ['ticker', 'reference_date_y'],
        how = 'left'
    )
)

p_val_cols = [col for col in statment_df.columns if 'pvalue' in col]
statment_df = (
    statment_df
    .assign(
        **{col: np.where(fin_df[col] == 0, 1, statment_df[col]) for col in p_val_cols},
        date_q=pd.to_datetime(fin_df['reference_date_y']) + pd.offsets.QuarterEnd(0)
    )
)

reference_date_y.to_csv('preprocessed_data/basic_financials_preproc.csv', index=False)