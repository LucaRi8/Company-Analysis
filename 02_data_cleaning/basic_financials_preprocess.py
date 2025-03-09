import pandas as pd
import numpy as np

# Load the data
fin_df = pd.read_csv('data/basic_financials.csv')
sect_df = pd.read_csv('data/company_profile.csv')

fin_df = (
    fin_df
    .rename(
        columns={
            "company": "ticker"
            'Unnamed: 0': 'date',
        }
    )
    .merge(
        sect_df[['ticker', 'finnhubIndustry']],
        on = 'ticker',
        how = 'left'
    )
)

# exploratory o the columns
ncol = fin_df.shape[1]
nrow = fin_df.shape[0]
def summary(df):
    summary_df = pd.DataFrame({
        'Column': df.columns,
        'Non-Null Count': [df[[col]].count().values/nrow for col in df.columns],
        'Unique Count': [df[[col]].nunique().values/nrow for col in df.columns],
        'Mean':[df[[col]].mean(numeric_only=True).values for col in df.columns],
        'Std': [df[[col]].std(numeric_only=True).values for col in df.columns],
        'Min': [df[[col]].min(numeric_only=True).values for col in df.columns],
        '25%': [df[[col]].quantile(0.25, numeric_only=True).values for col in df.columns],
        '50%': [df[[col]].median(numeric_only=True).values for col in df.columns],
        '75%': [df[[col]].quantile(0.75, numeric_only=True).values  for col in df.columns],
        'Max': [df[[col]].max(numeric_only=True).values  for col in df.columns]
    })
    return summary_df

fin_df_summary = summary(fin_df)
print(fin_df_summary)