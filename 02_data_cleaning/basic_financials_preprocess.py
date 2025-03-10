import pandas as pd
import numpy as np
from const import fin_col_to_rename, fin_cols_to_manipulate

# Load the data
fin_df = pd.read_csv('data/basic_financials.csv')
sect_df = pd.read_csv('data/company_profile.csv')

fin_df = (
    fin_df
    .rename(
        columns=fin_col_to_rename
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

# calculate sector yearly mean 
fin_df['date_q'] = pd.to_datetime(fin_df['date_q'])

sect_fin_df = (
    fin_df
    .assign(date_y=fin_df['date_q'].dt.year)
    .groupby(['date_y', 'finnhubIndustry'])
    .agg({col: "mean" for col in fin_cols_to_manipulate})  
    .rename(columns={col: f"{col}_sector_avg" for col in fin_cols_to_manipulate}) 
    .reset_index()  
)

# fill all the missing value with the nearest vaue
min_date = fin_df['date_q'].dt.year.min()
max_date = fin_df['date_q'].dt.year.max()
date_sect_df = (
    pd.DataFrame({"date_y" : range(min_date, max_date)})
    .join(
        pd.DataFrame({"finnhubIndustry": sect_df['finnhubIndustry'].unique()}), 
        how='cross'
    )
)
sect_fin_df = (
    sect_fin_df
    .sort_values("date_y")
    .set_index(["finnhubIndustry", "date_y"])  
    .groupby(level="finnhubIndustry")  
    .ffill()  
    .reset_index()  
)

fin_df = (
    fin_df
    .assign(date_y=fin_df['date_q'].dt.year)
    .merge(
        sect_fin_df,
        on = ['date_y', 'finnhubIndustry'],
        how = 'left'
    )
)

# calculate ratio between company and sector
fin_df = (
    fin_df
    .assign(
        **{f'{col}_sector_ratio': fin_df[col] / fin_df[f'{col}_sector_avg'] for col in fin_cols_to_manipulate}
    )
    .drop(columns=[f'{col}_sector_avg' for col in fin_cols_to_manipulate])
)
