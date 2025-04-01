from datetime import datetime

# date dict for sample time series data
sampled_ticker_date = {
    'start_date': '2019-01-01',
    'end_date': datetime.today().strftime('%Y-%m-%d')
}

# column renaming of financial basic data
fin_col_to_rename = {
    
}

fin_cols_to_manipulate = [
    'intangibleAssets',
    'shortTermDebt',
    'longTermDebt',
    'ebitda',
    'ebitdaratio',
    'researchAndDevelopmentExpenses',
    'revenue',
    'costOfRevenue',
    'grossProfit',
    'operatingExpenses',
    'operatingExpenses',
    'operatingIncomeRatio',   
    'netIncome',
    'eps',
]

# time series features extraction (tsfresh) param
fc_parameters = {
    "agg_linear_trend": [{"attr": 'slope', "chunk_len": 1, 'f_agg' : 'mean'}, {"attr": 'pvalue', "chunk_len": 1, 'f_agg' : 'mean'}],
}
