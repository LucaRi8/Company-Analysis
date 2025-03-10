from datetime import datetime

# date dict for sample time series data
sampled_ticker_date = {
    'start_date': '2019-01-01',
    'end_date': datetime.today().strftime('%Y-%m-%d')
}

# column renaming of financial basic data
fin_col_to_rename = {
    'assetTurnoverTTM' : 'assetTurnover',
    'fcfPerShareTTM' : 'fcfPerShare',
    'inventoryTurnoverTTM': 'inventoryTurnover', 
    'payoutRatioTTM' : 'payoutRatio',
    'peTTM' : 'pe', 
    'pfcfTTM' : 'pfcf',
    'psTTM' : 'ps', 
    'receivablesTurnoverTTM' : 'receivablesTurnover',
    'roaTTM' : 'roa', 
    'roeTTM' : 'roe', 
    'roicTTM' : 'roic', 
    'rotcTTM' : 'rotc', 
    "company": "ticker",
    'Unnamed: 0': 'date_q',
}

fin_cols_to_manipulate = [
    'roa',
    'roe',
    'roic',
    'rotc',
    'ev',
    'ebitPerShare',
]

# time series features extraction (tsfresh) param
fc_parameters = {
    "agg_linear_trend": [{"attr": 'slope', "chunk_len": 1}, {"attr": 'pvalue', "chunk_len": 1}],
}
