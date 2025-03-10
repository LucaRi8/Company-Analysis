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
}

