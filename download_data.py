import requests

def download_data(api_key, ticker, function='TIME_SERIES_DAILY', outputsize='compact'):
    base_url = 'https://www.alphavantage.co/query'
    params = {
        'function': function,
        'symbol': ticker,
        'apikey': api_key,
        'outputsize': outputsize,
        'datatype': 'json'
    }
    
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()



