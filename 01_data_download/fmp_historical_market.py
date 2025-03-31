import finnhub
from dotenv import load_dotenv
import os
import sys
from tqdm import tqdm
import json
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.const import sampled_ticker_date
from urllib.request import urlopen
import certifi
import json
import random

load_dotenv()
FMP_API_KEY = os.getenv("FMP_API_KEY")

def get_jsonparsed_data(url):
    response = urlopen(url, cafile=certifi.where())
    data = response.read().decode("utf-8")
    return json.loads(data)

# Load company_ticker.json
with open('data/company_tickers.json', 'r') as file:
    company_tickers = json.load(file)

ticker_list = [value['ticker'] for key, value in company_tickers.items()]
random.seed(42)
sampled_tickers = random.sample(ticker_list, 250)

# Download historical market cap data
hist_mk = []
for tk in tqdm(sampled_tickers, desc='Downloading historical market cap data'):
    url = (f"https://financialmodelingprep.com/api/v3/historical-market-capitalization/{tk}?limit=10000&from={sampled_ticker_date['start_date']}&to={sampled_ticker_date['end_date']}&apikey={FMP_API_KEY}")
    hist_mk.append(get_jsonparsed_data(url))

hist_mk_df = pd.concat([pd.DataFrame(mk) for mk in hist_mk], axis=0)
hist_mk_df.to_csv('data/historical_market_cap.csv', index=False)

