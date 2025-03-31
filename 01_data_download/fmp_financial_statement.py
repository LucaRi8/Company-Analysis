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

# Download income statement data
annual_fin_stmt = []
for tk in tqdm(sampled_tickers, desc='Downloading annual_financial statement data'):
    url = (f"https://financialmodelingprep.com/api/v3/income-statement/{tk}?period=annual&apikey={FMP_API_KEY}")
    annual_fin_stmt.append(get_jsonparsed_data(url))

annual_fin_stmt_df = pd.concat([pd.DataFrame(stmt) for stmt in annual_fin_stmt], axis=0)
annual_fin_stmt_df.to_csv('data/annual_income_statement.csv', index=False)

# Download balance sheet statement data
annual_bal_stmt = []
for tk in tqdm(sampled_tickers, desc='Downloading annual balance sheet statement data'):
    url = (f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{tk}?period=annual&apikey={FMP_API_KEY}")
    annual_bal_stmt.append(get_jsonparsed_data(url))

annual_bal_stmt_df = pd.concat([pd.DataFrame(stmt) for stmt in annual_bal_stmt], axis=0)
annual_bal_stmt_df.to_csv('data/annual_balance_sheet_statement.csv', index=False)


# Download cash flow statement data
annual_cash_stmt = []
for tk in tqdm(sampled_tickers, desc='Downloading annual cash flow statement data'):
    url = (f"https://financialmodelingprep.com/stable/cash-flow-statement?symbol={tk}&apikey={FMP_API_KEY}")
    annual_cash_stmt.append(get_jsonparsed_data(url))

annual_cash_stmt_df = pd.concat([pd.DataFrame(stmt) for stmt in annual_cash_stmt], axis=0)
annual_cash_stmt_df.to_csv('data/annual_cash_statement.csv', index=False)


