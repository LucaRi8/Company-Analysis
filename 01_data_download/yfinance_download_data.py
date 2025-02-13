
import yfinance as yf
import pandas as pd
import const as constants
import json

def get_yahoo_finance_data(tickers, start_date, end_date):
    """
    Fetches historical closing prices from Yahoo Finance for one or multiple tickers.
    
    :param tickers: String (single ticker) or list of tickers
    :param start_date: Start date in 'YYYY-MM-DD' format
    :param end_date: End date in 'YYYY-MM-DD' format
    :return: Dictionary of DataFrames with closing price data
    """
    
    # Ensure tickers is a list
    if isinstance(tickers, str):
        tickers = [tickers]
    
    data_dict = {}
    
    for ticker in tickers:
        try:
            # Fetch data from Yahoo Finance
            df = yf.download(ticker, start=start_date, end=end_date)
            
            # Check if 'Adj Close' exists, otherwise use 'Close'
            if "Adj Close" in df.columns:
                df = df[["Adj Close"]].rename(columns={"Adj Close": "close"})
            elif "Close" in df.columns:
                df = df[["Close"]].rename(columns={"Close": "close"})
            else:
                print(f"Error: No 'Close' or 'Adj Close' data for {ticker}")
                continue
            
            # Store data in dictionary
            data_dict[ticker] = df
        except Exception as e:
            print(f"Error retrieving data for {ticker}: {e}")
    
    return data_dict

# Load the data
ticker_data = pd.read_json('company_tickers.json')

# Sample 2000 random ticker to download from ticker_data.loc['ticker']
sampled_data = ticker_data.loc['ticker'].sample(n=2000, random_state=42).to_list()
# Get the data for the sampled tickers
data_dict = get_yahoo_finance_data(sampled_data, constants.sampled_ticker_date['start_date'], constants.sampled_ticker_date['end_date'])

# Combine all DataFrames into a single DataFrame
combined_df = pd.concat(data_dict.values(), axis=1)
# Save the combined DataFrame to a CSV file
combined_df.to_csv('stocks_daily_sampled.csv')





