#%%
import finnhub
from dotenv import load_dotenv
import os
import sys
from tqdm import tqdm
import time
import datetime
import json
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from const import sampled_ticker_date

load_dotenv()
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

# Setup client
finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)
#%%
# Load ticker the data
ticker_data = pd.read_json('02_downloaded_data/company_tickers.json')
# Sample 2000 random ticker to download from ticker_data.loc['ticker']
sampled_data = ticker_data.loc['ticker'].sample(n=2000, random_state=42).to_list()

# download basic financials informations
basic = []
for tk in tqdm(sampled_data, desc="Downloading basic financials"):
    try:
        basic.append(finnhub_client.company_basic_financials(tk, 'all'))
    except Exception as e:
        print(f"Error retrieving data for {tk}: {e}")
    time.sleep(1.1) # 1.1 second sleep to avoid rate limit
    
quartly_df = []
for b in basic:
    if b['series'] == {}:
        continue

    try:
        tmp_df = pd.concat(
            [pd.DataFrame(value)[['v']].rename(columns={'v' : key}) 
            for key, value in b['series']['quarterly'].items() if value != []], 
            axis=1
        )

        len_value_dict = {len(value) : key for key, value in b['series']['quarterly'].items()}
        max_len_feature = len_value_dict.get(max(len_value_dict.keys()))
        tmp_df.index = pd.DataFrame(b['series']['quarterly'][max_len_feature])['period'].to_list()
        tmp_df['company'] = b['symbol']
        quartly_df.append(tmp_df)
    except Exception as e:
        print(f"Error saving quarterly data for {tk}: {e}")

basic_feats_df = pd.concat(quartly_df, axis = 0)
basic_feats_df.to_csv('02_downloaded_data/basic_financials.csv')

# Company News
news = []
for tk in tqdm(sampled_data, desc="Downloading news"):
    try:
        news_tmp =finnhub_client.company_news(tk, _from=sampled_ticker_date['start_date'], to=sampled_ticker_date['end_date'])
        news_tmp = [{**n, 'ticker': tk} for n in news_tmp]
        news.append(news_tmp)
    except Exception as e:
        print(f"Error retrieving data for {tk}: {e}")
    time.sleep(1.1) # 1.1 second sleep to avoid rate limit
#%%
news_df = pd.concat([pd.DataFrame(n) for n in news], axis=0)
news_df['date'] = pd.to_datetime(news_df['datetime'], unit='s').dt.strftime('%Y-%m-%d')
news_df = news_df.drop(columns=['datetime', 'id', 'image', 'related', 'source', 'url'])
news_df.to_csv('02_downloaded_data/company_news.csv')


# Company Peers
peers_dict = {}
for tk in tqdm(sampled_data, desc="Downloading peers"):
    try:
        peers_list = finnhub_client.company_peers(tk)
        peers_dict[tk] = peers_list[1:]
    except Exception as e:
        print(f"Error retrieving data for {tk}: {e}")
    time.sleep(1.1) # 1.1 second sleep to avoid rate limit

with open('02_downloaded_data/company_peers.json', 'w') as f:
    json.dump(peers_dict, f, indent=4)
#%%

# Company Profile 2
profile = []
for tk in tqdm(sampled_data, desc="Downloading company profile"):
    try:
        profile.append(finnhub_client.company_profile2(symbol=tk))
    except Exception as e:
        print(f"Error retrieving data for {tk}: {e}")
    time.sleep(1.1) # 1.1 second sleep to avoid rate

profile_df = pd.concat([pd.DataFrame(p, index=[0]) for p in profile if p != {}], axis=0)
profile_df = profile_df[['ticker', 'name', 'marketCapitalization', 'ipo', 'country', 'exchange', 'weburl']]
profile_df.to_csv('02_downloaded_data/company_profile.csv')

# # Financials as reported
financials = []
for tk in tqdm(sampled_data, desc="Downloading financials"):
    try:
        financials.append(
            finnhub_client
            .financials_reported(
                symbol=tk, 
                freq='quarterly', 
                _from=sampled_ticker_date['start_date'], 
                to=sampled_ticker_date['end_date']
            )
        )
    except Exception as e:
        print(f"Error retrieving data for {tk}: {e}")
    time.sleep(1.1) # 1.1 second sleep to avoid rate limit

all_fin_prepoc = []
for fin in financials:
    if fin['data'] == []:
        continue
    try:
        date = [dt['filedDate'] for dt in fin['data'] if dt['report']['bs'] != []]
        enddate = [dt['endDate'] for dt in fin['data'] if dt['report']['bs'] != []]
        symbol = fin['data'][0]['symbol']

        bs = [
            pd.DataFrame(bs['report']['bs'])[['concept', 'value']]
            .pivot_table(values = 'value', columns='concept', aggfunc='sum') 
            for bs in fin['data']  if bs['report']['bs'] != []
        ]

        bs = pd.concat(bs, ignore_index=True)
        bs['date'] = date
        bs['enddate'] = enddate
        bs['symbol'] = symbol

        date = [dt['filedDate'] for dt in fin['data'] if dt['report']['ic'] != []]
        ic = [
            pd.DataFrame(ic['report']['ic'])[['concept', 'value']]
            .pivot_table(values = 'value', columns='concept', aggfunc='sum') 
            for ic in fin['data'] if ic['report']['ic'] != []
        ]
        ic = pd.concat(ic, ignore_index=True)
        ic['date'] = date

        date = [dt['filedDate'] for dt in fin['data'] if dt['report']['cf'] != []]

        cf = [
            pd.DataFrame(cf['report']['cf'])[['concept', 'value']]
            .pivot_table(values = 'value', columns='concept', aggfunc='sum') 
            for cf in fin['data'] if cf['report']['cf'] != []
        ]
        cf = pd.concat(cf, ignore_index=True)
        cf['date'] = date
        all_fin_prepoc.append(
            bs
            .merge(ic, on='date', how='outer')
            .merge(cf, on='date', how='outer')
        )
    except Exception as e:
        print(f"Error manipulating data for {tk}: {e}")

fin_reported_df = pd.concat(all_fin_prepoc, ignore_index=True)
fin_reported_df.to_csv('02_downloaded_data/financials_reported.csv')

# # IPO calendar
ipo = (finnhub_client
       .ipo_calendar(_from=sampled_ticker_date['start_date'], 
                                  to=sampled_ticker_date['end_date'])
)
ipo_df = pd.concat([pd.DataFrame([ip]) for ip in ipo['ipoCalendar']])
ipo_df.to_csv('02_downloaded_data/ipo_calendar.csv')


# # Recommendation trends
trend = []
for tk in tqdm(sampled_data, desc="Downloading recommendation trends"):
    try:
        trend_list = finnhub_client.recommendation_trends(tk)
    except Exception as e:
        print(f"Error retrieving data for {tk}: {e}")
    if trend_list == []:
        continue
    trend.append(pd.concat([pd.DataFrame([tr]) for tr in  trend_list]))
    time.sleep(1) # 1 second sleep to avoid rate limit

trend_df = pd.concat(trend, axis=0)
trend_df.to_csv('02_downloaded_data/recommendation_trends.csv')


# patent 
patent = []
for tk in tqdm(sampled_data, desc="Downloading patent"):
    try:
        patent_df = pd.DataFrame(finnhub_client.stock_uspto_patent(tk, sampled_ticker_date['start_date'], sampled_ticker_date['end_date'])['data'])
    except Exception as e:
        print(f"Error retrieving data for {tk}: {e}")
    patent_df['ticker'] = tk
    patent.append(patent_df)
    time.sleep(1) # 1.1 second sleep to avoid rate limit

patent_df = pd.concat(patent, axis=0)
patent_df.to_csv('02_downloaded_data/patent.csv')


visa = []
for tk in tqdm(sampled_data, desc="Downloading visa"):
    try:
        visa_df = pd.DataFrame(finnhub_client.stock_visa_application(tk, sampled_ticker_date['start_date'], sampled_ticker_date['end_date'])['data'])
    except Exception as e:
        print(f"Error retrieving data for {tk}: {e}")
    visa_df['ticker'] = tk
    visa.append(visa_df)
    time.sleep(1) # 1.1 second sleep to avoid rate limit

visa_df = pd.concat(visa, axis=0)
visa_df.to_csv('02_downloaded_data/visa.csv')

# Insider sentiment
insider_stock = []
for tk in tqdm(sampled_data, desc="Downloading insider sentiment"):
    try:
        insider_stock_df = pd.DataFrame(finnhub_client.stock_insider_sentiment(tk, sampled_ticker_date['start_date'], sampled_ticker_date['end_date'])['data'])
    except Exception as e:
        print(f"Error retrieving data for {tk}: {e}")
    insider_stock_df['ticker'] = tk
    insider_stock.append(insider_stock_df)
    time.sleep(1) # 1.1 second sleep to avoid rate limit

insider_stock_df = pd.concat(insider_stock, axis=0)
insider_stock_df.to_csv('02_downloaded_data/insider_sentiment.csv')


# Lobbying
lobbying = []
for tk in tqdm(sampled_data, desc="Downloading lobbying"):
    try:
        lobbying_df = pd.DataFrame(finnhub_client.stock_lobbying(tk, sampled_ticker_date['start_date'], sampled_ticker_date['end_date'])['data'])
    except Exception as e:
        print(f"Error retrieving data for {tk}: {e}")
    lobbying_df['ticker'] = tk
    lobbying.append(lobbying_df)
    time.sleep(1.1) # 1.1 second sleep to avoid rate limit

lobbying_df = pd.concat(lobbying, axis=0)
lobbying_df.to_csv('02_downloaded_data/lobbying.csv')


# insider transaction
insider_transaction = []
for tk in tqdm(sampled_data, desc="Downloading insider transaction"):
    try:
        insider_transaction_df = pd.DataFrame(finnhub_client.stock_insider_transactions(tk, sampled_ticker_date['start_date'], sampled_ticker_date['end_date'])['data'])
    except Exception as e:
        print(f"Error retrieving data for {tk}: {e}")
    insider_transaction_df['ticker'] = tk
    insider_transaction.append(insider_transaction_df)
    time.sleep(1.1) # 1.1 second sleep to avoid rate limit

insider_transaction_df = pd.concat(insider_transaction, axis=0)
insider_transaction_df.to_csv('02_downloaded_data/insider_transaction.csv')
