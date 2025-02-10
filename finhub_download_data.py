#%%
import finnhub
from dotenv import load_dotenv
import os
from tqdm import tqdm
import time
from const import sampled_ticker_date
import datetime
import json
import pandas as pd

#%%
load_dotenv()
#FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

# Setup client
finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)
#%%
# Load ticker the data
ticker_data = pd.read_json('company_tickers.json')
# Sample 2000 random ticker to download from ticker_data.loc['ticker']
sampled_data = ticker_data.loc['ticker'].sample(n=20, random_state=42).to_list()


# download basic financials informations
basic = []
for tk in tqdm(sampled_data, desc="Downloading basic financials"):
    basic.append(finnhub_client.company_basic_financials(tk, 'all'))
    time.sleep(1.1) # 1.1 second sleep to avoid rate limit
    
#%%
quartly_df = []
for b in basic:
    if b['series'] == {}:
        continue
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

basic_feats_df = pd.concat(quartly_df, axis = 0)
#%%
basic_feats_df.to_csv('basic_financials.csv')


#%%
# Company News
news = []
for tk in tqdm(sampled_data, desc="Downloading news"):
    news.append(finnhub_client.company_news(tk, _from=sampled_ticker_date['start_date'], to=sampled_ticker_date['end_date']))
    time.sleep(1.1) # 1.1 second sleep to avoid rate limit
#%%
news_df = pd.concat([pd.DataFrame(n) for n in news], axis=0)
news_df['date'] = pd.to_datetime(news_df['datetime'], unit='s').dt.strftime('%Y-%m-%d')
news_df = news_df.drop(columns=['datetime', 'id', 'image', 'related', 'source', 'url'])
news_df.to_csv('company_news.csv')


#%%
# Company Peers
peers_dict = {}
for tk in tqdm(sampled_data, desc="Downloading peers"):
    peers_list = finnhub_client.company_peers(tk)
    peers_dict[tk] = peers_list[1:]
    time.sleep(1.1) # 1.1 second sleep to avoid rate limit

with open('company_peers.json', 'w') as f:
    json.dump(peers_dict, f, indent=4)
#%%

# Company Profile 2
profile = []
for tk in tqdm(sampled_data, desc="Downloading company profile"):
    profile.append(finnhub_client.company_profile2(symbol=tk))
    time.sleep(1.1) # 1.1 second sleep to avoid rate

profile_df = pd.concat([pd.DataFrame(p, index=[0]) for p in profile if p != {}], axis=0)
profile_df = profile_df[['ticker', 'name', 'marketCapitalization', 'ipo', 'country', 'exchange', 'weburl']]
profile_df.to_csv('company_profile.csv')


# # Financials as reported
financials = []
for tk in tqdm(sampled_data, desc="Downloading financials"):
    financials.append(
        finnhub_client
        .financials_reported(
            symbol=tk, 
            freq='quarterly', 
            _from=sampled_ticker_date['start_date'], 
            to=sampled_ticker_date['end_date']
        )
    )
    time.sleep(1.1) # 1.1 second sleep to avoid rate limit
#%%
all_fin_prepoc = []
for fin in financials:
    if fin['data'] == []:
        continue
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
#%%
fin_reported_df = pd.concat(all_fin_prepoc, ignore_index=True)
fin_reported_df.to_csv('financials_reported.csv')

# # IPO calendar
ipo = (finnhub_client
       .ipo_calendar(_from=sampled_ticker_date['start_date'], 
                                  to=sampled_ticker_date['end_date'])
)
ipo_df = pd.concat([pd.DataFrame([ip]) for ip in ipo['ipoCalendar']])
ipo_df.to_csv('ipo_calendar.csv')


# # Recommendation trends
# print(finnhub_client.recommendation_trends('AAPL'))
trend = []
for tk in tqdm(sampled_data, desc="Downloading recommendation trends"):
    trend_list = finnhub_client.recommendation_trends(tk)
    if trend_list == []:
        continue
    trend.append(pd.concat([pd.DataFrame([tr]) for tr in  trend_list]))
    time.sleep(1.1) # 1.1 second sleep to avoid rate limit

trend_df = pd.concat(trend, axis=0)
trend_df.to_csv('recommendation_trends.csv')

# # Covid-19
# print(finnhub_client.covid19())

# # FDA Calendar
# print(finnhub_client.fda_calendar())


# # Insider transactions
# print(finnhub_client.stock_insider_transactions('AAPL', '2021-01-01', '2021-03-01'))

# # USPTO Patent
# print(finnhub_client.stock_uspto_patent("AAPL", "2021-01-01", "2021-12-31"))

# # Visa application
# print(finnhub_client.stock_visa_application("AAPL", "2021-01-01", "2022-06-15"))

# # Insider sentiment
# print(finnhub_client.stock_insider_sentiment('AAPL', '2021-01-01', '2022-03-01'))

# # Lobbying
# print(finnhub_client.stock_lobbying("AAPL", "2021-01-01", "2022-06-15"))

# # USA Spending
# print(finnhub_client.stock_usa_spending("LMT", "2021-01-01", "2022-06-15"))

# ## Market Holday / Status
# print(finnhub_client.market_holiday(exchange='US'))
# print(finnhub_client.market_status(exchange='US'))



# %%
