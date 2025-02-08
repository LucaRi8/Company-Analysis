#%%
import finnhub
from dotenv import load_dotenv
import os
#%%
load_dotenv()
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
print(API_KEY)
#%%
# Setup client
finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)

#%%
# Basic financials
basic = finnhub_client.company_basic_financials(['AAPL'], 'all')
quartly_df = pd.concat(
    [pd.DataFrame(value)[['v']].rename(columns={'v' : key}) 
     for key, value in basic['series']['quarterly'].items()], axis=1)

len_value_dict = {len(value) : key for key, value in basic['series']['quarterly'].items()}
max_len_feature = len_value_dict.get(max(len_value_dict.keys()))
quartly_df.index = pd.DataFrame(basic['series']['quarterly'][max_len_feature])['period'].to_list()


# #%%
# # Earnings surprises
# print(finnhub_client.company_earnings('TSLA', limit=5))
# #%%
# # Company News
# # Need to use _from instead of from to avoid conflict
# print(finnhub_client.company_news('AAPL', _from="2020-06-01", to="2020-06-10"))

# # Company Peers
# print(finnhub_client.company_peers('AAPL'))


# # Company Profile 2
# print(finnhub_client.company_profile2(symbol='AAPL'))

# # Revenue Estimates



# # Filings
# print(finnhub_client.filings(symbol='AAPL', _from="2020-01-01", to="2020-06-11"))


# # Financials as reported
# print(finnhub_client.financials_reported(symbol='AAPL', freq='annual'))

# # General news
# print(finnhub_client.general_news('AAPL', min_id=0))

# # IPO calendar
# print(finnhub_client.ipo_calendar(_from="2020-05-01", to="2020-06-01"))


# # Quote
# print(finnhub_client.quote('AAPL'))

# # Recommendation trends
# print(finnhub_client.recommendation_trends('AAPL'))

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


