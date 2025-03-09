import pandas as pd
import calendar
import numpy as np
import transformers
import requests
import os
import dotenv
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer, Pipeline, Trainer, TrainingArguments
#from huggingface_hub import InferenceClient
from tqdm import tqdm

# Load the data
news_df = pd.read_csv('data/company_news.csv')

# model name from model hub
model_name = "yiyanghkust/finbert-tone"
# load the model
model = AutoModelForSequenceClassification.from_pretrained(model_name)
# load tokenizer
tokenizer = AutoTokenizer.from_pretrained(model_name)

 
# Predict the sentiment of the news
news_pred = []
for news in tqdm(news_df.summary.to_list()):
    try:
        input = tokenizer(news, return_tensors='pt', padding=True, truncation=True)
        outputs = model(**input)
        news_pred.append(np.argmax(outputs.logits.detach().numpy(), axis=1))
    except Exception as e:
        print(f"Error predicting news sentiment: {e}")
        news_pred.append(None)
    
news_df['sentiment'] = [n[0] if n is not None else None for n in news_pred]

# create a monthly reference date 
news_df['date_m'] = (
    news_df['date']
    .apply(
        lambda x: 
        pd.to_datetime(x)
        .replace(day=calendar.monthrange(pd.to_datetime(x).year, pd.to_datetime(x).month)[1]))
)
# create a quarterly reference date
news_df['date_q'] = (
    news_df['date']
    .apply(
        lambda x: 
        pd.to_datetime(x)
        .to_period('Q')
        .end_time
    )
)

# recoding the sentiment 0 -> negative 1 -> neutral 2 -> positive
news_df['sentiment'] = news_df['sentiment'].map({0: 1, 1: 2, 2: 0})

# aggregation of the data to have montlhly and quartely sentiment (mean of sentiment)
quartely_sentiment = (
    news_df
    .groupby(['ticker', 'date_q'])
    .agg(
        sentiment_avg_q=('sentiment', 'mean'),
        sentiment_num_q=('sentiment', 'count'),
        sentiment_sd_q=('sentiment', 'std'),
    )
    .reset_index()
)
sentiment_df = (
    news_df
    .groupby(['ticker', 'date_m'])
    .agg(
        sentiment_avg_m=('sentiment', 'mean'),
        sentiment_num_m=('sentiment', 'count'),
        sentiment_sd_m=('sentiment', 'std'),
        date_q=('date_q', 'first')
    )
    .reset_index()
    .merge(
        quartely_sentiment,
        how='left',
        on=['ticker', 'date_q'],
    )
)

sentiment_df.to_csv('preprocessed_data/sentiment.csv')