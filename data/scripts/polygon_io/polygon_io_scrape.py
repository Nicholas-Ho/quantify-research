import asyncio
from multiprocessing import Process
import math

import json
import polygon
import pandas as pd
from polygon.enums import TickerType
from datetime import date, timedelta

# Parameters
BATCH_SIZE = 30
TIMESPAN = 'hour'
API_KEY_FILE = "/home/markhaoxiang/Projects/quantify/quantify-research/.secrets"
with open(API_KEY_FILE, "r") as f:
    API_KEY = json.load(f)["polygon.io"]["api_key"]

reference_client = polygon.ReferenceClient(API_KEY)
# stock_client = polygon.StocksClient(API_KEY, True)
# async client is broken for minute / hour timespans
# stock_client = polygon.StocksClient(API_KEY)


print("Fetching References")

all_stock_tickers = reference_client.get_tickers(symbol_type=TickerType.COMMON_STOCKS, all_pages=True)
all_etf_tickers = reference_client.get_tickers(symbol_type=TickerType.ETF, all_pages=True)
reference_client.close()

print("Fetching Prices")

def fetch_and_store(API_KEY, tickers, timespan = 'day'):
    stock_client = polygon.StocksClient(API_KEY)
    tickers = [t['ticker'] for t in tickers]
    for ticker in tickers:
        fetch_single_ticker(stock_client, ticker, timespan)
    stock_client.close()
    # producers = [asyncio.create_task(fetch_single_ticker(ticker, timespan)) for ticker in tickers]
    # await asyncio.gather(*producers)

def fetch_single_ticker(stock_client, ticker, timespan):
    try:
        sample = stock_client.get_aggregate_bars(
            ticker,
            timespan=timespan,
            from_date=date.today()-timedelta(days=3650),
            to_date=date.today(),
            full_range=True
        )
        df = pd.DataFrame(sample)
        df.to_csv(f"data/{ticker}.csv")
        print(f"Complete {ticker}")
    except:
        print(f"Error with {ticker}")

processes = []
#for i in range(math.ceil((len(all_etf_tickers)+1)/BATCH_SIZE)):
#    tickers = all_etf_tickers[i*BATCH_SIZE : min(len(all_etf_tickers), (i+1)*BATCH_SIZE)]
#    processes.append(Process(target=fetch_and_store, args=(API_KEY, tickers, TIMESPAN)))

for i in range(math.ceil((len(all_stock_tickers)+1)/BATCH_SIZE)):
    tickers = all_stock_tickers[i*BATCH_SIZE : min(len(all_stock_tickers), (i+1)*BATCH_SIZE)]
    processes.append(Process(target=fetch_and_store, args=(API_KEY, tickers, TIMESPAN)))

for p in processes:
    p.start()
for p in processes:
    p.join()

# stock_client.close()

