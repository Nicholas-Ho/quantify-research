# See the Polygon.io Python API documentation at https://polygon-api-client.readthedocs.io/en/latest/index.html

from polygon import RESTClient
from polygon.rest.models.aggs import Agg
from polygon.rest.models.tickers import Ticker
import json
from enum import Enum
from datetime import date, timedelta
from typing import List
from csv import writer
import os

# Constants
API_KEYS_FILE = ".secrets"
MAX_POLYGON_AGGS_LIMIT = 50000 # as defined by Polygon.io's API
MAX_POLYGON_TICKER_LIMIT = 1000 # as defined by Polygon.io's API

# Duration enum for the timespan argument
class Duration(Enum):
    SECONDS = "second"
    MINUTES = "minute"
    HOURS = "hour"
    DAYS = "day"
    WEEKS = "week"
    MONTHS = "month"
    QUARTER = "quarter"
    YEAR = "year"

class PolygonIOFetcher:
    def __init__(self, api_key_filename: str, max_date_range: timedelta = timedelta(days=90)) -> None:
        api_key = self.readAPIKey(api_key_filename, "polygon.io")
        self.client = RESTClient(api_key=api_key)
        self.max_date_range = max_date_range

    def readAPIKey(self, filename: str, service: str) -> str:
        with open(filename, "r") as f:
            json_content = json.load(f)
            return json_content[service]["api_key"]
        
    # Aggregates

    def getAggregateData(self, ticker: str, interval: int, interval_timespan: Duration, date_from: date, date_to: date, adjusted: bool, limit: int) -> List[Agg]:
        '''
        ticker: Target ticker

        interval: The granularity of the data. Multiply by interval_timespan (eg. interval=5, interval_timespan="minutes")

        interval_timespan: The granularity of the data. Valid values as written in the Duration enum

        date_from: Start date of the data fetched

        date_to: End date of the data fetched

        adjusted: Whether the data is adjusted for splits

        limit: Limit to the number of data points fetched. Polygon.io defines the maximum limit to be 50000
        '''
        output = []

        # Pagination by splitting the date range into multiple ranges of length self.max_date_range
        curr_date_to = date_to
        while curr_date_to - date_from >= self.max_date_range:
            curr_date_from = curr_date_to - self.max_date_range
            
            # Append date ranges, working backwards from date_to in increments of self.max_date_range
            aggs = self.client.get_aggs(
                ticker=ticker, 
                multiplier=interval,
                timespan=interval_timespan.value,
                from_=curr_date_from,
                to=curr_date_to,
                adjusted=adjusted,
                limit=limit
            )
            if len(aggs) >= limit:
                raise Exception("Polygon.io retrieval limit reached")
            if len(aggs) <= 0:
                return output
            output += aggs

            curr_date_to = curr_date_from - timedelta(days=1)

        # Lastly, append any remaining dates
        aggs = self.client.get_aggs(
            ticker=ticker, 
            multiplier=interval,
            timespan=interval_timespan.value,
            from_=date_from,
            to=curr_date_to,
            limit=limit
        )
        if len(aggs) >= limit:
            raise Exception("Polygon.io retrieval limit reached")
        if len(aggs) <= 0:
            return output
        output += aggs

        return output
    
    def writeAggsToCSV(self, filename: str, ticker: str, aggs: List[Agg], append: bool = True) -> None:
        # Add headers if the file has not been created or the file is being overwritten
        header = ("ticker", "open", "high", "close", "volume", "vwap", "timestamp", "transactions", "otc")
        if append and os.path.isfile(filename):
            add_header = False
        else:
            add_header = True

        # Set the mode to append or write
        mode = 'a' if append else 'w'
        with open(filename, mode, newline='', encoding='utf-8') as f:
            writer_obj = writer(f, delimiter=',')

            # Add headers if necessary
            if add_header:
                writer_obj.writerow(header)

            for agg in aggs:
                tickers_tuple = (
                    ticker,
                    agg.open,
                    agg.high,
                    agg.low,
                    agg.close,
                    agg.volume,
                    agg.vwap,
                    agg.timestamp,
                    agg.transactions,
                    agg.otc
                )
                writer_obj.writerow(tickers_tuple)

    def getWriteAggData(self, ticker: str, interval: int, interval_timespan: Duration, date_from: date, date_to: date, adjusted: bool, limit: int, filename: str) -> None:
        '''
        Gets aggregate data and writes it directly to file without returning the data. Avoids loading large amounts of data into memory. 


        ticker: Target ticker

        interval: The granularity of the data. Multiply by interval_timespan (eg. interval=5, interval_timespan="minutes")

        interval_timespan: The granularity of the data. Valid values as written in the Duration enum

        date_from: Start date of the data fetched

        date_to: End date of the data fetched

        adjusted: Whether the data is adjusted for splits

        limit: Limit to the number of data points fetched. Polygon.io defines the maximum limit to be 50000

        filename: File to write the data to
        '''

        # Pagination by splitting the date range into multiple ranges of length self.max_date_range
        curr_date_to = date_to
        while curr_date_to - date_from >= self.max_date_range:
            curr_date_from = curr_date_to - self.max_date_range
            
            # Append date ranges, working backwards from date_to in increments of self.max_date_range
            aggs = self.client.get_aggs(
                ticker=ticker, 
                multiplier=interval,
                timespan=interval_timespan.value,
                from_=curr_date_from,
                to=curr_date_to,
                adjusted=adjusted,
                limit=limit
            )
            if len(aggs) >= limit:
                raise Exception("Polygon.io retrieval limit reached")
            if len(aggs) <= 0:
                return
            
            self.writeAggsToCSV(filename, ticker, aggs, True)

            curr_date_to = curr_date_from - timedelta(days=1)

        # Lastly, append any remaining dates
        aggs = self.client.get_aggs(
            ticker=ticker, 
            multiplier=interval,
            timespan=interval_timespan.value,
            from_=date_from,
            to=curr_date_to,
            limit=limit
        )
        if len(aggs) >= limit:
            raise Exception("Polygon.io retrieval limit reached")
        if len(aggs) <= 0:
            return
        
        self.writeAggsToCSV(filename, ticker, aggs, True)

    # Tickers

    def getAllTickers(self, ticker_type: str, market: str, limit: int) -> List[Ticker]:
        '''
        ticker_type: Type of ticker. Pass None to query all types

        market: Market to search. Pass None to query all markets

        limit: Limit to the number of data points fetched. Polygon.io defines the maximum limit to be 1000
        '''
        # Pagination via sort and ticker_gt
        sorting_field = 'ticker'
        output: List[Ticker] = []
        
        tickers = self.client.list_tickers(type=ticker_type, market=market, sort=sorting_field, limit=limit)
        output += tickers

        while len(tickers) > 0:
            last_ticker = output[-1]
            tickers = self.client.list_tickers(type=ticker_type, market=market, sort=sorting_field, limit=limit, ticker_gt=last_ticker.ticker)
            output += tickers

        return output
    
    
    def writeTickersToCSV(self, filename: str, tickers: List[Ticker], append: bool = True) -> None:
        # Add headers if the file has not been created or the file is being overwritten
        header = (
            "ticker",
            "active",
            "cik",
            "composite_figi",
            "currency_name",
            "currency_symbol",
            "base_currency_symbol",
            "base_currency_name",
            "delisted_utc",
            "last_updated_utc",
            "locale",
            "market",
            "primary_exchange",
            "share_class_figi",
            "type",
            "source_feed"
            )
        if append and os.path.isfile(filename):
            add_header = False
        else:
            add_header = True

        # Set the mode to append or write
        mode = 'a' if append else 'w'
        with open(filename, mode, newline='', encoding='utf-8') as f:
            writer_obj = writer(f, delimiter=',')

            # Add headers if necessary
            if add_header:
                writer_obj.writerow(header)

            for ticker in tickers:
                tickers_tuple = (
                    ticker.ticker,
                    ticker.active,
                    ticker.cik,
                    ticker.composite_figi,
                    ticker.currency_name,
                    ticker.currency_symbol,
                    ticker.base_currency_symbol,
                    ticker.base_currency_name,
                    ticker.delisted_utc,
                    ticker.last_updated_utc,
                    ticker.locale,
                    ticker.market,
                    ticker.primary_exchange,
                    ticker.share_class_figi,
                    ticker.type,
                    ticker.source_feed
                    )
                writer_obj.writerow(tickers_tuple)
            
# Testing
if __name__ == "__main__":
    fetcher = PolygonIOFetcher(API_KEYS_FILE)
    ticker = 'GOOGL'
    fetcher.getWriteAggData(ticker, 1, Duration.MINUTES, date(year=2022, month=7, day=1), date(year=2023, month=7, day=31), False, MAX_POLYGON_AGGS_LIMIT, "aggs.csv")
    # tickers = fetcher.getAllTickers("CS", "stocks", 10)
    # fetcher.writeTickersToCSV('tickers.csv', tickers, append=True)