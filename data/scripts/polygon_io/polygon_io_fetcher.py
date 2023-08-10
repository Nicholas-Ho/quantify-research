# See the Polygon.io Python API documentation at https://polygon-api-client.readthedocs.io/en/latest/index.html

from polygon import RESTClient
from polygon.rest.models.aggs import Agg
import json
from enum import Enum
from datetime import date, timedelta
from typing import List
from csv import writer
import os

# Constants
API_KEYS_FILE = ".secrets"
MAX_POLYGON_LIMIT = 50000 # as defined by Polygon.io's API

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
        
    def getAggregateData(self, ticker: str, interval: int, interval_timespan: Duration, date_from: date, date_to: date, limit: int):
        '''
        ticker: Target ticker

        interval: The granularity of the data. Multiply by interval_timespan (eg. interval=5, interval_timespan="minutes")

        interval_timespan: The granularity of the data. Valid values as written in the Duration enum

        date_from: Start date of the data fetched

        date_to: End date of the data fetched

        limit: Limit to the number of data points fetched. Polygon.io defines the maximum limit to be 50000
        '''
        output = []

        # Pseudo-pagination by splitting the date range into multiple ranges of length self.max_date_range
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
                limit=limit
            )
            if len(aggs) >= limit:
                raise Exception("Polygon.io retrieval limit reached")
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
        output += aggs

        return output
    
    def writeAggsToFile(self, filename: str, ticker: str, aggs: List[Agg], append: bool = True) -> None:
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
                agg_tuple = (
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
                writer_obj.writerow(agg_tuple)
            
# Testing
if __name__ == "__main__":
    fetcher = PolygonIOFetcher(API_KEYS_FILE)
    ticker = 'GOOGL'
    aggs = fetcher.getAggregateData(ticker, 1, Duration.MINUTES, date(year=2022, month=7, day=1), date(year=2023, month=7, day=31), MAX_POLYGON_LIMIT)
    fetcher.writeAggsToFile("googl.csv", ticker, aggs, append=True)