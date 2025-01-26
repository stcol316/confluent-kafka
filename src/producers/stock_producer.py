#!/usr/bin/env python
# Ensure docker is running
# colima start
# confluent local kafka start

import requests
from confluent_kafka import Producer, KafkaException
import click
import time
import json
import sys
import logging
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Configure Producer
config = {
    # User-specific properties that you must set
    # Port can be found as Plaintext Ports after running confluent local kafka start
    "bootstrap.servers": f"{os.environ['BROKER_NAME']}:{os.environ['BROKER_LISTENER_PORT']}",
    # Fixed properties
    "acks": "all",
    "retries": 3,
    "retry.backoff.ms": 1000,
    "delivery.timeout.ms": 30000,
    "message.send.max.retries": 3,
}

producer = None
MAX_RETRIES = 10


class StockSnapshot:
    def __init__(self, ticker, data):
        self.ticker = ticker
        self.volume = data['v']          # The trading volume of the symbol in the given time period
        self.vwap = data['vw']           # The volume weighted average price
        self.open_price = data['o']      # The open price for the symbol in the given time period
        self.close_price = data['c']     # The close price for the symbol in the given time period
        self.high_price = data['h']      # The highest price for the symbol in the given time period
        self.low_price = data['l']       # The lowest price for the symbol in the given time period
        self.timestamp = data['t']       # The Unix Msec timestamp for the start of the aggregate window
        self.number_of_trades = data['n']# The number of transactions in the aggregate window
        
    @property
    def datetime(self):
        """Convert timestamp to datetime object"""
        return datetime.fromtimestamp(self.timestamp / 1000.0)


# Fetch some data
# Click sets command line params and their defaults
@click.command()
@click.option(
    "--ticker",
    type=str,
    default="CFLT",
    help="The stock ticker you want to analyse",
)
@click.option(
    "--start",
    type=str,
    default="2022-01-09",
    help="The starting date from which you want to analyse data formatted to YYYY-MM-DD or a millisecond timestamp (Max 2 yrs)",
)
@click.option(
    "--end",
    type=str,
    default=datetime.today().strftime("%Y-%m-%d"),
    help="The end date on which you want to stop analysing data formatted to YYYY-MM-DD or a millisecond timestamp",
)
@click.option(
    "--timespan",
    type=str,
    default="day",
    help="The granularity of the data [second, minute, hour, day, week, month, quarter, year]",
)
@click.option(
    "--multi",
    type=int,
    default=1,
    help="Timespan multiplier e.g. a timespan of 'hour' with a multiplyer of '2' will retrieve data for ever 2 hours",
)
def fetch_data(ticker, start, end, timespan, multi):
    logger.info(
        f"Ticker: {ticker}\nFrom: {start}\nTo: {end}\nTimespan: {timespan}\nMultiplier: {multi}"
    )
    # load params to be usable by requests
    reqStr = f"{os.environ['STOCK_URL']}ticker/{ticker}/range/{multi}/{timespan}/{start}/{end}"
    logger.debug(f"Request String: {reqStr}")

    try:
        topic = os.environ.get("STOCK_TOPIC")
        create_producer()

        # Fetch historic data for the stock
        for data in fetch_all_historic_data(reqStr, None):
            sdata = serialize_data(data, ticker)
            logger.debug(sdata)
            if sdata:
                logger.debug(f"Queuing data: {sdata}")
                if not queue_data(sdata, topic):
                    logger.error("Failed to queue data, stopping producer")
                    break
        logger.debug("DONE FETCHING HISTORIC DATA")

        # TODO: We can move to gathering live data here
        poll_live_data()
    except requests.RequestException as e:
        logger.error(f"HTTP Request failed: {e}")
    except Exception as e:
        logger.error(f"Unhandled error: {e}")
    except KeyboardInterrupt:
        pass
    finally:
        remaining = producer.flush(timeout=30)
        if remaining > 0:
            logger.info(f"{remaining} messages not delivered")


def fetch_all_historic_data(reqStr, next_url=None):
    logger.info("Fetching historic data")
    response = fetch_historic_data(reqStr, next_url)

    if response.status_code == 200:
        data = response.json()

        # Yield current page results and continue processing
        yield from data["results"]

        # Handle Pagination
        next_url = data.get("next_url")
        if next_url is not None:
            logger.debug("............Fetching next page.......")
            # Max 5 request per minute so we sleep for 15 seconds
            time.sleep(int(os.environ.get("REQUEST_INTERVAL", 15)))

            # Recursively fetch data until we reach the end
            yield from fetch_all_historic_data(reqStr, next_url)

        logger.info("Finished gathering historic data")
    else:
        logger.error(f"Error fetching data: {response}")

        # Max 5 request per minute so we sleep for 15 seconds
        time.sleep(int(os.environ.get("REQUEST_INTERVAL", 15)))


def fetch_historic_data(reqStr, next_url=None):
    request = next_url if next_url else reqStr
    current_retries = 0
    while current_retries < MAX_RETRIES:
        try:
            response = requests.get(request, params={"apiKey": os.environ["API_KEY"]})
            logging.debug(f"fetch_historic_data: {response}")
            if response.status_code == 200:
                return response
            else:
                logger.error(f"HTTP Request failed with status {response.status_code}")
        except requests.RequestException as re:
            logger.error(f"HTTP Request failed: {re}")
            current_retries += 1
            if current_retries >= MAX_RETRIES:
                raise Exception(f"Maximum retries reached for HTTP requests: {re}")
            # We could have a backoff mechanism here
            time.sleep(1)

    # Return None when retries exhausted
    return None


def poll_live_data():
    pass


def serialize_data(data, ticker):
    logger.debug("Serialising data")
    try:
        # TODO:Kafka Schema?
        stock_snapshot = StockSnapshot(ticker, data)

        # Convert to dictionary for JSON serialization
        serializable_data = {
            "ticker": stock_snapshot.ticker,
            "timestamp": stock_snapshot.timestamp,
            "datetime": stock_snapshot.datetime.isoformat(),
            "open": stock_snapshot.open_price,
            "close": stock_snapshot.close_price,
            "high": stock_snapshot.high_price,
            "low": stock_snapshot.low_price,
            "volume": stock_snapshot.volume,
            "vwap": stock_snapshot.vwap,
            "number_of_trades": stock_snapshot.number_of_trades,
        }
        return json.dumps(serializable_data)
    except KeyError as ke:
        logger.error(f"Key not found: {ke}")
        return None
    except TypeError as te:
        logger.error(f"JSON serialization failed: {te}")
        return None
    except Exception as err:
        logger.error(f"Unexpected error during serialization: {err}")
        return None


def delivery_callback(err, event):
    if err:
        # If err is retryable we raise Kafka exception in queue_data()
        if err.retriable():
            raise KafkaException(err)
        else:
            # If non-retryable we try to produce to dead letter queue
            logger.error(f"Produce to topic {event.topic()} failed with error: {err}")
            try:
                producer.produce("dlq", key=event.key(), value=event.value())
            except Exception as e:
                logger.error(f"Failed to send to DLQ: {e}")
    else:
        val = event.value().decode("utf8")
        logger.debug(f"{val} sent to {event.topic()} on partition {event.partition()}.")


def queue_data(data, topic):
    # Topic will be automatically created if it does not exist
    logger.debug(f"Sending data to topic: {topic}")

    current_retries = 0
    while current_retries < MAX_RETRIES:
        try:
            producer.produce(topic, value=data, on_delivery=delivery_callback)
            return True
        except KafkaException as ke:
            logger.error(f"Kafka Exception occurred: {ke}")
            error = ke.args[0]
            if error.retriable():
                logger.error(f"Retryable producer error: {error}")
                current_retries += 1
                # We could have a backoff mechanism here
                time.sleep(1)
            else:
                logger.error(f"Non-retryable producer error: {error}")
                return False
        except BufferError:
            # Flush messages
            logger.info("Producer queue full, waiting for space...")
            producer.poll(1)
        except Exception as e:
            logger.error(f"Unexpected error while producing: {e}")
            return False

    # Return False when retries exhausted
    return False


def create_producer():
    global producer
    producer = Producer(config)
    return producer


if __name__ == "__main__":
    logger.debug("Entered main")
    fetch_data()
