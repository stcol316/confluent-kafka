#!/usr/bin/env python
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
from dataclasses import dataclass

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

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


class MaxRetriesExceededError(Exception):
    pass


@dataclass
class Config:
    ticker: str
    start_date: str
    end_date: str
    timespan: str
    multiplier: int
    broker: str
    topic: str

    @classmethod
    def from_env(cls) -> "Config":
        start_date = os.environ.get("RETRIEVAL_START_DATE", get_start_date())
        if start_date == "":
            start_date = get_start_date()
        end_date = os.environ.get("RETRIEVAL_END_DATE", get_end_date())
        if end_date == "":
            end_date = get_end_date()

        return cls(
            ticker=os.environ.get("STOCK_TICKER", "CFLT"),
            start_date=start_date,
            end_date=end_date,
            timespan=os.environ.get("RETRIEVAL_TIMESPAN", "day"),
            multiplier=int(os.environ.get("RETRIEVAL_MULTIPLIER", "1")),
            broker=f"{os.environ['BROKER_NAME']}:{os.environ['BROKER_LISTENER_PORT']}",
            topic=os.environ["STOCK_TOPIC"],
        )

    def validate(self) -> None:
        """Validate configuration values"""
        if self.multiplier < 1:
            raise ValueError("Multiplier must be positive")

        if self.timespan not in [
            "second",
            "minute",
            "hour",
            "day",
            "week",
            "month",
            "quarter",
            "year",
        ]:
            raise ValueError("Invalid timespan")

        try:
            datetime.strptime(self.start_date, "%Y-%m-%d")
            datetime.strptime(self.end_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Invalid date format. Use YYYY-MM-DD")


def main():
    try:
        # Verify all necessary env vars are present
        logger.info("Verifying env variables")
        verify_env()

        # Get .env vars
        logger.info("Creating Config")
        config = Config.from_env()
        config.validate()

        logger.info(
            f"Configuration:\n"
            f"Ticker: {config.ticker}\n"
            f"From: {config.start_date}\n"
            f"To: {config.end_date}\n"
            f"Timespan: {config.timespan}\n"
            f"Multiplier: {config.multiplier}"
        )

        # load params to be usable by requests
        reqStr = f"{os.environ['STOCK_URL']}ticker/{config.ticker}/range/{config.multiplier}/{config.timespan}/{config.start_date}/{config.end_date}"
        logger.debug(f"Request String: {reqStr}")

        logger.info("Creating Producer")
        create_producer(config.broker)

        # Fetch historic data for the stock
        for data in fetch_all_historic_data(reqStr, None):
            sdata = serialize_data(data, config.ticker)
            logger.debug(sdata)
            if sdata:
                logger.debug(f"Queuing data: {sdata}")
                if not queue_data(sdata, config.topic):
                    logger.error("Failed to queue data, stopping producer")
                    break
        logger.info("DONE FETCHING HISTORIC DATA")

        # TODO: We can move to gathering live data here
        poll_live_data()
    except ValueError as ve:
        logger.error(f"ValueError: {ve}")
    except requests.RequestException as re:
        logger.error(f"HTTP Request failed: {re}")
    except Exception as e:
        logger.error(f"Unhandled error: {e}")
    except KeyboardInterrupt:
        logger.debug("Debug: Keyboard interrupt")
    finally:
        if producer:
            remaining = producer.flush(timeout=30)
            if remaining > 0:
                logger.info(f"{remaining} messages not delivered")


def get_start_date():
    today = datetime.today()
    return today.replace(year=today.year - 2).strftime("%Y-%m-%d")


def get_end_date():
    return datetime.today().strftime("%Y-%m-%d")


def verify_env():
    required_vars = [
        "BROKER_NAME",
        "BROKER_LISTENER_PORT",
        "STOCK_TOPIC",
        "STOCK_URL",
        "API_KEY",
    ]
    missing_vars = [var for var in required_vars if var not in os.environ]

    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )


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
            elif response.status_code == 429:  # Rate limit hit
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited. Waiting {retry_after} seconds")
                time.sleep(retry_after)
            elif (
                response.status_code == 403
            ):  # Forbidden, likely bad timeframe provided
                logger.error(
                    f"Forbidden. Likely incorrect timeframe provided. 2 years of history max."
                )
            else:
                logger.error(f"HTTP Request failed with status {response}")
                current_retries += 1
                time.sleep(1)
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
        # If non-retryable or retries exhausted we try to produce to dead letter queue
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


def create_producer(broker):
    global producer
    # Configure Producer
    producer_config = {
        "bootstrap.servers": broker,
        "acks": "all",
        "retries": 3,
        "retry.backoff.ms": 1000,
        "delivery.timeout.ms": 30000,
        "message.send.max.retries": 3,
        "socket.timeout.ms": 10000,
        "message.timeout.ms": 10000,
    }

    current_retries = 0
    while current_retries < MAX_RETRIES:
        try:
            producer = Producer(producer_config)
            return
        except KafkaException as ke:
            logger.error(f"Kafka producer creation error: {ke}")
            error = ke.args[0]
            if error.retriable():
                logger.error(f"Retryable kafka error during producer creation: {ke}")
                current_retries += 1
                # We could have a backoff mechanism here
                time.sleep(1)
            else:
                logger.error(
                    f"Non-retryable kafka error during producer creation: {ke}"
                )
                raise
        except Exception as e:
            logger.error(f"Unexpected error while creating producer: {e}")
            raise

    raise MaxRetriesExceededError(
        f"Failed to create producer after {MAX_RETRIES} attempts"
    )


if __name__ == "__main__":
    logger.debug("Entered main")
    main()
