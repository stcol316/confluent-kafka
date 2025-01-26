from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaOffsetResetStrategy,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.restart_strategy import RestartStrategies
from slack_sdk import WebClient
import time
import os
import json
import logging
import sys
from dotenv import load_dotenv

load_dotenv()

# TODO: Logging config is currently being overwritten by Flink's own logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)
slack_client = None


class TargetPriceDetector(KeyedProcessFunction):
    def __init__(self, target_price):
        self.target_price = target_price
        self.price_reached = None

    def open(self, context):
        state_descriptor = ValueStateDescriptor("price_reached", Types.BOOLEAN())
        self.price_reached = context.get_state(state_descriptor)

    def process_element(self, value, ctx):
        try:
            current_price = value["close"]
            price_reached = self.price_reached.value()

            if price_reached is None:
                self.price_reached.update(False)
                price_reached = False

            if not price_reached and current_price >= self.target_price:
                self.price_reached.update(True)
                result = {
                    "ticker": value["ticker"],
                    "datetime": value["datetime"],
                    "target_price": self.target_price,
                    "current_price": current_price,
                    "alert_type": "target_price_reached",
                }
                yield result
        except TypeError as te:
            logger.error(f"Error calculating target price: {te}")
            return
        except Exception as e:
            logger.error(f"Error processing element: {e}")
            return


class PriceChangeDetector(KeyedProcessFunction):
    def __init__(self, threshold_percentage, slack_client):
        self.threshold = threshold_percentage
        self.slack_client = slack_client
        self.last_price = None

    def open(self, context):
        state_descriptor = ValueStateDescriptor("last_price", Types.FLOAT())
        self.last_price = context.get_state(state_descriptor)

    def process_element(self, value, ctx):
        try:
            current_price = value["close"]
            previous_price = self.last_price.value()

            if previous_price is not None:
                price_change = round(
                    ((current_price - previous_price) / previous_price) * 100, 2
                )

                if abs(price_change) >= self.threshold:
                    result = {
                        "ticker": value["ticker"],
                        "datetime": value["datetime"],
                        "timestamp": value["timestamp"],
                        "previous_price": previous_price,
                        "current_price": current_price,
                        "price_change_percentage": price_change,
                    }

                    # TODO: Push these alerts to individual topics and let consumers write to slack
                    if self.slack_client is not None:
                        message = ""
                        if price_change > 0:
                            message = f"{value['datetime']} {value['ticker']} price has increased by {price_change}%"
                        else:
                            message = f"{value['datetime']} {value['ticker']} price has decreased by {price_change}%"

                        # self.slack_client.chat_postMessage(
                        #     channel=os.environ["SLACK_CHANNEL"],
                        #     text=message,
                        # )
                        # We are rate limited to 1 message per second
                        time.sleep(1.2)
                    yield result

            self.last_price.update(current_price)
        except TypeError as te:
            logger.error(f"Error calculating price change: {te}")
            return
        except Exception as e:
            logger.error(f"Error processing element: {e}")
            return


def create_kafka_source():
    try:
        broker = f"{os.environ['BROKER_NAME']}:{os.environ['BROKER_LISTENER_PORT']}"
        topic = os.getenv("STOCK_TOPIC")

        logger.debug(f"Connecting to broker: {broker}")
        logger.debug(f"Topic: {topic}")

        if not broker or not topic:
            raise ValueError(
                "Missing required environment variables BROKER_NAME, KAFKA_CLIENT_PORT, or STOCK_TOPIC"
            )

        return (
            KafkaSource.builder()
            .set_bootstrap_servers(broker)
            .set_topics(topic)
            .set_group_id("stock_flink_processor")
            .set_starting_offsets(
                KafkaOffsetsInitializer.committed_offsets(
                    KafkaOffsetResetStrategy.EARLIEST
                )
            )
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )
    except KeyError as ke:
        logger.error(f"Missing environment variable: {ke}")
        raise
    except Exception as e:
        logger.error(f"Failed to create Kafka source: {e}")
        raise


def process_stock_data():
    try:
        # Create the execution environment
        logger.debug("Creating the execution environment")
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 10000))

        # Add kafka connetor
        logger.debug("Adding Kafka connector jar")
        current_dir = os.path.dirname(os.path.abspath(__file__))
        kafka_jar = os.path.join(current_dir, "flink-sql-kafka-connector.jar")
        if not os.path.exists(kafka_jar):
            raise RuntimeError(f"Kafka connector JAR not found at {kafka_jar}")

        kafka_jar_url = f"file://{kafka_jar}"
        logger.debug(f"Adding JAR from path: {kafka_jar_url}")
        env.add_jars(kafka_jar_url)

        # Add Kafka source
        kafka_source = create_kafka_source()

        # Create watermark strategy
        logger.debug("Creating watermark strategy")
        wms = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(15))

        def process_record(record):
            try:
                return json.loads(record)
            except json.JSONDecodeError as je:
                logger.error(f"Failed to parse JSON record: {je}")
                logger.debug(f"Invalid record: {record}")
                return None
            except Exception as e:
                logger.error(f"Unexpected error processing record: {e}")
                return None

        # Send a message
        slack_token = os.environ.get("SLACK_TOKEN", None)
        if slack_token is not None:
            slack_client = WebClient(token=slack_token)

        # Create the data stream from Kafka and transform the data
        logger.debug("Processing the data stream")
        percentage_change_stream = (
            env.from_source(
                source=kafka_source,
                watermark_strategy=wms,
                source_name="Kafka Stock Data",
            )
            .map(process_record)
            .key_by(lambda x: x["ticker"])
            .process(
                PriceChangeDetector(float(os.environ["PERCENTAGE_ALERT"]), slack_client)
            )
        )

        target_price_stream = (
            env.from_source(
                source=kafka_source,
                watermark_strategy=wms,
                source_name="Kafka Stock Data",
            )
            .map(process_record)
            .key_by(lambda x: x["ticker"])
            .process(TargetPriceDetector(float(os.environ["TARGET_PRICE"])))
        )

        # TODO: Configure data sinks to send these alerts to Kafka topics
        percentage_change_stream.print()
        target_price_stream.print()

        # Execute the Flink job
        env.execute("Stock Data Processing Job")
    except ValueError as ve:
        logger.error(f"Configuration error: {ve}")
        raise
    except Exception as e:
        logger.error(f"Fatal error in stock processing: {e}")
        raise


if __name__ == "__main__":
    logger.debug("Starting stock processor")
    try:
        process_stock_data()
    except Exception:
        sys.exit(1)
