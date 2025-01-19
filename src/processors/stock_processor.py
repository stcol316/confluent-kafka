from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

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


class PriceChangeDetector(KeyedProcessFunction):
    def __init__(self, threshold_percentage):
        self.threshold = threshold_percentage
        self.last_price = None

    def open(self, context):
        state_descriptor = ValueStateDescriptor("last_price", Types.FLOAT())
        self.last_price = context.get_state(state_descriptor)

    def process_element(self, value, ctx):
        current_price = value["close"]
        previous_price = self.last_price.value()

        if previous_price is not None:
            price_change = ((current_price - previous_price) / previous_price) * 100

            if abs(price_change) >= self.threshold:
                result = {
                    "ticker": value["ticker"],
                    "datetime": value["datetime"],
                    "timestamp": value["timestamp"],
                    "previous_price": previous_price,
                    "current_price": current_price,
                    "price_change_percentage": price_change,
                }
                yield result

        self.last_price.update(current_price)


def create_kafka_source():
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
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def process_stock_data():
    # Create the execution environment
    logger.debug("Creating the execution environment")
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add kafka connetor
    logger.debug("Adding Kafka connector jar")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    kafka_jar = os.path.join(current_dir, "flink-sql-kafka-connector.jar")
    kafka_jar_url = f"file://{kafka_jar}"
    logger.debug(f"Adding JAR from path: {kafka_jar_url}")
    env.add_jars(kafka_jar_url)

    try:
        # Add Kafka source
        kafka_source = create_kafka_source()
    except ValueError as ve:
        logger.error(f"ValueError: {ve}")
        return
    except Exception as e:
        logger.error(f"Unexpected exception: {e}")
        return

    # Create watermark strategy
    logger.debug("Creating watermark strategy")
    wms = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(15))

    def process_record(record):
        return json.loads(record)

    # Create the data stream from Kafka and process the data
    logger.debug("Creating the data stream")
    stream = (
        env.from_source(
            source=kafka_source, watermark_strategy=wms, source_name="Kafka Stock Data"
        )
        .map(process_record)
        .key_by(lambda x: x["ticker"])
        .process(PriceChangeDetector(float(os.environ["PERCENTAGE_ALERT"])))
    )

    stream.print()

    # Execute the Flink job
    env.execute("Stock Data Processing Job")


if __name__ == "__main__":
    logger.debug("Starting stock processor")
    process_stock_data()
