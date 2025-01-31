from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaOffsetResetStrategy,
    KafkaSink,
    DeliveryGuarantee,
    KafkaRecordSerializationSchema,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration, Configuration
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup
import time
import os
import json
import logging
import sys
import signal
from dotenv import load_dotenv
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

load_dotenv()

# TODO: Logging config is currently being overwritten by Flink's own logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


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
                    "alert_type": f"{os.environ['TARGET_PRICE_ALERT']}",
                }

                yield result
        except TypeError as te:
            print(f"Error calculating target price: {te}")
            return
        except Exception as e:
            print(f"Error processing element: {e}")
            return


class PriceChangeDetector(KeyedProcessFunction):
    def __init__(self, threshold_percentage):
        self.threshold = threshold_percentage
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
                        "alert_type": f"{os.environ['PERCENTAGE_CHANGE_ALERT']}",
                    }

                    yield result

            self.last_price.update(current_price)
        except TypeError as te:
            print(f"Error calculating price change: {te}")
            return
        except Exception as e:
            print(f"Error processing element: {e}")
            return


class SavepointHandler:
    def __init__(self, env):
        self.env = env
        self.job_id = None
        self.job_client = None
        self.savepoint_path = "/opt/flink/savepoints"

    def register_signals(self):
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        print("Handle shutdown")
        if self.job_id and self.job_client:
            try:
                print("Creating savepoint...")
                os.makedirs(self.savepoint_path, exist_ok=True)                    

                # Trigger savepoint before shutdown
                savepoint_future = self.job_client.trigger_savepoint(
                    self.savepoint_path
                )
                savepoint = savepoint_future.result()
                if savepoint_future.exception():
                    raise Exception(
                        f"Error during savepoint creation: {savepoint_future.exception()}"
                    )
                elif savepoint_future.cancelled():
                    raise Exception("Savepoint creation cancelled")
                elif savepoint_future.done():
                    print(f"Savepoint created at: {savepoint}")
                else:
                    raise Exception("Unexpected issue during savepoint creation")
            except Exception as e:
                print(f"Failed to create savepoint: {e}")
        sys.exit(0)


def main():
    try:
        # Ensure env has correct values configured
        verify_env()

        # Configure Broker
        broker = f"{os.environ['BROKER_NAME']}:{os.environ['BROKER_LISTENER_PORT']}"

        # Create alert topic
        try:
            create_topic_if_not_exists(broker)
        except KafkaException as ke:
            print(f"KafkaException encountered while creating topic: {ke}")
            raise
        except Exception as e:
            print(f"Unexpected error occured while creating Kafka Topic: {e}")
            raise

        # Create the execution environment
        print("Creating the execution environment")
        env = configure_flink_env()

        # Setup savepoint handler
        savepoint_handler = SavepointHandler(env)
        savepoint_handler.register_signals()

        # Add kafka connetor
        print("Adding kafka connector")
        env = add_kafka_connectors(env)

        # Add Kafka source
        print("Creating kafka source")
        kafka_source = create_kafka_source(broker)

        # Create watermark strategy
        print("Creating watermark strategy")
        wms = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(15))

        # Create Kafka Sink
        print("Creating kafka sink")
        kafka_sink = build_kafka_sink(broker)

        # Create the data stream from Kafka and transform the data
        print("Processing the data stream")
        configure_percentage_change_stream(env, kafka_source, wms, kafka_sink)
        configure_target_price_stream(env, kafka_source, wms, kafka_sink)

        # Execute the Flink job
        execute_job(env, savepoint_handler)

    except ValueError as ve:
        print(f"Configuration error: {ve}")
        raise
    except Exception as e:
        print(f"Fatal error in stock processing: {e}")
        raise


# Check if the topic exists and create it if not
def create_topic_if_not_exists(broker):
    # Create the AdminClient
    admin_client = AdminClient({"bootstrap.servers": broker})
    metadata = admin_client.list_topics(timeout=10)
    topic = os.environ["ALERT_TOPIC"]
    if topic not in metadata.topics:
        new_topic = NewTopic(topic)
        tf = admin_client.create_topics([new_topic])
        # Block until done
        tf[topic].result()
        print(f"Topic {topic} created successfully.")
    else:
        print(f"Topic {topic} already exists.")


def create_kafka_source(broker):
    try:
        topic = os.getenv("STOCK_TOPIC")

        print(f"Connecting to broker: {broker}")
        print(f"Topic: {topic}")

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
            .set_properties(
                {
                    "enable.auto.commit": "false",
                    "auto.offset.reset": "earliest",
                    "isolation.level": "read_committed",
                }
            )
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )
    except KeyError as ke:
        print(f"Missing environment variable: {ke}")
        raise
    except Exception as e:
        print(f"Failed to create Kafka source: {e}")
        raise


def configure_flink_env():
    config = Configuration()
    config.set_string("state.checkpoints.dir", "file:///opt/flink/checkpoints")
    config.set_string("state.savepoints.dir", "file:///opt/flink/savepoints")

    env = StreamExecutionEnvironment.get_execution_environment(config)

    # Restart strategy
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 10000))

    # Checkpointing for fault tolerance
    env.enable_checkpointing(60000)
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config.set_min_pause_between_checkpoints(30000)
    checkpoint_config.set_checkpoint_timeout(20000)
    checkpoint_config.set_max_concurrent_checkpoints(1)
    checkpoint_config.enable_externalized_checkpoints(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )

    # Get latest savepoint if exists
    latest_savepoint = get_latest_savepoint("/opt/flink/savepoints")
    if latest_savepoint:
        # Set the savepoint path in the configuration
        config.set_string("execution.savepoint.path", latest_savepoint)

    return env


def build_kafka_sink(broker):
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(broker)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(os.environ["ALERT_TOPIC"])
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    return kafka_sink


def verify_env():
    required_vars = [
        "BROKER_NAME",
        "BROKER_LISTENER_PORT",
        "STOCK_TOPIC",
        "ALERT_TOPIC",
        "TARGET_PRICE_ALERT",
        "PERCENTAGE_CHANGE_ALERT",
        "TARGET_PRICE",
        "PERCENTAGE_THRESHOLD",
    ]
    missing_vars = [var for var in required_vars if var not in os.environ]

    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )


def configure_target_price_stream(env, kafka_source, wms, kafka_sink):
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

    # Monitor the stream
    target_price_stream.get_execution_environment().get_config().set_latency_tracking_interval(
        5000
    )

    # Explicitly serialize the data and use explicit Flink Type definition
    serialized_stream = target_price_stream.map(
        lambda x: json.dumps(x) if x is not None else "", output_type=Types.STRING()
    ).set_parallelism(1)

    # Print stream output to console and send to topic
    serialized_stream.sink_to(kafka_sink)
    serialized_stream.print()


def configure_percentage_change_stream(env, kafka_source, wms, kafka_sink):
    percentage_change_stream = (
        env.from_source(
            source=kafka_source,
            watermark_strategy=wms,
            source_name="Kafka Stock Data",
        )
        .map(process_record)
        .key_by(lambda x: x["ticker"])
        .process(PriceChangeDetector(float(os.environ["PERCENTAGE_THRESHOLD"])))
    )

    # Monitor the stream
    percentage_change_stream.get_execution_environment().get_config().set_latency_tracking_interval(
        5000
    )

    # Explicitly serialize the data and use explicit Flink Type definition
    serialized_stream = percentage_change_stream.map(
        lambda x: json.dumps(x) if x is not None else "", output_type=Types.STRING()
    ).set_parallelism(1)

    # Print stream output to console and send to topic
    serialized_stream.sink_to(kafka_sink)
    serialized_stream.print()


def process_record(record):
    try:
        return json.loads(record)
    except json.JSONDecodeError as je:
        print(f"Failed to parse JSON record: {je}")
        print(f"Invalid record: {record}")
        return None
    except Exception as e:
        print(f"Unexpected error processing record: {e}")
        return None


def add_kafka_connectors(env):
    print("Adding Kafka connector JAR files")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    kafka_sql_jar = os.path.join(
        current_dir, "flink-sql-connector-kafka-3.3.0-1.20.jar"
    )
    if not os.path.exists(kafka_sql_jar):
        raise RuntimeError(f"Kafka connector JAR not found at {kafka_sql_jar}")

    kafka_sql_jar_url = f"file://{kafka_sql_jar}"
    print(f"Adding JAR from path: {kafka_sql_jar_url}")
    env.add_jars(kafka_sql_jar_url)

    return env


def execute_job(env, savepoint_handler):
    job_client = env.execute_async("Stock Processing Pipeline")
    savepoint_handler.job_id = job_client.get_job_id()
    savepoint_handler.job_client = job_client
    print(f"Job started with ID: {savepoint_handler.job_id}")

    try:
        job_client.get_job_execution_result().result()
    except KeyboardInterrupt:
        print("Received interrupt signal")
        savepoint_handler.handle_shutdown(None, None)
    except Exception as e:
        print(f"Job execution error: {e}")
        raise


def get_latest_savepoint(savepoint_dir):
    try:
        print("Getting latest savepoint")
        if not os.path.exists(savepoint_dir):
            print(f"Savepoint directory not found at location {savepoint_dir}")
            return None

        savepoints = [
            f
            for f in os.listdir(savepoint_dir)
            if os.path.isdir(os.path.join(savepoint_dir, f))
        ]

        if not savepoints:
            print(f"No save points found at {savepoint_dir}")
            return None

        latest = max(
            savepoints,
            key=lambda x: os.path.getctime(os.path.join(savepoint_dir, x)),
        )

        full_path = os.path.join(savepoint_dir, latest)
        print(f"Found latest savepoint at: {full_path}")
        return full_path

    except Exception as e:
        print(f"Failed to get latest savepoint: {e}")
    return None


if __name__ == "__main__":
    print("Starting stock processor")
    try:
        main()
    except Exception:
        sys.exit(1)
