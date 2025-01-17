from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration
import os
import json
import logging
import sys
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

def create_kafka_source():
    broker = f"{os.environ['BROKER_NAME']}:{os.environ['BROKER_LISTENER_PORT']}"
    topic = os.getenv('STOCK_TOPIC')
    
    logger.debug(f"Connecting to broker: {broker}")
    logger.debug(f"Topic: {topic}")
    
    if not broker or not topic:
        raise ValueError("Missing required environment variables BROKER_NAME, KAFKA_CLIENT_PORT, or STOCK_TOPIC")
    
    return KafkaSource.builder() \
        .set_bootstrap_servers(broker) \
        .set_topics(topic) \
        .set_group_id('stock_flink_processor') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

def process_stock_data():
    # Create the execution environment
    logger.debug("Creating the execution environment")
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Add kafka connetor
    logger.debug("Adding Kafka connector jar")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    kafka_jar = os.path.join(current_dir, 'flink-sql-kafka-connector.jar')
    kafka_jar_url = f"file://{kafka_jar}"
    logger.debug(f"Adding JAR from path: {kafka_jar_url}")
    env.add_jars(kafka_jar_url)

    # Add Kafka source
    kafka_source = create_kafka_source()
    
    # Create watermark strategy
    logger.debug("Creating watermark strategy")
    wms = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(15))
    
    # Create the data stream from Kafka
    logger.debug("Creating the data stream")
    stream = env.from_source(
        source=kafka_source,
        watermark_strategy=wms,
        source_name="Kafka Stock Data"
    )
    
    # Process the data
    logger.debug("Starting data processing")
    def process_record(record):
        data = json.loads(record)
        return f"Processed: {data}"
    
    processed_stream = stream.map(process_record)
    processed_stream.print()
    
    # Execute the Flink job
    env.execute("Stock Data Processing Job")

if __name__ == '__main__':
    logger.debug("Starting stock processor")
    process_stock_data()
