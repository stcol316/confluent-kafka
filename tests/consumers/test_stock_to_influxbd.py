# tests/test_src.consumers.stock_to_influxdb.py
import os
import json
import logging
from unittest.mock import patch, MagicMock, call
import pytest
from confluent_kafka import KafkaError, KafkaException
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


# Test Configuration
@pytest.fixture
def mock_env(monkeypatch):
    """Mock required environment variables"""
    monkeypatch.setenv("BROKER_NAME", "kafka")
    monkeypatch.setenv("BROKER_LISTENER_PORT", "9092")
    monkeypatch.setenv("STOCK_TOPIC", "stock-data")
    monkeypatch.setenv("INFLUXDB_TOKEN", "test-token")
    monkeypatch.setenv("INFLUXDB_ORG", "test-org")
    monkeypatch.setenv("INFLUXDB_URL", "http://localhost:8086")
    monkeypatch.setenv("INFLUXDB_BUCKET", "test-bucket")
    monkeypatch.setenv("INFLUX_STOCK_MEASUREMENT", "stock_prices")
    monkeypatch.setenv("CONSUMER_GROUP", "test-group")


@pytest.fixture
def mock_consumer():
    with patch("src.consumers.stock_to_influxdb.Consumer") as mock:
        yield mock


@pytest.fixture
def mock_admin_client():
    with patch("src.consumers.stock_to_influxdb.AdminClient") as mock:
        yield mock


# Core Function Tests
def test_verify_env_success(mock_env):
    from src.consumers.stock_to_influxdb import verify_env

    verify_env()


def test_verify_env_missing_vars(monkeypatch):
    monkeypatch.delenv("BROKER_NAME", raising=False)
    from src.consumers.stock_to_influxdb import verify_env

    with pytest.raises(ValueError):
        verify_env()


def test_create_consumer_success(mock_consumer, mock_env):
    from src.consumers.stock_to_influxdb import create_consumer

    create_consumer("kafka:9092")
    mock_consumer.assert_called_once()


def test_create_consumer_retries_exceeded(mock_env, mock_consumer):
    from src.consumers.stock_to_influxdb import create_consumer, MaxRetriesExceededError

    mock_error = MagicMock()
    mock_error.retriable.return_value = True
    kafka_exception = KafkaException(mock_error)
    mock_consumer.side_effect = kafka_exception

    with patch("src.consumers.stock_to_influxdb.MAX_RETRIES", 1):
        with pytest.raises(MaxRetriesExceededError):
            create_consumer("kafka:9092")

    assert mock_consumer.call_count == 1


@patch("src.consumers.stock_to_influxdb.InfluxDBClient")
def test_create_influx_client_success(mock_influx, mock_env):
    from src.consumers.stock_to_influxdb import create_influx_client

    mock_influx.return_value.ping.return_value = True
    create_influx_client()
    mock_influx.assert_called_once_with(
        url="http://localhost:8086", token="test-token", org="test-org"
    )


@patch("src.consumers.stock_to_influxdb.InfluxDBClient")
def test_create_influx_client_failure(mock_influx, mock_env):
    from src.consumers.stock_to_influxdb import create_influx_client

    mock_influx.side_effect = Exception("Connection failed")
    with pytest.raises(Exception):
        create_influx_client()


def test_subscribe_to_topic_success(mock_env, mock_admin_client, mock_consumer):
    from src.consumers.stock_to_influxdb import create_consumer, subscribe_to_topic

    mock_topic_metadata = MagicMock()
    mock_topic_metadata.topics = {"stocks": MagicMock()}
    mock_admin_client.return_value.list_topics.return_value = mock_topic_metadata

    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance
    create_consumer("kafka:9092")

    subscribe_to_topic("kafka:9092", "stocks")

    mock_admin_client.return_value.list_topics.assert_called_with(timeout=10)
    mock_consumer_instance.subscribe.assert_called_once_with(["stocks"])


def test_subscribe_to_topic_retries_exceeded(
    mock_env, mock_admin_client, mock_consumer
):
    from src.consumers.stock_to_influxdb import (
        create_consumer,
        subscribe_to_topic,
        MaxRetriesExceededError,
    )

    mock_error = MagicMock()
    mock_error.retriable.return_value = True
    kafka_exception = KafkaException(mock_error)

    mock_topic_metadata = MagicMock()
    mock_topic_metadata.topics = {"stocks": MagicMock()}
    mock_admin_client.return_value.list_topics.return_value = mock_topic_metadata

    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance
    mock_consumer_instance.subscribe.side_effect = kafka_exception
    create_consumer("kafka:9092")
    with patch("src.consumers.stock_to_influxdb.MAX_RETRIES", 1):
        with pytest.raises(MaxRetriesExceededError):
            subscribe_to_topic("kafka:9092", "stocks")


# Message Processing Tests
@patch("src.consumers.stock_to_influxdb.write_to_influx")
@patch("src.consumers.stock_to_influxdb.deserialize_data")
@patch("src.consumers.stock_to_influxdb.consumer")
@patch("src.consumers.stock_to_influxdb.create_influx_client")
def test_poll_data_success(
    mock_create_influx, mock_consumer, mock_deserialize, mock_write_to_influx, caplog
):
    from src.consumers.stock_to_influxdb import poll_data

    caplog.set_level(logging.DEBUG)

    mock_event = MagicMock()
    mock_event.error.return_value = None
    mock_event.value.return_value = b'{"valid": "data"}'

    mock_consumer.poll.side_effect = [mock_event, KeyboardInterrupt()]
    mock_deserialize.return_value = {"valid": "data"}

    mock_influx_instance = MagicMock()
    mock_create_influx.return_value = mock_influx_instance

    mock_write_api = mock_influx_instance.writeapi.return_value = MagicMock()

    from src.consumers import stock_to_influxdb

    with patch.object(stock_to_influxdb, "influx_client", mock_influx_instance):
        with patch("src.consumers.stock_to_influxdb.MAX_RETRIES", 1):
            with pytest.raises(KeyboardInterrupt):
                poll_data("alerts", mock_write_api)

    mock_write_to_influx.assert_called_once_with(mock_write_api, {"valid": "data"})


@patch("src.consumers.stock_to_influxdb.write_to_influx")
@patch("src.consumers.stock_to_influxdb.create_influx_client")
def test_poll_data_retries_exceeded(
    mock_create_influx, mock_write_to_influx, mock_consumer
):
    from src.consumers.stock_to_influxdb import (
        create_consumer,
        poll_data,
        MaxRetriesExceededError,
    )

    mock_error = MagicMock()
    mock_error.retriable.return_value = True
    error = mock_error

    mock_event = MagicMock()
    mock_event.error.return_value = error

    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance

    mock_influx_instance = MagicMock()
    mock_create_influx.return_value = mock_influx_instance

    mock_write_api = mock_influx_instance.writeapi.return_value = MagicMock()

    create_consumer("kafka:9092")

    mock_consumer_instance.poll.return_value = mock_event

    with patch("src.consumers.stock_to_influxdb.MAX_RETRIES", 1):
        with pytest.raises(MaxRetriesExceededError):
            poll_data("stocks", mock_write_api)


# InfluxDB Write Tests
@patch("src.consumers.stock_to_influxdb.create_point")
@patch("src.consumers.stock_to_influxdb.influx_client", new_callable=MagicMock)
def test_write_to_influx_success(
    mock_influx_client, mock_create_point, mock_env, caplog
):
    from src.consumers.stock_to_influxdb import write_to_influx

    caplog.set_level(logging.INFO)

    mock_write_api = MagicMock()
    test_event = {"ticker": "TEST", "close": 150.0, "datetime": "2023-01-01T00:00:00"}

    mock_point = MagicMock()
    mock_create_point.return_value = mock_point

    with patch.dict(os.environ, {"INFLUXDB_BUCKET": "test-bucket"}):
        write_to_influx(mock_write_api, test_event)

    mock_write_api.write.assert_called_once_with(
        bucket="test-bucket", record=mock_point
    )
    assert "Writing to DB" in caplog.text
    assert "Data type error creating point" not in caplog.text
    assert "Failed to write to InfluxDB" not in caplog.text


def test_create_point_valid(caplog):
    from src.consumers.stock_to_influxdb import create_point

    caplog.set_level(logging.ERROR)

    with patch.dict(os.environ, {"INFLUX_STOCK_MEASUREMENT": "stocks"}):
        event = {
            "ticker": "TEST",
            "close": "150.00",
            "datetime": "2025-01-01T00:00:00Z",
        }
        point = create_point(event)
        line_protocol = point.to_line_protocol()
        assert "stocks" in line_protocol
        assert "ticker=TEST" in line_protocol
        assert "price=150" in line_protocol

        assert "Error encoutered while creating datapoint" not in caplog.text


# Shutdown Tests
def test_shutdown_consumer_success(mock_consumer):
    from src.consumers.stock_to_influxdb import shutdown_consumer, create_consumer

    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance

    create_consumer("kafka:9092")
    shutdown_consumer()
    mock_consumer_instance.commit.assert_called_once()
    mock_consumer_instance.close.assert_called_once()


@patch("src.consumers.stock_to_influxdb.InfluxDBClient")
@patch("src.consumers.stock_to_influxdb.create_influx_client")
def test_shutdown_influx(mock_create_influx, mock_influx, mock_env):
    from src.consumers.stock_to_influxdb import shutdown_influx, influx_client
    from src.consumers import stock_to_influxdb

    mock_influx_instance = MagicMock()
    mock_create_influx.return_value = mock_influx_instance

    with patch.object(stock_to_influxdb, "influx_client", mock_influx_instance):
        shutdown_influx()
    mock_create_influx.return_value.close.assert_called_once()


# Main Function Test
@patch("src.consumers.stock_to_influxdb.verify_env")
@patch("src.consumers.stock_to_influxdb.create_consumer")
@patch("src.consumers.stock_to_influxdb.create_influx_client")
@patch("src.consumers.stock_to_influxdb.subscribe_to_topic")
@patch("src.consumers.stock_to_influxdb.poll_data")
@patch("src.consumers.alert_consumer.shutdown_consumer")
def test_main_success(
    mock_shutdown,
    mock_poll,
    mock_subscribe,
    mock_influx,
    mock_consumer,
    mock_verify,
    mock_env,
    caplog,
):
    from src.consumers.stock_to_influxdb import main

    caplog.set_level(logging.INFO)
    mock_poll.side_effect = KeyboardInterrupt
    main()

    assert "Verifying env variables" in caplog.text
    assert "Creating Consumer" in caplog.text
    assert "Creating InfluxDB Client" in caplog.text
    assert "Subscribing to Topic" in caplog.text
    assert "Polling" in caplog.text
    assert "Shutting down consumer" in caplog.text
