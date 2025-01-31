import os
import json
from datetime import datetime
from unittest.mock import patch, MagicMock, call
import pytest
from confluent_kafka import KafkaException
import requests
from dotenv import load_dotenv
import logging

load_dotenv()  # Load .env for some base configuration


@pytest.fixture
def mock_env(monkeypatch):
    """Fixture to mock environment variables"""
    monkeypatch.setenv("BROKER_NAME", "kafka")
    monkeypatch.setenv("BROKER_LISTENER_PORT", "9092")
    monkeypatch.setenv("STOCK_TOPIC", "stocks")
    monkeypatch.setenv("STOCK_URL", "https://api.example.com/")
    monkeypatch.setenv("API_KEY", "test-key")
    monkeypatch.setenv("RETRIEVAL_START_DATE", "2022-01-01")
    monkeypatch.setenv("RETRIEVAL_END_DATE", "2022-01-02")


@pytest.fixture
def sample_stock_data():
    return {
        "v": 1000,
        "vw": 150.5,
        "o": 150.0,
        "c": 151.0,
        "h": 152.0,
        "l": 149.5,
        "t": 1640995200000,
        "n": 500,
    }


def test_stock_snapshot_init(sample_stock_data):
    from src.producers.stock_producer import (
        StockSnapshot,
    )

    snapshot = StockSnapshot("TEST", sample_stock_data)

    assert snapshot.ticker == "TEST"
    assert snapshot.volume == 1000
    assert snapshot.open_price == 150.0
    assert snapshot.datetime == datetime(2022, 1, 1)


def test_config_from_env(mock_env):
    from src.producers.stock_producer import Config

    config = Config.from_env()

    assert config.ticker == "CFLT"
    assert config.start_date == "2022-01-01"
    assert config.end_date == "2022-01-02"
    assert config.timespan == "day"
    assert config.multiplier == 1
    assert config.broker == "kafka:9092"


def test_config_validation_valid(mock_env):
    from src.producers.stock_producer import Config

    config = Config(
        ticker="TEST",
        start_date="2022-01-01",
        end_date="2022-01-02",
        timespan="day",
        multiplier=1,
        broker="kafka:9092",
        topic="stocks",
    )

    config.validate()  # Should not raise


def test_config_validation_invalid_timespan(mock_env):
    from src.producers.stock_producer import Config

    config = Config(
        ticker="TEST",
        start_date="2022-01-01",
        end_date="2022-01-02",
        timespan="invalid",
        multiplier=1,
        broker="kafka:9092",
        topic="stocks",
    )

    with pytest.raises(ValueError, match="Invalid timespan"):
        config.validate()

def test_config_validation_invalid_multiplier(mock_env):
    from src.producers.stock_producer import Config

    config = Config(
        ticker="TEST",
        start_date="2022-01-01",
        end_date="2022-01-02",
        timespan="day",
        multiplier=0,
        broker="kafka:9092",
        topic="stocks",
    )

    with pytest.raises(ValueError, match="Multiplier must be positive"):
        config.validate()

def test_config_validation_invalid_datetime(mock_env):
    from src.producers.stock_producer import Config

    config = Config(
        ticker="TEST",
        start_date="2022-01-01",
        end_date="invalid",
        timespan="day",
        multiplier=1,
        broker="kafka:9092",
        topic="stocks",
    )

    with pytest.raises(ValueError, match="Invalid date format. Use YYYY-MM-DD"):
        config.validate()

def test_verify_env_missing_vars(monkeypatch):
    from src.producers.stock_producer import verify_env

    monkeypatch.delenv("BROKER_NAME", raising=False)

    with pytest.raises(
        ValueError, match="Missing required environment variables: BROKER_NAME"
    ):
        verify_env()


def test_serialize_data_valid(sample_stock_data):
    from src.producers.stock_producer import serialize_data

    result = serialize_data(sample_stock_data, "TEST")
    data = json.loads(result)

    assert data["ticker"] == "TEST"
    assert data["open"] == 150.0
    assert data["datetime"] == "2022-01-01T00:00:00"


def test_serialize_data_invalid():
    from src.producers.stock_producer import serialize_data

    result = serialize_data({"invalid": "data"}, "TEST")
    assert result is None


@patch("src.producers.stock_producer.requests.get")
def test_fetch_historic_data_success(mock_get, mock_env):
    from src.producers.stock_producer import fetch_historic_data

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    response = fetch_historic_data("http://test.url")
    assert response.status_code == 200


@patch("src.producers.stock_producer.requests.get")
def test_fetch_historic_data_retry(mock_get, mock_env):
    from src.producers.stock_producer import fetch_historic_data

    mock_response_fail = MagicMock()
    mock_response_fail.status_code = 429
    mock_response_fail.headers = {"Retry-After": "1"}

    mock_response_success = MagicMock()
    mock_response_success.status_code = 200

    # Simulate 2 retries followed by success
    mock_get.side_effect = [
        mock_response_fail,
        mock_response_fail,
        mock_response_success,
    ]

    with patch("src.producers.stock_producer.MAX_RETRIES", 2):
        response = fetch_historic_data("http://test.url")

    assert response.status_code == 200


@patch("src.producers.stock_producer.Producer")
def test_create_producer_success(MockProducer, mock_env):
    from src.producers.stock_producer import create_producer

    create_producer("kafka:9092")
    MockProducer.assert_called_once_with(
        {
            "bootstrap.servers": "kafka:9092",
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 1000,
            "delivery.timeout.ms": 30000,
            "message.send.max.retries": 3,
            "socket.timeout.ms": 10000,
            "message.timeout.ms": 10000,
        }
    )


@patch("src.producers.stock_producer.Producer")
def test_queue_data_success(MockProducer, mock_env):
    from src.producers.stock_producer import (
        create_producer,
        queue_data,
        delivery_callback,
    )

    mock_producer = MagicMock()
    MockProducer.return_value = mock_producer
    create_producer("kafka:9092")

    result = queue_data("test-data", "test-topic")
    assert result is True
    mock_producer.produce.assert_called_once_with(
        "test-topic", value="test-data", on_delivery=delivery_callback
    )


def test_delivery_callback_success(caplog):
    from src.producers.stock_producer import delivery_callback

    caplog.set_level(logging.DEBUG)
    mock_event = MagicMock()
    mock_event.topic.return_value = "test-topic"
    mock_event.partition.return_value = 0
    mock_event.value.return_value = b"test-value"

    delivery_callback(None, mock_event)
    assert "test-value sent to test-topic" in caplog.text


def test_delivery_callback_error(caplog):
    from src.producers.stock_producer import delivery_callback

    mock_event = MagicMock()
    mock_err = MagicMock()

    delivery_callback(mock_err, mock_event)
    assert "Produce to topic" in caplog.text


@patch("src.producers.stock_producer.requests.get")
@patch("src.producers.stock_producer.Config.from_env")
@patch("src.producers.stock_producer.queue_data", return_value=True)
@patch(
    "src.producers.stock_producer.serialize_data",
    return_value='{"ticker": "TEST", "timestamp": 1640995200000}',
)
def test_main_success(
    mock_serialize_data, mock_queue_data, mock_config, mock_get, mock_env, caplog
):
    from src.producers.stock_producer import main

    mock_config.return_value.validate.return_value = None
    caplog.set_level(logging.INFO)
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "results": [
            {
                "v": 1000,
                "vw": 150.5,
                "o": 150.0,
                "c": 151.0,
                "h": 152.0,
                "l": 149.5,
                "t": 1640995200000,
                "n": 500,
            }
        ],
        "next_url": None,
    }
    mock_get.return_value = mock_response

    mock_producer_instance = MagicMock()
    mock_producer_instance.flush.return_value = 0

    with patch("src.producers.stock_producer.producer", mock_producer_instance):
        main()

    assert "DONE FETCHING HISTORIC DATA" in caplog.text
