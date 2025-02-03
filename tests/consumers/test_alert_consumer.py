# tests/test_src.consumers.alert_consumer.py
import os
import json
import logging
from unittest.mock import patch, MagicMock, call
from typing import Any
import pytest
from confluent_kafka import KafkaError, KafkaException
from slack_sdk.errors import SlackApiError
from src.consumers.alert_consumer import (
    verify_env,
    create_consumer,
    subscribe_to_topic,
    deserialize_data,
    slack_alert,
    handle_tp_alert,
    handle_pc_alert,
    get_slack_client,
    shutdown_consumer,
    MaxRetriesExceededError,
    main,
    poll_data,
)


# Fixtures
@pytest.fixture
def mock_env(monkeypatch):
    """Mock required environment variables"""
    monkeypatch.setenv("BROKER_NAME", "kafka")
    monkeypatch.setenv("BROKER_LISTENER_PORT", "9092")
    monkeypatch.setenv("ALERT_TOPIC", "alerts")
    monkeypatch.setenv("TARGET_PRICE_ALERT", "target_price")
    monkeypatch.setenv("PERCENTAGE_CHANGE_ALERT", "percentage_change")
    monkeypatch.setenv("SLACK_TOKEN", "xoxb-test-token")
    monkeypatch.setenv("SLACK_CHANNEL", "test-channel")
    monkeypatch.setenv("CONSUMER_GROUP", "test-group")


@pytest.fixture
def mock_slack_client():
    with patch("src.consumers.alert_consumer.WebClient") as mock:
        yield mock


@pytest.fixture
def mock_consumer():
    with patch("src.consumers.alert_consumer.Consumer") as mock:
        yield mock


@pytest.fixture
def mock_admin_client():
    with patch("src.consumers.alert_consumer.AdminClient") as mock:
        yield mock


@pytest.fixture
def sample_alert_event():
    return {
        "alert_type": "target_price",
        "ticker": "TEST",
        "datetime": "2023-01-01T00:00:00",
        "target_price": 100.0,
        "current_price": 105.0,
    }


# Test Cases
def test_verify_env_success(mock_env):
    verify_env()


def test_verify_env_missing_vars(monkeypatch):
    monkeypatch.delenv("BROKER_NAME", raising=False)
    with pytest.raises(ValueError, match="BROKER_NAME"):
        verify_env()


def test_create_consumer_success(mock_env, mock_consumer):
    create_consumer("kafka:9092")
    mock_consumer.assert_called_once_with(
        {
            "bootstrap.servers": "kafka:9092",
            "group.id": "test-group",
            "auto.offset.reset": "earliest",
        }
    )


def test_create_consumer_retries_exceeded(mock_env, mock_consumer):
    mock_error = MagicMock()
    mock_error.retriable.return_value = True
    kafka_exception = KafkaException(mock_error)
    mock_consumer.side_effect = [kafka_exception] * 2

    with patch("src.consumers.alert_consumer.MAX_RETRIES", 2):
        with pytest.raises(MaxRetriesExceededError):
            create_consumer("kafka:9092")

    assert mock_consumer.call_count == 2


def test_subscribe_to_topic_success(mock_env, mock_admin_client, mock_consumer):
    mock_topic_metadata = MagicMock()
    mock_topic_metadata.topics = {"alerts": MagicMock()}
    mock_admin_client.return_value.list_topics.return_value = mock_topic_metadata

    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance
    create_consumer("kafka:9092")

    subscribe_to_topic("kafka:9092", "alerts")

    mock_admin_client.return_value.list_topics.assert_called_with(timeout=10)
    mock_consumer_instance.subscribe.assert_called_once_with(["alerts"])


def test_subscribe_to_topic_retries_exceeded(
    mock_env, mock_admin_client, mock_consumer
):
    mock_error = MagicMock()
    mock_error.retriable.return_value = True
    kafka_exception = KafkaException(mock_error)

    mock_topic_metadata = MagicMock()
    mock_topic_metadata.topics = {"alerts": MagicMock()}
    mock_admin_client.return_value.list_topics.return_value = mock_topic_metadata

    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance
    mock_consumer_instance.subscribe.side_effect = [kafka_exception] * 2
    create_consumer("kafka:9092")
    with patch("src.consumers.alert_consumer.MAX_RETRIES", 2):
        with pytest.raises(MaxRetriesExceededError):
            subscribe_to_topic("kafka:9092", "alerts")


def test_deserialize_data_valid():
    data = json.dumps({"test": "data"})
    assert deserialize_data(data) == {"test": "data"}


def test_deserialize_data_invalid():
    assert deserialize_data("invalid") is None


def test_slack_alert_success(mock_slack_client, mock_env, sample_alert_event):
    slack_alert(sample_alert_event, mock_slack_client.return_value)
    mock_slack_client.return_value.chat_postMessage.assert_called_once_with(
        channel="test-channel",
        text="2023-01-01T00:00:00 TEST price has reached the target of $100.0. Current price: $105.0",
    )


def test_slack_alert_rate_limiting(
    mock_slack_client, mock_env, sample_alert_event, caplog
):
    caplog.set_level(logging.DEBUG)
    mock_client = mock_slack_client.return_value

    response = MagicMock()
    response.status_code = 429
    response.headers = {"Retry-After": "1"}
    error = SlackApiError("rate_limited", response=response)

    mock_client.chat_postMessage.side_effect = [
        error,
        MagicMock(),
    ]

    with patch("src.consumers.alert_consumer.MAX_RETRIES", 3):
        slack_alert(sample_alert_event, mock_client)

    assert "Rate limited. Retrying in 1 seconds" in caplog.text
    assert mock_client.chat_postMessage.call_count == 2


def test_handle_tp_alert(sample_alert_event):
    message = handle_tp_alert(sample_alert_event)
    assert (
        message
        == "2023-01-01T00:00:00 TEST price has reached the target of $100.0. Current price: $105.0"
    )


def test_handle_pc_alert_positive():
    event = {
        "alert_type": "percentage_change",
        "price_change_percentage": 5.0,
        "current_price": 105.0,
        "ticker": "TEST",
        "datetime": "2023-01-01T00:00:00",
    }
    message = handle_pc_alert(event)
    assert "increased by 5.0%" in message


def test_handle_pc_alert_negative():
    event = {
        "alert_type": "percentage_change",
        "price_change_percentage": -5.0,
        "current_price": 95.0,
        "ticker": "TEST",
        "datetime": "2023-01-01T00:00:00",
    }
    message = handle_pc_alert(event)
    assert "decreased by -5.0%" in message


def test_shutdown_consumer_success(mock_consumer):
    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance

    create_consumer("kafka:9092")
    shutdown_consumer()
    mock_consumer_instance.commit.assert_called_once()
    mock_consumer_instance.close.assert_called_once()


def test_shutdown_consumer_retries(mock_consumer, caplog):
    caplog.set_level(logging.ERROR)

    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance

    mock_error = MagicMock()
    mock_error.retriable.return_value = True
    kafka_exception = KafkaException(mock_error)

    create_consumer("kafka:9092")
    mock_consumer_instance.commit.side_effect = [kafka_exception] * 2
    with patch("src.consumers.alert_consumer.MAX_RETRIES", 2):
        shutdown_consumer()

    assert "Retryable kafka error during shutdown" in caplog.text
    assert mock_consumer.return_value.commit.call_count == 2
    mock_consumer_instance.close.assert_called_once()


@patch("src.consumers.alert_consumer.shutdown_consumer")
@patch("src.consumers.alert_consumer.verify_env")
@patch("src.consumers.alert_consumer.create_consumer")
@patch("src.consumers.alert_consumer.subscribe_to_topic")
@patch("src.consumers.alert_consumer.poll_data")
@patch("src.consumers.alert_consumer.get_slack_client")
def test_main_success(
    mock_get_slack,
    mock_poll,
    mock_subscribe,
    mock_create,
    mock_verify,
    mock_shutdown,
    mock_env,
    caplog,
):
    caplog.set_level(logging.DEBUG)
    mock_poll.side_effect = KeyboardInterrupt

    main()

    mock_verify.assert_called_once()
    mock_create.assert_called_once_with("kafka:9092")
    mock_subscribe.assert_called_once_with("kafka:9092", "alerts")
    mock_get_slack.assert_called_once()
    mock_poll.assert_called_once_with("alerts")
    mock_shutdown.assert_called_once()

    assert "Keyboard interrupt" in caplog.text


@patch("src.consumers.alert_consumer.verify_env")
@patch("src.consumers.alert_consumer.create_consumer")
@patch("src.consumers.alert_consumer.shutdown_consumer")
def test_main_kafka_error(mock_shutdown, mock_create, mock_verify, mock_env, caplog):
    caplog.set_level(logging.ERROR)
    mock_create.side_effect = KafkaException(KafkaError(KafkaError._ALL_BROKERS_DOWN))
    main()
    assert "Exiting due to Kafka Exception" in caplog.text


@patch("src.consumers.alert_consumer.slack_alert")
@patch("src.consumers.alert_consumer.deserialize_data")
@patch("src.consumers.alert_consumer.consumer")
@patch("src.consumers.alert_consumer.get_slack_client")
def test_poll_data_success(
    mock_get_slack, mock_consumer, mock_deserialize, mock_slack, caplog
):
    caplog.set_level(logging.DEBUG)
    
    mock_event = MagicMock()
    mock_event.error.return_value = None
    mock_event.value.return_value = b'{"valid": "data"}'
    
    mock_consumer.poll.side_effect = [mock_event, KeyboardInterrupt()]
    mock_deserialize.return_value = {"valid": "data"}
    
    mock_slack_instance = MagicMock()
    mock_get_slack.return_value = mock_slack_instance
    
    from src.consumers import alert_consumer
    with patch.object(alert_consumer, "slack_client", mock_slack_instance):
        with patch("src.consumers.alert_consumer.MAX_RETRIES", 1):
            with pytest.raises(KeyboardInterrupt):
                poll_data("alerts")
    
    mock_slack.assert_called_once_with({"valid": "data"}, mock_slack_instance)


def test_poll_data_max_retries(mock_consumer):
    mock_error = MagicMock()
    mock_error.retriable.return_value = True
    error = mock_error

    mock_event = MagicMock()
    mock_event.error.return_value = error
    
    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance
    create_consumer("kafka:9092")

    mock_consumer_instance.poll.return_value = mock_event

    with patch("src.consumers.alert_consumer.MAX_RETRIES", 1):
        with pytest.raises(MaxRetriesExceededError):
            poll_data("alerts")


def test_get_slack_client(mock_env):
    with patch("src.consumers.alert_consumer.WebClient") as mock_slack:
        get_slack_client()
        mock_slack.assert_called_once_with(token="xoxb-test-token")
