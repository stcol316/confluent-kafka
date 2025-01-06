import pytest
from unittest.mock import Mock, patch
from src.producers.producer import fetch_data, queue_data
from click.testing import CliRunner

TEST_ARGS = [
        "--url", "https://api.open-meteo.com/v1/forecast",
        "--topic", "test_topic",
        "--lat", 54.51,
        "--long", -6.04,
        "--params", '["temperature_2m"]'
    ]

# Mock response for the weather API
@pytest.fixture
def mock_weather_response():
    print("Mocking weather response")
    return {"weather": "response"}

@pytest.fixture
def mock_requests_get(mock_weather_response):
    print("Mocking requests.get")
    with patch('requests.get') as mock_get:
        mock_get.return_value = Mock(
            status_code=200,
            json=Mock(return_value=mock_weather_response)
        )
        yield mock_get

@pytest.fixture
def mock_producer():
    print("Mocking producer")
    with patch('src.producers.producer.producer') as mock_prod:
        mock_prod.produce = Mock()
        mock_prod.flush = Mock()
        yield mock_prod

def test_queue_data(mock_producer):
    print("Testing queue_data")
    # Test data
    test_data = {"test": "data"}
    test_topic = "test_topic"
    
    # Call the function
    queue_data(test_data, test_topic)
    
    # Assert the producer was called correctly
    mock_producer.produce.assert_called_once()
    mock_producer.flush.assert_called_once()

def test_fetch_data_success(mock_requests_get, mock_producer):    
    print("Testing fetch_data success")
    
    runner = CliRunner()
    counter =  0
    mock_get = Mock()
    
    #Set up mock to succeed once and exit on the second loop
    def side_effect(*args, **kwargs):
        nonlocal counter
        counter += 1
        
        mock_response = Mock()
        if counter == 1:
            mock_response.status_code = 200
            mock_response.json.return_value = {"data": "test"}
        else:
            mock_response.status_code = 500
            mock_response.json.side_effect = Exception("BOOOOM")
        return mock_response

    mock_get.side_effect = side_effect
    with patch('requests.get', mock_get), \
         patch('time.sleep', return_value=None):
             
        result = runner.invoke(fetch_data, TEST_ARGS)
   
    print(f"Mock after invoke: {mock_get.call_count}")
    assert mock_get.call_count == 2
       
def test_fetch_data_api_error(mock_requests_get, mock_producer):
    print("Testing fetch_data api error")
    
    # Setup mock to return error
    mock_requests_get.return_value.status_code = 404
    runner = CliRunner()
    result = runner.invoke(fetch_data, TEST_ARGS)
    print(f"Result: {result.exit_code}")
    
    assert result.exit_code == 1
    mock_producer.flush.assert_called_once()

def test_fetch_data_exception(mock_requests_get, mock_producer):
    print("Testing fetch_data exception")
    
    # Setup mock to raise an exception
    mock_requests_get.side_effect = Exception("Test error")
    
    runner = CliRunner()
    runner.invoke(fetch_data, TEST_ARGS)
    
    mock_producer.flush.assert_called_once()
    
