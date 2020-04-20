import pytest
from io import BytesIO
from unittest.mock import Mock, patch

from src.loader import app, aws_urls, s3_backed_serializer


@pytest.fixture()
def test_app(event_loop):
    app.finalize()
    app.conf.store = 'memory://'
    app.flow_control.resume()
    return app


def mock_coro(return_value=None, **kwargs):
    async def wrapped(*args, **kwargs):
        return return_value

    return Mock(wraps=wrapped, **kwargs)


@pytest.mark.asyncio()
async def test_loader(test_app):
    test_string = 'test_string'
    mocked_response_get_object = {
        "Body": BytesIO(test_string.encode())
    }
    mocked_response_put_object = {
        "ResponseMetadata": {
            "HTTPStatusCode": 200
        }
    }

    bucket = 's3://testbucket/test.txt'

    with patch('src.loader.input_topic') as mocked_input_topic:
        with patch('src.loader.OUTPUT_TOPIC') as mocker_output_topic:
            with patch('src.loader.s3_client.get_object', return_value=mocked_response_get_object):
                with patch('src.loader.s3_backed_serializer._s3_client.get_object',
                           return_value=mocked_response_get_object):
                    with patch('src.loader.s3_backed_serializer._s3_client.put_object',
                               return_value=mocked_response_put_object):
                        mocked_input_topic.send = mock_coro()
                        mocker_output_topic.send = mock_coro()
                        s3_path = bucket.encode()
                        async with aws_urls.test_context() as agent:
                            event = await agent.put(key=s3_path, value=s3_path)
                            assert test_string == agent.results[event.message.offset]
                            mocker_output_topic.send.assert_called_with(
                                key=bucket,
                                value=test_string,
                                value_serializer=s3_backed_serializer)
