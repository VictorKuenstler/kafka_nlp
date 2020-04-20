import asyncio
import math
from unittest.mock import patch, Mock

import pytest

from src.nlp_processor.models import TermFrequency, TFIDF_Score
from src.nlp_processor.processor import process_document
from src.nlp_processor.app import app, documents, nlp, ALLOWED_ENTITIES, tf_idf, out, most_important_term_tfidf


@pytest.fixture()
def test_app(event_loop):
    app.finalize()
    app.conf.store = 'memory://'
    app.flow_control.resume()
    return app


@pytest.fixture
def test_document():
    return 'test document asdas John Doe, and Foo Bar or Foo Bar'


def mock_coro(return_value=None, **kwargs):
    async def wrapped(*args, **kwargs):
        return return_value

    return Mock(wraps=wrapped, **kwargs)


@pytest.mark.asyncio()
async def test_documents(test_app, test_document):
    result = process_document(test_document, nlp, ALLOWED_ENTITIES)
    executor_return_value = asyncio.Future()
    executor_return_value.set_result(result)

    s3_path = 's3://testbucket/test.txt'

    with patch('src.nlp_processor.app.input_topic') as mocked_input_topic:
        with patch('src.nlp_processor.app.tf_topic') as mocked_tf_topic:
            with patch('src.nlp_processor.app.app.loop.run_in_executor', return_value=executor_return_value):
                mocked_input_topic.send = mock_coro()
                mocked_tf_topic.send = mock_coro()
                async with documents.test_context() as agent:
                    await agent.put(key=s3_path, value=test_document)

                    expected_tf_message = TermFrequency(document=s3_path, term='Foo Bar', tf=2.0 / 3.0,
                                                        inverse_document=1,
                                                        document_count=1)
                    mocked_tf_topic.send.assert_called_with(
                        key=s3_path,
                        value=expected_tf_message
                    )


@pytest.mark.asyncio()
async def test_tf_idf(test_app):
    s3_path = 's3://testbucket/test.txt'.encode()

    term = 'John Doe'
    tf = 0.33
    id = 5
    doc_count = 20
    input_message = TermFrequency(document=s3_path,
                                  term=term,
                                  tf=tf,
                                  inverse_document=id,
                                  document_count=doc_count)
    with patch('src.nlp_processor.app.tf_topic') as mocked_input_topic:
        with patch('src.nlp_processor.app.tfidf_score_topic') as mocked_out_topic:
            mocked_input_topic.send = mock_coro()
            mocked_out_topic.send = mock_coro()

            async with tf_idf.test_context() as agent:
                await agent.put(key=s3_path, value=input_message)

                expected_tf_idf = math.log(doc_count / id) * tf
                expected_tfidf_score = TFIDF_Score(
                    document=s3_path,
                    tfidf=expected_tf_idf,
                    term=term
                )

                mocked_out_topic.send.assert_called_with(
                    key=s3_path,
                    value=expected_tfidf_score
                )


@pytest.mark.asyncio()
async def test_outf(test_app):
    s3_path = 's3://testbucket/test.txt'

    input_message = TFIDF_Score(
        document=s3_path,
        tfidf=0.33,
        term='John Doe'
    )

    input_message2 = TFIDF_Score(
        document=s3_path,
        tfidf=0.23,
        term='Jane Doe'
    )

    input_message3 = TFIDF_Score(
        document=s3_path,
        tfidf=0.66,
        term='Foo Bar'
    )

    with patch('src.nlp_processor.app.tfidf_score_topic') as mocked_input_topic:
        with patch('src.nlp_processor.app.most_important_term') as mocked_out_topic:
            mocked_input_topic.send = mock_coro()
            mocked_out_topic.send = mock_coro()

            async with out.test_context() as agent:
                await agent.put(key=s3_path, value=input_message)
                mocked_out_topic.send.assert_called_with(
                    key=s3_path,
                    value=input_message
                )
                assert 1 == mocked_out_topic.send.call_count
                assert most_important_term_tfidf[input_message.document] == input_message.tfidf

                await agent.put(key=s3_path, value=input_message2)
                # ensure that no other message was send to most important term topic
                assert 1 == mocked_out_topic.send.call_count
                assert most_important_term_tfidf[input_message.document] == input_message.tfidf

                await agent.put(key=s3_path, value=input_message3)
                mocked_out_topic.send.assert_called_with(
                    key=s3_path,
                    value=input_message3
                )
                assert 2 == mocked_out_topic.send.call_count
                assert input_message3.tfidf == most_important_term_tfidf[input_message3.document]
