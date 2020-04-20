import pytest
import spacy

from src.nlp_processor.processor import prepare_document, process_document


@pytest.fixture
def test_doc():
    return b"test document\n asdas John Doe, and Foo Bar or Foo Bar"


@pytest.fixture
def test_doc_string():
    return "test document asdas John Doe, and Foo Bar or Foo Bar"


def test_prepare_document(test_doc):
    expected_doc = "test document asdas John Doe, and Foo Bar or Foo Bar"
    assert expected_doc == prepare_document(test_doc)


def test_process_document(test_doc, test_doc_string):
    nlp = spacy.load('en_core_web_sm')
    allowed_entities = {'PERSON'}

    tf, num_ents = process_document(test_doc, nlp, allowed_entities)
    assert 1 == tf['John Doe']
    assert 2 == tf['Foo Bar']
    assert 3 == num_ents

    tf, num_ents = process_document(test_doc_string, nlp, allowed_entities)
    assert 1 == tf['John Doe']
    assert 2 == tf['Foo Bar']
    assert 3 == num_ents
