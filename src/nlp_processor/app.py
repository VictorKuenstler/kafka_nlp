import inspect
import logging
import os
import math
from concurrent.futures.thread import ThreadPoolExecutor

import faust
import spacy

from src.nlp_processor.processor import process_document
from src.nlp_processor.models import TFIDF_Score, TermFrequency
from src.utils import S3BackedSerializerEndpoint

S3_ENDPOINT = 'http://localhost:4572'

logger = logging.getLogger(__name__)
s3_backed_serializer = S3BackedSerializerEndpoint(base_path="s3://data",
                                                  region_name="eu-central-1",
                                                  max_size=0,
                                                  is_key=False,
                                                  s3endpoint_url=S3_ENDPOINT)

app = faust.App('loader', broker=os.getenv('KAFKA_BROKER_URL'), stream_wait_empty=False)

# Topics
input_topic = app.topic('Documents', key_type=str, value_serializer=s3_backed_serializer)
tfidf_score_topic = app.topic('TFIDFScore', key_type=str, value_type=TFIDF_Score, partitions=1)
tf_topic = app.topic('TermFrequencies', key_type=str, value_type=TermFrequency, partitions=1)
most_important_term = app.topic('MostImportantTerm', key_type=str, value_type=TermFrequency, partitions=1)

# Tables
named_entities = app.Table('named_entities', default=int, partitions=1)
document_count = app.Table('document_count', default=int, partitions=1)
DOCUMENTS_KEY = 'documents'
document_frequency = app.Table('document_frequency', default=int, partitions=1)
most_important_term_tfidf = app.Table('most-imp-term-tfidf', default=int, partitions=1)

# spacy labels for allowed entities
ALLOWED_ENTITIES = {'PERSON'}
nlp = spacy.load('en_core_web_sm')

thread_pool = ThreadPoolExecutor(max_workers=1)


@app.agent(input_topic)
async def documents(stream):
    async for key, document in stream.items():
        document_count[DOCUMENTS_KEY] += 1
        # set the value for the highest tf-idf score per document
        most_important_term_tfidf[key] = 0
        tf, num_ents = await app.loop.run_in_executor(thread_pool, process_document, document, nlp, ALLOWED_ENTITIES)
        for term in tf.keys():
            document_frequency[term] += 1
            named_entities[term] += tf[term]
            value = TermFrequency(document=key, term=term, tf=tf[term] / num_ents,
                                  inverse_document=document_frequency[term],
                                  document_count=document_count[DOCUMENTS_KEY])
            await tf_topic.send(key=key, value=value)
            yield value


@app.agent(tf_topic)
async def tf_idf(stream: faust.Stream[TermFrequency]):
    async for key, tf in stream.items():
        doc_freq = document_frequency[tf.term]
        doc_freq = float(max(doc_freq, tf.inverse_document))
        num_documents = max(float(document_count[DOCUMENTS_KEY]), tf.document_count)
        idf: float = math.log(num_documents / doc_freq)
        value = TFIDF_Score(document=tf.document, tfidf=tf.tf * idf, term=tf.term)

        await tfidf_score_topic.send(key=key, value=value)
        yield value


@app.agent(tfidf_score_topic)
async def out(stream: faust.Stream[TFIDF_Score]):
    async for key, document_tfs in stream.items():
        most_important = most_important_term_tfidf[document_tfs.document]
        if float(document_tfs.tfidf) > most_important:
            most_important_term_tfidf[document_tfs.document] = document_tfs.tfidf
            logging.info(
                f'Update the most important term for document {document_tfs.document} with term: {document_tfs.term}'
            )
            value = TFIDF_Score(document=document_tfs.document, tfidf=document_tfs.tfidf, term=document_tfs.term)
            await most_important_term.send(key=key, value=value)
            yield 1
        yield 0


if __name__ == '__main__':
    app.main()
