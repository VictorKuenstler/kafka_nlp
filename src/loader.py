import logging
import os

import boto3
import faust

from src.utils import S3BackedSerializerEndpoint, parse_uri_s3

S3_ENDPOINT = 'http://localhost:4572'
S3_BASE_PATH = 's3://data'
S3_REGION_NAME = 'eu-central-1'

INPUT_TOPIC = 'InputAWS'
OUTPUT_TOPIC = 'Documents'

s3_backed_serializer = S3BackedSerializerEndpoint(output_topic=OUTPUT_TOPIC,
                                                  base_path=S3_BASE_PATH,
                                                  region_name=S3_REGION_NAME,
                                                  max_size=0,
                                                  is_key=False,
                                                  s3endpoint_url=S3_ENDPOINT)


logger = logging.getLogger(__name__)
app = faust.App('loader', broker=os.getenv('KAFKA_BROKER_URL'))

input_topic = app.topic(INPUT_TOPIC, value_serializer='raw')
OUTPUT_TOPIC = app.topic(OUTPUT_TOPIC, value_serializer=s3_backed_serializer)

s3_client = boto3.client('s3', endpoint_url=S3_ENDPOINT)


@app.agent(input_topic)
async def aws_urls(input):
    async for url in input:
        bucket, key = parse_uri_s3(url.decode())
        object_metadata = s3_client.get_object(Bucket=bucket, Key=key)
        text = object_metadata["Body"].read().decode()
        logging.info(f'Load file {url.decode()}')
        await OUTPUT_TOPIC.send(key=url.decode(), value=text, value_serializer=s3_backed_serializer)


if __name__ == '__main__':
    app.main()