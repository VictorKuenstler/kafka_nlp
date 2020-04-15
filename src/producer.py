import glob
import os
from time import sleep

import boto3
import click
from kafka import KafkaProducer
import logging


def get_files_in_dir(dir_path, file_filter):
    return glob.glob(f'{dir_path}/{file_filter}')


def on_send_error(excp):
    logging.error('Failed to send message', exc_info=excp)


@click.command()
@click.option('--broker_url', help='Kafka broker url', required=True)
@click.option('--data_path', help='Path to the data', required=True)
@click.option('--s3_bucket', help='Bucket name', required=True)
@click.option('--s3_bucket_dir', help='Directory in the bucket', required=True)
@click.option('--output_topic', help='The topic the producer sends to', required=True)
@click.option('--file_filter', default='*.txt', help='Filter to select specific files')
@click.option('--s3endpoint_url', default=None, help='Custom endpoint for s3')
def start_producer(broker_url, data_path, s3_bucket, s3_bucket_dir, output_topic, file_filter, s3endpoint_url):
    producer = KafkaProducer(bootstrap_servers=broker_url)

    files = get_files_in_dir(data_path, file_filter)

    s3_client = boto3.client('s3')
    if s3endpoint_url is not None:
        s3_client = boto3.client('s3', endpoint_url=s3endpoint_url)

    # check if bucket exists:
    if s3_bucket not in [bucket['Name'] for bucket in s3_client.list_buckets()['Buckets']]:
        s3_client.create_bucket(Bucket=s3_bucket)
        logging.info(f'Created bucket: {s3_bucket}')

    for file in files:
        filename = os.path.basename(file)
        key = f'{s3_bucket_dir}/{filename}'
        s3_client.upload_file(file, s3_bucket, key)
        message = f's3://{s3_bucket}/{key}'.encode()
        producer.send(output_topic, key=message, value=message) \
            .add_callback(logging.info(f'Send message for file {file}')) \
            .add_errback(on_send_error)
        sleep(2)


if __name__ == '__main__':
    start_producer()
