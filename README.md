# NLP processing example in Kafka with spaCy 

A pipeline that utilizes the [faust-s3-backed-serializer](https://github.com/bakdata/faust-s3-backed-serializer) to process large text files in Kafka and runs the spaCy named entity recognition for persons and organizations. Finally, it computes the frequency of every named entity per document, the TF-IDF score, and extracts the most important named entity for every document.

The example consists of three different applications:

* **Producer**: Loads files from a local source directory, uploads them to S3, and sends a record with the corresponding S3 pointer to a specific Kafka topic.
* **Loader**: Stream processor that processes the records produced by the **Producer**, loads the content of the files from S3, and uses [faust-s3-backed-serializer](https://github.com/bakdata/faust-s3-backed-serializer) for serialization.
* **NLP-Stream Processor**: Stream processor that processes the records produced by the **Loader**. It calculates the TF-IDF score for all recognized named entities in incoming documents.

## Example

The example uses a S3 mock for testing the application locally.
Start kafka and a s3 mock for testing locally:

```
docker-compose up
```

To get the example data run: 

```
sh get_data.sh
```

it will create a folder `./data` containing text files to run the example.

The example was developed with Python 3.7.4.
Install all python dependencies:

```
pip install requirements.txt
```

Install the spaCy language model:

```
python -m spacy download en_core_web_sm
```

### Producer

Start:
```
cd src
python producer.py --data_path=../data --s3_bucket=data --s3_bucket_dir=input_data --s3endpoint_url=http://localhost:4572 --output_topic=InputAWS --broker_url=localhost:9094
```

### Loader

Start:
```
export KAFKA_BROKER_URL=localhost:9094
faust -A src.loader worker -l info
```


## NLP Stream Application

Start:

```
export KAFKA_BROKER_URL=localhost:9094
faust -A src.nlp_stream_processor worker -l info -p 6060
```