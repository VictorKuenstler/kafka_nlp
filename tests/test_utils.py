from src.utils import parse_uri_s3


def test_parse_uri_s3():
    s3_uri = 's3://test_bucket/path'
    bucket, path = parse_uri_s3(s3_uri)
    assert 'test_bucket' == bucket
    assert 'path' == path
