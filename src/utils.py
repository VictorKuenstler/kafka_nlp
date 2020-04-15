from typing import Any, Tuple, Optional, Dict
from urllib.parse import urlparse

import boto3
from faust_s3_backed_serializer import S3BackedSerializer


class S3BackedSerializerEndpoint(S3BackedSerializer):
    def __init__(self, output_topic: Optional[str] = None, base_path: Optional[str] = None,
                 region_name: Optional[str] = None,
                 s3_credentials: Optional[Dict[str, str]] = None,
                 max_size: int = int(1e6),
                 s3endpoint_url: str = None,
                 is_key: bool = False,
                 **kwargs):
        super().__init__(output_topic=output_topic, base_path=base_path, max_size=max_size,
                         s3_credentials=s3_credentials, region_name=region_name, is_key=is_key,
                         **kwargs)

        if s3endpoint_url is not None:
            self._s3_config.update(endpoint_url=s3endpoint_url)
            self._s3_client = boto3.client("s3", **self._s3_config)


def parse_uri_s3(uri):
    result = urlparse(uri)
    bucket = result.netloc
    if result.path[0] == "/":
        path = result.path[1:]
    else:
        path = result.path
    return bucket, path