import io
import gzip
import boto3
import pickle
import logging
import datetime
from time import time
from botocore.exceptions import ClientError
from collections import OrderedDict
from typing import Optional, Type, TypeVar, Dict
import dbm
from w3lib.url import file_uri_to_path

from os.path import basename, dirname, exists, join
from scrapy.exceptions import CloseSpider, NotConfigured
from scrapy.http import Headers
from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.responsetypes import responsetypes
from scrapy.utils.project import data_path
from scrapy.utils.request import request_fingerprint
from scrapy.settings import Settings
from scrapy.statscollectors import StatsCollector
from six.moves import cPickle as pickle

logger = logging.getLogger(__name__)


def hash_request(request: Request) -> str:
    return request_fingerprint(request)


def serialize_response(response: Response) -> Dict:
    return {
        "status": response.status,
        "url": response.url,
        "headers": dict(response.headers),
        "body": gzip.compress(response.body),

    }


def deserialize_response(response_serialized: Dict) -> Response:
    url = response_serialized["url"]
    status = response_serialized["status"]
    headers = Headers(response_serialized["headers"])
    body = gzip.decompress(response_serialized["body"])
    respcls = responsetypes.from_args(headers=headers, url=url)
    original_response = respcls(url=url, headers=headers, status=status, body=body)
    return original_response


class DbmTimeMachineStorage:
    time_machine_dir = "timemachine"

    def __init__(self, settings):
        self.db = None
        self.snapshot_uri = None

    def set_uri(self, uri, uri_params, retrieve=False):
        self.snapshot_uri = file_uri_to_path(uri % uri_params)
        path = dirname(self.snapshot_uri)
        path = data_path(path, createdir=True)
        db_name = basename(self.snapshot_uri)
        self.snapshot_uri = join(path, db_name)

    def is_uri_valid(self):
        return exists(self.snapshot_uri)

    def open_spider(self, spider):
        self._spider = spider

        self._prepare_time_machine()

        if not self.snapshot_uri:
            raise CloseSpider("Snapshot uri not configured.")

        self.db = dbm.open(self.snapshot_uri, "c")

        logger.debug(
            "Using DBM time machine storage in %(dbpath)s"
            % {"dbpath": self.snapshot_uri},
            extra={"spider": spider},
        )

    def _prepare_time_machine(self):
        pass

    def close_spider(self, spider):
        if self.db:
            self.db.close()

        self._finish_time_machine()

    def _finish_time_machine(self):
        pass

    def retrieve_response(self, spider, request):
        data = self._read_data(spider, request)
        if data is None:
            return  # not stored
        url = data["url"]
        status = data["status"]
        headers = Headers(data["headers"])
        body = gzip.decompress(data["body"])
        respcls = responsetypes.from_args(headers=headers, url=url)
        response = respcls(url=url, headers=headers, status=status, body=body)
        return response

    def store_response(self, spider, request, response):
        key = self._request_key(request)
        data = {
            "status": response.status,
            "url": response.url,
            "headers": dict(response.headers),
            "body": gzip.compress(response.body),
        }
        data = pickle.dumps(data, protocol=2)
        self.db["%s_data" % key] = data
        self.db["%s_time" % key] = str(time())

    def _read_data(self, spider, request):
        key = self._request_key(request)
        db = self.db
        tkey = f"{key}_time"
        if tkey not in db:
            return  # not found

        return pickle.loads(db[f"{key}_data"])

    def _request_key(self, request):
        return request_fingerprint(request)


class S3TimeMachineStorage:
    def __init__(self, settings: Settings, stats: StatsCollector) -> None:
        if not settings.get("ADDONS_AWS_ACCESS_KEY_ID"):
            raise NotConfigured
        if not settings.get("ADDONS_AWS_SECRET_ACCESS_KEY"):
            raise NotConfigured
        if not settings.get("ADDONS_S3_BUCKET"):
            raise NotConfigured

        self.stats = stats

        self.start_date = datetime.datetime.now()

        self.invalid = False

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=settings.get("ADDONS_AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=settings.get("ADDONS_AWS_SECRET_ACCESS_KEY"),
        )

        self.s3_bucket = settings.get("ADDONS_S3_BUCKET")

        self.response_cache: FixedSizedDict = FixedSizedDict(max_size=self.buffer_size)

    def setS3bucket(self, uri_params, retrieve=False):
        s3bucket = self.s3_bucket
        return s3bucket
    def is_uri_valid(self):
        return exists(self.s3_bucket)
    def _request_key(self, request):
        return request_fingerprint(request)

    def open_spider(self, spider):
        self._spider = spider

        self._prepare_time_machine()

        if not self.s3_bucket:
            raise CloseSpider("S3 bucket not configured.")

        logger.debug(
            "Using S3 time machine storage in %(s3path)s"
            % {"s3path": self.s3_bucket},
            extra={"spider": spider},
        )

    def _prepare_time_machine(self):
        pass

    def close_spider(self, spider):
        self._finish_time_machine()

    def _finish_time_machine(self):
        pass

    def restore_response(self, request: Request):
        request_hash = hash_request(request)
        if request_hash in self.response_cache:
            return self.response_cache[request_hash]
        s3_object_key = f"{self.s3_bucket}/{request_hash}"
        binary_object_data = self.download_s3_object(s3_object_key)

        if not binary_object_data:
            logger.error(f"Hash {request_hash} not found for URL {request.url}")
            return
        serialized_response = pickle.loads(binary_object_data)
        response = deserialize_response(serialized_response)
        response.flags.append("cached_replay")
        self.response_cache.append(request_hash, response)
        return response

    def download_s3_object(self, object_key):
        data_buffer = io.BytesIO()
        try:
            self.s3_client.download_fileobj(self.s3_bucket, object_key, data_buffer)
            return data_buffer.getvalue()
        except ClientError:

            return

class FixedSizedDict:

    def __init__(self, max_size):
        self._data = OrderedDict()

        self._max_size = max_size

    def __contains__(self, data):
        return (data in self._data)

    def __getitem__(self, key):
        return self._data[key]

    def append(self, key, data):
        self._data[key] = data

        if len(self._data) > self._max_size:
            self._data.popitem(last=False)
