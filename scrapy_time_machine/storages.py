import gzip
import logging
from time import time
import dbm
from w3lib.url import file_uri_to_path
import boto3

from os.path import basename, dirname, exists, join
from scrapy.exceptions import CloseSpider
from scrapy.http import Headers
from scrapy.responsetypes import responsetypes
from scrapy.utils.project import data_path
from scrapy.utils.request import request_fingerprint
from six.moves import cPickle as pickle

logger = logging.getLogger(__name__)


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


class S3TimeMachineStorage(DbmTimeMachineStorage):
    def __init__(self, settings):
        self.s3 = settings.get("TIME_MACHINE_URI")
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=settings.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=settings.get("AWS_SECRET_ACCESS_KEY"),
        )

    def store_response(self, spider, request, response):
        key = self._request_key(request)
        data = {
            "status": response.status,
            "url": response.url,
            "headers": dict(response.headers),
            "body": gzip.compress(response.body),
        }
        data = pickle.dumps(data, protocol=2)
        self.s3_client.put_object(
            Body=data,
            Bucket=self.s3,
            Key=key)

    def retrieve_response(self, spider, request):
        key = self._request_key(request)
        s3_bucket = self.s3
        if key not in s3_bucket:
            return  # not found
        response = self.s3_client.restore_object(s3_bucket, key)
        return response

