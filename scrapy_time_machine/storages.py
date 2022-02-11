import json
import gzip
import logging
import os
import subprocess
from contextlib import suppress
from datetime import datetime
from time import time
import dbm

import boto3
from botocore.exceptions import ClientError
from botocore.stub import Stubber
from scrapy.exceptions import NotConfigured, CloseSpider
from scrapy.http import Headers
from scrapy.responsetypes import responsetypes
from scrapy.utils.project import data_path
from scrapy.utils.request import request_fingerprint
from scrapy.utils.url import canonicalize_url
from scrapy_splash.utils import dict_hash
from six.moves import cPickle as pickle
from six.moves.urllib.parse import urlparse

logger = logging.getLogger(__name__)


def get_job_version():
    shub_job_version = json.loads(os.environ.get("SHUB_JOB_DATA", "{}")).get("version")
    if shub_job_version:
        return shub_job_version
    with suppress(subprocess.CalledProcessError):
        git_hash = subprocess.check_output(["git", "describe", "--always"]).strip()
        return git_hash.decode("utf-8")
    return "no-version"


def configure_log_level(level):
    import s3transfer  # NOQA used to configure log level

    boto_modules = ["boto", "s3transfer", "boto3", "botocore"]
    for name in logging.Logger.manager.loggerDict.keys():
        for module in boto_modules:
            if module in name:
                logging.getLogger(name).setLevel(level)


class DbmTimeMachineStorage:
    def __init__(self, settings):
        self.snapshot_dir = data_path(settings["TIME_MACHINE_DIR"], createdir=True)
        self.db = None

    def open_spider(self, spider):
        """Create or load a previous database

        If setting S3CACHE_RETRIEVE is True this method tries to download the database file from s3 and use it.
        """
        self._spider = spider
        self._dbpath = os.path.join(self.snapshot_dir, "%s.db" % spider.name)

        self._prepare_time_machine()

        self.db = dbm.open(self._dbpath, "c")

        logger.debug(
            "Using DBM time machine storage in %(dbpath)s" % {"dbpath": self._dbpath},
            extra={"spider": spider},
        )

    def _prepare_time_machine(self):
        pass

    def close_spider(self, spider):
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

        ts = db[tkey]
        if 0 < self.expiration_secs < time() - float(ts):
            return  # expired

        return pickle.loads(db[f"{key}_data"])

    def _request_key(self, request):
        return request_fingerprint(request)


class S3DbmTimeMachineStorage(DbmTimeMachineStorage):
    def __init__(self, settings):
        super().__init__(settings)

        urifmt = settings.get("TIME_MACHINE_S3_URI", "")
        if not urifmt:
            raise NotConfigured("TIME_MACHINE_S3_URI must be specified")

        # Parse URI
        u = urlparse(urifmt)
        self.keyname_fmt = u.path[1:]
        if not self.keyname_fmt:
            raise NotConfigured("Could not get key name from TIME_MACHINE_S3_URI")

        self.access_key = u.username or settings["AWS_ACCESS_KEY_ID"]
        if self.access_key is None:
            raise NotConfigured("AWS_ACCESS_KEY_ID must be specified")

        self.secret_key = u.password or settings["AWS_SECRET_ACCESS_KEY"]
        if self.secret_key is None:
            raise NotConfigured("AWS_SECRET_ACCESS_KEY must be specified")

        self.bucket_name = u.hostname
        if self.bucket_name is None:
            raise NotConfigured("Could not get bucket name from TIME_MACHINE_S3_URI")

        self.retrieve = False

        self._client = None
        self._spider = None
        self._keyname = None
        self._dbpath = None

        # Configure log level for all modules related do s3 access
        configure_log_level(logging.INFO)

    @property
    def _client_stubber(self):
        """Returns a stubber for the s3 client object to help on unit tests
        See: https://botocore.amazonaws.com/v1/documentation/api/latest/reference/stubber.html
        """
        return Stubber(self.client)

    @property
    def client(self):
        """Connect to S3 and return the connection"""
        if self._client is None:
            self._client = boto3.client(
                "s3",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
            )
        return self._client

    @property
    def keyname(self):
        """Get the keyname as specified in S3CACHE_URI"""

        def get_uri_params(obj):
            """Convert an object to a dict"""
            params = {}
            for k in dir(obj):
                params[k] = getattr(obj, k)
            params["day"] = datetime.utcnow().strftime("%Y-%m-%d")
            params["time"] = (
                datetime.utcnow().replace(microsecond=0).isoformat().replace(":", "-")
            )
            params["version"] = get_job_version()
            return params

        if not self._keyname:
            self._keyname = self.keyname_fmt % get_uri_params(self.spider)
        return self._keyname

    @property
    def spider(self):
        if not self._spider:
            raise NotConfigured("Could not get spider! Aborting...")
        return self._spider

    def upload_file_to_key(self, file, bucket, key):
        file.seek(0)
        self.client.upload_fileobj(file, bucket, key)
        file.close()

    def download_key_to_file(self, file, bucket, key):
        self.client.download_fileobj(bucket, key, file)
        file.seek(0)

    def _prepare_time_machine(self):
        if self.retrieve:
            try:
                with open(self._dbpath, "wb") as db:
                    self.download_key_to_file(db, self.bucket_name, self.keyname)
                logger.info(
                    f"Using cache downloaded from key {self.keyname} on bucket {self.bucket_name}"
                )
            except ClientError:
                raise CloseSpider(
                    f"Failed to download key {self.keyname} on bucket {self.bucket_name}"
                )
                return

    def _finish_time_machine(self, spider):
        """Store db snapshot in the S3 bucket"""
        if self.retrieve:
            logger.info("Will not store cache because this is a retrieval run.")
            return
        try:
            with open(self._dbpath, "rb") as db:
                self.upload_file_to_key(db, self.bucket_name, self.keyname)
            logger.info(
                f"Cache db stored on key {self.keyname} on bucket {self.bucket_name}"
            )
            spider.crawler.stats.set_value(
                "cache_uri", f"s3://{self.bucket_name}/{self.keyname}"
            )
        except (ClientError, FileNotFoundError) as e:
            logger.error(f"Error storing cache on key {self.keyname}: {e}")


class SplashAwareS3DbmTimeMachineStorage(S3DbmTimeMachineStorage):
    def _request_key(self, request):
        """
        A custom `splash_request_fingerprint` that uses the original URL as the only
        extra info for generate fingerprints
        """
        fp = request_fingerprint(request, include_headers=False)
        if "splash" not in request.meta:
            return fp

        url = request.meta["splash"].get("args", {}).get("url", "")
        if url:
            url = canonicalize_url(url, keep_fragments=True)

        return dict_hash(url, fp)
