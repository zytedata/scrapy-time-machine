import gzip
import logging
import os
from time import time
import dbm

from scrapy.http import Headers
from scrapy.responsetypes import responsetypes
from scrapy.utils.project import data_path
from scrapy.utils.request import request_fingerprint
from six.moves import cPickle as pickle


logger = logging.getLogger(__name__)


class DbmTimeMachineStorage:

    time_machine_dir = "timemachine"

    def __init__(self, settings):
        self.snapshot_dir = data_path(
            settings.get("TIME_MACHINE_DIR", self.time_machine_dir), createdir=True
        )
        self.db = None

    def open_spider(self, spider):
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

        return pickle.loads(db[f"{key}_data"])

    def _request_key(self, request):
        return request_fingerprint(request)
