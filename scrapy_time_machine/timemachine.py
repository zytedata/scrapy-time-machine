from datetime import datetime
from typing import Optional, Type, TypeVar

from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.exceptions import CloseSpider, NotConfigured
from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.settings import Settings
from scrapy.spiders import Spider
from scrapy.statscollectors import StatsCollector
from scrapy.utils.misc import load_object
from twisted.internet import defer
from twisted.internet.error import (
    ConnectError,
    ConnectionDone,
    ConnectionLost,
    ConnectionRefusedError,
    DNSLookupError,
    TCPTimedOutError,
    TimeoutError,
)
from twisted.web.client import ResponseFailed

TimeMachineMiddlewareTV = TypeVar(
    "TimeMachineMiddlewareTV", bound="TimeMachineMiddleware"
)


class TimeMachineMiddleware:
    DOWNLOAD_EXCEPTIONS = (
        defer.TimeoutError,
        TimeoutError,
        DNSLookupError,
        ConnectionRefusedError,
        ConnectionDone,
        ConnectError,
        ConnectionLost,
        TCPTimedOutError,
        ResponseFailed,
        IOError,
    )

    def __init__(self, settings: Settings, stats: StatsCollector) -> None:
        if not settings.getbool("TIME_MACHINE_ENABLED"):
            raise NotConfigured

        try:
            self.storage = load_object(settings["TIME_MACHINE_STORAGE"])(settings)
        except TypeError:
            raise NotConfigured("Time Machine Extension enabled but no storage found.")
        if not self.storage.uri:
            raise NotConfigured("Missing TIME_MACHINE_URI setting")

        if not (self.storage.retrieve_mode ^ self.storage.snapshot_mode):
            raise NotConfigured(
                "Either TIME_MACHINE_RETRIEVE or TIME_MACHINE_SNAPSHOT must be enabled"
            )

        self.stats = stats
        self.invalid = False

    @classmethod
    def from_crawler(
        cls: Type[TimeMachineMiddlewareTV], crawler: Crawler
    ) -> TimeMachineMiddlewareTV:
        o = cls(crawler.settings, crawler.stats)
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o

    def spider_opened(self, spider: Spider) -> None:
        uri_params = self._get_uri_params(spider)
        self.storage.set_uri(uri_params)

        if self.storage.retrieve_mode and not self.storage.is_uri_valid():
            self.invalid = True
            raise CloseSpider(f"Invalid URI {self.uri}")
        self.storage.open_spider(spider)

    def spider_closed(self, spider: Spider) -> None:
        self.storage.close_spider(spider)

    def process_request(self, request: Request, spider: Spider) -> Optional[Response]:
        if self.invalid:
            return None

        if not self.storage.retrieve_mode:
            return None

        snapshotted_response = self.storage.retrieve_response(spider, request)
        if not snapshotted_response:
            raise CloseSpider(
                "Unknown request! Did you modify the spider request chain?"
            )

        snapshotted_response.flags.append("snapshot")

        # Keep a reference to snapshotted response to avoid
        # a second lookup on process_response hook
        request.meta["snapshotted_response"] = snapshotted_response

        return snapshotted_response

    def process_response(
        self, request: Request, response: Response, spider: Spider
    ) -> Response:
        if self.invalid:
            return response

        if not self.storage.snapshot_mode:
            return response

        # Is a retrieve run
        if "snapshot" in response.flags:
            return response

        self._snapshot_response(spider, response, request)
        return response

    def process_exception(
        self, request: Request, exception: Exception, spider: Spider
    ) -> Optional[Response]:
        snapshot_response = request.meta.pop("snapshot_response", None)
        if snapshot_response is not None and isinstance(
            exception, self.DOWNLOAD_EXCEPTIONS
        ):
            self.stats.inc_value("time_machine/errorrecovery", spider=spider)
            return snapshot_response
        return None

    def _snapshot_response(
        self,
        spider: Spider,
        response: Response,
        request: Request,
    ) -> None:
        self.stats.inc_value("time_machine/store", spider=spider)
        self.storage.store_response(spider, request, response)

    def _get_uri_params(
        self,
        spider: Spider,
    ) -> dict:
        params = {}
        for k in dir(spider):
            params[k] = getattr(spider, k)
        utc_now = datetime.utcnow()
        params["time"] = utc_now.replace(microsecond=0).isoformat().replace(":", "-")
        params["batch_time"] = utc_now.isoformat().replace(":", "-")
        return params
