from email.utils import formatdate
from typing import Optional, Type, TypeVar

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

from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.exceptions import IgnoreRequest, NotConfigured, CloseSpider
from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.settings import Settings
from scrapy.spiders import Spider
from scrapy.statscollectors import StatsCollector
from scrapy.utils.misc import load_object


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
        self.storage = load_object(settings["TIME_MACHINE_STORAGE"])(settings)
        self.retrieve = settings.getbool("TIME_MACHINE_RETRIEVE")
        self.storage.retrieve = self.retrieve
        self.stats = stats

    @classmethod
    def from_crawler(
        cls: Type[TimeMachineMiddleware], crawler: Crawler
    ) -> TimeMachineMiddleware:
        o = cls(crawler.settings, crawler.stats)
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o

    def spider_opened(self, spider: Spider) -> None:
        self.storage.open_spider(spider)

    def spider_closed(self, spider: Spider) -> None:
        self.storage.close_spider(spider)

    def process_request(self, request: Request, spider: Spider) -> Optional[Response]:
        # Look for cached response and check if expired
        snapshotresponse = self.storage.retrieve_response(spider, request)
        if self.retrieve and not snapshotresponse:
            raise CloseSpider(
                "Unknown request! Did you modify the spider request chain?"
            )

        # Keep a reference to snapshot response to avoid a second cache lookup on
        # process_response hook
        request.meta["snapshot_response"] = snapshotresponse

        return None

    def process_response(
        self, request: Request, response: Response, spider: Spider
    ) -> Response:
        # Do not validate first-hand responses
        cachedresponse = request.meta.pop("snapshot_response", None)
        if cachedresponse is None:
            self.stats.inc_value("time_machine/store", spider=spider)
            self._cache_response(spider, response, request, cachedresponse)
            return response

        self.stats.inc_value("httpcache/invalidate", spider=spider)
        self._cache_response(spider, response, request, cachedresponse)
        return response

    def process_exception(
        self, request: Request, exception: Exception, spider: Spider
    ) -> Optional[Response]:
        cachedresponse = request.meta.pop("cached_response", None)
        if cachedresponse is not None and isinstance(
            exception, self.DOWNLOAD_EXCEPTIONS
        ):
            self.stats.inc_value("httpcache/errorrecovery", spider=spider)
            return cachedresponse
        return None

    def _cache_response(
        self,
        spider: Spider,
        response: Response,
        request: Request,
        cachedresponse: Optional[Response],
    ) -> None:
        self.stats.inc_value("httpcache/store", spider=spider)
        self.storage.store_response(spider, request, response)
