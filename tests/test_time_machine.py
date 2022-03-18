import shutil
import tempfile
import unittest
from contextlib import contextmanager

import pytest
from scrapy.exceptions import NotConfigured
from scrapy.http import Request, Response
from scrapy.settings import Settings
from scrapy.spiders import Spider
from scrapy.utils.test import get_crawler

from scrapy_time_machine.timemachine import TimeMachineMiddleware


class TimeMachineMiddlewareTest(unittest.TestCase):
    def setUp(self):
        self.crawler = get_crawler(Spider)
        self.spider_name = "timemachine_spider"
        self.spider = self.crawler._create_spider(self.spider_name)
        self.tmpdir = tempfile.mkdtemp()
        self.request = Request("http://www.example.com", headers={"User-Agent": "test"})
        self.response = Response(
            "http://www.example.com",
            headers={"Content-Type": "text/html"},
            body=b"test body",
            status=202,
        )
        self.crawler.stats.open_spider(self.spider)

    def tearDown(self):
        self.crawler.stats.close_spider(self.spider, "")
        shutil.rmtree(self.tmpdir)

    def _get_settings(self, **new_settings):
        settings = {
            "TIME_MACHINE_ENABLED": True,
            "TIME_MACHINE_STORAGE": "scrapy_time_machine.storages.DbmTimeMachineStorage",
            "TIME_MACHINE_URI": self.tmpdir + "/test.db",
        }
        settings.update(new_settings)
        return Settings(settings)

    @contextmanager
    def _storage(self, **new_settings):
        with self._middleware(**new_settings) as mw:
            yield mw.storage

    @contextmanager
    def _middleware(self, **new_settings):
        settings = self._get_settings(**new_settings)
        mw = TimeMachineMiddleware(settings, self.crawler.stats)
        mw.spider_opened(self.spider)
        try:
            yield mw
        finally:
            mw.spider_closed(self.spider)

    def assertEqualResponse(self, response1, response2):
        self.assertEqual(response1.url, response2.url)
        self.assertEqual(response1.status, response2.status)
        self.assertEqual(response1.headers, response2.headers)
        self.assertEqual(response1.body, response2.body)

    def test_not_enabled(self):
        settings = {"TIME_MACHINE_ENABLED": False}
        with pytest.raises(NotConfigured):
            with self._middleware(**settings) as _:
                pass

    def test_uri_not_configured(self):
        with pytest.raises(NotConfigured):
            with self._middleware(**{}) as _:
                pass

    def test_retrieval_or_snapshot_not_configured(self):
        with pytest.raises(NotConfigured):
            with self._middleware(**{}) as _:
                pass

    def test_storage_not_configured(self):
        settings = {
            "TIME_MACHINE_SNAPSHOT": True,
            "TIME_MACHINE_STORAGE": None,
        }
        with pytest.raises(NotConfigured):
            with self._middleware(**settings) as _:
                pass

    def test_init_sucess(self):
        settings = {
            "TIME_MACHINE_SNAPSHOT": True,
        }
        with self._middleware(**settings) as mw:
            assert mw


if __name__ == "__main__":
    unittest.main()
