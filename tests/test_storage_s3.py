from contextlib import contextmanager
from unittest.mock import MagicMock, mock_open, patch

import pytest
from scrapy import Spider
from scrapy.settings import Settings
from scrapy.utils.test import get_crawler

from scrapy_time_machine.storages import S3TimeMachineStorage


@contextmanager
def get_storage(**settings):
    yield S3TimeMachineStorage(Settings(settings))


@contextmanager
def get_spider():
    class DummySpider(Spider):
        name = "dummy_spider"

    crawler = get_crawler()
    yield DummySpider.from_crawler(crawler)


def test_get_netloc_and_path():
    with get_storage(**{"TIME_MACHINE_URI": "s3://bucket/path/to/file"}) as storage:
        bucket, path = storage.get_netloc_and_path(storage.uri)
        assert bucket == "bucket"
        assert path == "/path/to/file"

    with pytest.raises(ValueError):
        with get_storage(
            **{"TIME_MACHINE_URI": "no_s3://bucket/path/to/file"}
        ) as storage:
            bucket, path = storage.get_netloc_and_path(storage.uri)

    with pytest.raises(ValueError):
        with get_storage(**{"TIME_MACHINE_URI": "s3://bucket_without_path"}) as storage:
            bucket, path = storage.get_netloc_and_path(storage.uri)


def test_set_uri():
    with get_storage(**{"TIME_MACHINE_URI": "s3://bucket/%(spider)s"}) as storage:
        storage.set_uri({"spider": "dummy_spider"})
        assert storage.snapshot_uri == "s3://bucket/dummy_spider"


def test_is_uri_valid():
    with get_storage(**{"TIME_MACHINE_URI": "s3://bucket/path/to/file"}) as storage:
        storage.set_uri({})
        assert storage.is_uri_valid()

    with get_storage(**{"TIME_MACHINE_URI": "http://invalid_url"}) as storage:
        storage.set_uri({})
        assert not storage.is_uri_valid()


def test_prepare_time_machine_snapshot_mode():
    with get_storage(**{"TIME_MACHINE_URI": "s3://bucket/path/to/file"}) as storage:
        mock_dbm_open = mock_open()
        mock_tempfile = MagicMock()
        mock_tempfile.name = "fake/path/to/local/file.db"
        # Mock Tempfile creation to force having expected path
        with patch(
            "scrapy_time_machine.storages.NamedTemporaryFile",
            return_value=mock_tempfile,
        ) as mock_tempfile_class:
            # Mock the open method of the DB to avoid creating a real db
            with patch("scrapy_time_machine.storages.dbm.open", mock_dbm_open):
                storage._prepare_time_machine()
                mock_tempfile_class.assert_called_once_with(
                    mode="wb",
                    suffix=".db",
                )
                # DB was opened with the expected tempfile path and argument
                mock_dbm_open.assert_called_once_with(mock_tempfile.name, "n")


def test_prepare_time_machine_retrieve_mode():
    with get_storage(
        **{
            "TIME_MACHINE_URI": "s3://bucket/path/to/file",
            "TIME_MACHINE_RETRIEVE": True,
        }
    ) as storage:
        # Mock s3 method calls
        storage.s3_client.download_fileobj = MagicMock()
        mock_dbm_open = mock_open()
        mock_tempfile = MagicMock()
        mock_tempfile.name = "fake/path/to/local/file.db"
        with patch(
            "scrapy_time_machine.storages.NamedTemporaryFile",
            return_value=mock_tempfile,
        ) as mock_tempfile_class:
            with patch("scrapy_time_machine.storages.dbm.open", mock_dbm_open):
                # Configure internal s3 uri value
                storage.set_uri({})
                storage._prepare_time_machine()
                mock_tempfile_class.assert_called_once_with(
                    mode="wb",
                    suffix=".db",
                )
                storage.s3_client.download_fileobj.assert_called_once_with(
                    "bucket", "path/to/file", mock_tempfile
                )
                mock_dbm_open.assert_called_once_with(mock_tempfile.name, "c")


def test_finish_time_machine_snapshot_mode():
    with get_storage(
        **{
            "TIME_MACHINE_URI": "s3://bucket/path/to/file",
            "TIME_MACHINE_SNAPSHOT": False,
        }
    ) as storage:
        # Configure internal s3 uri value
        storage.set_uri({})
        # Mock attributes used during the method execution
        storage.path_to_local_file = MagicMock()
        storage.s3_client.download_fileobj = MagicMock()

        # Execute method
        storage._finish_time_machine()

        # Check that mock were not called
        storage.path_to_local_file.flush.assert_not_called()
        storage.s3_client.download_fileobj.assert_not_called()

        storage.path_to_local_file.close.assert_called_once()


def test_finish_time_machine_retrieve_mode():
    with get_storage(
        **{
            "TIME_MACHINE_URI": "s3://bucket/path/to/file",
            "TIME_MACHINE_SNAPSHOT": True,
        }
    ) as storage:
        # Configure internal s3 uri value
        storage.set_uri({})
        # Mock attributes used during the method execution
        storage.path_to_local_file = MagicMock()
        fake_path = "fake/path/to/local/file.db"
        storage.path_to_local_file.name = fake_path
        storage.s3_client.upload_file = MagicMock()

        # Execute method
        storage._finish_time_machine()

        # Check that mock were not called
        storage.path_to_local_file.flush.assert_called()
        storage.s3_client.upload_file.assert_called_with(
            fake_path,
            "bucket",
            "path/to/file",
        )

        storage.path_to_local_file.close.assert_called_once()
