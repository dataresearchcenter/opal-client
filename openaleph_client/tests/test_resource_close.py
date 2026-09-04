"""Tests that HTTP/DB resources are closed, guarding against the leaks that
caused long-running crawldir sessions to exhaust file descriptors and memory.

Run just these with:  pytest openaleph_client/tests -k close -v

The responses/connections are plain mocks rather than real requests/sqlite
objects: we are asserting that the client calls .close() / uses the context
manager, not exercising real IO, and real objects either can't be spied
(sqlite's C-level close is read-only) or fight the mock.
"""
import json
from pathlib import Path

from openaleph_client.api import AlephAPI
from openaleph_client import fetchdir
from openaleph_client.crawldir import crawl_dir

TESTDATA = str(Path(__file__).parent / "testdata")


def _mock_response(mocker, lines=None):
    """A mock HTTP response usable as a context manager and iterable."""
    res = mocker.MagicMock(name="response")
    res.__enter__.return_value = res
    res.__exit__.return_value = False
    res.iter_lines.return_value = iter(lines or [])
    res.iter_content.return_value = iter([b"data"])
    return res


class TestApiClose:
    def setup_method(self):
        self.api = AlephAPI(host="http://openaleph.test/api/2/", api_key="fake_key")

    def test_close_closes_session(self, mocker):
        spy = mocker.patch.object(self.api.session, "close")
        self.api.close()
        spy.assert_called_once()

    def test_close_is_idempotent(self, mocker):
        spy = mocker.patch.object(self.api.session, "close")
        self.api.close()
        self.api.close()
        spy.assert_called_once()

    def test_context_manager_closes_session(self, mocker):
        spy = mocker.patch.object(self.api.session, "close")
        with self.api as api:
            assert api is self.api
        spy.assert_called_once()


class TestStreamEntitiesClose:
    def setup_method(self):
        self.api = AlephAPI(host="http://openaleph.test/api/2/", api_key="fake_key")

    def test_stream_entities_closes_response_when_consumed(self, mocker):
        line = json.dumps({"id": "1", "schema": "Thing"}).encode()
        res = _mock_response(mocker, lines=[line])
        mocker.patch.object(self.api.session, "get", return_value=res)
        list(self.api.stream_entities(collection={"id": "c1"}))
        res.close.assert_called_once()

    def test_stream_entities_closes_response_when_abandoned(self, mocker):
        # Two lines available, but the caller stops after the first: finally
        # must still close the streamed socket.
        lines = [
            json.dumps({"id": "1", "schema": "Thing"}).encode(),
            json.dumps({"id": "2", "schema": "Thing"}).encode(),
        ]
        res = _mock_response(mocker, lines=lines)
        mocker.patch.object(self.api.session, "get", return_value=res)
        gen = self.api.stream_entities(collection={"id": "c1"})
        next(gen)
        gen.close()
        res.close.assert_called_once()


class TestBulkChunkClose:
    def setup_method(self):
        self.api = AlephAPI(host="http://openaleph.test/api/2/", api_key="fake_key")

    def test_bulk_chunk_closes_response(self, mocker):
        # The fix uses `with session.post(...) as response:`; assert the
        # context manager is exited (which closes the response).
        res = _mock_response(mocker)
        mocker.patch.object(self.api.session, "post", return_value=res)
        self.api._bulk_chunk("c1", [{"id": "1", "schema": "Thing"}])
        res.__exit__.assert_called_once()


class TestFetchArchiveClose:
    def test_fetch_archive_closes_response(self, mocker, tmp_path):
        res = _mock_response(mocker)
        mocker.patch.object(fetchdir.requests, "get", return_value=res)
        fetchdir.fetch_archive("http://x/file", tmp_path / "out.bin")
        res.__exit__.assert_called_once()


class TestCrawlDirDbClose:
    def setup_method(self):
        self.api = AlephAPI(host="http://openaleph.test/api/2/", api_key="fake_key")

    def test_crawl_dir_closes_connection(self, mocker):
        import sqlite3

        mocker.patch.object(self.api, "ingest_upload", return_value={"id": 42})
        mocker.patch.object(
            self.api, "load_collection_by_foreign_id", return_value={"id": 2}
        )
        mocker.patch.object(self.api, "update_collection")

        real_connect = sqlite3.connect
        close_calls = []

        class ConnProxy:
            """Delegate to a real connection but count close() calls, since a
            real sqlite3.Connection.close is C-level and can't be spied."""

            def __init__(self, conn):
                self._conn = conn

            def close(self):
                close_calls.append(1)
                return self._conn.close()

            def __getattr__(self, name):
                return getattr(self._conn, name)

        def tracking_connect(*args, **kwargs):
            return ConnProxy(real_connect(*args, **kwargs))

        mocker.patch(
            "openaleph_client.crawldir.sqlite3.connect", side_effect=tracking_connect
        )
        crawl_dir(self.api, TESTDATA, "test153", {}, True, True)

        assert close_calls, "crawl_dir did not close its sqlite connection"
