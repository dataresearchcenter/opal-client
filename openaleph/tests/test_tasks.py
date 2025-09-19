import os

from openaleph.crawldir import crawl_dir
from openaleph.api import AlephAPI


class TestTasks(object):
    def setup_method(self):
        self.api = AlephAPI(host="http://openaleph.test/api/2/", api_key="fake_key")

    def test_new_collection(self, mocker):
        mocker.patch.object(self.api, "filter_collections", return_value=[])
        mocker.patch.object(self.api, "create_collection")
        mocker.patch.object(self.api, "update_collection")
        mocker.patch.object(self.api, "ingest_upload")
        crawl_dir(self.api, "openaleph/tests/testdata", "test153", {}, True, True)
        self.api.create_collection.assert_called_once_with(
            {
                "category": "other",
                "foreign_id": "test153",
                "label": "test153",
                "languages": [],
                "summary": "",
                "casefile": False,
            }
        )

    def test_write_entity(self, mocker):
        mocker.patch.object(self.api, "write_entity", return_value={"id": 24})
        collection_id = 8
        entity = {
            "id": 24,
            "schema": "Article",
            "properties": {
                "title": "",
                "author": "",
                "publishedAt": "",
                "bodyText": "",
            },
        }

        res = self.api.write_entity(collection_id, entity)
        assert res["id"] == 24

    def test_delete_entity(self, mocker):
        mocker.patch.object(self.api, "write_entity", return_value={"id": 24})
        mocker.patch.object(self.api, "_request")
        collection_id = 8
        entity = {
            "id": 24,
            "schema": "Article",
            "properties": {
                "title": "",
                "author": "",
                "publishedAt": "",
                "bodyText": "",
            },
        }

        res = self.api.write_entity(collection_id, entity)
        assert res["id"] == 24
        self.api.delete_entity(res["id"])
        self.api._request.assert_called_once_with(
            "DELETE", "http://openaleph.test/api/2/entities/24"
        )

    def test_ingest(self, mocker):
        mocker.patch.object(self.api, "ingest_upload", return_value={"id": 42})
        mocker.patch.object(
            self.api, "load_collection_by_foreign_id", return_value={"id": 2}
        )
        mocker.patch.object(self.api, "update_collection")
        crawl_dir(self.api, "openaleph/tests/testdata", "test153", {}, True, True)
        base_path = os.path.abspath("openaleph/tests/testdata")
        assert self.api.ingest_upload.call_count == 7

        # Verify that the expected directories and files were processed
        call_args_list = [call.args for call in self.api.ingest_upload.call_args_list]
        processed_paths = [str(args[1]) for args in call_args_list]

        expected_paths = [
            os.path.join(base_path, "feb"),
            os.path.join(base_path, "jan"),
            os.path.join(base_path, "dec"),
            os.path.join(base_path, "jan/week1"),
            os.path.join(base_path, "feb/2.txt"),
            os.path.join(base_path, "dec/.3.txt"),
            os.path.join(base_path, "jan/week1/1.txt"),
        ]

        for expected_path in expected_paths:
            assert expected_path in processed_paths
