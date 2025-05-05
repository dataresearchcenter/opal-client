import os
from pathlib import Path

from openaleph.api import AlephAPI
from openaleph.crawldir import CrawlDirectory

class TestCrawlDirectory:
    base_path = os.path.abspath("openaleph/tests/testdata")

    def test_get_foreign_id_with_dir(self):
        path = Path(self.base_path) / "jan" / "week1"
        crawldir = CrawlDirectory(AlephAPI, {}, path)
        foreign_id = crawldir.get_foreign_id(path)
        assert foreign_id is None

    def test_get_foreign_id_with_file(self):
        path = Path(self.base_path) / "feb" / "2.txt"
        crawldir = CrawlDirectory(AlephAPI, {}, path)
        foreign_id = crawldir.get_foreign_id(path)
        assert foreign_id == "2.txt"

    def test_get_foreign_id_different_path(self):
        path = Path(self.base_path) / "lib" / "test.txt"
        crawldir = CrawlDirectory(AlephAPI, {}, path)
        # override root to a common base
        crawldir.root = Path(self.base_path)
        foreign_id = crawldir.get_foreign_id(path)
        assert foreign_id == "lib/test.txt"

    def test_is_ignored_default(self):
        # No ignore patterns: nothing should be ignored
        path = Path(self.base_path) / "feb" / "2.txt"
        crawldir = CrawlDirectory(AlephAPI, {}, Path(self.base_path))
        crawldir.ignore_patterns = []
        assert not crawldir.is_ignored(path)

    def test_is_ignored_file_pattern(self):
        path = Path(self.base_path) / "feb" / "2.txt"
        crawldir = CrawlDirectory(AlephAPI, {}, Path(self.base_path))
        crawldir.ignore_patterns = ["*.txt"]
        assert crawldir.is_ignored(path)

    def test_is_ignored_dir_pattern(self):
        path = Path(self.base_path) / "jan" / "week1"
        crawldir = CrawlDirectory(AlephAPI, {}, Path(self.base_path))
        crawldir.ignore_patterns = ["jan/"]
        assert crawldir.is_ignored(path)
