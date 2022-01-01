import unittest

import wikitools.wikidownload as wikidownload

from pathlib import Path


class CheckPathTest(unittest.TestCase):
    def test_valid(self):
        assert wikidownload.check_path(Path.cwd()) is None

    def test_invalid(self):
        bad_path = Path("/no/way/do/i/exist")
        with self.assertRaisesRegex(FileNotFoundError, r"^Path (.+) does not exist$"):
            wikidownload.check_path(bad_path)


class ReadConfigTest(unittest.TestCase):
    def setUp(self) -> None:
        pass
