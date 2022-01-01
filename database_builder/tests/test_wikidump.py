"""Tests for the wikitools.wikidump module

Coverage
--------
verify_contiguous_dump

Missing
-------
load_wikifile_list
"""
from pathlib import Path
import unittest

from wikitools.wikixml import WikiXMLFile
import wikitools.wikidump


class FindMissingDumpFilesTest(unittest.TestCase):
    def test_find_missing_dump_files(self):
        # verify for valid data
        expected_dict = {
            "contiguous": True,
            "last_valid_file": WikiXMLFile(11, 15, Path("test_11_15")),
        }
        files = [
            WikiXMLFile(1, 5, Path("test_1_5")),
            WikiXMLFile(6, 10, Path("test_6_10")),
            WikiXMLFile(11, 15, Path("test_11_15")),
        ]
        results = wikitools.wikidump.find_missing_dump_files(files)
        self.assertDictEqual(results, expected_dict)
        # verify for invalid data (end_idx[i] > start_idx[i+1])
        expected_dict = {
            "contiguous": False,
            "last_valid_file": WikiXMLFile(1, 8, Path("test_1_8")),
        }
        invalid_files = [
            WikiXMLFile(1, 8, Path("test_1_8")),
            WikiXMLFile(6, 10, Path("test_6_10")),
            WikiXMLFile(11, 15, Path("test_11_15")),
        ]
        results = wikitools.wikidump.find_missing_dump_files(invalid_files)
        self.assertDictEqual(results, expected_dict)
        # verify for invalid data (end_idx[i] + 1 < start_idx[i+1])
        expected_dict = {
            "contiguous": False,
            "last_valid_file": WikiXMLFile(6, 10, Path("test_6_10")),
        }
        invalid_files = [
            WikiXMLFile(1, 5, Path("test_1_5")),
            WikiXMLFile(6, 10, Path("test_6_10")),
            WikiXMLFile(13, 15, Path("test_13_15")),
        ]
        results = wikitools.wikidump.find_missing_dump_files(invalid_files)
        self.assertDictEqual(results, expected_dict)


if __name__ == "__main__":
    unittest.main()
