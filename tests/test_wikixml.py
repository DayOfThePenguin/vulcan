"""Tests for the wikitools module

Current coverage:
None
"""
import bz2
from pathlib import Path
import re
import unittest
from xml.etree.ElementTree import Element, iterparse

from wikitools.wikixml import WikiXMLFile, get_next_article_title_and_element
import wikitools as wikitools


class WikiXMLFileTest(unittest.TestCase):
    def test_constructor_valid_params(self):
        start_idx = 5
        end_idx = 8
        path = Path("no/way/do/i/exist.json")
        test_file = WikiXMLFile(start_idx, end_idx, path)
        assert test_file.start_idx == start_idx
        assert test_file.end_idx == end_idx
        assert test_file.path == path

    def test_constructor_invalid_params(self):
        # invalid arguments
        invalid_start_idx = [74]
        invalid_end_idx = "not an int"
        invalid_path = "im/not/a/pathlib/path/object"
        # valid arguments
        start_idx = 5
        end_idx = 8
        path = Path("no/way/do/i/exist.json")
        with self.assertRaisesRegex(TypeError, "invalid start_idx"):
            WikiXMLFile(invalid_start_idx, end_idx, path)
        with self.assertRaisesRegex(TypeError, "invalid end_idx"):
            WikiXMLFile(start_idx, invalid_end_idx, path)
        with self.assertRaisesRegex(TypeError, "invalid path"):
            WikiXMLFile(start_idx, end_idx, invalid_path)

    def test_is_real_xml_bz2(self):
        test_stem = "enwiki-20210420-pages-articles-multistream21"
        test_suffix_one = ".xml-p37022433p38522432"
        test_suffix_two = ".bz2"
        files_to_test = [
            test_stem + test_suffix_one + test_suffix_two,  # valid format
            test_stem + ".txt" + test_suffix_two,  # invalid first suffix
            test_stem + test_suffix_one + ".txt",  # invalid second suffix
            test_stem + ".txt" + ".txt",  # both suffixes invalid
        ]
        for i, f in enumerate(files_to_test):
            path = Path(f)
            path.touch()  # create temp file
            self.assertTrue(path.is_file())
            test_file = WikiXMLFile(0, 3, path)
            if i == 0:
                self.assertTrue(test_file.is_real_xml_bz2())
            else:
                self.assertFalse(test_file.is_real_xml_bz2())
            path.unlink()  # delete temp file


class getNextArticleTitleAndElementTest(unittest.TestCase):
    """Test wikixml.get_next_article_title_and_element
    Tests
    -----
    - params: typechecking works
    - return_types: return types are correct
    - returns_only_articles: only ns 0 pages are returned
    - elem:

    """

    def test_params(self):
        """Verify parameter type-checking is correct"""
        invalid_parser = None
        with self.assertRaisesRegex(
            TypeError, "parser has to be an iterator craeted by xml.etree.ElementTree"
        ):
            get_next_article_title_and_element(invalid_parser)

    def test_return_types(self):
        """Verify that the return types are (str, xml.etree.ElementTree.Element)"""
        path = Path(
            "enwiki-20210420-pages-articles-multistream16.xml-p20460153p20570392.bz2"
        )
        wiki_file = WikiXMLFile(20460153, 20570392, path)
        with bz2.open(wiki_file.path, "r") as f:
            parser = iter(iterparse(f))
            title, elem = wikitools.wikixml.get_next_article_title_and_element(parser)
            self.assertIsInstance(title, str)
            self.assertIsInstance(elem, Element)

    def test_returns_only_articles(self):
        """Verify that all page namespaces except ns 0 (Main/Article) are excluded
        from results"""
        path = Path(
            "enwiki-20210420-pages-articles-multistream16.xml-p20460153p20570392.bz2"
        )
        wiki_file = WikiXMLFile(20460153, 20570392, path)
        namespaces = [  # listed at https://en.wikipedia.org/wiki/Wikipedia:Namespace
            "User",
            "Wikipedia",
            "File",
            "MediaWiki",
            "Template",
            "Help",
            "Category",
            "Portal",
            "Draft",
            "TimedText",
            "Module",
        ]
        with bz2.open(wiki_file.path, "r") as f:
            parser = iter(iterparse(f))
            while True:
                title, elem = None, None
                try:
                    title, elem = wikitools.wikixml.get_next_article_title_and_element(
                        parser
                    )
                except StopIteration:
                    break
                self.assertLessEqual(len(title), 200)  # titles are limited to 200 chars
                for ns in namespaces:
                    self.assertIsNone(re.search(r"^{}:(.+)$".format(ns), title))

    def test_elem(self):
        """
        TODO: TBH doesn't do anything, not sure how to actually test that the content
        coming out of the files is valid...
        """
        path = Path(
            "enwiki-20210420-pages-articles-multistream16.xml-p20460153p20570392.bz2"
        )
        wiki_file = WikiXMLFile(20460153, 20570392, path)
        with bz2.open(wiki_file.path, "r") as f:
            parser = iter(iterparse(f))
            title, elem = wikitools.wikixml.get_next_article_title_and_element(parser)
            print(title)
            print()
            print(type(elem.text))


if __name__ == "__main__":
    unittest.main()
