"""Tests for the wikitools.wikixml module

Coverage
--------
WikiXMLFile
get_next_title_element

Missing
-------
get_headsings_sections
"""
from pathlib import Path
import re
import unittest
from xml.etree.ElementTree import Element

import wikitools.wikixml as wikixml

from wikitools.wikipage import WikipediaPage
from wikitools.wikixml import WikiXMLFile


class WikiXMLFileTest(unittest.TestCase):
    """Test WikiXMLFile class

    Tests
    -----
    constructor_valid_params
        are constructor parameters set correctly?
    constructor_invalid_params
        will the constructor raise the correct errors for different invalid parameter
        combinations?
    is_real_xml_bz2
        will the self.is_real_xml_bz2 method identify invalid .xml-p(.+)p(.+) files?
    eq
        does the __eq__ method work as expected?
    """

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
        # cases
        with self.assertRaisesRegex(TypeError, "invalid start_idx"):
            WikiXMLFile(invalid_start_idx, end_idx, path)
        with self.assertRaisesRegex(TypeError, "invalid end_idx"):
            WikiXMLFile(start_idx, invalid_end_idx, path)
        with self.assertRaisesRegex(TypeError, "invalid path"):
            WikiXMLFile(start_idx, end_idx, invalid_path)
        with self.assertRaisesRegex(
            ValueError, r"start_idx \((.+)\) must be less than end_idx \((.+)\)"
        ):
            WikiXMLFile(end_idx, start_idx, path)

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
        for i, file in enumerate(files_to_test):
            path = Path(file)
            path.touch()  # create temp file
            self.assertTrue(path.is_file())
            test_file = WikiXMLFile(0, 3, path)
            if i == 0:
                self.assertTrue(test_file.is_real_xml_bz2())
            else:
                self.assertFalse(test_file.is_real_xml_bz2())
            path.unlink()  # delete temp file

    def test_eq(self):
        start_idx = 5
        end_idx = 8
        path = Path("im/the/same/but/does/python/know/it.json")
        test_self = WikiXMLFile(start_idx, end_idx, path)
        # same parameters
        test_other = WikiXMLFile(start_idx, end_idx, path)
        self.assertEqual(test_self, test_other)
        # different start_idx
        test_other = WikiXMLFile(3, end_idx, path)
        self.assertNotEqual(test_self, test_other)
        # different end_idx
        test_other = WikiXMLFile(start_idx, 15, path)
        self.assertNotEqual(test_self, test_other)
        # different path
        test_other = WikiXMLFile(start_idx, end_idx, Path("not/the/same"))
        self.assertNotEqual(test_self, test_other)


class GetHeadingsSections(unittest.TestCase):
    pass


class GetNextTitleElementTest(unittest.TestCase):
    """Test wikixml.get_next_title_element

    Tests
    -----
    params : typechecking works
    return_types : return types are correct
    returns_only_articles : only ns 0 pages are returned
    elem :
    """

    def setUp(self):
        self.path = Path(
            "tests/test_data/enwiki-20210420-pages-articles-multistream16.xml-p20460153p20570392.bz2"
        )

    def test_params(self):
        """Verify parameter type-checking is correct"""
        wiki_file = WikiXMLFile(20460153, 20570392, self.path)
        with wiki_file.parser() as parser:
            title, elem = wikixml.get_next_title_element(parser)
            self.assertIsInstance(title, str)
            self.assertIsInstance(elem, Element)
        invalid_parser = None
        with self.assertRaisesRegex(
            TypeError,
            r"(.+)invalid parser: (.+)",
        ):
            wikixml.get_next_title_element(invalid_parser)

    def test_return_types(self):
        """Verify that the return types are (str, xml.etree.ElementTree.Element)"""
        wiki_file = WikiXMLFile(20460153, 20570392, self.path)
        with wiki_file.parser() as parser:
            title, elem = wikixml.get_next_title_element(parser)
            self.assertIsInstance(title, str)
            self.assertIsInstance(elem, Element)

    def test_returns_only_articles(self):
        """Verify that all page namespaces except ns 0 (Main/Article) are excluded
        from results"""
        wiki_file = WikiXMLFile(20460153, 20570392, self.path)
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
        with wiki_file.parser() as parser:
            while True:
                try:
                    title, elem = wikixml.get_next_title_element(parser)
                    del elem
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
        wiki_file = WikiXMLFile(20460153, 20570392, self.path)
        with wiki_file.parser() as parser:
            title, elem = wikixml.get_next_title_element(parser)
            print(title)
            print()
            print(type(elem.text))


class IntegrationTest(unittest.TestCase):
    parent = "/hdd/datasets/wikipedia_4_20_21/"
    name = "enwiki-20210420-pages-articles-multistream1.xml-p1p41242.bz2"
    data_path = Path(parent + name)
    test_file = WikiXMLFile(1, 41242, data_path)
    with test_file.parser() as parser:
        title, element = wikixml.get_next_title_element(parser)
        links = wikixml.get_pagelinks(element)
        # for link in links:
        #     print(link)
        # print(links)
        headings, sections = wikixml.get_headings_sections(element)
        page = WikipediaPage(
            title=title, headings=headings, sections=sections, links=links
        )
        print(page)


if __name__ == "__main__":
    unittest.main()
