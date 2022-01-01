"""Tests for the wikitools.wikipage module

Coverage
--------
WikipediaPage

Missing
-------

"""
import unittest

from pydantic import ValidationError

from wikitools.wikipage import WikipediaPage
from database.models import PageText


class WikipediaPageTest(unittest.TestCase):
    """Test WikpediaPage class

    Tests
    -----
    constructor_valid_params
        are constructor parameters set correctly?
    constructor_invalid_params
        will the constructor raise the correct errors for different invalid parameter
        combinations?
    eq
        does the __eq__ method work as expected?
    """

    def test_constructor_valid_params(self):
        title = "Quantum Mechanics"
        headings = ["Lead", "Science", "Physics"]
        sections = ["Lead's content", "Science's content", "Physics's content"]
        links = ["Albert Einstein", "String Theory", "Physics Theories"]
        test_page = WikipediaPage(
            title=title, headings=headings, sections=sections, links=links
        )
        self.assertEqual(test_page.title, title)
        self.assertEqual(test_page.headings, headings)
        self.assertEqual(test_page.sections, sections)
        self.assertEqual(test_page.links, links)

    def test_constructor_invalid_params(self):
        # invalid arguments
        invalid_title = [74]
        invalid_not_list = "not a list"
        invalid_not_string_list = ["list", "with", "non-string", "element", 56]
        invalid_long_headings = ["i", "have", "more", "elements", "than", "string_list"]
        # valid arguments
        title = "Quantum Mechanics"
        string_list = ["list", "with", "only", "strings"]
        # cases
        # title
        with self.assertRaises(ValidationError):
            WikipediaPage(
                title=invalid_title,
                headings=string_list,
                sections=string_list,
                links=string_list,
            )
        # headings
        with self.assertRaises(ValidationError):
            WikipediaPage(
                title=title,
                headings=invalid_not_list,
                sections=string_list,
                links=string_list,
            )
        # sections
        with self.assertRaises(ValidationError):
            WikipediaPage(
                title=title,
                headings=string_list,
                sections=invalid_not_list,
                links=string_list,
            )
        # links
        with self.assertRaises(ValidationError):
            WikipediaPage(
                title=title,
                headings=string_list,
                sections=string_list,
                links=invalid_not_list,
            )
        # len(headings) != len(sections)
        with self.assertRaises(ValidationError):
            WikipediaPage(
                title=title,
                headings=invalid_long_headings,
                sections=string_list,
                links=string_list,
            )

    def test_eq(self):
        title = "Quantum Mechanics"
        headings = ["Lead", "Physics", "Science"]
        sections = ["Lead's content", "Science's content", "Physics's content"]
        links = ["Albert Einstein", "String Theory", "Physics Theories"]
        test_self = WikipediaPage(
            title=title, headings=headings, sections=sections, links=links
        )
        # same parameters
        test_other = WikipediaPage(
            title=title, headings=headings, sections=sections, links=links
        )
        self.assertEqual(test_self, test_other)
        # different title
        test_other = WikipediaPage(
            title="different title", headings=headings, sections=sections, links=links
        )
        self.assertNotEqual(test_self, test_other)
        # different headings
        test_other = WikipediaPage(
            title=title, headings=["not", "the", "same"], sections=sections, links=links
        )
        self.assertNotEqual(test_self, test_other)
        # different sections
        test_other = WikipediaPage(
            title=title, headings=headings, sections=["not", "the", "same"], links=links
        )
        self.assertNotEqual(test_self, test_other)
        # different links
        test_other = WikipediaPage(
            title=title,
            headings=headings,
            sections=sections,
            links=["not", "the", "same"],
        )
        self.assertNotEqual(test_self, test_other)

    def test_pagetext(self):
        title = "Quantum Mechanics"
        headings = ["Lead", "Science", "Physics"]
        sections = ["Lead's content", "Science's content", "Physics's content"]
        links = ["Albert Einstein", "String Theory", "Physics Theories"]
        test_page = WikipediaPage(
            title=title, headings=headings, sections=sections, links=links
        )
        p = test_page.page()
        self.assertIsInstance(p, PageText)
        self.assertEqual(p.title, title)
        self.assertEqual(p.headings, headings)
        self.assertEqual(p.sections, sections)
        self.assertEqual(p.links, links)


if __name__ == "__main__":
    unittest.main()
