"""Tests for the wikitools.wikipage module

Coverage
--------
WikipediaPage

Missing
-------

"""
import unittest

from wikitools.wikipage import WikipediaPage


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
        test_page = WikipediaPage(title, headings, sections, links)
        self.assertEqual(test_page.title, title)
        self.assertEqual(test_page.headings, headings)
        self.assertEqual(test_page.sections, sections)
        self.assertEqual(test_page.links, links)

    #         title: str,
    #         headings: List[str],
    #         sections: List[str],
    #         links: List[str],

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
        with self.assertRaisesRegex(TypeError, "invalid title"):
            WikipediaPage(invalid_title, string_list, string_list, string_list)
        # headings
        with self.assertRaisesRegex(TypeError, "invalid headings"):
            WikipediaPage(title, invalid_not_list, string_list, string_list)
        with self.assertRaisesRegex(TypeError, "invalid headings element"):
            WikipediaPage(title, invalid_not_string_list, string_list, string_list)
        # sections
        with self.assertRaisesRegex(TypeError, "invalid sections"):
            WikipediaPage(title, string_list, invalid_not_list, string_list)
        with self.assertRaisesRegex(TypeError, "invalid sections element"):
            WikipediaPage(title, string_list, invalid_not_string_list, string_list)
        # links
        with self.assertRaisesRegex(TypeError, "invalid links"):
            WikipediaPage(title, string_list, string_list, invalid_not_list)
        with self.assertRaisesRegex(TypeError, "invalid links element"):
            WikipediaPage(title, string_list, string_list, invalid_not_string_list)
        # len(headings) != len(sections)
        with self.assertRaisesRegex(
            ValueError,
            r"len\(headings\) \((.+)\) must equal len\(sections\) \((.+)\)(.+)",
        ):
            WikipediaPage(title, invalid_long_headings, string_list, string_list)

    def test_eq(self):
        title = "Quantum Mechanics"
        headings = ["Lead", "Physics", "Science"]
        sections = ["Lead's content", "Science's content", "Physics's content"]
        links = ["Albert Einstein", "String Theory", "Physics Theories"]
        test_self = WikipediaPage(title, headings, sections, links)
        # same parameters
        test_other = WikipediaPage(title, headings, sections, links)
        self.assertEqual(test_self, test_other)
        # different title
        test_other = WikipediaPage("different title", headings, sections, links)
        self.assertNotEqual(test_self, test_other)
        # different headings
        test_other = WikipediaPage(title, ["not", "the", "same"], sections, links)
        self.assertNotEqual(test_self, test_other)
        # different sections
        test_other = WikipediaPage(title, headings, ["not", "the", "same"], links)
        self.assertNotEqual(test_self, test_other)
        # different links
        test_other = WikipediaPage(title, headings, sections, ["not", "the", "same"])
        self.assertNotEqual(test_self, test_other)


if __name__ == "__main__":
    unittest.main()
