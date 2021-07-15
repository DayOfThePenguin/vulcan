import bz2
import re

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List, Tuple
from xml.etree.ElementTree import iterparse, Element

import mwparserfromhell as wp

from unidecode import unidecode


class WikiXMLFile(object):
    """Represent an XML chunk of a Wikipedia database dump"""

    def __init__(self, start_idx: int, end_idx: int, path: Path, chunk_size: int = 2000) -> None:
        if not isinstance(start_idx, int):
            msg = "WikiXMLFile.start_idx must be an integer. invalid start_idx: {}"
            msg = msg.format(start_idx)
            raise TypeError(msg)
        if not isinstance(end_idx, int):
            msg = "WikiXMLFile.end_idx must be an integer. invalid end_idx: {}"
            msg = msg.format(end_idx)
            raise TypeError(msg)
        if not isinstance(path, Path):
            msg = "WikiXMLFile.path must be a pathlib.Path. invalid path: {}"
            msg = msg.format(path)
            raise TypeError(msg)
        if start_idx > end_idx:
            msg = "start_idx ({}) must be less than end_idx ({})"
            msg = msg.format(start_idx, end_idx)
            raise ValueError(msg)
        self.start_idx = start_idx
        self.end_idx = end_idx
        self.path = path
        self.chunk_size = chunk_size
        self.pages = 0
        self.additions = 0
        self.duplicates = 0
        self.errors = 0

    def __eq__(self, other):
        """Equality method

        Parameters
        ----------
        other : WikiXMLMap
            other object to compare to

        Returns
        -------
        True
            if start_idx, end_idx, and path are equal
        False
            else
        """
        if self.path != other.path:
            return False
        elif self.start_idx != other.start_idx:
            return False
        elif self.end_idx != other.end_idx:
            return False
        else:
            return True

    def is_real_xml_bz2(self):
        """Checks if the file specified in self.path is really a bzipped xml file

        Valid files meet these criteria:
        - .xml-p(.+)p(.+).bz2 files (i.e. have two suffixes)
        - the first extension is a flavor of .xml-p(.+)p(.+)
        - the second extension is .bz2
        """
        if self.path.is_file() is False:
            return False
        elif len(self.path.suffixes) != 2:
            return False
        elif re.search(r"^.xml-p(.+)p(.+)$", self.path.suffixes[0]) is None:
            return False
        elif self.path.suffixes[1] != ".bz2":
            return False
        return True

    @contextmanager
    def parser(self):
        """Context manager to yield a parser (iterator from
        xml.etree.ElementTree.iterparse) for the .xml.bz2 file at self.path

        Parameters
        ----------

        Yields
        -------
        parser : Iterator
            from xml.etree.ElementTree.iterparse

        Raises
        ------
        FileNotFoundError
            if the file doesn't actually exist
        """
        if not self.is_real_xml_bz2():
            msg = "unable to create iterparser because the path is invalid: {}"
            msg = msg.format(self.path.as_posix())
            raise FileNotFoundError(msg)
        file = bz2.open(self.path, "r")
        parser = iterparse(file)
        try:
            yield parser
        finally:
            file.close()


###################
# Utility methods #
###################


def get_headings_sections(
    element: Element,
) -> Tuple[List[str], List[str]]:
    """Extract a list of headings and the text of their respective sections from an article Element

    Parameters
    ----------
    element : Element
        xml.etree.ElementTree.Element, the element to get headings and sections from

    Returns
    -------
    Tuple[clean_headings : List[str], clean_sections : List[str]]
        clean_headings : List[str]
            headings of the article, with a heading 'Lead' for the first, unnamed
            section of the page
        clean_sections List[str]
            section of the page. NOTE: some of these strings will be '' when their
            respective heading is used to group a series of subheadings but there
            isn't any actual text under the heading itself

    Notes
    -----
    - intermediate results are explicitly deleted because otherwise they seem to build
    up (THEORY: something in parser is stopping their reference counts from going to 0)
    - Length of clean_headings is enforced to be same as length of clean_sections
    - This function will also transliterate any unicode to ascii using the unidecode
    (https://github.com/avian2/unidecode) module
    """
    if not isinstance(element, Element):
        msg = "invalid element. element must be an xml.etree.ElementTree.Element"
        raise TypeError(msg)
    wikicode = wp.parse(element.text)
    raw_headings = wikicode.filter_headings()
    clean_headings = []

    raw_sections = []
    remaining_text = element.text
    for i, heading in enumerate(raw_headings):
        if i == 0:  # The first section (Lead) won't have a title because it is implicitly assumed
            clean_headings.append("Lead")
            clean_headings.append(
                raw_headings[i].title.strip_code().strip()
            )  # the titles are wrapped in wikicode and spaces
        else:
            clean_headings.append(
                raw_headings[i].title.strip_code().strip()
            )  # the titles are wrapped in wikicode and spaces
            # when we split on a heading, we get the previous section and the rest of the document
        splits = remaining_text.split(str(heading), maxsplit=1)
        raw_sections.append(splits[0])
        if i == len(raw_headings) - 1:
            raw_sections.append(splits[1])
            remaining_text = ""
        else:
            remaining_text = splits[1]

    clean_sections = []
    for section in raw_sections:
        wikicode_free_section = wp.parse(section).strip_code().strip()
        unicode_transliterated_section = unidecode(wikicode_free_section)
        clean_sections.append(unicode_transliterated_section)
        del wikicode_free_section
        del unicode_transliterated_section
    del raw_sections
    del raw_headings
    del wikicode

    return clean_headings, clean_sections


def get_pagelinks(
    element: Element,
) -> List[str]:
    """Extract a list of pagelinks from an article Element

    Parameters
    ----------
    element : Element
        xml.etree.ElementTree.Element, the element to get pagelinks from

    Returns
    -------
    links : List[str]
        links from the article to other articles. each element is unidecoded

    Notes
    -----
    This function will also transliterate any unicode to ascii using the unidecode
    (https://github.com/avian2/unidecode) module
    """
    if not isinstance(element, Element):
        msg = "invalid element. element must be an xml.etree.ElementTree.Element"
        raise TypeError(msg)
    wikicode = wp.parse(element.text)
    raw_pagelinks = wikicode.filter_wikilinks()
    clean_pagelinks = []
    for raw_link in raw_pagelinks:
        clean_link = wp.parse(raw_link).strip_code()
        if not isinstance(clean_link, str):
            continue  # drop non-strings
        if re.search(r"thumb\|(.+)", clean_link) is not None:
            continue  # drop image links
        if re.search(r"Category:(.+)", clean_link) is not None:
            continue  # drop category links
        if clean_link.strip() == "":
            continue  # drop empty/whitespace-only strings
        clean_link = unidecode(clean_link)
        clean_pagelinks.append(clean_link)
    return clean_pagelinks


def get_next_title_element(
    parser: Iterator,
) -> Tuple[str, Element]:
    """Get the next title and Element in namespace 0 (Main/Article) from an iterator

    Parameters
    ----------
    parser : Iterator (generated by iterparse)
        iterator that yields Tuple[event : str, elem : Element]. By design this should
        be created with xml.etree.ElementTree.iterparse, but the function should
        theoretically work as long as the Iterator yields Elements as the second part
        of the tuple becaues we immediately throw away the event. See comment on the
        events in Notes below

    Returns
    -------
    title : str
        the title of the next page in namespace 0 (Main/Article) from parser. unidecoded
    elem : Element
        the Element with all the info about the page corresponding to title

    Raises
    ------
    TypeError
        if parser isn't an Iterator
    StopIteration
        when we reach the end of parser

    Notes
    -----
    title is unidecoded before being returned

    the events yielded as the first item from next(parser) have the form
    <class 'str'>
    'end'
    and are basically junk so we can delete these right away

    redirects and valid articles both have text tags,
    but since we set title to none when we find a redirect tag
    between the title and text tags, we never return a redirect

    according to https://en.wikipedia.org/wiki/Wikipedia:Page_name, any title with a
    colon in it indicates that the page isn't in namespace 0 (Main/Article). All
    namespaces are documented at
    https://en.wikipedia.org/wiki/Wikipedia:Namespace


    A redirect tag will occur after the title if the
    page is a redirect, so we set the title back to none so the text
    matcher won't return a match for this page
    """
    if not isinstance(parser, Iterator):
        msg = "parser must be an iterator from xml.etree.ElementTree.parser(). invalid parser: {}"
        msg = msg.format(parser)
        raise TypeError(msg)
    title = None

    while True:
        event, elem = None, None
        try:
            event, elem = next(parser)
            del event
        except StopIteration as e:
            raise StopIteration("Reached the end of the parser") from e
        # article title (title) matcher
        matches = re.search(r"{(.+)}(title)", elem.tag)
        if matches is not None:
            if re.search(r"^(.+):(.+)$", elem.text) is not None:
                continue  # skips any namespaces outside ns 0
            elif len(elem.text) > 200:
                continue  # parsed all titles and no valid ones appear to be over 200 characters
            else:
                title = elem.text
        # article text (text) matcher
        matches = re.search(r"{(.+)}(text)", elem.tag)
        if matches is not None:
            if title is not None:
                return unidecode(title), elem
        # article redirect (redirect) matcher
        matches = re.search(r"{(.+)}(redirect)", elem.tag)
        if matches is not None:
            title = None


def wikifile_num_articles_longest_article(wiki_file: WikiXMLFile) -> Tuple[int, str]:
    """Get the number of articles and longest article title in a WikiXMLFile

    Parameters
    ----------
    wiki_file : WikiXMLFile
        WikiXMLFile to load and read articles from. if invalid file, raises
        FileNotFoundError

    Returns
    -------
    num_articles : int
        how many articles in namespace 0 (Main/Article) are in wiki_file
    longest_article : str
        name of longest article in namespace 0 (Main/Article) in wiki_file

    Raises
    ------
    TypeError
        if wiki_file isn't a WikiXMLFile
    FileNotFoundError
        if wiki_file isn't a valid file on disk

    Notes
    -----
    """
    if not isinstance(wiki_file, WikiXMLFile):
        msg = "invalid wiki_file. wiki_file has to be an instance of WikiXMLFile: {}"
        msg = msg.format(wiki_file)
        raise TypeError(msg)
    if wiki_file.is_real_xml_bz2() == False:
        msg = "invalid wikifile. the file at {} doesn't exist"
        msg.format(wiki_file.path.as_posix)
        raise FileNotFoundError(msg)
    num_articles = 0
    max_title_len = 0
    longest_title = None
    with wiki_file.parser() as parser:
        while True:
            title, elem = None, None
            try:
                title, elem = get_next_title_element(parser)
                num_articles += 1
            except StopIteration:
                return num_articles, longest_title
            if len(title) > max_title_len:
                max_title_len = len(title)
                longest_title = title
            del title
            elem.clear()
            del elem
