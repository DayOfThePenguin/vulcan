import re
from typing import Iterator, List, Tuple

from xml.etree.ElementTree import iterparse, Element

import mwparserfromhell as wp
from pathlib import Path
from unidecode import unidecode


class WikiXMLFile(object):
    """Represent an XML chunk of a Wikipedia database dump"""

    def __init__(self, start_idx: int, end_idx: int, path: Path) -> None:
        if not isinstance(start_idx, int):
            raise TypeError(
                "WikiXMLFile.start_idx must be an integer. invalid start_idx: {}".format(
                    start_idx
                )
            )
        if not isinstance(end_idx, int):
            raise TypeError(
                "WikiXMLFile.end_idx must be an integer. invalid end_idx: {}".format(
                    end_idx
                )
            )
        if not isinstance(path, Path):
            raise TypeError(
                "WikiXMLFile.path must be a pathlib.Path. invalid path: {}".format(path)
            )
        self.start_idx = start_idx
        self.end_idx = end_idx
        self.path = path

    def is_real_xml_bz2(self):
        """Checks if the file specified in self.path is really a bzipped xml file

        Valid files meet these criteria:
        - xml-(.+).bz2 files (i.e. have two suffixes)
        - the first extension is a flavor of .xml-
        - the second extension is .bz2
        """
        if self.path.is_file() == False:
            return False
        elif len(self.path.suffixes) != 2:
            return False
        elif re.search(r"^.xml-(.+)$", self.path.suffixes[0]) is None:
            return False
        elif self.path.suffixes[1] != ".bz2":
            return False
        return True


def get_headings_and_sections_from_element(
    element_text: str,
) -> Tuple[List[str], List[str]]:
    """extract a list of headings and a list with the text of the article from an
    element.text

    This function will also transliterate any unicode to ascii using the unidecode
    (https://github.com/avian2/unidecode) module by default

    Keyword Arguments:
    element_text (elem.text) -

    Returns:
    headings (list) - all the headings of the article, with a heading
    'Lead' for the first, unnamed section of the page

    sections (list) - list of strings each containing the contents of a
    section of the page. Note: some of these strings will be '' when a
    heading is used to group a series of subheadings but there isn't
    any actual text under the heading itself

    """
    wikicode = wp.parse(element_text)
    raw_headings = wikicode.filter_headings()
    clean_headings = []

    raw_sections = []
    remaining_text = element_text
    for i, heading in enumerate(raw_headings):
        if (
            i == 0
        ):  # The first section (Lead) won't have a title because it is implicitly assumed
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


def get_next_article_title_and_element(
    parser: iterparse,
) -> Tuple[str, Element]:
    """redirects and valid articles both have text tags,
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
        raise TypeError(
            "parser has to be an iterator craeted by xml.etree.ElementTree. invalid parser: {}".format(
                parser
            )
        )
    title = None

    while True:
        event, elem = None, None
        try:
            event, elem = next(parser)
        except StopIteration:
            raise StopIteration("Reached the end of the parser")
        # article title (title) matcher
        matches = re.search(r"{(.+)}(title)", elem.tag)
        if matches is not None:
            if re.search(r"^(.+):(.+)$", elem.text) is not None:
                continue
            elif (
                len(elem.text) > 200
            ):  # parsed all titles and no valid ones appear to be over 200 characters
                continue
            else:
                title = elem.text

        # article text (text) matcher
        matches = re.search(r"{(.+)}(text)", elem.tag)
        if matches is not None:
            if title is not None:
                return title, elem

        # article redirect (redirect) matcher
        matches = re.search(r"{(.+)}(redirect)", elem.tag)
        if matches is not None:
            title = None
