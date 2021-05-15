"""wikipage : module for defining WikipediaPage, an object fot storing information
about a Wikipedia page
"""
from typing import List

from db.models import Page


class WikipediaPage(object):
    """Represent a page in a Wikipedia database dump"""

    def __init__(
        self,
        title: str,
        headings: List[str],
        sections: List[str],
        links: List[str],
    ) -> None:
        # Type-checking
        # title
        if not isinstance(title, str):
            msg = "WikipediaPage.title must be a string. invalid title: {}"
            raise TypeError(msg.format(title))
        # headings
        if not isinstance(headings, list):
            msg = "WikipediaPage.headings must be a list. invalid headings: {}"
            raise TypeError(msg.format(headings))
        for heading in headings:
            if not isinstance(heading, str):
                msg = "WikipediaPage.headings items must be strings. invalid headings element: {}"
                raise TypeError(msg.format(heading))
        # sections
        if not isinstance(sections, list):
            msg = "WikipediaPage.sections must be a list. invalid sections: {}"
            raise TypeError(msg.format(sections))
        for section in sections:
            if not isinstance(section, str):
                msg = "WikipediaPage.sections items must be strings. invalid sections element: {}"
                raise TypeError(msg.format(section))
        # links
        if not isinstance(links, list):
            msg = "WikipediaPage.links must be a list. invalid links: {}"
            raise TypeError(msg.format(links))
        for link in links:
            if not isinstance(link, str):
                msg = "WikipediaPage.links items must be strings. invalid links element: {}"
                raise TypeError(msg.format(link))
        # every heading must have a section
        if len(sections) != len(headings):
            msg = "len(headings) ({}) must be equal to len(sections) ({}). "
            msg += "every heading must have a section"
            msg = msg.format(sections, headings)
            raise ValueError(msg)
        # variables
        self.title = title
        self.headings = headings
        self.sections = sections
        self.links = links

    def __eq__(self, other):
        """Equality method

        Parameters
        ----------
        other : WikipediaPage
            other object to compare to

        Returns
        -------
        True
            if title, headings, sections, and links are equal
        False
            else
        """
        if self.title != other.title:
            return False
        elif self.headings != other.headings:
            return False
        elif self.sections != other.sections:
            return False
        elif self.headings != other.headings:
            return False
        elif self.links != other.links:
            return False
        else:
            return True

    def __repr__(self):
        msg = "<Page: (\n\t"
        msg += "title='{}',\n\t".format(self.title)
        msg += "headings[:10]={},\n\t".format(self.headings[:10])
        msg += "sections[0]={},\n\t".format(self.sections)
        msg += "links[:10]={}\n".format(self.links[:10])
        msg += ")>"
        return msg

    def page(self):
        """Make sqlalchemy representation of self
        Returns
        -------
        page : models.Page
            self represented as a Page from models.py to add to a database
        """
        page = Page(
            title=self.title,
            headings=self.headings,
            sections=self.sections,
            links=self.links,
        )
        return page
