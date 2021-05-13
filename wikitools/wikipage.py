from typing import List


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
            raise TypeError(
                "WikipediaPage.title must be a string. invalid title: {}".format(title)
            )
        # headings
        if not isinstance(headings, list):
            raise TypeError(
                "WikipediaPage.headings must be a list. invalid headings: {}".format(
                    headings
                )
            )
        for heading in headings:
            if not isinstance(heading, str):
                raise TypeError(
                    "WikipediaPage.headings items must be strings. invalid headings element: {}".format(
                        heading
                    )
                )
        # sections
        if not isinstance(sections, list):
            raise TypeError(
                "WikipediaPage.sections must be a list. invalid sections: {}".format(
                    sections
                )
            )
        for section in sections:
            if not isinstance(section, str):
                raise TypeError(
                    "WikipediaPage.sections items must be strings. invalid sections element: {}".format(
                        section
                    )
                )
        # links
        if not isinstance(links, list):
            raise TypeError(
                "WikipediaPage.links must be a list. invalid links: {}".format(links)
            )
        for link in links:
            if not isinstance(link, str):
                raise TypeError(
                    "WikipediaPage.links items must be strings. invalid links element: {}".format(
                        link
                    )
                )
        # every heading must have a section
        if len(sections) != len(headings):
            raise ValueError(
                "headings ({}) must be the same length as sections ({}). every heading must have a section".format(
                    sections, headings
                )
            )
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
