"""wikipage : module for defining WikipediaPage, an object fot storing information
about a Wikipedia page
"""
from typing import List

from pydantic import BaseModel, validator

from database.models import PageText


class WikipediaPage(BaseModel):
    """Represent a page in a Wikipedia database dump"""

    title: str
    headings: List[str]
    sections: List[str]
    links: List[str]

    @validator("sections")
    def headings_sections_same_length(cls, v, values):
        if "headings" in values and len(v) != len(values["headings"]):
            raise ValueError("headings must be the same length as sections")
        return v

    def page(self):
        """Make sqlalchemy representation of self
        Returns
        -------
        page : database.models.PageText
            self represented as a PageText from database.models to add to a database
        """
        page = PageText(
            title=self.title,
            headings=self.headings,
            sections=self.sections,
            links=self.links,
        )
        return page
