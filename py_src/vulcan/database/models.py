""" sqlalalchemy models for Wikimap's Postgres db

This file is part of Wikimap.

    Wikimap is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Wikimap is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with Wikimap.  If not, see <https://www.gnu.org/licenses/>.

Resources
---------
https://www.learndatasci.com/tutorials/using-databases-python-postgres-sqlalchemy-and-alembic/
https://stackoverflow.com/questions/28829236/is-it-possible-to-ignore-one-single-specific-line-with-pylint
https://stackoverflow.com/questions/53063778/case-insensitive-indexing-in-postgres-with-sqlalchemy
https://stackoverflow.com/questions/14419299/adding-indexes-to-sqlalchemy-models-after-table-creation
"""

from sqlalchemy import func, Column, Integer, Text, String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import Comparator, hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.schema import ForeignKey

from .utils import create_logger


Base = declarative_base()


class CaseInsensitiveComparator(Comparator):  # pylint: disable=abstract-method
    """Comparator for implementing lowercase comparisons on db fields

    Extends
    ----------
    Comparator :
        sqlalchemy.ext.hybrid.Comparator
    """

    def __eq__(self, other):
        return func.lower(self.__clause_element__()) == func.lower(other)


class PageText(Base):
    """ORM class for representing the headings and text content of a Wikipedia page

    Properties
    ----------
    __tablename__ : text
    title_insensitive : case-insensitive function for PageText.title (create
    PageText indices from database.indices before using, otherwise
    using this property will be prohibitively slow)

    Columns
    -------
    title : String(200)
        page's title
    headings : ARRAY(String(200))
        list of section headings
    sections : ARRAY(TEXT)
        list containing the text content under each heading
    """

    __tablename__ = "text"
    title = Column(String(200), primary_key=True)
    headings = Column(ARRAY(String(200)))
    sections = Column(ARRAY(Text()))
    links = Column(ARRAY(String(200)))

    @hybrid_property
    def title_insensitive(self):
        """case-insensitive title property"""
        return self.title.lower()

    @title_insensitive.comparator
    def title_insensitive(cls):  # pylint: disable=no-self-argument
        """case-insensitive title comparator"""
        return CaseInsensitiveComparator(cls.title)

    def __repr__(self):
        msg = "<PageText: (\n\t"
        msg += "title='{}',\n\t".format(self.title)
        msg += "headings[:10]={},\n\t".format(self.headings[:10])
        msg += "sections[0]={},\n\t".format(self.sections)
        msg += "links[:10]={}\n".format(self.links[:10])
        msg += ")>"
        return msg


class Page(Base):
    """ORM class for representing the ID and title of a page

    Properties
    ----------
    __tablename__ : page
    title_insensitive : case-insensitive function for Page.page_title (create
    Page indices from database.indices before using, otherwise using this
    property will be prohibitively slow)

    Columns
    -------
    page_id : Integer
        page's ID
    page_title : String(200)
        page's title
    """

    __tablename__ = "page"
    page_id = Column(Integer, primary_key=True)
    page_title = Column(String(200), unique=True)

    def __repr__(self):
        msg = "<Page: (\n\t"
        msg += "page_id={},\n\t".format(self.page_id)
        msg += "page_title='{}',\n\t".format(self.page_title)
        msg += ")>"
        return msg

    @hybrid_property
    def title_insensitive(self):
        """case-insensitive title property"""
        return self.page_title.lower()

    @title_insensitive.comparator
    def title_insensitive(cls):  # pylint: disable=no-self-argument
        """case-insensitive title comparator"""
        return CaseInsensitiveComparator(cls.page_title)


class PageLink(Base):
    """ORM class for representing the list of links found on a page

    Properties
    ----------
    __tablename__ : link

    Columns
    -------
    pl_from : Integer, ForeignKey(Page.page_id)
        id of the page with the below links
    pl_title : ARRAY(String(200))
        list of the titles of all links to other ns 0 pages on the page
    """

    __tablename__ = "link"
    pl_from = Column(Integer, ForeignKey(Page.page_id), primary_key=True)
    pl_titles = Column(ARRAY(String(200)))

    page = relationship("Page", foreign_keys="PageLink.pl_from")

    def __repr__(self):
        msg = "<PageLink: (\n\t"
        msg += "pl_from={},\n\t".format(self.pl_from)
        msg += "pl_titles[:10]='{}',\n\t".format(self.pl_titles[:10])
        msg += ")>"
        return msg


class PageTalk(Base):
    """ORM class for relating a talk page to its respective content page

    This relationship is important because the categories that Wikipedia uses
    to quantify the quality of each article include article talk pages (not content
    pages). So to figure out what quality category an article belongs to, you have
    to find the quality measure on the article's associated talk page and dereference
    the talk page (using its title) to get the quality for an article.

    Properties
    ----------
    __tablename__ : talk

    Columns
    -------
    page_id : Integer
        id of the ns 1 talk page corresponding to the ns 0 page with title page_title
    pl_title : String(200), ForeignKey(Page.page_title)
        title of the ns 1 talk page, also the same as the title of the associated ns 0
        page, so we can join on the titles across Page and PageTalk
    """

    __tablename__ = "talk"
    page_id = Column(Integer, primary_key=True)
    page_title = Column(String(200), ForeignKey(Page.page_title), unique=True)

    page = relationship("Page", foreign_keys="PageTalk.page_title")

    def __repr__(self):
        msg = "<PageTalk: (\n\t"
        msg += "page_id={},\n\t".format(self.page_id)
        msg += "page_title='{}',\n\t".format(self.page_title)
        msg += ")>"
        return msg

    @hybrid_property
    def title_insensitive(self):
        """case-insensitive title property"""
        return self.page_title.lower()

    @title_insensitive.comparator
    def title_insensitive(cls):  # pylint: disable=no-self-argument
        """case-insensitive title comparator"""
        return CaseInsensitiveComparator(cls.page_title)


class PageQuality(Base):
    """ORM class for representing the quality of a page

    Connects a talk page to its associated quality

    Properties
    ----------
    __tablename__ : quality

    Columns
    -------
    page_id : Integer, ForeignKey(PageTalk.page_id)
        id of the ns 1 talk page
    page_quality : String(2)
        quality of the page (FA, FL, A, GA, B, C)
    """

    __tablename__ = "quality"
    page_id = Column(Integer, ForeignKey(PageTalk.page_id), primary_key=True)
    page_quality = Column(String(2))

    page_talk = relationship("PageTalk", foreign_keys="PageQuality.page_id")

    def __repr__(self):
        msg = "<PageQuality: (\n\t"
        msg += "page_id={},\n\t".format(self.page_id)
        msg += "page_quality='{}',\n\t".format(self.page_quality)
        msg += ")>"
        return msg


def drop_recreate_all_tables_except_text(eng: Engine) -> None:
    """Delete and create empty instances of the tables defined above EXCEPT PageText

    Doesn't delete data from the PageText table due to the time (currently 14.5 hrs
    on 12 cores) required to rebuild it from scratch

    Parameters
    ----------
    eng : Engine
        sqlalchemy.engine.Engine to drop and create tables with
    """
    logger = create_logger()
    try:
        PageLink.__table__.drop(eng)
    except ProgrammingError:
        logger.info("Table link does not exist")
    try:
        PageQuality.__table__.drop(eng)
    except ProgrammingError:
        logger.info("Table quality does not exist")
    try:
        PageTalk.__table__.drop(eng)
    except ProgrammingError:
        logger.info("Table talk does not exist")
    try:
        Page.__table__.drop(eng)
    except ProgrammingError:
        logger.info("Table page does not exist")
    create_all_tables(eng)


def create_all_tables(eng: Engine) -> None:
    """create all tables, won't overwrite if tables exist

    Parameters
    ----------
    eng : Engine
        Engine to create the tables on
    """
    Base.metadata.create_all(eng)


if __name__ == "__main__":
    from database.config import get_engine

    DATABASE_URI = "postgresql://postgres:postgres@localhost:5432/complete_wikipedia"
    with get_engine(DATABASE_URI) as engine:
        Base.metadata.create_all(engine)
        drop_recreate_all_tables_except_text(engine)
