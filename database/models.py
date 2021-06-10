"""

Resources
---------
https://www.learndatasci.com/tutorials/using-databases-python-postgres-sqlalchemy-and-alembic/
https://stackoverflow.com/questions/28829236/is-it-possible-to-ignore-one-single-specific-line-with-pylint
https://stackoverflow.com/questions/53063778/case-insensitive-indexing-in-postgres-with-sqlalchemy
https://stackoverflow.com/questions/14419299/adding-indexes-to-sqlalchemy-models-after-table-creation
"""
import logging

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import Comparator, hybrid_property
from sqlalchemy import func, Column, Index, Integer, Text, String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import relationship
from sqlalchemy.schema import ForeignKey

Base = declarative_base()


class CaseInsensitiveComparator(Comparator):
    def __eq__(self, other):
        return func.lower(self.__clause_element__()) == func.lower(other)


class Page(Base):
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
        return self.page_title.lower()

    @title_insensitive.comparator
    def title_insensitive(cls):  # pylint: disable=no-self-argument
        return CaseInsensitiveComparator(cls.page_title)


class PageTalk(Base):
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
        return self.page_title.lower()

    @title_insensitive.comparator
    def title_insensitive(cls):  # pylint: disable=no-self-argument
        return CaseInsensitiveComparator(cls.page_title)


class PageQuality(Base):
    __tablename__ = "quality"
    page_id = Column(Integer, ForeignKey(PageTalk.page_id), primary_key=True)
    page_quality = Column(String(2))

    page = relationship("PageTalk", foreign_keys="PageQuality.page_id")

    def __repr__(self):
        msg = "<PageQuality: (\n\t"
        msg += "page_id={},\n\t".format(self.page_id)
        msg += "page_quality='{}',\n\t".format(self.page_quality)
        msg += ")>"
        return msg


class PageText(Base):
    __tablename__ = "text"
    title = Column(String(200), primary_key=True)
    headings = Column(ARRAY(String(200)))
    sections = Column(ARRAY(Text()))
    links = Column(ARRAY(String(200)))

    @hybrid_property
    def title_insensitive(self):
        return self.title.lower()

    @title_insensitive.comparator
    def title_insensitive(cls):  # pylint: disable=no-self-argument
        return CaseInsensitiveComparator(cls.title)

    def __repr__(self):
        msg = "<PageText: (\n\t"
        msg += "title='{}',\n\t".format(self.title)
        msg += "headings[:10]={},\n\t".format(self.headings[:10])
        msg += "sections[0]={},\n\t".format(self.sections)
        msg += "links[:10]={}\n".format(self.links[:10])
        msg += ")>"
        return msg


# Page
page_title_index = Index("page_title", Page.page_title)
page_title_lower_index = Index("page_title_lower", func.lower(Page.page_title))

# PageTalk
talk_title_index = Index("talk_title_lower", func.lower(PageTalk.page_title))

# PageQuality
quality_quality_index = Index("quality_quality", PageQuality.page_quality)

# PageText
text_title_lower_index = Index("text_title_lower", func.lower(PageText.title))


def create_indices(engine):
    """Create indices for all tables in a wikimap database

    if you're recreating the database from scratch, comment these indices
    out so you can insert without recomputing the index every query
    After you've built the DB, come back and add these before doing any queries
    do you can query lowercased indexed titles
    To delete: Index("title", func.lower(PageText.title)).drop(bind=engine)
    """

    # Page indices
    try:
        page_title_index.create(bind=engine)
        logging.info("Created Page Index page_title")
    except ProgrammingError:
        logging.info("Page Index page_title already exists")
    try:
        page_title_lower_index.create(bind=engine)
        logging.info("Created Page Index page_title_lower")
    except ProgrammingError:
        logging.info("Page Index page_title_lower already exists")
    # PageTalk indices
    try:
        talk_title_index.create(bind=engine)
        logging.info("Created PageTalk Index talk_title_lower")
    except ProgrammingError:
        logging.info("PageTalk Index talk_title_lower already exists")
    # PageQuality indices
    try:
        quality_quality_index.create(bind=engine)
        logging.info("Created PageQuality Index quality_lower")
    except ProgrammingError:
        logging.info("PageQuality Index quality_lower already exists")
    # PageText indices
    try:
        text_title_lower_index.create(bind=engine)
        logging.info("Created PageText Index text_title_lower")
    except ProgrammingError:
        logging.info("PageText Index text_title_lower already exists")


if __name__ == "__main__":
    from database.config import get_engine

    logging.basicConfig(level=logging.INFO)
    DATABASE_URI = "postgresql://postgres:postgres@localhost:5432/complete_wikipedia"
    engine = get_engine(DATABASE_URI)
    Base.metadata.create_all(engine)
    create_indices(engine)
