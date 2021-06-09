"""

Resources
---------
https://www.learndatasci.com/tutorials/using-databases-python-postgres-sqlalchemy-and-alembic/
https://stackoverflow.com/questions/28829236/is-it-possible-to-ignore-one-single-specific-line-with-pylint
https://stackoverflow.com/questions/53063778/case-insensitive-indexing-in-postgres-with-sqlalchemy
https://stackoverflow.com/questions/14419299/adding-indexes-to-sqlalchemy-models-after-table-creation
"""
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
    page_title = Column(String(200))

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


page_title_index = Index("page_title_index", func.lower(Page.page_title))


class PageQuality(Base):
    __tablename__ = "quality"
    page_id = Column(Integer, ForeignKey(Page.page_id), primary_key=True)
    page_quality = Column(String(2))

    page = relationship("Page", foreign_keys="PageQuality.page_id")

    @hybrid_property
    def quality_insensitive(self):
        return self.page_quality.lower()

    @quality_insensitive.comparator
    def quality_insensitive(cls):  # pylint: disable=no-self-argument
        return CaseInsensitiveComparator(cls.page_quality)


quality_index = Index("quality_index", func.lower(PageQuality.page_quality))


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


text_title_index = Index("title_index", func.lower(PageText.title))

if __name__ == "__main__":
    from database.config import get_engine

    DATABASE_URI = "postgresql://postgres:postgres@localhost:5432/complete_wikipedia"
    engine = get_engine(DATABASE_URI)
    Base.metadata.create_all(engine)
    # if you're recreating the database from scratch, comment these indices
    # out so you can insert without recomputing the index every query
    # After you've built the DB, come back and add these before doing any queries
    # do you can query lowercased indexed titles
    try:
        text_title_index.create(bind=engine)
    except ProgrammingError:
        "Index already exists, passing"

    try:
        page_title_index.create(bind=engine)
    except ProgrammingError:
        "Index already exists, passing"

    # try:
    #     quality_index.create(bind=engine)
    # except ProgrammingError:
    #     "Index already exists, passing"
