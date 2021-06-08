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
from sqlalchemy import func, Column, Index, Text, String
from sqlalchemy.dialects.postgresql import ARRAY

Base = declarative_base()


class CaseInsensitiveComparator(Comparator):
    def __eq__(self, other):
        return func.lower(self.__clause_element__()) == func.lower(other)


class Page(Base):
    __tablename__ = "pages"
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
        msg = "<Page: (\n\t"
        msg += "title='{}',\n\t".format(self.title)
        msg += "headings[:10]={},\n\t".format(self.headings[:10])
        msg += "sections[0]={},\n\t".format(self.sections)
        msg += "links[:10]={}\n".format(self.links[:10])
        msg += ")>"
        return msg


title_index = Index("title_index", func.lower(Page.title))

if __name__ == "__main__":
    from database.config import get_engine

    DATABASE_URI = "postgresql://postgres:postgres@localhost:5432/complete_wikipedia"
    engine = get_engine(DATABASE_URI)
    title_index.create(bind=engine)
