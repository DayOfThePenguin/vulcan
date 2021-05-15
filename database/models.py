"""

Resources
---------
https://www.learndatasci.com/tutorials/using-databases-python-postgres-sqlalchemy-and-alembic/
"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Text, String
from sqlalchemy.dialects.postgresql import ARRAY

Base = declarative_base()


class Page(Base):
    __tablename__ = "pages"
    title = Column(String(200), primary_key=True)
    headings = Column(ARRAY(String(200)))
    sections = Column(ARRAY(Text()))
    links = Column(ARRAY(String(200)))

    def __repr__(self):
        msg = "<Page: (\n\t"
        msg += "title='{}',\n\t".format(self.title)
        msg += "headings[:10]={},\n\t".format(self.headings[:10])
        msg += "sections[0]={},\n\t".format(self.sections)
        msg += "links[:10]={}\n".format(self.links[:10])
        msg += ")>"
        return msg
