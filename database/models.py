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
        return "<Page: (\n\t title='{}',\n\t headings[:10]={},\n\t sections[0]={},\n\t links[:10]={}\n)>".format(
            self.title, self.headings[:10], self.sections[0], self.links[:10]
        )
