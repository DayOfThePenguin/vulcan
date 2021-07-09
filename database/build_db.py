from pydantic import BaseModel

from database.pages import etl_page


class DatabaseConfig(BaseModel):
    pass


def build_db():
    etl_page()
