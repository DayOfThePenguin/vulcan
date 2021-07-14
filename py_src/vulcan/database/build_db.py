from pydantic import BaseModel

from database.page import etl_page


class DatabaseConfig(BaseModel):
    pass


def build_db():
    pass
    # etl_page()
