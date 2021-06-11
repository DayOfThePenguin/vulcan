"""

Notes
-----
URI Scheme: postgresql:(engine)://<USERNAME>:<PASSWORD>@<IP_ADDRESS>:<PORT>/<DATABASE_NAME>

- the :engine part is optional - sqlalchemy defaults to psycopg2

- don't worry this is just the local database config. no secret info is in the database running
  on localhost

Resources
---------
SQLAlchemy URI documentation: https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


DATABASE_URI = "postgresql://postgres:postgres@localhost:5432/wikipedia"
# 5432 is default postgres port. to change, change port definition (new:5432) in docker-compose
# docker port definition is host_port:container_port. change host port to new desired port,
# leave container port the same


def get_sessionmaker(db_uri=None, engine=None):
    if engine is None:
        engine = get_engine(db_uri)
    smaker = sessionmaker(bind=engine)
    return smaker


def get_engine(db_uri=None):
    if db_uri is None:
        db_uri = DATABASE_URI
    engine = create_engine(db_uri)
    return engine
