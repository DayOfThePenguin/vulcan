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


def get_sessionmaker():
    engine = get_engine()
    sm = sessionmaker(bind=engine)
    return sm


def get_engine():
    engine = create_engine(DATABASE_URI)
    return engine
