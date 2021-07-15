"""Module for connecting to a Postgres database with SQL Alchemy

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

Notes
-----
URI Scheme: postgresql:(engine)://<USERNAME>:<PASSWORD>@<IP_ADDRESS>:<PORT>/<DATABASE_NAME>

- 5432 is default postgres port. to change, change port definition (new:5432)
  in docker-compose docker port definition is host_port:container_port. change host port
  to new desired port, leave container port the same
- the :engine part is optional - sqlalchemy defaults to psycopg2
- don't worry this is just the local database config. no secret info is in the
  database running on localhost

Resources
---------
SQLAlchemy URI documentation: https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls
"""
import contextlib
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker


DATABASE_URI = "postgresql://postgres:postgres@localhost:5432/complete_wikipedia"


def get_sessionmaker(db_uri: str = None, engine: Engine = None) -> Generator:
    """return a sessionmaker for db with uri db_uri

    Parameters
    ----------
    db_uri : [str], optional
        database uri, in the format described in the module notes, by default
        None (in this case, db_uri will be DATABASE_URI)
    engine : [Engine], optional
        if you want a sessionmaker for a specific engine (besides the one generated
        by default from DATABASE_URI), by default None

    Returns
    -------
    smaker : [Generator]
        sqlalchemy.orm.Session factory.
    """
    if engine is None:
        with get_engine(db_uri) as engine:
            smaker = sessionmaker(bind=engine)
    else:
        smaker = sessionmaker(bind=engine)
    return smaker


@contextlib.contextmanager
def get_engine(db_uri: str = None) -> Engine:
    """context manager for an sqlalchemy.engine.Engine; yields a single Engine

    Parameters
    ----------
    db_uri : [str], optional
        database uri, in the format described in the module notes, by default
        None (in this case, db_uri will be DATABASE_URI)

    Yields
    -------
    Iterator[Engine]
        sqlalchemy.engine.Engine for database at db_uri
    """
    if db_uri is None:
        db_uri = DATABASE_URI
    engine = create_engine(db_uri)
    yield engine
    engine.dispose()
