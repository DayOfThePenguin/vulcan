""" Postgres database indices for models defined in database.models

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
    along with Wikimap.  If not, see <https://www.gnu.org/licenses/>."""

from sqlalchemy import func, Index
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.engine import Engine

from database import get_engine
from database import Page, PageText, PageTalk, PageQuality
from .utils import create_logger


page_title_index = Index("page_title", Page.page_title)
page_title_lower_index = Index("page_title_lower", func.lower(Page.page_title))
talk_title_index = Index("talk_title_lower", func.lower(PageTalk.page_title))
quality_quality_index = Index("quality_quality", PageQuality.page_quality)
text_title_lower_index = Index("text_title_lower", func.lower(PageText.title))


def drop_indices(eng: Engine) -> None:
    """Delete all of the indices defined above if they exist on database engine

    Parameters
    ----------
    eng : Engine
        sqlalchemy.engine.Engine to attempt to delete the indices from
    """
    logger = create_logger()
    try:
        page_title_index.drop(bind=eng)
    except ProgrammingError:
        logger.warning("Index page_title on Page doesn't exist")
    try:
        page_title_lower_index.drop(bind=eng)
    except ProgrammingError:
        logger.warning("Index page_title_lower on Page doesn't exist")
    try:
        talk_title_index.drop(bind=eng)
    except ProgrammingError:
        logger.warning("Index talk_title_lower on PageTalk doesn't exist")
    try:
        quality_quality_index.drop(bind=eng)
    except ProgrammingError:
        logger.warning("Index quality_quality on PageQuality doesn't exist")
    try:
        text_title_lower_index.drop(bind=eng)
    except ProgrammingError:
        logger.warning("Index text_title_lower on PageText doesn't exist")


def create_page_indices(eng: Engine) -> None:
    """Attempt to create indices for Page model, skip if they exist

    Parameters
    ----------
    eng : Engine
        sqlalchemy.engine.Engine to attempt to create indices on
    """
    logger = create_logger()
    try:
        page_title_index.create(bind=eng)
        logger.info("Created Page Index page_title")
    except ProgrammingError:
        logger.info("Page Index page_title already exists")
    try:
        page_title_lower_index.create(bind=eng)
        logger.info("Created Page Index page_title_lower")
    except ProgrammingError:
        logger.info("Page Index page_title_lower already exists")


def create_page_talk_indices(eng: Engine) -> None:
    """Attempt to create indices for PageTalk model, skip if they exist

    Parameters
    ----------
    eng : Engine
        sqlalchemy.engine.Engine to attempt to create indices on
    """
    logger = create_logger()
    try:
        talk_title_index.create(bind=eng)
        logger.info("Created PageTalk Index talk_title_lower")
    except ProgrammingError:
        logger.info("PageTalk Index talk_title_lower already exists")


def create_page_quality_indices(eng: Engine) -> None:
    """Attempt to create indices for PageQuality model, skip if they exist

    Parameters
    ----------
    eng : Engine
        sqlalchemy.engine.Engine to attempt to create indices on
    """
    logger = create_logger()
    try:
        quality_quality_index.create(bind=eng)
        logger.info("Created PageQuality Index quality_lower")
    except ProgrammingError:
        logger.info("PageQuality Index quality_lower already exists")


def create_page_text_indices(eng: Engine) -> None:
    """Attempt to create indices for PageText model, skip if they exist

    Parameters
    ----------
    eng : Engine
        sqlalchemy.engine.Engine to attempt to create indices on
    """
    logger = create_logger()
    try:
        text_title_lower_index.create(bind=eng)
        logger.info("Created PageText Index text_title_lower")
    except ProgrammingError:
        logger.info("PageText Index text_title_lower already exists")


def create_indices(eng: Engine) -> None:
    """Create indices for all tables in a wikimap database

    if you're recreating the database from scratch, comment these indices
    out so you can insert without recomputing the index every query
    After you've built the DB, come back and add these before doing any queries
    do you can query lowercased indexed titles
    To delete: Index("title", func.lower(PageText.title)).drop(bind=eng)
    """
    create_page_indices(eng)
    create_page_talk_indices(eng)
    create_page_quality_indices(eng)
    create_page_text_indices(eng)


if __name__ == "__main__":
    DATABASE_URI = "postgresql://postgres:postgres@localhost:5432/complete_wikipedia"

    with get_engine(DATABASE_URI) as engine:
        # drop_indices(engine)
        create_page_indices(engine)
        create_page_quality_indices(engine)
        create_page_talk_indices(engine)
