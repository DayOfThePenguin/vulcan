""" module for loading data from Wikipedia's page table

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

page table schema: https://www.mediawiki.org/wiki/Manual:Page_table

Python <3.9 has a bug in concurrent.futures that causes the context manager for
ProcessPoolExecutor to raise an OSError - https://bugs.python.org/issue39098.
So don't use the context manager and instead explicity define the executor and
shut it down with wait=True
"""
import gc

from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable, List

from blist import blist
from pydantic import validate_arguments

from database import get_sessionmaker, get_engine
from .indices import create_page_indices
from .models import Page, PageTalk, drop_recreate_all_tables_except_text
from .sql import get_line_iterator, LineEnum, data_lines_generator
from .utils import create_logger

DATABASE_URI = "postgresql://postgres:postgres@localhost:5432/complete_wikipedia"
NAMESPACE = None


def transform_pages(line_iter: Iterable, namespace: int) -> blist:
    """Given an iterator for all items in a line, return a list of the items in ns

    Parameters
    ----------
    line_iter : Iterable
        Iterator from database.sql.get_line_iterator that will yield chunks
    namespace : int, optional
        namespace of the page

    Returns
    -------
    pages_chunk : blist
        list of the pages with namespace ns found in line_iter
    """
    logger = create_logger()
    pages_chunk = blist()
    for entry in line_iter:
        ns_b_string = bytes(f"{namespace}", "utf-8")
        if entry.group(2) == ns_b_string and entry.group(5) == b"0":
            pages_chunk.append([entry.group(1), entry.group(3)])
            if len(pages_chunk) % 250000 == 0:
                logger.debug("matched %i pages", len(pages_chunk))
    return pages_chunk


def load_pages(pages_chunk: blist) -> List[List]:
    """Load a blist of pages into the page table database using Page ORM

    Parameters
    ----------
    pages_chunk : blist
        blist of pages from transform_pages to be loaded into the database
        at DATABASE_URI

    Returns
    -------
    List[added_count : int, skipped_pages : List]
        List containing the number of pages that were added and information
        about the pages that were skipped because their title was > 200 characters
    """
    logger = create_logger()
    sess_maker = get_sessionmaker(db_uri=DATABASE_URI)
    chunk_len = len(pages_chunk)
    added_count = 0
    skipped_pages = []
    with sess_maker() as sess:
        with sess.begin():
            for i, row in enumerate(pages_chunk):
                title = row[1].decode("utf-8").replace("_", " ")
                if len(title) > 200:
                    skipped_pages.append(row)
                    continue  # ignore titles > 200 chars
                page = Page(
                    page_id=int(row[0]),
                    page_title=title,
                )
                sess.add(page)
                added_count += 1
                if added_count % 250000 == 0 or i == chunk_len - 1:
                    # sess.commit()
                    logger.debug("%i added", added_count)
                    logger.debug("%i skipped", len(skipped_pages))
            # sess.close()
    return [added_count, skipped_pages]


def load_page_talks(pages_chunk: blist) -> List[List]:
    """Load a blist of pages into the talk table database using PageTalk ORM

    Parameters
    ----------
    pages_chunk : blist
        blist of talk pages from transform_pages to be loaded into the database
        at DATABASE_URI

    Returns
    -------
    List[added_count : int, skipped_pages : List]
        List containing the number of pages that were added and information
        about the pages that were skipped because their title was > 200 characters
    """
    logger = create_logger()
    sess_maker = get_sessionmaker(db_uri=DATABASE_URI)
    chunk_len = len(pages_chunk)
    added_count = 0
    skipped_pages = []
    with sess_maker() as sess:
        with sess.begin():
            for i, row in enumerate(pages_chunk):
                title = row[1].decode("utf-8").replace("_", " ")
                if len(title) > 200:
                    skipped_pages.append(row)
                    continue  # ignore titles > 200 chars
                result = sess.query(Page).filter(
                    Page.page_title == title).first()
                if result is None:
                    skipped_pages.append(row)
                    if len(skipped_pages) % 50000 == 0:
                        print(f"{added_count} added")
                        print(f"{len(skipped_pages)} skipped")
                    continue
                page = PageTalk(
                    page_id=int(row[0]),
                    page_title=title,
                )
                sess.add(page)
                added_count += 1
                if added_count % 250000 == 0 or i == chunk_len - 1:
                    # sess.commit()
                    logger.debug("%i added", added_count)
                    logger.debug("%i skipped", len(skipped_pages))
            # sess.close()
    return [added_count, skipped_pages]


def transform_load_line(chunk_path: Path):
    """Transform (select pages from namespace ns) and load into a database

    This function is intended to be mapped to an iterator of files to
    transform and load

    Parameters
    ----------
    chunk_path : Path
        Path to temporary file containing the line to load
    ns : int, optional
        namespace of pages being transformed and loaded

    Returns
    -------
    List
        See Returns section of load_pages
    """
    with open(chunk_path, "rb") as chunk_file:
        line = chunk_file.read()
        line_iter = get_line_iterator(line, LineEnum.PAGE)
    if line_iter is None:
        return None
    pages = transform_pages(line_iter, NAMESPACE)
    if NAMESPACE == 0:
        results = load_pages(pages)
    elif NAMESPACE == 1:
        results = load_page_talks(pages)
    return results


@validate_arguments
def extract_pages(page_file: Path, tmp_path: Path) -> Path:
    """Extract pages from page_file and save each line into a file in tmp_path

    Parameters
    ----------
    page_file : Path
        path to sql file to extract lines from
    tmp_path : Path
        path to the tmp folder to store each line's file in

    Returns
    -------
    Path
        path to the subdirectory where chunks are stored
    """
    logger = create_logger()
    chunk_dir = tmp_path.joinpath("page_chunks")
    chunk_dir.mkdir()
    logger.info(chunk_dir)
    with open(page_file, "rb") as page_file:
        for i, line in enumerate(data_lines_generator(page_file)):
            line_file_path = chunk_dir.joinpath(f"{i}.sql")
            with open(line_file_path, "wb") as tmp_f:
                tmp_f.write(line)
            if i % 1000 == 0:
                logger.info("wrote %s tmp page files", i)
    return chunk_dir


@validate_arguments
def etl_page(page_file: Path, namespace: int = 0):
    """Run an extract, transform, load pipeline on pages from namespace in page_file

    Parameters
    ----------
    page_file : Path
        Path to sql file for ETL
    ns : int, optional
        namespace of pages to load, by default 0
    """
    logger = create_logger()
    with TemporaryDirectory(prefix="/hdd/") as tmp_dir, Path(tmp_dir) as tmp_path:
        chunk_dir = extract_pages(page_file, tmp_path)
        pages_added = 0
        skipped_pages = []
        global NAMESPACE
        NAMESPACE = namespace
        with ProcessPoolExecutor() as executor:
            for i, result in enumerate(executor.map(transform_load_line, chunk_dir.iterdir())):
                pages_added += result[0]
                for skip in result[1]:
                    skipped_pages.append(skip)
                if i % 1000 == 0:
                    logger.info(
                        "%i lines, %i pages loaded, %i skipped",
                        i,
                        pages_added,
                        len(skipped_pages),
                    )
        logger.info("FINAL: %i pages loaded", pages_added)
        logger.info("Skipped %i namespace %i pages",
                    len(skipped_pages), namespace)

        print("post-shutdown")
    gc.collect()
    print("post-tmp context manager")


if __name__ == "__main__":
    main_logger = create_logger()
    data_file = Path(
        "/home/user/Developer/active/restore_wiki/data/enwiki-20210520-page.sql")
    with get_engine(DATABASE_URI) as engine:
        drop_recreate_all_tables_except_text(engine)
        smaker = get_sessionmaker(engine=engine)
        session = smaker()
        if session.query(Page.page_title).count() != 6301565:
            session.close()  # don't want a copy on every forked process
            drop_recreate_all_tables_except_text(engine)
            try:
                etl_page(data_file, namespace=0)
            except OSError:
                main_logger.warning("OSError from concurrent.futures, passing")
        else:
            main_logger.info("Page table already full, passing")
        print("post-function")
        session = smaker()
        create_page_indices(engine)
        talk_deleted = session.query(PageTalk).delete()
        session.commit()
        session.close()  # don't want a copy on every forked process
        main_logger.info("Deleted %i rows from PageTalk", talk_deleted)
        try:
            etl_page(data_file, namespace=1)
        except OSError:
            main_logger.warning("OSError from concurrent.futures, passing")
