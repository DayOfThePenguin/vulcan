import prefect

from os import getpid
from pathlib import Path
from prefect import task, Flow
from prefect.executors import DaskExecutor
from typing import List, Tuple

import bz2
from xml.etree.ElementTree import iterparse

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import wikitools.wikidump as wikidump
import wikitools.wikixml as wikixml
from wikitools.wikixml import WikiXMLFile
from wikitools.wikipage import WikipediaPage
from db.models import Page, Base
import db.crud

DATABASE_URI = "postgresql://postgres:postgres@localhost:8001/wikipedia"


@task
def load_database_in_chunks(wiki_file: WikiXMLFile) -> None:
    """
    Seems like the bottleneck is in the creation of articles
    """
    logger = prefect.context.get("logger")
    engine = create_engine(DATABASE_URI)
    Session = sessionmaker(bind=engine)
    articles = []
    chunk_size = 1000
    chunk_num = 1
    with bz2.open(wiki_file.path, "r") as f:
        parser = iterparse(f)
        s = Session()
        while True:
            title, element = None, None
            try:
                title, element = wikixml.get_next_title_element(parser)
                links = wikixml.get_pagelinks(element)
                headings, sections = wikixml.get_headings_sections(element)
                article = WikipediaPage(title, headings, sections, links)
                articles.append(article)
                del title
                element.clear()
                del element
            except StopIteration:
                break
            if len(articles) == chunk_size:
                logger.info("{} started writing chunk to db".format(chunk_num))
                for article in articles:
                    page = Page(
                        title=article.title,
                        headings=article.headings,
                        sections=article.sections,
                        links=article.links,
                    )
                    s.add(page)
                s.commit()  # commit every chunk_size
                logger.info("{} Wrote chunk to db".format(chunk_num))
                chunk_num += 1
                articles = []
        s.commit()
        s.close()


@task
def is_dump_contiguous(files: List[WikiXMLFile]) -> bool:
    """ """
    logger = prefect.context.get("logger")
    result = wikidump.verify_contiguous_dump(files)
    if not result["contiguous"]:
        logger.error(
            """Missing files detected. Please download them and rerun this script to
            check for a complete set of database chunks. The last valid file in your
            database dump is: {}""".format(
                result["last_valid_file"].path.name
            )
        )
        raise FileNotFoundError(
            "Incomplete database dump. The last valid file in your database dump is: {}".format(
                result["last_valid_file"]
            )
        )


@task
def get_xml_files_from_data_path(data_path: Path) -> List[WikiXMLFile]:
    sorted_files = wikidump.load_wikifile_list(data_path)
    return sorted_files


if __name__ == "__main__":
    data_path = Path("/hdd/datasets/wikipedia_4_20_21")
    prefect.config.logging.level = "DEBUG"

    with Flow("wikipedia-etl") as flow:
        sorted_files = get_xml_files_from_data_path(data_path)
        check_for_complete = is_dump_contiguous(sorted_files)
        # test run
        # load_futures = load_database_in_chunks.map(sorted_files[:1])
        # full run
        load_futures = load_database_in_chunks.map(sorted_files)
        load_futures.set_dependencies(upstream_tasks=[check_for_complete])

    # test run
    # flow.run()

    # full, prefect server, agent, dask-scheduler, dask-worker run
    flow.executor = DaskExecutor("tcp://192.168.0.12:8786")
    flow.register(project_name="wikipedia")
