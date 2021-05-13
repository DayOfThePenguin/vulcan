import bz2
import prefect

from xml.etree.ElementTree import iterparse
from os import getpid
from pathlib import Path
from prefect import task, Flow
from prefect.executors import DaskExecutor
from typing import List, Tuple

import wikitools.wikidump
from wikitools.wikixml import WikiXMLFile


@task
def find_longest_article_in_xml_chunk(wiki_file: WikiXMLFile) -> Tuple[int, str]:
    logger = prefect.context.get("logger")
    with bz2.open(wiki_file.path, "r") as f:
        parser = iter(iterparse(f))
        article_num = 0
        max_title_len = 0
        max_title_name = None

        while True:
            title, elem = None, None
            try:
                title, elem = wikitools.wikixml.get_next_title_element(parser)
                article_num += 1
            except StopIteration:
                logger.info(
                    "{} - total Number of Articles: {}\nlongest title ({}): {}".format(
                        getpid(), article_num, max_title_len, max_title_name
                    )
                )
                return max_title_len, max_title_name
            if len(title) > max_title_len:
                max_title_len = len(title)
                max_title_name = title
                logger.debug(
                    "{} - New max length ({}): {}".format(
                        getpid(), max_title_len, max_title_name
                    )
                )
            del title
            elem.clear()
            del elem


@task
def is_dump_contiguous(files: List[WikiXMLFile]) -> bool:
    """Verifies is there are any gaps in the xml files in the dump.
    THIS FUNCTION CAN NOT GUARANTEE THAT ALL FILES ARE PRESENT, IT CAN ONLY
    VERIFY THAT YOU HAVE ALL FILES BETWEEN THE LOWEST START_IDX AND HIGHEST
    END_IDX ARE PRESENT.

    For ease of use, when downloading a new dump, it is best to download the
    start and end files first, filling in the files inbetween after. In that
    specific case, a contiguous dump will also be a complete dump.
    """
    logger = prefect.context.get("logger")
    result = wikitools.wikidump.verify_contiguous_dump(files)
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
    sorted_files = wikitools.wikidump.load_wikifile_list(data_path)
    return sorted_files


@task
def show_longest(longest_articles):
    logger = prefect.context.get("logger")
    for article in longest_articles:
        logger.info(article)


@task
def say_hello():
    logger = prefect.context.get("logger")
    logger.info("Hello, Cloud!")


if __name__ == "__main__":
    data_path = Path("/hdd/datasets/wikipedia_4_20_21")
    prefect.config.logging.level = "DEBUG"

    with Flow("find-longest-article") as flow:
        sorted_files = get_xml_files_from_data_path(data_path)
        say_hello()
        check_for_complete = is_dump_contiguous(sorted_files)
        # test run
        longest_articles = find_longest_article_in_xml_chunk.map(sorted_files[:2])
        # full run
        # longest_articles = find_longest_article_in_xml_chunk.map(sorted_files[:2])
        longest_articles.set_dependencies(upstream_tasks=[check_for_complete])
        show_longest(longest_articles)

    # test run
    flow.run()

    # full, prefect server, agent, dask-scheduler, dask-worker run
    # flow.executor = DaskExecutor("tcp://192.168.0.12:8786")
    # flow.register(project_name="wikipedia")
