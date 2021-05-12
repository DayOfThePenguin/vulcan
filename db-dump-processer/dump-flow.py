import bz2
import prefect
import re

from xml.etree.ElementTree import iterparse
from os import getpid
from pathlib import Path
from prefect import task, Flow
from prefect.executors import DaskExecutor
from typing import List, Tuple

import wikitools
from wikitools.wikixml import WikiXMLFile


@task
def find_longest_article_in_xml_chunk(wiki_file: WikiXMLFile) -> Tuple[int, str]:
    logger = prefect.context.get("logger")
    with bz2.open(wiki_file.path, "r") as f:
        parser = iter(iterparse(f))
        article_num = 0
        max_title_len = 0
        max_title_name = None
        max_elem = None

        while True:
            title, elem = None, None
            try:
                title, elem = wikitools.wikixml.get_next_article_title_and_element(
                    parser
                )
                article_num += 1
            except StopIteration:
                logger.info(
                    "{} - total Number of Articles: {}\nlongest title ({}): {}".format(
                        getpid(), article_num, max_title_len, max_title_name
                    )
                )
                return max_title_len, max_title_name
            if len(title) > max_title_len:
                (
                    headings,
                    sections,
                ) = wikitools.wikixml.get_headings_and_sections_from_element(elem.text)
                if (
                    len(headings) < 3
                ):  # looking at the data, anything with less than 3 headings is usually not a valid article
                    continue
                max_title_len = len(title)
                max_title_name = title
                max_elem = elem
                logger.debug(
                    "{} - New max length ({}): {}".format(
                        getpid(), max_title_len, max_title_name
                    )
                )
            del title
            elem.clear()
            del elem


@task
def is_dump_contiguous(sorted_files: List[WikiXMLFile]) -> bool:
    """Verifies is there are any gaps in the xml files in the dump.
    THIS FUNCTION CAN NOT GUARANTEE THAT ALL FILES ARE PRESENT, IT CAN ONLY
    VERIFY THAT YOU HAVE ALL FILES BETWEEN THE LOWEST START_IDX AND HIGHEST
    END_IDX ARE PRESENT.

    For ease of use, when downloading a new dump, it is best to download the
    start and end files first, filling in the files inbetween after. In that
    specific case, a contiguous dump will also be a complete dump.
    """

    logger = prefect.context.get("logger")
    all_present, missing_file = wikitools.wikidump.verify_contiguous_dump()
    if not all_present:
        logger.error("You are missing the file(s) after {}".format(missing_file))
        raise FileNotFoundError(
            """Missing files detected. Please download them
                                and rerun this script to check for a complete
                                set of database chunks"""
        )


@task
def get_xml_files_from_data_path(data_path: Path) -> List[WikiXMLFile]:
    logger = prefect.context.get("logger")
    if data_path.exists() == False:
        raise FileNotFoundError("Could not find directory {}".format(data_path))
    elif data_path.is_file():
        raise NotADirectoryError(
            """data_path '{}' is a file. Please set
                                 data_path to the directory containing the
                                 multistream bzipped
                                 files""".format(
                data_path
            )
        )
    else:
        logger.info("Verified {} is a valid directory".format(data_path))

    unsorted_files = []  # [start_idx, end_idx, file_path]
    for item in data_path.iterdir():
        if re.search(r"\.part$", str(item)) is not None:
            all_present = False
            logger.warning(
                """(incomplete file download) - {}\n
            The supplied data directory contains a .part file, indicating that
            the download of that file didn't complete. Please verify the file
            downloaded completely""".format(
                    item
                )
            )
        elif re.search(r"\.bz2$", str(item)) is None:
            logger.warning("(non-bz2 file) - {}\n".format(item))
            continue
        try:
            start_idx, end_idx = re.split(
                r"wiki-(.+)-pages-articles-multistream(.+).xml-p(.+)p(.+).bz2",
                str(item),
            )[3:5]
            unsorted_files.append(WikiXMLFile(int(start_idx), int(end_idx), item))
            logger.debug("Found multistream bz2 file at {}".format(item))
        except ValueError:
            logger.warning("(not pages-articles-multistream file) - {}\n".format(item))
    print()
    sorted_files = sorted(unsorted_files, key=lambda file: file.start_idx)
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
        longest_articles = find_longest_article_in_xml_chunk.map(sorted_files)
        longest_articles.set_dependencies(upstream_tasks=[check_for_complete])
        show_longest(longest_articles)

    flow.executor = DaskExecutor("tcp://192.168.0.12:8786")

    # flow.run()
    flow.register(project_name="wikipedia")
