import datetime
import logging
import multiprocessing as mp
from time import sleep
from typing import List, Tuple

from os import getpid
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# local modules from PYTHONPATH
import wikitools.wikidump as wikidump
import wikitools.wikixml as wikixml

from wikitools.wikixml import WikiXMLFile
from wikitools.wikipage import WikipediaPage
from db.models import Page

DATABASE_URI = "postgresql://postgres:postgres@localhost:8001/wikipedia"


def commit_list(pages_to_commit
    commit_logger, session_gen
) -> Tuple[int, int, int]:
    """write a list to a database

    takes a list to commit, create a session, check if each page is in the database, note duplicate if duplicate, else stage
    after all files staged, commit"""

    sess = session_gen()
    errors = 0
    new_additions = 0
    new_duplicates = 0
    for i in range(len(pages_to_commit)):
        page = pages_to_commit.pop()
        if sess.query(Page).filter_by(title=page.title).first() is None:
            try:  # if page isn't in db
                sess.add(page.page())
                new_additions += 1
            except Exception as msg:
                errors += 1
                mtext = "page:{} => msg:{}\n".format(page, msg)
                stats_file.write(mtext)
                commit_logger.error(mtext)
                sess.rollback()
        else:  # if page is in db
            new_duplicates += 1
            commit_logger.info("pid: %i duplicate %s", getpid(), page.title)
            duplicates_file.write(page.title + "\n")
    sess.commit()
    sess.close()
    num_additions += new_additions
    num_errors += errors
    num_duplicates += new_duplicates
    del sess
    msg = "pid: {} adds - {}/{}\tduplicates - {}/{}\terrors - {}/{}"
    msg = msg.format(
        getpid(),
        new_additions,
        num_additions,
        new_duplicates,
        num_duplicates,
        errors,
        num_errors,
    )
    commit_logger.info(msg)
    stats_file.write(msg + "\n")


def file_etl(file: WikiXMLFile):
    """[summary]

    [extended_summary]

    Parameters
    ----------
    file : WikiXMLFile
        [description]

    Returns
    -------
    [type]
        [description]
    """
    etl_logger = mp.get_logger()
    # database
    engine = create_engine(DATABASE_URI)
    session_generator = sessionmaker(bind=engine)
    # loop
    num_pages = 0
    num_additions = 0
    num_duplicates = 0
    num_errors = 0
    chunk_size = 2000
    worker_start = datetime.datetime.now().isoformat()
    duplicates_file_name = "logs/duplicates_{}_{}_{}.txt".format(
        file.start_idx, file.end_idx, worker_start
    )
    duplicates_file = open(duplicates_file_name, "w")
    stats_file = open(
        "logs/statspid_{}_{}_{}_{}.txt".format(
            getpid(), worker_start, file.start_idx, file.end_idx
        ),
        "w",
    )
    pages = []

    with file.parser() as parser:
        while True:
            try:
                title, element = wikixml.get_next_title_element(parser)
            except StopIteration:
                sess = Session()
                new_errors = 0
                new_additions = 0
                new_duplicates = 0
                for i in range(len(pages)):
                    page = pages.pop()
                    if sess.query(Page).filter_by(title=page.title).first() is None:
                        try:  # if page isn't in db
                            sess.add(page.page())
                            new_additions += 1
                        except Exception as msg:
                            new_errors += 1
                            msg = "page:{} => msg:{}\n".format(page, msg)
                            stats_file.write(msg)
                            etl_logger.error(msg)
                            sess.rollback()
                    else:  # if page is in db
                        new_duplicates += 1
                        etl_logger.info("pid: %i duplicate %s", getpid(), page.title)
                        duplicates_file.write(page.title + "\n")
                etl_logger.info(
                    "pid: %i - staged %i new files. %i duplicates ignored",
                    getpid(),
                    new_additions,
                    new_duplicates,
                )
                sess.commit()
                sess.close()
                num_additions += new_additions
                num_errors += new_errors
                num_duplicates += new_duplicates
                del sess
                msg = "pid: {} committed. {} errors".format(getpid(), new_errors)
                etl_logger.info(msg)
                stats_file.write(msg + "\n")
                msg = "********\nfinished processing {}\n".format(str(file.path))
                msg += "{} total pages\n".format(num_pages)
                msg += "{} total additions\n".format(num_additions)
                msg += "{} total duplicates\n".format(num_duplicates)
                msg += "{} total errors\n".format(num_errors)
                msg += "********"
                etl_logger.info(msg)
                stats_file.write(msg + "\n")
                duplicates_file.close()
                stats_file.close()
                return 0

            links = wikixml.get_pagelinks(element)
            headings, sections = wikixml.get_headings_sections(element)
            page = WikipediaPage(title, headings, sections, links)
            pages.append(page)
            num_pages += 1
            element.clear()
            del title, element, headings, sections, page
            # every chunksize
            if num_pages % chunk_size == 0:
                commit_logger.info("pid: %i processed %i files", getpid(), num_pages)
                new_additions, new_duplicates, new_errors = commit_list()


def is_dump_contiguous(files: List[WikiXMLFile]) -> bool:
    """Verifies is there are any gaps in the xml files in the dump.
    THIS FUNCTION CAN NOT GUARANTEE THAT ALL FILES ARE PRESENT, IT CAN ONLY
    VERIFY THAT YOU HAVE ALL FILES BETWEEN THE LOWEST START_IDX AND HIGHEST
    END_IDX ARE PRESENT.

    For ease of use, when downloading a new dump, it is best to download the
    start and end files first, filling in the files inbetween after. In that
    specific case, a contiguous dump will also be a complete dump.
    """
    contig_logger = mp.get_logger()
    result = wikidump.verify_contiguous_dump(files)
    if not result["contiguous"]:
        contig_logger.error(
            """Missing files detected. Please download them and rerun this script to
            check for a complete set of database chunks. The last valid file in your
            database dump is: {}""".format(
                result["last_valid_file"].path.name
            )
        )
        raise FileNotFoundError(
            "Incomplete database dump. The last valid file in your database dump is: {}".format(
                result["last_valid_file"].path.name
            )
        )
    else:
        return True


def get_xml_files_from_data_path(data_dir: Path) -> List[WikiXMLFile]:
    sorted_files = wikidump.load_wikifile_list(data_dir)
    return sorted_files


if __name__ == "__main__":
    logger = mp.log_to_stderr()
    logger.setLevel(logging.INFO)
    data_path = Path("/hdd/datasets/wikipedia_4_20_21")

    files = get_xml_files_from_data_path(data_path)

    CHECK_FOR_COMPLETE = is_dump_contiguous(files)
    # main thread log file
    main_start = datetime.datetime.now().isoformat()
    main_stats = open("logs/main_{}.txt".format(main_start), "w")
    # workers
    MAX_WORKERS = 6
    workers = []
    worker_counter = 0
    while len(files) != 0:
        for i, worker in enumerate(workers):  # verify workers aren't dead
            if worker.exitcode is not None:
                workers.pop(i)
                msg = "waiting to join completed worker {}".format(worker.pid)
                logger.info(msg)
                main_stats.write(msg + "\n")
                worker.join()
                msg = "successfully joined completed worker {}".format(worker.pid)
                logger.info(msg)
                main_stats.write(msg + "\n")
        if len(workers) == MAX_WORKERS:  # if all workers still alive
            logger.info("no completed workers. sleeping for 10 minutes")
            sleep(10 * 60)  # wait 10 minutes
            logger.info(
                "main thread woke up, checking if there are any completed workers"
            )
        else:
            for i in range(MAX_WORKERS - len(workers)):  # add workers
                try:
                    wikifile = files.pop(0)
                    p = mp.Process(
                        target=file_etl,
                        name="Xml_consumer_{}".format(worker_counter),
                        args=(wikifile,),
                    )
                    workers.append(p)
                    p.start()
                    MSG = "started process pid - {}. worker count is {}\n".format(
                        p.pid, len(workers)
                    )
                    main_stats.write(msg)
                    logger.info(msg)
                    worker_counter += 1
                except IndexError:  # reached end of list
                    logger.info("all XML files have been assigned to workers")
                    break

    # wait for wikifiles to be consumed
    logger.info("waiting for workers to finish")
    for worker in workers:
        worker.join()
    logger.info("all work complete")
    main_stats.close()
