import datetime
import logging
import multiprocessing as mp
from time import sleep
from typing import List, Tuple

from os import getpid
from pathlib import Path


# local modules from PYTHONPATH
import vulcan.wikitools.wikidump as wikidump
import vulcan.wikitools.wikixml as wikixml

import vulcan.database.crud
import vulcan.database.config

from vulcan.wikitools.wikixml import WikiXMLFile
from vulcan.wikitools.wikipage import WikipediaPage


def closing_msg(file: WikiXMLFile):

    close_msg = "********\nFINISHED PROCESSING {}\n".format(
        str(file.path.name))
    close_msg += "{} total pages\n".format(file.pages)
    close_msg += "{} total additions\n".format(file.additions)
    close_msg += "{} total duplicates\n".format(file.duplicates)
    close_msg += "{} total errors\n".format(file.errors)
    close_msg += "********"
    return close_msg


def get_stats_dup_names(file: WikiXMLFile) -> Tuple[str, str]:
    worker_start = datetime.datetime.now().isoformat()
    dup_name = "logs/duplicates_{}_{}_{}.txt"
    dup_name = dup_name.format(file.start_idx, file.end_idx, worker_start)
    stats_name = "logs/statspid_{}_{}_{}_{}.txt"
    stats_name = stats_name.format(
        getpid(), worker_start, file.start_idx, file.end_idx)
    return stats_name, dup_name


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
    session_generator = database.config.get_sessionmaker()
    # loop
    stats_name, dup_name = get_stats_dup_names(file)
    pages = []
    with file.parser() as parser, open(dup_name, "w") as duplicates_file, open(
        stats_name, "w"
    ) as stats_file:
        while True:
            try:
                title, element = wikixml.get_next_title_element(parser)
                if file.pages >= 2000:
                    raise StopIteration
            except StopIteration:
                add, dup, err = database.crud.commit_list_to_db(
                    file, pages, session_generator, stats_file, duplicates_file
                )
                file.additions += add
                file.duplicates += dup
                file.errors += err
                # closing-specific
                close_msg = closing_msg(file)
                etl_logger.info(close_msg)
                stats_file.write(close_msg + "\n")
                return 0

            links = wikixml.get_pagelinks(element)
            headings, sections = wikixml.get_headings_sections(element)
            page = WikipediaPage(title, headings, sections, links)
            pages.append(page)
            file.pages += 1
            element.clear()
            del title, element, headings, sections, page
            # every chunksize
            if file.pages % file.chunk_size == 0:
                add, dup, err = database.crud.commit_list_to_db(
                    file, pages, session_generator, stats_file, duplicates_file
                )
                file.additions += add
                file.duplicates += dup
                file.errors += err
                pages = []  # reset pages after committing the existing ones


if __name__ == "__main__":
    # change me to customize
    data_path = Path("/hdd/datasets/wikipedia_4_20_21")
    sleep_time = 2  # in minutes
    MAX_WORKERS = 12  # nprocesses, 1 thread/process

    # don't change below here
    logger = mp.log_to_stderr()
    logger.setLevel(logging.INFO)
    files = wikidump.load_wikifile_list(data_path)
    CHECK_FOR_CONTIGUOUS = wikidump.is_dump_contiguous(files)
    logger.info("database dump contiguous: %s", CHECK_FOR_CONTIGUOUS)
    # main thread log file
    main_start = datetime.datetime.now().isoformat()
    main_stats = open("logs/main_{}.txt".format(main_start), "w")
    # workers
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
                msg = "successfully joined completed worker {}".format(
                    worker.pid)
                logger.info(msg)
                main_stats.write(msg + "\n")
        if len(workers) == MAX_WORKERS:  # if all workers still alive
            logger.info(
                "no completed workers. sleeping for %i minutes", sleep_time)
            sleep(sleep_time * 60)  # wait 10 minutes
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
                    msg = "started process pid - {}. worker count is {}"
                    msg = msg.format(p.pid, len(workers))
                    logger.info(msg)
                    main_stats.write(msg + "\n")
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
