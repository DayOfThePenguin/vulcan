import datetime
import logging
import multiprocessing as mp

from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from time import sleep
from typing import List

# local modules from PYTHONPATH
import wikitools.wikidump as wikidump
import wikitools.wikixml as wikixml

from wikitools.wikixml import WikiXMLFile
from wikitools.wikipage import WikipediaPage
from db.models import Page


def queue_consumer(queue: mp.Queue, done_loading: mp.Value):
    logger = mp.get_logger()
    # database
    DATABASE_URI = "postgresql://postgres:postgres@localhost:8001/wikipedia"
    engine = create_engine(DATABASE_URI)
    Session = sessionmaker(bind=engine)
    s = Session()
    # loop variables
    num_articles = 0
    chunk_size = 1000
    duplicates_file = open(
        "duplicates_{}.txt".format(datetime.datetime.now().isoformat()), "w"
    )
    while True:
        if done_loading == True and queue.qsize() == 0:
            logger.info(
                "there are no more xml files to load and I have reached the end of queue"
            )
            s.commit()
            s.close()
            logger.info(
                "committed final chunk to databases. {} records total".format(
                    num_articles
                )
            )
            duplicates_file.close()
            return 0
        # if not done and queue has items
        item = queue.get()
        s.add(
            Page(
                title=item.title,
                headings=item.headings,
                sections=item.sections,
                links=item.links,
            )
        )
        try:
            s.commit()
            num_articles += 1
        except IntegrityError as e:
            logger.info("Duplicate page: {}".format(item.title))
            logger.info(e)
            duplicates_file.write("{}\n".format(item.title))
            s.rollback()
            del item
            continue
        if num_articles % chunk_size == 0:
            logger.info("{} committed articles".format(num_articles))
            logger.info("{} current queue size".format(queue.qsize()))
            s.close()
            s = Session()
        del item


def load_xml_into_queue(wikifile: WikiXMLFile, article_queue: mp.Queue) -> None:
    """ """
    logger = mp.get_logger()
    logger.info("started processing xml {}".format(str(wikifile.path)))
    num_articles = 0
    chunk_size = 500
    # with bz2.open(wikifile.path, "r") as f:
    #     parser = iterparse(f)
    with wikifile.parser() as parser:
        while True:
            title, element = None, None
            try:
                title, element = wikixml.get_next_title_element(parser)
                links = wikixml.get_pagelinks(element)
                headings, sections = wikixml.get_headings_sections(element)
                article = WikipediaPage(title, headings, sections, links)
                article_queue.put(article)
                num_articles += 1
                if num_articles % chunk_size == 0:
                    logger.info("{} processed files".format(num_articles))

                element.clear()
                del title, element, headings, sections, article
            except StopIteration:
                logger.info("{} reached end of xml file".format(num_articles))
                logger.info("finished processing {}".format(str(wikifile.path)))
                return 0


def is_dump_contiguous(files: List[WikiXMLFile]) -> bool:
    """Verifies is there are any gaps in the xml files in the dump.
    THIS FUNCTION CAN NOT GUARANTEE THAT ALL FILES ARE PRESENT, IT CAN ONLY
    VERIFY THAT YOU HAVE ALL FILES BETWEEN THE LOWEST START_IDX AND HIGHEST
    END_IDX ARE PRESENT.

    For ease of use, when downloading a new dump, it is best to download the
    start and end files first, filling in the files inbetween after. In that
    specific case, a contiguous dump will also be a complete dump.
    """
    logger = mp.get_logger()
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
                result["last_valid_file"].path.name
            )
        )


def get_xml_files_from_data_path(data_path: Path) -> List[WikiXMLFile]:
    sorted_files = wikidump.load_wikifile_list(data_path)
    return sorted_files


if __name__ == "__main__":
    logger = mp.log_to_stderr()
    logger.setLevel(logging.INFO)
    data_path = Path("/hdd/datasets/wikipedia_4_20_21")

    sorted_files = get_xml_files_from_data_path(data_path)

    check_for_complete = is_dump_contiguous(sorted_files)
    sorted_files = sorted_files[2:]
    # queue
    logger.info("creating queue")
    article_queue = mp.Queue(maxsize=5000)
    done_loading = mp.Value("i", False)
    done_loading.value = False
    # wikifile_queue = mp.Queue()

    # for i in range(len(sorted_files)):
    #     wikifile_queue.put(sorted_files.pop())
    # consumer process
    queue_consumer_p = mp.Process(
        target=queue_consumer,
        name="Queue_Consumer",
        args=(
            article_queue,
            done_loading,
        ),
    )
    queue_consumer_p.start()
    # xml producer processes
    max_processes = 3
    processes = []
    process_counter = 0
    while True:
        for i, process in enumerate(processes):  # verify processes aren't dead
            if process.exitcode is not None:
                processes.pop(i)
                logger.info("waiting to join completed process {}".format(process.pid))
                process.join()
                logger.info(
                    "successfully joined completed process {}".format(process.pid)
                )
        # if no processes are running and there are no more files to process
        if len(sorted_files) == 0:
            if len(processes) == 0:
                with done_loading.get_lock():
                    done_loading.value == True
                    logger.info("XML GENERATION COMPLETE")
                    break
            else:
                logger.info(
                    "all wikifiles have been sent to a process. waiting for processes to finish queueing WikipediaPages"
                )
                sleep(10 * 60)
                logger.info(
                    "main thread woke up, checking if there are any complete processes"
                )
        elif len(processes) == max_processes:  # if all processes still alive
            logger.info("no completed processes. sleeping for 10 minutes")
            sleep(10 * 60)  # wait 10 minutes
            logger.info(
                "main thread woke up, checking if there are any completed processes"
            )
        else:
            for i in range(max_processes - len(processes)):  # add processes
                try:
                    wikifile = sorted_files.pop(0)
                    p = mp.Process(
                        target=load_xml_into_queue,
                        name="Xml_consumer_{}".format(process_counter),
                        args=(
                            wikifile,
                            article_queue,
                        ),
                    )
                    processes.append(p)
                    p.start()
                    process_counter += 1
                except IndexError:  # reached end of list
                    logger.info("all XML files have been assigned to workers")
                    break

    # wait for wikifiles to be consumed
    logger.info("waiting for consumer to send all WikipediaPages to database")
    queue_consumer_p.join()
    logger.info("all work complete")
