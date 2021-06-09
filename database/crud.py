import multiprocessing as mp

from os import getpid
from typing import List, TextIO, Tuple

from sqlalchemy.orm import sessionmaker

from database.models import PageText
from wikitools.wikipage import WikipediaPage
from wikitools.wikixml import WikiXMLFile


def commit_list_to_db(
    file: WikiXMLFile,
    pages_to_commit: List[WikipediaPage],
    session_gen: sessionmaker,
    stats_file: TextIO,
    duplicates_file: TextIO,
) -> Tuple[int, int, int]:
    """write a list to a database

    takes a list to commit, create a session, check if each page is in the database,
    note duplicate if duplicate, else stage after all files staged, commit"""
    commit_logger = mp.get_logger()
    sess = session_gen()
    errors = 0
    additions = 0
    duplicates = 0
    for _ in range(len(pages_to_commit)):
        page = pages_to_commit.pop()
        if sess.query(PageText).filter_by(title=page.title).first() is None:
            try:  # if page isn't in db
                sess.add(page.page())
                additions += 1
            except Exception as msg:
                errors += 1
                mtext = "page:{} => msg:{}\n".format(page, msg)
                stats_file.write(mtext)
                commit_logger.error(mtext)
                sess.rollback()
        else:  # if page is in db
            duplicates += 1
            commit_logger.info("pid: %i duplicate %s", getpid(), page.title)
            duplicates_file.write(page.title + "\n")
    sess.commit()
    sess.close()
    del sess
    msg = "pid: {} committed. adds - {}/{}\tduplicates - {}/{}\nerrors - {}/{}\ttotal - {}/{}"
    msg = msg.format(
        getpid(),
        additions,
        file.additions,
        duplicates,
        file.duplicates,
        errors,
        file.errors,
        file.chunk_size,
        file.pages,
    )
    commit_logger.info(msg)
    stats_file.write(msg + "\n")
    return additions, duplicates, errors
