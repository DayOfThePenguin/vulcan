import bz2
import prefect
import re

import mwparserfromhell as wp
from xml.etree.ElementTree import iterparse
from os import getpid, environ
from pathlib import Path
from prefect import task, Flow, Parameter
from prefect.engine.results import LocalResult
from prefect.executors import DaskExecutor
from typing import List, Tuple
from unidecode import unidecode

class WikiXMLFile(object):
    def __init__(self, start_idx: int, end_idx: int, path: Path) -> None:
        self.start_idx = start_idx
        self.end_idx = end_idx
        self.path = path

def get_headings_and_sections_from_element(element_text):
    """extract a list of headings and a list with the text of the article from an element.text
    
    This function will also transliterate any unicode to ascii using the
    unidecode (https://github.com/avian2/unidecode) module by default
    
    Keywork Arguments:
    element_text (elem.text) - 
    
    Returns:
    headings (list) - all the headings of the article, with a heading
    'Lead' for the first, unnamed section of the page
    
    sections (list) - list of strings each containing the contents of a
    section of the page. Note: some of these strings will be '' when a
    heading is used to group a series of subheadings but there isn't
    any actual text under the heading itself
    
    """
    wikicode = wp.parse(element_text)
    raw_headings = wikicode.filter_headings()
    clean_headings = []

    raw_sections = []
    remaining_text = element_text
    for i, heading in enumerate(raw_headings):
        if i == 0: # The first section (Lead) won't have a title because it is implicitly assumed
            clean_headings.append('Lead')
            clean_headings.append(raw_headings[i].title.strip_code().strip()) # the titles are wrapped in wikicode and spaces
        else:
            clean_headings.append(raw_headings[i].title.strip_code().strip()) # the titles are wrapped in wikicode and spaces
            # when we split on a heading, we get the previous section and the rest of the document 
        splits = remaining_text.split(str(heading), maxsplit=1)
        raw_sections.append(splits[0])
        if i == len(raw_headings) - 1:
            raw_sections.append(splits[1])
            remaining_text = ""
        else:
            remaining_text = splits[1]
    
    clean_sections = []
    for section in raw_sections:
        wikicode_free_section = wp.parse(section).strip_code().strip()
        unicode_transliterated_section = unidecode(wikicode_free_section)
        clean_sections.append(unicode_transliterated_section)
        del wikicode_free_section
        del unicode_transliterated_section
    del raw_sections
    del raw_headings
    del wikicode
    
    return clean_headings, clean_sections

def get_next_article_title_and_element(parser):
    """redirects and valid articles both have text tags,
            but since we set title to none when we find a redirect tag
            between the title and text tags, we never return a redirect
            
            
        A redirect tag will occur after the title if the
        page is a redirect, so we set the title back to none so the text
        matcher won't return a match for this page
            """
    title = None

    while True:
        event, elem = next(parser)
        
        # article title (title) matcher
        matches = re.search(r'{(.+)}(title)', elem.tag)
        if matches is not None:
            if re.search(r'^File:(.+)$', elem.text) is not None: # found in title of media pages, not content pages
                continue
            elif re.search(r'^Wikipedia:(.+)$', elem.text) is not None: # found in title of internal discussion pages
                continue
            elif re.search(r'^Help:(.+)$', elem.text) is not None: # found in title of internal help pages
                continue
            elif re.search(r'^Template:(.+)$', elem.text) is not None: # found in title of internal template pages
                continue
            elif re.search(r'^Draft:(.+)$', elem.text) is not None: # found in title of internal page drafts
                continue
            elif len(elem.text) > 200: # parsed all titles and no valid ones appear to be over 200 characters
                continue
            else:
                title = elem.text
                
        # article text (text) matcher
        matches = re.search(r'{(.+)}(text)', elem.tag)    
        if matches is not None:
            if title is not None:
                return title, elem
        
        # article redirect (redirect) matcher
        matches = re.search(r'{(.+)}(redirect)', elem.tag)
        if matches is not None:
            title = None

@task
def find_longest_article_in_xml_chunk(wiki_file: WikiXMLFile) -> Tuple[int, str]:
    logger = prefect.context.get("logger")
    with bz2.open(wiki_file.path, 'r') as f:
        parser = iter(iterparse(f))
        article_num = 0
        max_title_len = 0
        max_title_name = None
        max_elem = None

        while True:
            title, elem = None, None
            try:
                title, elem = get_next_article_title_and_element(parser)
                article_num += 1
            except StopIteration:
                logger.info("{} - total Number of Articles: {}\nlongest title ({}): {}".format(getpid(), article_num, max_title_len, max_title_name))
                return max_title_len, max_title_name
            if len(title) > max_title_len:
                headings, sections = get_headings_and_sections_from_element(elem.text)
                if len(headings) < 3: # looking at the data, anything with less than 3 headings is usually not a valid article
                    continue
                max_title_len = len(title)
                max_title_name = title
                max_elem = elem
                logger.debug("{} - New max length ({}): {}".format(getpid(), max_title_len, max_title_name))
            del title
            elem.clear()
            del elem

@task
def check_for_complete_dump_files(sorted_files: List[WikiXMLFile]) -> None:
    logger = prefect.context.get("logger")
    all_present = True
    
    last_end = 0
    for i, file in enumerate(sorted_files):
        if file.start_idx != (last_end + 1):
            all_present = False
            logger.error("You are missing the file(s) after {}".format(sorted_files[i-1].path.name))
    #         print("no - {}".format(file))   
        last_end = file.end_idx
    if not all_present:
        raise FileNotFoundError("""Missing files detected. Please download them
                                and rerun this script to check for a complete
                                set of database chunks""")

@task
def get_xml_files_from_data_path(data_path: Path) -> List[WikiXMLFile]:
    logger = prefect.context.get("logger")
    if data_path.exists() == False:
        raise FileNotFoundError("Could not find directory {}".format(data_path))
    elif data_path.is_file():
        raise NotADirectoryError("""data_path '{}' is a file. Please set
                                 data_path to the directory containing the
                                 multistream bzipped
                                 files""".format(data_path))
    else:
        logger.info("Verified {} is a valid directory".format(data_path))

    unsorted_files = [] # [start_idx, end_idx, file_path]
    for item in data_path.iterdir():
        if re.search(r'\.part$', str(item)) is not None:
            all_present = False
            logger.warning("""(incomplete file download) - {}\n
            The supplied data directory contains a .part file, indicating that
            the download of that file didn't complete. Please verify the file
            downloaded completely""".format(item))
        elif re.search(r'\.bz2$', str(item)) is None:
            logger.warning("(non-bz2 file) - {}\n".format(item))
            continue
        try:
            start_idx, end_idx = re.split(r'wiki-(.+)-pages-articles-multistream(.+).xml-p(.+)p(.+).bz2', str(item))[3:5]
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
        check_for_complete = check_for_complete_dump_files(sorted_files)
        longest_articles = find_longest_article_in_xml_chunk.map(sorted_files)
        longest_articles.set_dependencies(upstream_tasks=[check_for_complete])
        show_longest(longest_articles)


    flow.executor = DaskExecutor("tcp://192.168.0.12:8786")

    # flow.run()
    flow.register(project_name="wikipedia")