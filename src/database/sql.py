from pathlib import Path
import re
from enum import Enum
from typing import BinaryIO, Iterable

from pydantic import validate_arguments

line_start_ex = re.compile(b"INSERT INTO (.*?) VALUES ")
# pylint: disable=anomalous-backslash-in-string
categorylink_item_ex = re.compile(
    b"\((.*?),'(.*?)','(.*?)','(.*?)','(.*?)','(.*?)','(.*?)'\)"
)
page_item_ex = re.compile(b"\((.*?),(.*?),'(.*?)',(.*?),(.*?),(.*?)\)")
link_item_ex = re.compile(b"\((.*?),(.*?),'(.*?)',(.*?)\)")


class LineEnum(str, Enum):
    CATEGORYLINK = "categorylink"
    LINK = "link"
    PAGE = "page"
    # redirect = "redirect"


def data_lines_generator(file: BinaryIO):
    for line in file:
        if line_start_ex.search(line) is not None:
            yield line


@validate_arguments
def get_line_iterator(line: bytes, line_type: LineEnum) -> Iterable:
    if line_start_ex.search(line) is not None:
        if line_type is LineEnum.CATEGORYLINK:
            item_iter = categorylink_item_ex.finditer(line)
        elif line_type is LineEnum.LINK:
            item_iter = link_item_ex.finditer(line)
        elif line_type is LineEnum.PAGE:
            item_iter = page_item_ex.finditer(line)
        # elif line_type is LineEnum.redirect:
        #     pass # need to build
        return item_iter
    else:
        return None
