from pathlib import Path
import re
from typing import Dict, List

from wikitools.wikixml import WikiXMLFile


def verify_contiguous_dump(
    unsorted_files: List[WikiXMLFile],
) -> Dict[bool, WikiXMLFile]:
    """Verifies is there are any gaps in the xml files in the dump.

    Parameters
    ----------
    unsorted_files : List[WikiXMLFile]
        a list of WikiXMLFiles, not necessarily sorted by lowest WikiXMLFile.start_idx

    Returns
    -------
    result : Dict["contiguous": bool, "last_valid_file": WikiXMLFile]
        dict indicating whether the list of files is contiguous and the last valid file
        in the database dump. If "contiguous" is False, the "last_valid_file" will be
        the last file where the starting page index was equal to the last ending page
        index + 1. If "contiguous" is True, the "last_valid_file" will be the file in
        the list with the greatest end index.

    Notes
    -----
    THIS FUNCTION CAN NOT GUARANTEE THAT ALL FILES ARE PRESENT, IT CAN ONLY
    VERIFY THAT YOU HAVE ALL FILES BETWEEN THE LOWEST START_IDX AND HIGHEST
    END_IDX ARE PRESENT.

    For ease of use, when downloading a new dump, it is best to download the
    start and end files first, filling in the files inbetween after. In that
    specific case, a contiguous dump will also be a complete dump.
    """
    sorted_files = sorted(unsorted_files, key=lambda file: file.start_idx)
    result = {"contiguous": True, "last_valid_file": sorted_files[-1]}

    last_end_idx = 0
    for i, file in enumerate(sorted_files):
        if file.start_idx != (last_end_idx + 1):
            result["contiguous"] = False
            result["last_valid_file"] = sorted_files[i - 1]
            return result
        last_end_idx = file.end_idx
    return result


def load_wikifile_list(data_path: Path) -> List[WikiXMLFile]:
    """Load and create WikiXMLFiles for all .xml-p(.+)p(.+).bz2 files

    Parameters
    ----------
    data_path : Path
        path to where the .xml-p(.+)p(.+).bz2 files are stored

    Returns
    -------
    sorted_files : List[WikiXMLFile]
        list of WikiXMLFile from all  that is ordered

    Raises
    ------
    FileNotFoundError
        If data_path isn't a valid path on your system
    NotADirectoryError
        If data_path points to a file and not a directory
    """
    if data_path.exists() == False:
        raise FileNotFoundError("Could not find directory {}".format(data_path))
    elif data_path.is_file():
        raise NotADirectoryError(
            """data_path '{}' is a file. Please set data_path to the directory
            containing the multistream bzipped files""".format(
                data_path
            )
        )
    unsorted_files = []
    for item in data_path.iterdir():
        if (
            re.search(
                r"^(.+)wiki-(.+)-pages-articles-multistream(.+).xml-p(.+)p(.+).bz2$",
                str(item),
            )
            is None
        ):
            continue  # skip if the file isn't part of a pages-articles-multistream
        start_idx, end_idx = re.split(
            r"wiki-(.+)-pages-articles-multistream(.+).xml-p(.+)p(.+).bz2",
            str(item),
        )[3:5]
        unsorted_files.append(WikiXMLFile(int(start_idx), int(end_idx), item))
    sorted_files = sorted(unsorted_files, key=lambda file: file.start_idx)
    return sorted_files
