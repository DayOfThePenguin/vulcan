import configparser
import re

from contextlib import contextmanager
from ftplib import FTP, error_perm
from pathlib import Path
from typing import Dict, List


def check_path(path: Path) -> None:
    """Verifies path is a valid Path

    Parameters
    ----------
    data_path : Path
        Filesystem path to check

    Raises
    ------
    FileNotFoundError
        If path doesn't exist
    """
    if path.exists() == False:
        raise FileNotFoundError(f"Path {path} does not exist")


def read_config(config_path="config.ini") -> Dict:
    """Read a config.ini file to get ftp parameters

    Returns
    -------
    Dict
        configuration used by main
    """
    parser = configparser.ConfigParser()
    parser.read(config_path)
    config = {}
    config["host"] = parser["Data"]["MediaWikiDumpURL"]
    config["mirror_path"] = parser["Data"]["MediaWikiMirrorPath"]
    config["wiki_name"] = parser["Data"]["MediaWikiName"]
    config["data_path"] = Path(parser["Data"]["DataPath"])
    check_path(config["data_path"])
    config["download_url_prefix"] = (
        "https://"
        + config["host"]
        + "/"
        + config["mirror_path"]
        + "/"
        + config["wiki_name"]
        + "/"
    )
    return config


@contextmanager
def ftp_connect(config: Dict) -> FTP:
    """Connect to a ftp server specified by config & navigate to the wiki's folder

    Parameters
    ----------
    config : Dict
        configuration from read_config

    Yields
    -------
    FTP
        ftp server context manager
    """
    ftp = FTP(config["host"])
    ftp.login()

    ftp.cwd(config["mirror_path"])
    try:
        ftp.cwd(config["wiki_name"])
    except error_perm:
        print(f"{config['wiki_name']} is an invalid wiki for host {config['host']}")
        raise
    try:
        yield ftp
    finally:
        ftp.close()


def find_newest_dump(avail_dates: List) -> str:
    """Finds greatest integer (newest date) in a list and returns it in string form

    Parameters
    ----------
    avail_dates : List
        str List of the dates of db dumps in the form YYYYMMDD ("20211220" -> 2021 12 20)

    Returns
    -------
    str
        most recent date, converted back to string form as YYYYMMDD
    """
    date_selection = 0  # track the most recent dump date...all dates will be > 0
    for date in avail_dates:
        if int(date) > date_selection:  # if this entry is from a later date, replace
            date_selection = int(date)
    return str(date_selection)


def establish_dump_path(config: Dict) -> Path:

    dump_path = config["data_path"].joinpath(config["wiki_name"], config["newest_dump"])

    try:
        check_path(dump_path)
    except FileNotFoundError:
        print(f"Creating {dump_path.as_posix()}")
        dump_path.mkdir(parents=True)
    return dump_path


def download_files(config: Dict):
    file_prefix = config["wiki_name"] + "-" + config["newest_dump"] + "-"
    titles_re = re.compile(file_prefix + "(all-titles|all-titles-in-ns0).gz$")
    titles = []
    categories = []
    links = []
    pages_articles = []
    for f_name in config["avail_files"]:
        title_match = titles_re.match(f_name)
        if title_match is not None:
            titles.append(f_name)


if __name__ == "__main__":
    config = read_config()
    with ftp_connect(config) as ftp:
        avail_dates = ftp.nlst()
        config["newest_dump"] = find_newest_dump(avail_dates)
        config["dump_path"] = establish_dump_path(config)

        ftp.cwd(config["newest_dump"])
        config["avail_files"] = ftp.nlst()

        download_files(config)
    pass
