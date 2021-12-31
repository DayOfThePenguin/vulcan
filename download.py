import configparser

from ftplib import FTP, error_perm
from pathlib import Path


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


config = configparser.ConfigParser()
config.read("config.ini")
host = config["Data"]["MediaWikiDumpURL"]
mirror_path = config["Data"]["MediaWikiMirrorPath"]
wiki_name = config["Data"]["MediaWikiName"]
data_path = Path(config["Data"]["DataPath"])
check_path(data_path)

ftp = FTP(host)
ftp.login()

ftp.cwd(mirror_path)
try:
    ftp.cwd(wiki_name)
except error_perm:
    print(f"{wiki_name} is an invalid wiki for host {host}")
    raise

avail_dates = ftp.nlst()
date_selection = 0  # the most recent dump date
for date in avail_dates:
    if int(date) > date_selection:  # if this entry is from a later date, replace
        date_selection = int(date)

dump_path = data_path.joinpath(wiki_name, str(date_selection))
try:
    check_path(dump_path)
except FileNotFoundError:
    print(f"Creating {dump_path.as_posix()}")
    dump_path.mkdir(parents=True)

ftp.cwd(str(date_selection))
avail_files = ftp.nlst()

file_prefix = wiki_name + "-" + str(date_selection) + "-"
desired_files = []

for file_name in avail_files:
    pass
