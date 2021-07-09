""" Utility functions used throughout Wikimap

This file is part of Wikimap.

    Wikimap is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Wikimap is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with Wikimap.  If not, see <https://www.gnu.org/licenses/>.
    """
import logging
import multiprocessing as mp

from logging import Logger
from pathlib import Path


from pydantic import BaseModel, validate_arguments, validator


class LogConfig(BaseModel):
    """Pydantic model for logging config"""

    file_name = "wikimap"

    @validator("file_name")
    def name_must_not_be_path(cls, file_name):  # pylint: disable=no-self-argument
        """Validate file_name to ensure it doesn't write to an arbitrary path

        Raises
        ------
        ValueError
            file_name can't have an arbitrary suffix
        """
        if Path(file_name).name != file_name:
            raise ValueError("must not be a path")

    @validator("file_name")
    def name_must_not_have_suffix(cls, file_name):  # pylint: disable=no-self-argument
        """Validate file_name to ensure it doesn't have an arbitrary suffix

        Raises
        ------
        ValueError
            file_name can't be a path and sneak out of the logs directory
        """
        if Path(file_name).suffix != "":
            raise ValueError("must not have a suffix")


@validate_arguments
def create_logger(config: LogConfig = LogConfig()) -> Logger:
    """Create a mp-friendly logger instance that collates logs into a single file

    Parameters
    ----------
    config : [LogConfig]
        logging configuration to use to build the Logger, by
        default LogConfig("wikimap")

    Returns
    -------
    logger : [logging.Logger]
        Logger that can be used with multiprocessing and will put all outputs
    """

    logger = mp.get_logger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "[%(asctime)s| %(levelname)s| %(processName)s] %(message)s"
    )
    handler = logging.FileHandler(f"logs/{config.file_name}.log")
    handler.setFormatter(formatter)

    # this bit will make sure you won't have
    # duplicated messages in the output
    if not len(logger.handlers):  # pylint: disable=len-as-condition
        logger.addHandler(handler)
    return logger
