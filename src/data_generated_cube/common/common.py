import dill

from datetime import datetime, timezone
from loguru import logger
from pathlib import Path


def to_pickle(data, path: str, protocol: int = 3) -> None:
    """
    pickle data to a file

    :param data: data to pickle
    :param path: path to write data to
    :param protocol: pickle protocol level to be used (python's current default is 3)
    """

    with open(path, 'wb') as file:
        dill.dump(data, file, protocol=protocol)


def from_pickle(path: str):
    """
    load data from a pickle file

    :param path: path to load
    :return: unpickled data
    """

    with open(path, 'rb') as file:
        return dill.load(file)


def ensure_dir_exists(path):
    """
    Ensure a directory exists. Create it if it doesn't.

    :param path: path to check (string or Path object)
    :return: absolute path of the directory (Path object)
    """
    path_obj = Path(path)

    if not path_obj.exists():
        logger.debug(f'Path does not exist, creating: {path_obj}')
        try:
            path_obj.mkdir(parents=True)
        except FileExistsError:
            pass

    return path_obj.absolute()


def get_utc_time() -> str:
    """
    Get the current UTC time in the format YYYYMMDD HHMMSS
    """
    now_utc = datetime.now(timezone.utc)
    return now_utc.strftime('%Y-%m-%d %H:%M:%S')
