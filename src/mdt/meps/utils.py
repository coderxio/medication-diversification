import os
from pathlib import Path
from typing import Callable
import requests


def get_dataset(
        dat_name: str,
        dest: os.PathLike = Path.cwd(),
        handler: Callable[[any], None] = None
):
    """Get a MEPS Dataset given a dat name + extension

    Args:
        dat_name (str): MEPS dat file name, ie: h206adat.zip
        dest (Path): Destination path to save file, defaults to CWD
        hander (func, optional): Function to bypass CWD save
    """
    url = f'https://www.meps.ahrq.gov/mepsweb/data_files/pufs/{dat_name}'
    response = requests.get(url)

    if handler:
        return handler(response.content)

    (dest / url.split('/')[-1]).write_bytes(response.content)

    return response
