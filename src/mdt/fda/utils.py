import requests
from pathlib import Path


def get_dataset(
    dest = Path.cwd(),
    handler = None
):
    url = f'https://www.accessdata.fda.gov/cder/ndctext.zip'
    response = requests.get(url)

    if handler:
        return handler(response.content)

    (dest / url.split('/')[-1]).write_bytes(response.content)

    return response
