import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests
from loguru import logger

from data_generated_cube.common.constants import CUBE_CREATION_RESOURCES_DIRECTORY


class ScryfallCache:
    data_dir = CUBE_CREATION_RESOURCES_DIRECTORY

    def __init__(self):
        self.cache = self.get_scryfall_cache()

    def get_scryfall_cache(self):

        if not self.verify_local_cache_is_up_to_date():
            logger.info("Downloading new cache from Scryfall...")
            default_bulk_info = self.get_bulk_data_item_info_by_name("Default Cards")
            cache_identifier = default_bulk_info['download_uri']
            self.download_bulk_data_from_url(cache_identifier)
        else:
            logger.info("Using local Scryfall cache...")
            cache_identifier = self.get_most_recent_local_cache_filename()

        cache = self.load_local_scryfall_cache(cache_identifier)
        self.remove_old_caches()

        return cache

    def get_bulk_data_item_info_by_name(self, name: str):
        bulk_items_response = self.get_scryfall_bulk_data_items()
        for item in bulk_items_response['data']:
            if item['name'] == name:
                return item

    @staticmethod
    def get_scryfall_bulk_data_items():
        request_url = "https://api.scryfall.com/bulk-data"
        response = requests.get(request_url).json()

        return response

    def verify_local_cache_is_up_to_date(self) -> bool:
        current_date = datetime.today()
        local_cache_update_date = self.get_max_local_cache_date()
        if (current_date - local_cache_update_date).days > 7:
            return False

        return True

    @staticmethod
    def extract_date_from_download_uri(download_uri):
        match = re.search(r"\d{8}", download_uri)
        if match:
            date_str = match.group()
            date = datetime.strptime(date_str, "%Y%m%d")
            return date
        else:
            return None

    def get_max_local_cache_date(self) -> datetime:
        caches = list(Path(self.data_dir).glob("default-cards*"))
        try:
            max_date = max([self.extract_date_from_download_uri(str(cache)) for cache in caches])
        except ValueError:
            max_date = datetime(1970, 1, 1)

        return max_date

    def download_bulk_data_from_url(self, url: str) -> None:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        file_name = Path(url).name

        with open(self.data_dir / file_name, "wb") as download_file_stream:
            for chunk in response.iter_content(chunk_size=8192):
                download_file_stream.write(chunk)

    def get_most_recent_local_cache_filename(self) -> str:
        caches = list(Path(self.data_dir).glob("default-cards*"))
        max_date = datetime(1970, 1, 1)
        max_cache = ""
        for cache in caches:
            date = self.extract_date_from_download_uri(str(cache))
            if date > max_date:
                max_date = date
                max_cache = cache.name

        return max_cache

    def remove_old_caches(self) -> None:

        newest_cache = self.get_most_recent_local_cache_filename()
        caches = list(Path(self.data_dir).glob("default-cards*"))

        for cache in caches:
            if cache.name != newest_cache:

                cache.unlink()

    def load_local_scryfall_cache(self, url: str) -> defaultdict[list]:
        file_name = Path(url).name
        with open(self.data_dir / file_name, "r") as fstream:
            cache = json.loads(fstream.read())

        card_cache_dictionary = defaultdict(list)
        for card in cache:
            name = card["name"]
            card_cache_dictionary[name].append(card)

        return card_cache_dictionary
