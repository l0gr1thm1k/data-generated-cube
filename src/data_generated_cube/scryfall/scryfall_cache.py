import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests
from loguru import logger

from src.common.common import async_fetch_data
from src.common.constants import CUBE_CREATION_RESOURCES_DIRECTORY


class ScryfallCache:
    data_dir = CUBE_CREATION_RESOURCES_DIRECTORY

    def __init__(self):
        self.cache = self.get_scryfall_cache()

    @classmethod
    def get_scryfall_cache(cls):

        if not cls.verify_local_cache_is_up_to_date():
            logger.info("Downloading new cache from Scryfall...")
            default_bulk_info = cls.get_bulk_data_item_info_by_name("Default Cards")
            cache_identifier = default_bulk_info['download_uri']
            cls.download_bulk_data_from_url(cache_identifier)
        else:
            logger.info("Using local Scryfall cache...")
            cache_identifier = cls.get_most_recent_local_cache_filename()

        cache = cls.load_local_scryfall_cache(cache_identifier)
        cls.remove_old_caches()

        return cache

    @classmethod
    def get_bulk_data_item_info_by_name(cls, name: str):
        bulk_items_response = cls.get_scryfall_bulk_data_items()
        for item in bulk_items_response['data']:
            if item['name'] == name:
                return item

    @staticmethod
    def get_scryfall_bulk_data_items():
        request_url = "https://api.scryfall.com/bulk-data"
        response = requests.get(request_url).json()

        return response

    @classmethod
    def verify_local_cache_is_up_to_date(cls) -> bool:
        current_date = datetime.today()
        local_cache_update_date = cls.get_max_local_cache_date()
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

    @classmethod
    def get_max_local_cache_date(cls) -> datetime:
        caches = list(Path(cls.data_dir).glob("default-cards*"))
        try:
            max_date = max([cls.extract_date_from_download_uri(str(cache)) for cache in caches])
        except ValueError:
            max_date = datetime(1970, 1, 1)

        return max_date

    @classmethod
    def download_bulk_data_from_url(cls, url: str) -> None:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        file_name = Path(url).name

        with open(cls.data_dir / file_name, "wb") as download_file_stream:
            for chunk in response.iter_content(chunk_size=8192):
                download_file_stream.write(chunk)

    @classmethod
    def get_most_recent_local_cache_filename(cls) -> str:
        caches = list(Path(cls.data_dir).glob("default-cards*"))
        max_date = datetime(1970, 1, 1)
        max_cache = ""
        for cache in caches:
            date = cls.extract_date_from_download_uri(str(cache))
            if date > max_date:
                max_date = date
                max_cache = cache.name

        return max_cache

    @classmethod
    def remove_old_caches(cls) -> None:

        newest_cache = cls.get_most_recent_local_cache_filename()
        caches = list(Path(cls.data_dir).glob("default-cards*"))

        for cache in caches:
            if cache.name != newest_cache:

                cache.unlink()

    @classmethod
    def load_local_scryfall_cache(cls, url: str) -> defaultdict[list]:
        file_name = Path(url).name
        with open(cls.data_dir / file_name, "r") as fstream:
            cache = json.loads(fstream.read())

        card_cache_dictionary = defaultdict(list)
        for card in cache:
            name = card["name"]
            card_cache_dictionary[name].append(card)

        return card_cache_dictionary

    def has_foil_printing(self, card_name: str) -> bool:
        """
        Determine if a card has a foil printing.

        :param card_name:
        :return: by default return False, return True if any of a cards printings has a foil printing.
        """
        foil_printing = False
        card_versions = self.cache.get(card_name, [])

        if not card_versions:
            extended_card_name = self.get_extended_name(card_name)
            card_versions = self.cache.get(extended_card_name, [])
        if not card_versions:
            # No extended name found, return False
            logger.debug(f"No card with name '{card_name}' or variants of this found in Scryfall data.")
        else:
            for card_version in card_versions:
                if card_version['foil'] and 'paper' in card_version['games']:
                    foil_printing = True
                    break

        return foil_printing

    def get_extended_name(self, name: str) -> str:
        extended_name_regex = f"{name}\s*\/\/.*"
        post_extended_name_regex = f".*?\/\/\s*{name}"
        for card in self.cache:
            if re.match(extended_name_regex, card):
                return card

        for card in self.cache:
            if re.match(post_extended_name_regex, card):
                return card

    async def get_card_stats_from_scryfall_async(self, card_name: str) -> dict:
        # time.sleep(1.0)
        normalized_card_name = self.normalize_card_name(card_name)
        scryfall_get_url = f"https://api.scryfall.com/cards/named?exact={normalized_card_name}"
        try:
            response = await async_fetch_data(scryfall_get_url)
        except Exception as e:
            logger.debug(f"No card named {card_name} in the Scryfall database", error=e)
            response = {}

        return response

    @staticmethod
    def normalize_card_name(card_name: str) -> str:
        card_name = card_name.lower()
        card_name = re.sub(r'\s+', '%20', card_name)
        card_name = card_name.replace('&', ' ')

        return card_name


shared_scryfall_cache = ScryfallCache()
