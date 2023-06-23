import re
import time
from datetime import datetime
from pathlib import Path

import requests
from loguru import logger
from retrying import retry

from data_generated_cube.common.common import from_pickle, to_pickle
from data_generated_cube.common.constants import CUBE_CREATION_RESOURCES_DIRECTORY
from data_generated_cube.scryfall.scryfall_cache import ScryfallCache


class ELOFetcher:
    data_dir = CUBE_CREATION_RESOURCES_DIRECTORY
    cache_file_path = Path(data_dir) / 'elo_cache.pickle'
    elo_pattern = re.compile(r'"elo".{0,10}')
    elo_digit_pattern = re.compile(r"\d+.\d+")

    def __init__(self):
        scryfall = ScryfallCache()
        self.scryfall_cache = scryfall.cache
        self.elo_cache = self.load_cache()

    def load_cache(self) -> dict:
        return from_pickle(self.cache_file_path)

    def save_cache(self) -> None:
        to_pickle(self.elo_cache, self.cache_file_path)

    def get_card_elo(self, card_name: str) -> float:
        cache_data = self.elo_cache.get(card_name)
        today = datetime.today()
        cube_updated_more_than_a_week_ago = False

        if cache_data:
            last_updated = cache_data.get('last_updated')
            if last_updated:
                cube_updated_more_than_a_week_ago = (today - last_updated).days > 7

        if cache_data is None or cache_data.get('elo') is None or cube_updated_more_than_a_week_ago:
            self.update_card_elo(card_name)
            cache_data = self.elo_cache.get(card_name)

            if cache_data is None:
                return -1.0

        return cache_data["elo"]

    @retry(
        stop_max_attempt_number=3,
        wait_exponential_multiplier=1000,
        wait_exponential_max=10000,
    )
    def get_elo_from_id(self, card_id: str) -> float:
        url = f"https://cubecobra.com/tool/card/{card_id}?tab=1"
        response = requests.get(url)
        html_content = response.content.decode("utf-8")
        matches = self.elo_pattern.findall(html_content)
        if not matches:
            logger.debug(f"Could not find any Elo data on card with ID {card_id}")
        else:
            elo_score = float(self.elo_digit_pattern.findall(matches[0])[0])
            return elo_score

    @staticmethod
    def normalize_card_name(card_name: str) -> str:
        card_name = card_name.lower()
        card_name = re.sub(r'\s+', '%20', card_name)
        card_name = card_name.replace('&', ' ')

        return card_name

    def get_card_stats_from_scryfall(self, card_name: str) -> dict:
        #TODO: Add retry with backoff decortor
        time.sleep(1.0)
        normalized_card_name = self.normalize_card_name(card_name)
        try:
            scryfall_get_url = f"https://api.scryfall.com/cards/named?exact={normalized_card_name}"
            response = requests.get(scryfall_get_url).json()
        except Exception as e:
            logger.debug(f"No card named {card_name} in the Scryfall database", error=e)
            response = {}

        return response

    def get_card_elo_from_cube_cobra(self, card_name: str) -> float:
        # TODO: Refactor this to reduce cognitive complexity

        scryfall_data = self.get_card_by_name_with_max_id(card_name)
        if not scryfall_data:
            normalized_card_name = self.normalize_card_name(card_name)
            scryfall_data = self.get_card_stats_from_scryfall(normalized_card_name)

        try:
            elo_score = self.get_elo_from_id(scryfall_data["id"])
            if elo_score is None:
                for card_version in self.scryfall_cache[card_name]:
                    card_id = card_version["id"]
                    try:
                        elo_score = self.get_elo_from_id(card_id)
                    except:
                        continue
                    if elo_score is not None:
                        break
                logger.info(f"Elo score for {card_name} is {elo_score}")

        except KeyError:
            logger.debug(f"Scryfall data for {card_name} with id {scryfall_data['id']} not found in Cube Cobra. Backing off to direct API call to Scryfall")
            normalized_card_name = self.normalize_card_name(card_name)
            scryfall_data = self.get_card_stats_from_scryfall(normalized_card_name)
            elo_score = self.get_elo_from_id(scryfall_data["id"])

        return elo_score

    def get_card_by_name_with_max_id(self, name: str) -> dict:
        card_versions = self.scryfall_cache[name]
        try:
            max_card = max(card_versions, key=lambda card: card["id"])
        except ValueError:
            logger.debug(f"No card with name '{name}' found in Scryfall data.")

            return {}

        return max_card

    def update_card_elo(self, card_name: str):
        try:
            elo_score = self.get_card_elo_from_cube_cobra(card_name)

            if elo_score is not None or card_name not in self.elo_cache:
                self.elo_cache[card_name] = {
                    "elo": elo_score,
                    "lastUpdated": datetime.now()
                }

        except KeyError as e:
            logger.debug(f"Could not find card {card_name} in Cube Cobra data.", error=e)

            return
