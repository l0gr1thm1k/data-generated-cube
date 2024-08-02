import asyncio
import re
from typing import Union
from datetime import datetime
from pathlib import Path
from typing import Union

from loguru import logger

from src.common.common import async_fetch_data, from_pickle, to_pickle
from src.common.constants import CUBE_CREATION_RESOURCES_DIRECTORY
from src.data_generated_cube.scryfall.scryfall_cache import shared_scryfall_cache


class ELOFetcher:
    data_dir = CUBE_CREATION_RESOURCES_DIRECTORY
    cache_file_path = Path(data_dir) / 'elo_cache.pickle'
    elo_pattern = re.compile(r'"elo".{0,10}')
    elo_digit_pattern = re.compile(r"\d+.\d+")
    scryfall = shared_scryfall_cache
    scryfall_cache = scryfall.cache

    def __init__(self):
        self.elo_cache = self.load_cache()
        self.lock = asyncio.Lock()
        self.chunk = 0

    def load_cache(self) -> dict:
        return from_pickle(self.cache_file_path.__str__())

    def save_cache(self) -> None:
        to_pickle(self.elo_cache, self.cache_file_path)

    async def get_card_elo(self, card_name: str) -> float:
        cache_data = self.elo_cache.get(card_name)
        today = datetime.today()
        cube_updated_more_than_a_week_ago = False

        if cache_data and cache_data.get('lastUpdated'):
            cube_updated_more_than_a_week_ago = (today - cache_data['lastUpdated']).days > 1

        if cache_data is None or cache_data.get('elo') is None or cube_updated_more_than_a_week_ago:
            logger.info(f'ELO score for "{card_name} needs to be refreshed')
            await self.update_card_elo(card_name)
            cache_data = self.elo_cache.get(card_name)

            if cache_data is None:
                return -1.0

        return cache_data["elo"]

    async def update_card_elo(self, card_name: str):

        try:
            elo_score = await self.get_card_elo_from_cube_cobra(card_name)

            if elo_score is not None or card_name not in self.elo_cache:
                async with self.lock:
                    self.elo_cache[card_name] = {
                        "elo": elo_score,
                        "lastUpdated": datetime.now()
                    }
                logger.info(f'ELO score for "{card_name}" updated to {elo_score}')
            elif card_name in self.elo_cache:
                async with self.lock:
                    self.elo_cache[card_name]["lastUpdated"] = datetime.now()
                logger.info(f'Bad Cube Cobra ID for "{card_name}"')

            self.chunk += 1

            if self.chunk % 100 == 0:
                logger.info(f"Updated ELO for {self.chunk} unique cards, saving the cache")
                self.save_cache()
                self.chunk = 0

        except KeyError as e:
            logger.debug(f"Could not find card {card_name} in Cube Cobra data.", error=e)

            return

    async def get_card_elo_from_cube_cobra(self, card_name: str) -> float:
        scryfall_data = await self.get_card_by_name_with_max_id(card_name)
        if "id" in scryfall_data:
            elo_score = await self.get_elo_from_id_async(scryfall_data["id"])
        else:
            card_versions = self.scryfall_cache.get(card_name)
            if card_versions:
                elo_score = await self.try_multiple_ids_for_elo(card_versions)
            else:
                elo_score = 1200.0

        return elo_score

    async def get_card_by_name_with_max_id(self, name: str, extended_name=False) -> dict:
        max_card = {}
        # Attempt to get card versions from the cache
        card_versions = self.scryfall_cache.get(name, [])

        # If no versions found and not using extended name, try with extended name
        if not card_versions and not extended_name:
            extended_name = self.scryfall.get_extended_name(name)
            if extended_name:
                # Recursively call with extended name
                max_card = await self.get_card_by_name_with_max_id(extended_name, extended_name=True)
            else:
                # No extended name found, log and return empty dict
                logger.debug(f"No card with name '{name}' or variants of this found in Scryfall data.")

        # Find the card version with the maximum 'id'
        if card_versions:
            try:
                max_card = max(card_versions, key=lambda card_version: card_version["id"])
            except ValueError:
                # Log error and return empty dict if max operation fails
                logger.debug(f"Error processing versions for card '{name}'.")

        return max_card

    async def get_elo_from_id_async(self, card_id: str) -> Union[float, None]:
        url = f"https://cubecobra.com/tool/card/{card_id}?tab=1"
        html_content = await async_fetch_data(url)
        matches = self.elo_pattern.findall(html_content)
        if not matches:
            logger.debug(f"Could not find any Elo data on card with ID {card_id}")
        else:
            return float(self.elo_digit_pattern.findall(matches[0])[0])

    async def try_multiple_ids_for_elo(self, card_versions) -> Union[float, None]:
        for card_version in card_versions:
            card_id = card_version["id"]
            elo_score = await self.get_elo_from_id_async(card_id)
            if elo_score is not None:
                return elo_score
