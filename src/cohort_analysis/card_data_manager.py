

import re
from typing import List, Tuple, Union
from collections import defaultdict

from src.common.constants import (TRIOMES, EVERGREEN_KEYWORDS, INSTANT_SPEED_KEYWORDS,
                                  ORACLE_TEXT_FLASH_PATTERN, CYCLE_PATTERN, ACTIVATE_FROM_HAND_ACTION_PATTERN,
                                  ACTIVATED_ABILITY_PATTERN, ALTERNATIVE_COST_PATTERN, ADDITIONAL_COST_PATTERN,
                                  COST_REDUCTION_PATTERN)
from src.data_generated_cube.scryfall.scryfall_cache import shared_scryfall_cache
from src.data_generated_cube.elo.elo_fetcher import ELOFetcher
from src.common.common import strip_reminder_text

import numpy as np


class CardDataManager:
    def __init__(self):
        self.scryfall_cache = shared_scryfall_cache
        self.elo_fetcher = ELOFetcher()
        self.triomes = TRIOMES
        self.evergreen_keywords = EVERGREEN_KEYWORDS
        self.instant_speed_keywords = INSTANT_SPEED_KEYWORDS
        self.oracle_flash_pattern = ORACLE_TEXT_FLASH_PATTERN
        self.cycle_pattern = CYCLE_PATTERN
        self.hand_action_pattern = ACTIVATE_FROM_HAND_ACTION_PATTERN
        self.activated_ability_pattern = ACTIVATED_ABILITY_PATTERN
        self.alternative_cost_pattern = ALTERNATIVE_COST_PATTERN
        self.additional_cost_pattern = ADDITIONAL_COST_PATTERN
        self.cost_reduction_pattern = COST_REDUCTION_PATTERN

    def get_keywords(self, card_name: str, counter: defaultdict) -> List[str]:
        """
        Get data for a specific card.

        :param card_name: the name of the card
        :param counter: a shared counter dictionary to keep track of keyword counts.
        :return: a list of keywords for the card.
        """
        data = self.fetch_scryfall_data(card_name)
        keywords = data.get('keywords', [])
        if card_name not in self.triomes:
            for keyword in keywords:
                if keyword not in self.evergreen_keywords:
                    counter[keyword] += 1

            oracle = data.get('oracle_text', '')
            for phrase, keyword in [('you become the monarch', 'Monarch'), ('Ring tempts you', 'The Ring tempts you')]:
                if phrase in oracle:
                    counter[keyword] += 1
                    keywords.append(keyword)

        return keywords

    def fetch_scryfall_data(self, card_name: str) -> dict:
        """
        Fetch card data from Scryfall.

        :param card_name: a card name string.
        :return: a dictionary with card data.
        """
        try:
            data = self.scryfall_cache.get(card_name, {})[0]
        except KeyError:
            # backoff for adventure and DF Cards
            extended_name = self.scryfall_cache.get_extended_name(card_name)
            data = self.scryfall_cache.get(extended_name, {})[0]

        return data

    def _get_card_data(self, card_name: str) -> Union[dict, None]:
        """
        Get card data from Scryfall. Get a card object based on the card name, or the extended name if it cannot be
        found given the card name parameter. Example extended name: "Giant Killer // Chop Down"

        :param card_name: The name of the card
        :return: Card data dictionary or None if not found
        """
        card_data = self.scryfall_cache.get(card_name)
        if card_data is None or not card_data:
            extended_name = self.scryfall_cache.get_extended_name(card_name)
            card_data = self.scryfall_cache.get(extended_name)

        return card_data[0] if card_data and len(card_data) > 0 else None

    def _process_card_parts(self, card_data: dict) -> Tuple[set, set]:
        """
        Process all parts of the card to find unique oracles and names.

        :param card_data: a dictionary of card data
        :return: A tuple of sets containing unique oracle IDs and card object names
        """
        card_oracles, card_object_names = set(), set()
        all_parts = card_data.get('all_parts', [])
        relevant_types = {'Token', 'Emblem', 'Dungeon'}
        for part in all_parts:
            if any(typ in part['type_line'] for typ in relevant_types):
                card_object = self.scryfall_cache.get(part['name'])
                if card_object:
                    card_object = card_object[0]
                    card_oracles.add(card_object['oracle_id'])
                    card_object_names.add(card_object['name'])

        return card_oracles, card_object_names

    def count_unique_tokens_and_emblems(self, cube_cards: list) -> Tuple[set, int]:
        """
        Count the unique tokens and emblems in a list of cube cards.

        :param cube_cards: A list of cube cards to check for unique tokens and emblems.
        :return: A tuple of sets of unique oracles and total token generators count.
        """
        unique_oracles = set()
        total_token_generators = 0
        for card_name in cube_cards:
            card_data = self._get_card_data(card_name)
            if card_data:
                card_oracles, _ = self._process_card_parts(card_data)
                total_token_generators += len(card_oracles)
                unique_oracles.update(card_oracles)

        return unique_oracles, total_token_generators

    def oracle_text_token_count(self, card_name: str) -> int:
        """
        Count the number of tokens in the oracle text of a card.

        :param card_name: Name of the card.
        :return: Number of tokens in the oracle text.
        """
        from nltk.tokenize import word_tokenize

        data = self._get_card_data(card_name)
        if data is None:
            return 0
        oracle_text = data.get('oracle_text', '')
        return len(word_tokenize(oracle_text))

    def get_instant_speed_vector(self, card_list: List[str]) -> List[int]:
        """
        Get the spell timing restrictions as a boolean integer value.

        :param card_list: a list of card names.
        :return: a vector of integer values consisting of 0 or 1.
        """
        instant_speed_vector = []

        for card_name in card_list:
            data = self.fetch_scryfall_data(card_name)
            type_line = data.get('type_line', '')
            keywords = set(data.get('keywords', []))
            oracle = data.get('oracle_text', '')

            if (
                    'Instant' in type_line
                    or self.instant_speed_keywords.intersection(keywords)
                    or self.oracle_flash_pattern.search(oracle)
                    or ('Cycling' in keywords and self.cycle_pattern.search(oracle))
                    or self.hand_action_pattern.search(oracle)
            ):
                instant_speed_vector.append(1)
            else:
                instant_speed_vector.append(0)

        return instant_speed_vector

    def count_activated_abilities(self, card_name: str) -> int:
        """
        Count the number of activated abilities on a card.

        :param card_name: Name of the card
        :return: Number of activated abilities
        """
        card_data = self.fetch_scryfall_data(card_name)
        lines = card_data.get('oracle_text', '').split('\n')
        ability_count = 0

        for line in lines:
            if self.activated_ability_pattern.match(line.strip()):
                ability_count += 1

        return ability_count

    def quantify_casting_cost_complexity(self, card_name: str) -> float:
        """
        Quantify the complexity of a card's casting cost.

        :param card_name: The name of the card to analyze.
        :return: A float representing the total casting cost complexity.
        """
        card_data = self.fetch_scryfall_data(card_name)
        total_complexity = 0

        if 'card_faces' in card_data:
            face_count = len(card_data['card_faces'])
            for face in card_data['card_faces']:
                face_complexity = self._calculate_face_complexity(face)
                total_complexity += face_complexity

            total_complexity += np.log1p(face_count) * 2
        else:
            total_complexity = self._calculate_face_complexity(card_data)

        return total_complexity

    def _calculate_face_complexity(self, face_data: dict) -> float:
        """
        Calculate the complexity of a single face of a card.

        :param face_data: Dictionary containing data for one face of a card.
        :return: Float representing the complexity of the face.
        """
        mana_cost = face_data.get('mana_cost', '')
        oracle_text = face_data.get('oracle_text', '')

        face_complexity = np.log1p(self._calculate_mana_value(mana_cost))

        unique_symbols = set(re.findall(r'{[^}]+}', mana_cost))
        symbol_complexity = len(unique_symbols) * 0.75
        face_complexity += symbol_complexity
        cleaned_oracle_text = strip_reminder_text(oracle_text)

        alt_costs_strings = []
        additional_costs = []
        for line in cleaned_oracle_text.split('\n'):
            line = line.strip()
            alt_cost_match = self.alternative_cost_pattern.match(line)
            add_cost_match = self.additional_cost_pattern.match(line)
            if alt_cost_match:
                alt_costs_strings.append(alt_cost_match.group(1))
            if add_cost_match:
                additional_costs.append(add_cost_match.group(1))

        if alt_costs_strings:
            alt_cost_complexity = np.log1p(len(' '.join(alt_costs_strings)))
            face_complexity += alt_cost_complexity

        if additional_costs:
            add_cost_complexity = np.log1p(len(' '.join(additional_costs)))
            face_complexity += add_cost_complexity

        cost_reductions = re.findall(self.cost_reduction_pattern, cleaned_oracle_text)
        for reduction in cost_reductions:
            face_complexity += np.log1p(len(reduction)) * 1.5

        return face_complexity

    @staticmethod
    def _calculate_mana_value(mana_cost: str) -> float:
        """
        Calculate the mana value from a mana cost string.

        :param mana_cost: A string representing the mana cost (e.g., "{X}{W}")
        :return: A float representing the mana value
        """
        if not mana_cost:
            return 0.0

        symbols = mana_cost.replace("{", "").replace("}", "").split("/")

        if isinstance(symbols, str):
            symbols = [char for char in symbols]
        else:
            _ = []
            for symbol_string in symbols:
                _.extend([char for char in symbol_string])
            symbols = _

        total_value = 0
        for symbol in symbols:
            if symbol.isdigit():
                total_value += int(symbol)
            elif symbol in "WUBRG":
                total_value += 1
            elif symbol == "X":
                total_value += 1  # Count X as 1 for complexity purposes

        return float(total_value)
