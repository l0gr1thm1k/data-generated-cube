import asyncio
import heapq
import re
import warnings
from collections import defaultdict
from pathlib import Path
from typing import List, Tuple, Union

import nltk
import numpy as np
import pandas as pd
from cube_config.cube_configuration import CubeConfig
from loguru import logger
from src.common.args import process_args
from src.common.common import ensure_dir_exists, min_max_normalize_sklearn
from src.common.constants import (COHORT_ANALYSIS_DIRECTORY_PATH, DATA_DIRECTORY_PATH, EVERGREEN_KEYWORDS, TRIOMES,
                                  ACTIVATE_FROM_HAND_ACTION_PATTERN, ACTIVATED_ABILITY_PATTERN, CYCLE_PATTERN,
                                  INSTANT_SPEED_KEYWORDS, ORACLE_TEXT_FLASH_PATTERN, ALTERNATIVE_COST_PATTERN,
                                  ADDITIONAL_COST_PATTERN, COST_REDUCTION_PATTERN, CUBE_COMPLEXITY_WEIGHTS)
from src.data_generated_cube.elo.elo_fetcher import ELOFetcher
from src.data_generated_cube.scryfall.scryfall_cache import shared_scryfall_cache
from src.pipeline_object.pipeline_object import PipelineObject

try:
    from nltk.tokenize import sent_tokenize, word_tokenize
except:
    nltk.download('punkt')
    from nltk.tokenize import sent_tokenize, word_tokenize

warnings.simplefilter("ignore", category=UserWarning)
# Suppress FutureWarning about pandas' deprecated use_inf_as_na option
warnings.filterwarnings("ignore", message=".*use_inf_as_na.*", category=FutureWarning)


class CohortAnalyzer(PipelineObject):
    evergreen_keywords = EVERGREEN_KEYWORDS
    triomes = TRIOMES
    activated_ability_pattern = ACTIVATED_ABILITY_PATTERN
    oracle_flash_pattern = ORACLE_TEXT_FLASH_PATTERN
    cycle_pattern = CYCLE_PATTERN
    instant_speed_keywords = INSTANT_SPEED_KEYWORDS
    hand_action_pattern = ACTIVATE_FROM_HAND_ACTION_PATTERN
    alternative_cost_pattern = ALTERNATIVE_COST_PATTERN
    additional_cost_pattern = ADDITIONAL_COST_PATTERN
    scryfall_cache = shared_scryfall_cache

    @process_args
    def __init__(self, config: Union[str, CubeConfig]):
        super().__init__(config)
        self._set_data_dir(self.config.cubeName)
        self._set_analysis_directory(self.config.cubeName)
        self._set_cube_name_map()
        self.elo_fetcher = ELOFetcher()

    def _set_data_dir(self, data_dir: str) -> None:
        """
        Set the data directory for the CSV file.

        :param data_dir:
        :return:
        """
        data_dir_path = DATA_DIRECTORY_PATH / data_dir
        self.data_dir = ensure_dir_exists(data_dir_path)

    def _set_analysis_directory(self, analysis_dir):
        """
        Set the analysis directory for the CSV file.

        :param analysis_dir:
        :return:
        """
        analysis_dir_path = COHORT_ANALYSIS_DIRECTORY_PATH / analysis_dir
        self.analysis_dir = ensure_dir_exists(analysis_dir_path)

    def _set_cube_name_map(self) -> None:
        _ = pd.read_csv(self.analysis_dir / "cube_names_map.csv")
        self.cube_name_map = _.set_index('Cube ID')['Cube Name'].to_dict()

    def _set_cube_data(self) -> None:
        """
        Set the cube data from the CSV files crawled from Cube Cobra.
        """
        cubes = []
        for cube_file_path in self.data_dir.glob('*.csv'):
            data = pd.read_csv(cube_file_path)
            data['Cube ID'] = cube_file_path.stem
            data['Cube Name'] = self.cube_name_map[cube_file_path.stem]
            cubes.append(data)
        self.aggregate_cube_data = pd.concat(cubes)
        self.aggregate_cube_data.to_csv(self.analysis_dir / "aggregate_cube_data.csv", index=False)

    async def _set_card_data(self) -> None:
        """
        Set the card data from the aggregated cube data.
        """
        if not hasattr(self, 'aggregate_cube_data'):
            self._set_cube_data()
        cube_data_with_elo_scores = await self.update_elo_scores(self.aggregate_cube_data)
        grouped = cube_data_with_elo_scores.groupby('name').agg({
            'Cube ID': ['nunique', lambda x: list(x.unique())],
            'Type': 'first',
            'ELO': 'first',
            'CMC': 'first'
        })
        raw_frequency = cube_data_with_elo_scores['name'].value_counts()
        grouped.columns = ['Cube Frequency', 'Included in Cubes', 'Type', 'ELO', 'CMC']
        total_cubes = cube_data_with_elo_scores['Cube ID'].nunique()
        grouped['Card Uniqueness'] = np.log(total_cubes / grouped['Cube Frequency'])
        grouped['Card Uniqueness'] = min_max_normalize_sklearn(grouped['Card Uniqueness'].values)
        grouped['Non-Land'] = ~grouped['Type'].str.contains('land', case=False)
        grouped['Raw Frequency'] = raw_frequency
        grouped['Instant Speed'] = self.get_instant_speed_vector(grouped.index)
        grouped['Activated Abilities'] = grouped.index.map(lambda x: self.count_activated_abilities(x))
        grouped['Casting Cost Complexity'] = grouped.index.map(lambda x: self.quantify_casting_cost_complexity(x))

        grouped.drop(columns='Type', inplace=True)
        self.card_stats = grouped.sort_values(by=['Cube Frequency', 'ELO'],
                                              ascending=[False, False]).reset_index().rename(
            columns={'index': 'Card Name'})
        self.card_stats.to_csv(self.analysis_dir / "card_stats.csv", index=False)

    async def update_elo_scores(self, freq_frame) -> pd.DataFrame:
        """
        Update the ELO scores for each card in the frame passed in. Get back an updated data frame with ELO scores.

        :param freq_frame: a DataFrame with card names.
        :return: a DataFrame with ELO scores added.
        """

        async def update_elo_cache(fetcher, cards):
            tasks = [fetcher.get_card_elo(card) for card in cards if card is not None]

            return await asyncio.gather(*tasks)

        unique_cards = freq_frame.name.unique()
        await update_elo_cache(self.elo_fetcher, unique_cards)
        self.elo_fetcher.save_cache()

        elo_scores = []
        for card in freq_frame.name:
            if card is None:
                elo_scores.append(0.0)
            else:
                elo = await self.elo_fetcher.get_card_elo(card)
                elo_scores.append(elo)
        freq_frame['ELO'] = elo_scores

        return freq_frame

    def get_instant_speed_vector(self, card_list: pd.Series) -> List[int]:
        """
        Get the spell timing restrictions as a boolean integer value. Returns a vector of integer values that
        corresponds to the list of card names supplied.

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

    def fetch_scryfall_data(self, card_name: str) -> dict:
        """
        fetch card data from scryfall.

        :param card_name: a card name string.
        :return: a dictionary with card data.
        """
        try:
            data = self.elo_fetcher.scryfall_cache.get(card_name, {})[0]
        except KeyError:
            # backoff for adventure and DF Cards
            extended_name = self.scryfall_cache.get_extended_name(card_name)
            data = self.elo_fetcher.scryfall_cache.get(extended_name, {})[0]

        return data

    def count_activated_abilities(self, card_name: str) -> int:
        card_data = self.fetch_scryfall_data(card_name)
        lines = card_data.get('oracle_text', '').split('\n')
        ability_count = 0

        for line in lines:
            if self.activated_ability_pattern.match(line.strip()):
                ability_count += 1

        return ability_count

    @staticmethod
    def strip_reminder_text(text: str) -> str:
        """Remove reminder text (text in parentheses)."""
        return re.sub(r'\([^()]*\)', '', text)

    def has_alternative_cost(self, card_text: str) -> bool:
        """Check if the card has an alternative cost. Returns True if the card has an alternative cost."""
        cleaned_text = self.strip_reminder_text(card_text)
        return re.search(self.alternative_cost_pattern, cleaned_text, re.IGNORECASE) is not None

    def count_alternative_costs(self, card_text):
        """Count the number of alternative costs in the card text."""
        cleaned_text = self.strip_reminder_text(card_text)
        return len(self.alternative_cost_pattern.findall(cleaned_text))

    def quantify_casting_cost_complexity(self, card_name: str) -> float:
        """
        Quantify the complexity of a card's casting cost, including additional and alternate costs from oracle text.
        This method handles complex split cards with multiple faces and adjusts the impact of high CMC values.

        :param card_name: The name of the card to analyze.
        :return: A float representing the total casting cost complexity.
        """
        card_data = self.fetch_scryfall_data(card_name)
        total_complexity = 0

        # Check if the card has multiple faces
        if 'card_faces' in card_data:
            face_count = len(card_data['card_faces'])
            for face in card_data['card_faces']:
                face_complexity = self._calculate_face_complexity(face)
                total_complexity += face_complexity

            # Add additional complexity for having multiple faces
            total_complexity += np.log1p(face_count) * 2
        else:
            total_complexity = self._calculate_face_complexity(card_data)

        return total_complexity

    def _calculate_face_complexity(self, face_data: dict) -> float:
        mana_cost = face_data.get('mana_cost', '')
        oracle_text = face_data.get('oracle_text', '')

        # Base complexity from the mana value, using log1p to reduce impact of high values
        face_complexity = np.log1p(self._calculate_mana_value(mana_cost))

        # Add complexity based on the variety of mana symbols in the cost
        unique_symbols = set(re.findall(r'{[^}]+}', mana_cost))
        symbol_complexity = len(unique_symbols) * 0.75
        face_complexity += symbol_complexity
        cleaned_oracle_text = self.strip_reminder_text(oracle_text)

        # Check for alternative costs
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
            try:
                alt_cost_complexity = np.log1p(len(' '.join(alt_costs_strings)))
            except:
                raise TypeError
            face_complexity += alt_cost_complexity

        if additional_costs:
            add_cost_complexity = np.log1p(len(' '.join(additional_costs)))
            face_complexity += add_cost_complexity

        cost_reductions = re.findall(COST_REDUCTION_PATTERN, cleaned_oracle_text)
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

        # Remove {} and split the string
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

    async def analyze_cohort(self) -> None:
        """
        This is the main method of this class. Analyze the cohort of cubes and write the results to a set of CSV files.
        """
        logger.info(f"Analyzing {self.get_number_of_cubes_sampled(self.data_dir)} cubes in cohort")
        self._set_cube_data()
        await self._set_card_data()
        results = self.combine_cubes()

        for column in ["Keyword Breadth", "Keyword Depth", "Keyword Balance"]:
            results[column] = min_max_normalize_sklearn(results[column].values)

        results["Oracle Text Normalized Mean Word Count"] = min_max_normalize_sklearn(results["Oracle Text Mean Word Count"].values)

        results["Cube Name"] = self.set_cube_name_hyperlinks(results["Cube ID"].values)
        results["Unique Card Count"] = self.format_unique_cards_column(results)
        results["Cross-Cube Card Overlap"] = self.format_duplicate_cards_column(results)

        cube_uniqueness_scores = []
        unique_card_object_counts = []
        token_generators = []
        instant_speed_ratios = []
        activated_ability_ratios = []
        for cube_id in results['Cube ID']:
            cube_cards = self.aggregate_cube_data[self.aggregate_cube_data['Cube ID'] == cube_id]['name'].tolist()
            cube_uniqueness_scores.append(self.calculate_uniqueness_score(cube_cards))
            unique_oracle_ids, total_token_generators = self.count_unique_tokens_and_emblems(cube_cards)
            unique_card_object_counts.append(len(unique_oracle_ids))
            token_generators.append(total_token_generators)
            instant_speed_ratios.append(
                sum(self.card_stats["Instant Speed"][self.card_stats["name"].isin(cube_cards)]) / len(cube_cards))
            activated_ability_ratios.append(sum(self.card_stats["Activated Abilities"][self.card_stats["name"].isin(cube_cards)]) / len(cube_cards))

        results["Cube Uniqueness"] = min_max_normalize_sklearn(cube_uniqueness_scores)
        results["Unique Token Count"] = unique_card_object_counts
        results["Normalized Unique Tokens"] = min_max_normalize_sklearn([xx/yy for xx, yy in zip(unique_card_object_counts, results['Cube Size'])])
        results["Normalized Token Generators"] = min_max_normalize_sklearn([xx/yy for xx, yy in zip(token_generators, results['Cube Size'])])
        results["Instant Speed Ratio"] = instant_speed_ratios
        results["Normalized Instant Speed Ratio"] = min_max_normalize_sklearn(instant_speed_ratios)
        results["Activated Ability Ratio"] = activated_ability_ratios
        results["Normalized Activated Ability Ratio"] = min_max_normalize_sklearn(activated_ability_ratios)
        results["Median Casting Cost Complexity"] = results["Cube ID"].map(lambda x: self.card_stats["Casting Cost Complexity"][self.card_stats["name"].isin(self.aggregate_cube_data[self.aggregate_cube_data['Cube ID'] == x]['name'])].median())
        results["Normalized Median Casting Cost Complexity"] = min_max_normalize_sklearn(results["Median Casting Cost Complexity"].values)

        complexity_columns = ['Keyword Breadth', 'Keyword Depth', 'Oracle Text Normalized Mean Word Count', 'Cube Uniqueness',
             'Normalized Unique Tokens', 'Normalized Token Generators',
             "Normalized Instant Speed Ratio", "Normalized Activated Ability Ratio",
             "Normalized Median Casting Cost Complexity"]
        #results['Cube Complexity'] = results[[column * CUBE_COMPLEXITY_WEIGHTS[column] for column in
        #                                      complexity_columns]].sum(axis=1)
        results['Cube Complexity'] = results.apply(
            lambda row: sum(row[column] * CUBE_COMPLEXITY_WEIGHTS[column] for column in complexity_columns),
            axis=1
        )
        results['Cube Complexity'] = min_max_normalize_sklearn(results['Cube Complexity'].values)

        results = results.sort_values(by='Cube Name')

        column_order = ["Cube Name", "Cube Size", "Cross-Cube Card Overlap", "Unique Card Count",
                        "Unique Card Percentage", "Keyword Breadth", "Keyword Depth", "Defining Keyword Frequency",
                        "Oracle Text Mean Word Count", "Median CMC", "Mean CMC", "Unique Token Count",
                        "Normalized Unique Tokens", "Normalized Token Generators", "Instant Speed Ratio",
                        "Normalized Instant Speed Ratio", "Activated Ability Ratio",
                        "Normalized Activated Ability Ratio", "Median Casting Cost Complexity",
                        "Normalized Median Casting Cost Complexity", "Cube Uniqueness", "Cube Complexity"]
        results = results[column_order]

        results.to_csv(self.analysis_dir / "cube_stats.csv", index=False)
        logger.info(f"Analysis complete, results written to file in analysis directory: file://{self.analysis_dir}")

    @staticmethod
    def get_number_of_cubes_sampled(directory_path) -> int:
        return len(list(Path(directory_path).glob('*.csv')))

    def combine_cubes(self) -> pd.DataFrame:
        cube_dicts = {}
        for cube_file_path in Path(self.data_dir).glob('*.csv'):
            file_name = cube_file_path.stem
            cube_dicts[file_name] = self.analyze_cube(cube_file_path)
        results = pd.DataFrame.from_dict(cube_dicts)
        results = results.T
        results = results.reset_index()
        results.rename(columns={'index': 'Cube ID'}, inplace=True)

        return results

    def analyze_cube(self, filepath) -> dict:
        """
        Analyze a cube file and return a dictionary of the results.

        :param filepath: Path to the cube file.
        :return: Dictionary of analysis results.
        """
        cube_id = filepath.stem
        keyword_counter = defaultdict(int)
        cube_data = {}
        cube = pd.read_csv(filepath)
        word_count = 0
        cube['CMC'] = cube['CMC'].fillna(0)
        cube['CMC'] = pd.to_numeric(cube['CMC'], errors='coerce')
        nonland_cards = cube[~cube['Type'].str.contains('land', case=False)]
        mean_cmc = nonland_cards['CMC'].mean()
        median_cmc = int(nonland_cards['CMC'].median())

        for index in range(cube.shape[0]):
            row = cube.iloc[index]
            cube_data[row["name"]] = self.get_card_data(row["name"], keyword_counter)
            try:
                word_count += self.oracle_text_token_count(row["name"])
            except:
                continue

        keyword_breadth = len(keyword_counter) / cube.shape[0]
        keyword_depth = sum(keyword_counter.values()) / cube.shape[0]
        keyword_balance = keyword_breadth / keyword_depth
        most_frequent_keywords = self.get_k_most_frequent(keyword_counter, 3)
        mean_word_count = word_count / cube.shape[0]

        unique_card_count, unique_card_names = self.get_unique_card_count_and_card_names(cube_id)
        unique_card_percentage = unique_card_count / cube.shape[0]

        return {"Keyword Breadth": keyword_breadth, "Keyword Depth": keyword_depth, "Keyword Balance": keyword_balance,
                "Keyword Frequency": dict(keyword_counter), "Defining Keyword Frequency": most_frequent_keywords,
                "Oracle Text Mean Word Count": mean_word_count, "Cube Size": cube.shape[0],
                "Unique Card Count": unique_card_count, "Unique Card Percentage": unique_card_percentage,
                "Unique Card Names": unique_card_names, "Mean CMC": mean_cmc, "Median CMC": median_cmc}

    def get_card_data(self, card_name, counter: defaultdict) -> List[str]:
        """
        Get data for a specific card.

        :param card_name: the name of the card
        :param counter: a shard counter dictionary to keep track of keyword counts. This parameter's values are updated
        for future use.
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
                    keywords.append(keyword),

        return keywords

    @staticmethod
    def get_k_most_frequent(d, k):
        # Using heapq.nlargest to get the k keys with the largest values
        k_keys = heapq.nlargest(k, d, key=d.get)

        # Creating a new dictionary with these keys and their corresponding values
        k_most_frequent = {key: d[key] for key in k_keys}

        return k_most_frequent

    def oracle_text_token_count(self, card_name: str) -> int:
        """
        Tokenize the oracle text of a card.

        :param card_name: Oracle text of a card.
        :return: List of tokens.
        """
        try:
            data = self.elo_fetcher.scryfall_cache.get(card_name, {})[0]
        except KeyError:
            # backoff for adventure and DF Cards
            data = self.elo_fetcher.scryfall_cache.get(card_name, {})
        oracle_text = data.get('oracle_text', '')
        return len(word_tokenize(oracle_text))

    def get_unique_card_count_and_card_names(self, cube_id) -> Tuple[int, List[str]]:
        """
        Get the number of unique cards in a cube and the names of those cards.

        :param cube_id: as string uniquely identifying a cube.
        :return: get back a tuple of the number of unique cards and a list of the names of those cards.
        """
        cube_data_rows = self.aggregate_cube_data[self.aggregate_cube_data['Cube ID'] == cube_id]
        cube_card_names = set(cube_data_rows['name'])
        other_cube_card_names = set(self.aggregate_cube_data[self.aggregate_cube_data['Cube ID'] != cube_id]['name'])
        names_exclusive_to_data = cube_card_names - other_cube_card_names

        return len(names_exclusive_to_data), list(names_exclusive_to_data)

    def set_cube_name_hyperlinks(self, cube_ids):
        formatted_names = []
        for cube_id in cube_ids:
            formatted_names.append(
                f'''=HYPERLINK("https://cubecobra.com/cube/overview/{cube_id}", "{self.cube_name_map[cube_id]}")''')

        return formatted_names

    def format_unique_cards_column(self, data):
        values = []
        for row in data.iterrows():
            id = row[1]['Cube ID']
            cards = row[1]['Unique Card Names']
            scryfall_url = self.make_cube_cobra_visual_spoiler_url(id, cards)
            values.append(f'''=HYPERLINK("{scryfall_url}", "{row[1]["Unique Card Count"]}")''')
        return values

    def format_duplicate_cards_column(self, data):
        values = []
        for row in data.iterrows():
            cube_id = row[1]['Cube ID']
            cube_size, _ = self.get_unique_card_count_and_card_names(cube_id)
            cards = row[1]['Unique Card Names']
            duplicate_card_count = int(row[1]['Cube Size'] - cube_size)
            scryfall_url = self.make_cube_cobra_visual_spoiler_url(cube_id, cards, exclusion=True)
            values.append(f'''=HYPERLINK("{scryfall_url}", "{duplicate_card_count}")''')
        return values

    def make_cube_cobra_visual_spoiler_url(self, cube_id, card_list, exclusion=False) -> str:
        """
        Make a URL to a Scryfall search for a list of cards.

        :param cube_id: a string id from Cube Cobra.
        :param card_list: a list of card names
        :param exclusion: a boolean indicating whether the search should be for cards not in the list.
        :return: get back a url string for a CubeCobra filtered search.
        """
        url_start = f"https://cubecobra.com/cube/list/{cube_id}?f="
        joiner = '%22+or+' if exclusion is False else '%22+and+'
        url_end = "%22&view=spoiler"
        card_list = joiner.join([self.format_card_name(card_name, exclusion) for card_name in card_list])

        return url_start + card_list + url_end

    @staticmethod
    def format_card_name(card_name, exclusion=False):
        beginning = "-name%3A%22" if exclusion else "name%3D%22"
        return beginning + card_name.replace(" ", "+")

    def calculate_uniqueness_score(self, card_names: List[str]) -> float:
        """
        Calculate the uniqueness score for a list of card names. This list of card names will be the cards in a cube.

        :param card_names: a list of string card names
        :return: a float value denoting the uniqueness score of the cube normalized by its size.
        """
        numerator = self.card_stats[self.card_stats['name'].isin(card_names)]['Card Uniqueness'].sum()
        denominator = self.card_stats[self.card_stats['name'].isin(card_names)]['Card Uniqueness'].count()

        return numerator / denominator

    def count_unique_tokens_and_emblems(self, cube_cards: list) -> Tuple[set, int]:
        """
        Count the unique tokens and emblems in a list of cube cards.

        :param cube_cards: A list of cube cards to check for unique tokens and emblems.
        :return: get back a tuple of sets of unique oracles and card object names.
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

    def _get_card_data(self, card_name: str) -> Union[dict, None]:
        """
        Get card data from Scryfall. Get a card object based on the card name, or the extended name if it cannot be
        found given the card name parameter. Example extended name: "Giant Killer // Chop Down"

        :param card_name: The name of the card data
        :return:
        """
        card_data = self.elo_fetcher.scryfall_cache.get(card_name)
        if card_data is None or not card_data:
            extended_name = self.scryfall_cache.get_extended_name(card_name)
            card_data = self.elo_fetcher.scryfall_cache.get(extended_name)

        return card_data[0] if card_data and len(card_data) > 0 else None

    def _process_card_parts(self, card_data: dict) -> Tuple[set, set]:
        """
        Process all parts of the card to find unique oracles and names.

        :param card_data: a dictionary of card data
        return
        """
        card_oracles, card_object_names = set(), set()
        all_parts = card_data.get('all_parts', [])
        relevant_types = {'Token', 'Emblem', 'Dungeon'}
        for part in all_parts:
            if any(typ in part['type_line'] for typ in relevant_types):
                card_object = self.elo_fetcher.scryfall_cache.get(part['name'])
                if card_object:
                    card_object = card_object[0]
                    card_oracles.add(card_object['oracle_id'])
                    card_object_names.add(card_object['name'])

        return card_oracles, card_object_names
