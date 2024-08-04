import asyncio
import heapq
import warnings
from collections import defaultdict
from loguru import logger
from pathlib import Path
from typing import List, Tuple, Union

import nltk
import numpy as np
import pandas as pd
from src.common.common import ensure_dir_exists, min_max_normalize_sklearn
from src.common.constants import DATA_DIRECTORY_PATH, COHORT_ANALYSIS_DIRECTORY_PATH, EVERGREEN_KEYWORDS, TRIOMES
from src.common.args import process_args
from src.cube_config.cube_configuration import CubeConfig
from src.pipeline_object.pipeline_object import PipelineObject

from src.data_generated_cube.elo.elo_fetcher import ELOFetcher

try:
    from nltk.tokenize import sent_tokenize, word_tokenize
except:
    nltk.download('punkt')
    from nltk.tokenize import sent_tokenize, word_tokenize

warnings.simplefilter("ignore", category=UserWarning)


class CohortAnalyzer(PipelineObject):
    evergreen_keywords = EVERGREEN_KEYWORDS
    triomes = TRIOMES

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
        for cube_id in results['Cube ID']:
            cube_cards = self.aggregate_cube_data[self.aggregate_cube_data['Cube ID'] == cube_id]['name'].tolist()
            cube_uniqueness_scores.append(self.calculate_uniqueness_score(cube_cards))
            unique_oracle_ids, total_token_generators = self.count_unique_tokens_and_emblems(cube_cards)
            unique_card_object_counts.append(len(unique_oracle_ids))
            token_generators.append(total_token_generators)
        results["Cube Uniqueness"] = min_max_normalize_sklearn(cube_uniqueness_scores)
        results["Unique Token Count"] = unique_card_object_counts
        results["Normalized Unique Tokens"] = min_max_normalize_sklearn([xx/yy for xx, yy in zip(unique_card_object_counts, results['Cube Size'])])
        results["Normalized Token Generators"] = min_max_normalize_sklearn([xx/yy for xx, yy in zip(token_generators, results['Cube Size'])])
        results['Cube Complexity'] = results[
            ['Keyword Breadth', 'Keyword Depth', 'Oracle Text Normalized Mean Word Count', 'Cube Uniqueness',
             'Unique Card Percentage', 'Normalized Unique Tokens', 'Normalized Token Generators']].sum(axis=1)
        results['Cube Complexity'] = min_max_normalize_sklearn(results['Cube Complexity'].values)

        results = results.sort_values(by='Cube Name')

        column_order = ["Cube Name", "Cube Size", "Cross-Cube Card Overlap", "Unique Card Count", "Unique Card Percentage",
                        "Keyword Breadth", "Keyword Depth", "Defining Keyword Frequency", "Oracle Text Mean Word Count",
                        "Median CMC", "Mean CMC", "Unique Token Count", "Normalized Unique Tokens", "Normalized Token Generators",
                        "Cube Uniqueness", "Cube Complexity"]
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
        try:
            data = self.elo_fetcher.scryfall_cache.get(card_name, {})[0]
        except KeyError:
            # backoff for adventure and DF Cards
            data = self.elo_fetcher.scryfall_cache.get(card_name, {})
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
            extended_name = self.elo_fetcher.get_extended_name(card_name)
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
