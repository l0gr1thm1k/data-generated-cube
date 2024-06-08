import asyncio
import heapq
import warnings
from collections import defaultdict
from pathlib import Path
from typing import List, Tuple, Union
from cube_config.cube_configuration import CubeConfig

import nltk
import numpy as np
import pandas as pd
from common.common import ensure_dir_exists, min_max_normalize_sklearn
from common.constants import DATA_DIRECTORY_PATH
from common.args import process_args
from pipeline_object.pipeline_object import PipelineObject

from data_generated_cube.elo.elo_fetcher import ELOFetcher

try:
    from nltk.tokenize import sent_tokenize, word_tokenize
except:
    nltk.download('punkt')
    from nltk.tokenize import sent_tokenize, word_tokenize

warnings.simplefilter("ignore", category=UserWarning)


class CubeAnalyzer(PipelineObject):
    evergreen_keywords = {
        "Activate", "Attach", "Cast", "Counter", "Create", "Deathtouch", "Defender", "Destroy", "Discard",
        "Double strike", "Enchant", "Equip", "Exchange", "Exile", "Fight", "First strike", "Flash", "Flying", "Haste",
        "Hexproof", "Indestructible", "Lifelink", "Menace", "Mill", "Play", "Protection", "Reach", "Reveal",
        "Sacrifice", "Scry", "Search", "Shuffle", "Tap/Untap", "Trample", "Vigilance", "Ward"}
    triomes = {"Savai Triome", "Indatha Triome", "Ketria Triome", "Raugrin Triome", "Zagoth Triome", "Raffine's Tower",
               "Spara's Headquarters", "Xander's Lounge", "Jetmir's Garden", "Ziatora's Proving Ground"}

    @process_args
    def __init__(self, config: Union[str, CubeConfig]):
        super().__init__(config)
        self._set_data_dir(self.config.cubeName)
        self.analysis_dir = ensure_dir_exists(Path(__file__).parent / "analysis" / self.config.cubeName)
        self.elo_fetcher = ELOFetcher()

    def _set_data_dir(self, data_dir: str) -> None:
        """
        Set the data directory for the CSV file.

        :param data_dir:
        :return:
        """
        data_dir_path = DATA_DIRECTORY_PATH / data_dir
        self.data_dir = ensure_dir_exists(data_dir_path)

    @staticmethod
    def _clear_directory(directory_path: str) -> None:
        """
        Clear the contents of a directory.

        :param directory_path:
        :return:
        """
        directory = Path(directory_path)
        for file_path in directory.iterdir():
            if file_path.is_file():
                file_path.unlink()

    def _set_cube_data(self) -> None:
        """
        Set the cube data from the CSV files crawled from Cube Cobra.
        """
        cubes = []
        for cube_file_path in self.data_dir.glob('*.csv'):
            data = pd.read_csv(cube_file_path)
            data['Cube ID'] = cube_file_path.stem
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
            'Cube ID': 'nunique',
            'Type': 'first',
            'ELO': 'first',
            'CMC': 'first'
        })
        raw_frequency = cube_data_with_elo_scores['name'].value_counts()
        grouped.rename(columns={'Cube ID': 'Cube Frequency'}, inplace=True)
        total_cubes = cube_data_with_elo_scores['Cube ID'].nunique()
        grouped['Card Uniqueness'] = np.log(total_cubes / grouped['Cube Frequency'])
        grouped['Card Uniqueness'] = min_max_normalize_sklearn(grouped['Card Uniqueness'].values)
        grouped['Non-Land'] = ~grouped['Type'].str.contains('land', case=False)
        grouped['Raw Frequency'] = raw_frequency
        grouped.drop(columns='Type', inplace=True)
        self.card_stats = grouped.sort_values(by='Cube Frequency', ascending=False).reset_index().rename(
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
        self._set_cube_data()
        await self._set_card_data()
        results = self.combine_cubes()

        for column in ["Keyword Breadth", "Keyword Depth", "Keyword Balance"]:
            results[column] = min_max_normalize_sklearn(results[column].values)

        results["Oracle Text Normalized Mean Word Count"] = min_max_normalize_sklearn(results["Oracle Text Mean Word Count"].values)

        results['Cube Complexity'] = results[['Keyword Breadth', 'Keyword Depth', 'Oracle Text Normalized Mean Word Count']].sum(axis=1)
        results['Normalized Cube Complexity'] = min_max_normalize_sklearn(results['Cube Complexity'].values)
        results = results.sort_values(by='Cube Complexity', ascending=False)

        results["Cube Name"] = self.set_cube_name_hyperlinks(results["Cube ID"].values)
        results["Unique Card Count"] = self.format_unique_cards_column(results)
        results["Duplicate Card Count"] = self.format_duplicate_cards_column(results)

        column_order = ["Cube Name", "Cube Size", "Duplicate Card Count", "Unique Card Count", "Unique Card Percentage",
                        "Keyword Breadth", "Keyword Depth", "Defining Keyword Frequency", "Oracle Text Mean Word Count",
                        "Oracle Text Normalized Mean Word Count", "Normalized Cube Complexity"]
        results = results[column_order]

        results.to_csv(self.analysis_dir / "cube_stats.csv", index=False)

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
                "Unique Card Names": unique_card_names}

    def get_card_data(self, card_name, counter: defaultdict):
        """
        Get data for a specific card.

        :param card_name:
        :param counter:
        :return:
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

    @staticmethod
    def set_cube_name_hyperlinks(cube_ids):
        cube_name_frame = pd.read_csv("/home/daniel/Code/mtg/data-generated-cube/src/cohort_analysis/cube_names_map.csv")
        cube_name_map = {}
        for row in cube_name_frame.iterrows():
            cube_name_map[row[1]['Cube ID']] = row[1]['Cube Name']
        formatted_names = []
        for cube_id in cube_ids:
            formatted_names.append(
                f'''=HYPERLINK("https://cubecobra.com/cube/overview/{cube_id}", "{cube_name_map[cube_id]}")''')

        return formatted_names

    def format_unique_cards_column(self, data):
        values = []
        for row in data.iterrows():
            id = row[1]['Cube ID']
            cards = row[1]['Unique Card Names']
            scryfall_url = self.make_scryfall_url(id, cards)
            values.append(f'''=HYPERLINK("{scryfall_url}", "{row[1]["Unique Card Count"]}")''')
        return values

    def format_duplicate_cards_column(self, data):
        values = []
        for row in data.iterrows():
            cube_id = row[1]['Cube ID']
            cube_size, _ = self.get_unique_card_count_and_card_names(cube_id)
            cards = row[1]['Unique Card Names']
            duplicate_card_count = int(row[1]['Cube Size'] - cube_size)
            scryfall_url = self.make_scryfall_url(cube_id, cards, exclusion=True)
            values.append(f'''=HYPERLINK("{scryfall_url}", "{duplicate_card_count}")''')
        return values

    def make_scryfall_url(self, cube_id, card_list, exclusion=False):
        example = """https://cubecobra.com/cube/list/RGCC?f=name%3D%22Aboleth+Spawn%22+or+name%3D%22Access+Denied%22&view=spoiler"""
        base_url = f"https://cubecobra.com/cube/list/{cube_id}?f="
        joiner = '%22+or+' if exclusion is False else '%22+and+'
        url_end = "%22&view=spoiler"
        card_list = joiner.join([self.format_card_name(card_name, exclusion) for card_name in card_list])

        return base_url + card_list + url_end

    @staticmethod
    def format_card_name(card_name, exclusion=False):
        beginning = "-name%3A%22" if exclusion else "name%3D%22"
        return beginning + card_name.replace(" ", "+")


if __name__ == '__main__':
    analyzer = CubeAnalyzer("/home/daniel/Code/mtg/data-generated-cube/src/cube_config/example_configs/cubecon2024_lists.json")
    analyzer.analyze_cohort()