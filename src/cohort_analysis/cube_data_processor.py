import asyncio
import numpy as np
import pandas as pd
from collections import defaultdict
from typing import Dict, List, Tuple
from pathlib import Path

from src.common.common import ensure_dir_exists
from src.data_generated_cube.elo.elo_fetcher import ELOFetcher
from src.common.constants import DATA_DIRECTORY_PATH, COHORT_ANALYSIS_DIRECTORY_PATH
from src.common.common import min_max_normalize_sklearn, get_k_most_frequent
from card_data_manager import CardDataManager


class CubeDataProcessor:
    def __init__(self, config):
        self.config = config
        self.card_data_manager = CardDataManager()
        self.elo_fetcher = ELOFetcher()
        self._set_data_dir(self.config.cubeName)
        self._set_analysis_directory(self.config.cubeName)
        self._set_cube_name_map()
        self.aggregate_cube_data = None

    def _set_data_dir(self, data_dir: str) -> None:
        """Set the data directory for the CSV file."""
        data_dir_path = DATA_DIRECTORY_PATH / data_dir
        self.data_dir = ensure_dir_exists(data_dir_path)

    def _set_analysis_directory(self, analysis_dir: str):
        """Set the analysis directory for the CSV file."""
        analysis_dir_path = COHORT_ANALYSIS_DIRECTORY_PATH / analysis_dir
        self.analysis_dir = ensure_dir_exists(analysis_dir_path)

    def _set_cube_name_map(self) -> None:
        """Set the cube name map from CSV file."""
        _ = pd.read_csv(self.analysis_dir / "cube_names_map.csv")
        self.cube_name_map = _.set_index('Cube ID')['Cube Name'].to_dict()

    def _set_cube_data(self) -> None:
        """Set the cube data from the CSV files crawled from Cube Cobra."""
        cubes = []
        for cube_file_path in self.data_dir.glob('*.csv'):
            data = pd.read_csv(cube_file_path)
            data['Cube ID'] = cube_file_path.stem
            data['Cube Name'] = self.cube_name_map[cube_file_path.stem]
            cubes.append(data)
        self.aggregate_cube_data = pd.concat(cubes)
        self.aggregate_cube_data.to_csv(self.analysis_dir / "aggregate_cube_data.csv", index=False)

    async def _set_card_data(self) -> None:
        """Set the card data from the aggregated cube data."""
        if self.aggregate_cube_data is None:
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

    def combine_cubes(self) -> pd.DataFrame:
        """Combine data from all cube CSV files."""
        cube_dicts = {}
        for cube_file_path in Path(self.data_dir).glob('*.csv'):
            file_name = cube_file_path.stem
            cube_dicts[file_name] = self.analyze_cube(cube_file_path)
        results = pd.DataFrame.from_dict(cube_dicts, orient='index')
        results = results.reset_index().rename(columns={'index': 'Cube ID'})
        return results

    def analyze_cube(self, filepath: Path) -> Dict:
        """Analyze a cube file and return a dictionary of the results."""
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

        for _, row in cube.iterrows():
            cube_data[row["name"]] = self.card_data_manager.get_keywords(row["name"], keyword_counter)
            word_count += self.card_data_manager.oracle_text_token_count(row["name"])

        keyword_breadth = len(keyword_counter) / cube.shape[0]
        keyword_depth = sum(keyword_counter.values()) / cube.shape[0]
        keyword_balance = keyword_breadth / keyword_depth
        most_frequent_keywords = get_k_most_frequent(keyword_counter, 3)  # Use imported function
        mean_word_count = word_count / cube.shape[0]

        unique_card_count, unique_card_names = self.get_unique_card_count_and_card_names(cube_id)
        unique_card_percentage = unique_card_count / cube.shape[0]

        return {
            "Keyword Breadth": keyword_breadth,
            "Keyword Depth": keyword_depth,
            "Keyword Balance": keyword_balance,
            "Keyword Frequency": dict(keyword_counter),
            "Defining Keyword Frequency": most_frequent_keywords,
            "Oracle Text Mean Word Count": mean_word_count,
            "Cube Size": cube.shape[0],
            "Unique Card Count": unique_card_count,
            "Unique Card Percentage": unique_card_percentage,
            "Unique Card Names": unique_card_names,
            "Mean CMC": mean_cmc,
            "Median CMC": median_cmc
        }

    def get_unique_card_count_and_card_names(self, cube_id: str) -> Tuple[int, List[str]]:
        """Get the number of unique cards in a cube and the names of those cards."""
        cube_data_rows = self.aggregate_cube_data[self.aggregate_cube_data['Cube ID'] == cube_id]
        cube_card_names = set(cube_data_rows['name'])
        other_cube_card_names = set(self.aggregate_cube_data[self.aggregate_cube_data['Cube ID'] != cube_id]['name'])
        names_exclusive_to_data = cube_card_names - other_cube_card_names
        return len(names_exclusive_to_data), list(names_exclusive_to_data)

    async def update_elo_scores(self, freq_frame: pd.DataFrame) -> pd.DataFrame:
        """Update the ELO scores for each card in the frame."""

        async def update_elo_cache(cards):
            tasks = [self.elo_fetcher.get_card_elo(card) for card in cards if card is not None]
            return await asyncio.gather(*tasks)

        unique_cards = freq_frame['name'].unique()
        await update_elo_cache(unique_cards)
        self.elo_fetcher.save_cache()

        elo_scores = []
        for card in freq_frame['name']:
            if card is None:
                elo_scores.append(0.0)
            else:
                elo = await self.elo_fetcher.get_card_elo(card)
                elo_scores.append(elo)
        freq_frame['ELO'] = elo_scores

        return freq_frame
