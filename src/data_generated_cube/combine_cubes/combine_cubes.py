import os
import json
import pandas as pd

import numpy as np

from loguru import logger
from pathlib import Path

from common.common import min_max_normalize_sklearn
from common.constants import CARD_COLOR_MAP, CUBE_CREATION_RESOURCES_DIRECTORY
from data_generated_cube.elo.elo_fetcher import ELOFetcher


class CubeCombiner:
    manual_card_color_mapping = pd.read_csv(CUBE_CREATION_RESOURCES_DIRECTORY / 'manually_mapped_color_cards.csv')

    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.elo_fetcher = ELOFetcher()
        self.cube_weights = self.load_cube_weights()

    def load_cube_weights(self):
        with open(self.data_dir / 'cube_weights.json', 'r') as f:
            return json.load(f)

    def combine_cubes_from_directory(self) -> pd.DataFrame:
        """
        Combines cubes from a specified directory into a single DataFrame. Additionally, perform cleaning and filtering.

        :return: Combined DataFrame.

        """
        chunks = []
        for cube_file_path in Path(self.data_dir).glob('*.csv'):
            chunk = self.process_cube_file(cube_file_path)
            if chunk is not None:
                chunks.append(chunk)

        if chunks:
            logger.info(f"Sampling {len(chunks)} cubes...")
            concatted_frame = pd.concat(chunks, ignore_index=True)
        else:
            logger.debug("No cubes found in the specified directory...", directory=self.data_dir)
            concatted_frame = pd.DataFrame()

        concatted_frame = self.get_new_columns(concatted_frame)

        return concatted_frame

    def process_cube_file(self, file_path: str) -> pd.DataFrame:
        try:

            chunk = pd.read_csv(file_path)
            chunk = self.remove_maybeboard_cards(chunk)
            chunk = self.manually_map_card_colors(chunk)

            cube_id = os.path.basename(file_path).replace('.csv', '')
            cube_weight = self.cube_weights.get(cube_id, 1)
            chunk['Cube Weight'] = cube_weight

            chunk.to_csv(file_path, index=False)

            return chunk

        except pd.errors.EmptyDataError:
            raise ValueError(f"Empty cube: {os.path.basename(file_path)}")

        except pd.errors.ParserError:
            raise ValueError(f"Error parsing cube: {os.path.basename(file_path)}")

    @staticmethod
    def remove_maybeboard_cards(frame: pd.DataFrame) -> pd.DataFrame:
        filtered_frame = frame[~frame['maybeboard']]
        filtered_frame.reset_index(inplace=True, drop=True)

        return filtered_frame

    def manually_map_card_colors(self, frame: pd.DataFrame) -> pd.DataFrame:
        """
        Maps card colors in the DataFrame using a manually mapped color file.

        :param frame: Input DataFrame containing card data.
        :return: DataFrame with mapped card colors.
        """
        merged_frame = frame.merge(self.manual_card_color_mapping[['Name', 'Color Category']], left_on='name',
                                   right_on='Name', how='left')
        merged_frame['Color Category'] = merged_frame['Color Category_y'].fillna(merged_frame['Color Category_x'])
        merged_frame.drop(columns=['Color Category_x', 'Color Category_y'], inplace=True)

        merged_frame.drop(columns=['Name'], inplace=True)

        self.clean_color_category_strings(merged_frame)

        return merged_frame

    def clean_color_category_strings(self, dataframe: pd.DataFrame) -> None:
        """
        Cleans the color category strings in the DataFrame.
        """
        dataframe['Color Category'] = dataframe['Color Category'].apply(
            lambda x: ('m' if x == 'Multicolored' else ('l' if x == 'Lands' else x)))
        dataframe['Color Category'] = dataframe['Color Category'].apply(self.map_color_name)

    @staticmethod
    def map_color_name(color_string: str) -> str:
        try:

            return CARD_COLOR_MAP[color_string]

        except KeyError as err:

            raise KeyError(f"Missing a key {color_string}: {err}")

    def get_new_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Adds new columns to the DataFrame. The only required column in the data is 'name'.

        * Frequency
        * Inclusion Rate
        * ELO
        * Log ELO
        * Log Inclusion Rate
        * Normalized ELO
        * Normalized Inclusion Rate
        * Inclusion Rate ELO Diff
        * Weighted Rank
        """
        data = self.calculate_frequency(data)
        data = self.calculate_inclusion_rate(data)
        data = self.get_elo_scores(data)
        data = self.calculate_card_weight(data)
        data['Log ELO'] = data['ELO'].apply(np.log)
        data['Log Inclusion Rate'] = data['Inclusion Rate'].apply(np.log)
        for new_col, norm_col in [('Normalized ELO', 'ELO'), ('Normalized Inclusion Rate', 'Inclusion Rate')]:
            data[new_col] = min_max_normalize_sklearn(data[norm_col])
        data['Inclusion Rate ELO Diff'] = data.apply(self.get_elo_coverage_diff, axis=1)
        data['Weighted Rank'] = data['Log ELO'] * data['Card Weight']
        data['Weighted Rank'] = min_max_normalize_sklearn(data['Weighted Rank'])

        data = data.drop_duplicates(subset=['name'])

        return data

    @staticmethod
    def calculate_frequency(frequency_dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates the frequency of each card in the DataFrame.
        """
        frequency_dataframe['Frequency'] = frequency_dataframe.groupby('name')['name'].transform('size')

        return frequency_dataframe

    def calculate_inclusion_rate(self, frequency_dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates the inclusion rate of each card in the DataFrame.
        """
        number_of_sampled_cubes = self.get_number_of_cubes_sampled(self.data_dir)
        frequency_dataframe['Inclusion Rate'] = frequency_dataframe['Frequency'] / number_of_sampled_cubes
        frequency_dataframe['Inclusion Rate'] = frequency_dataframe['Inclusion Rate'].round(4)

        return frequency_dataframe

    @staticmethod
    def get_number_of_cubes_sampled(directory_path) -> int:
        return len(list(Path(directory_path).glob('*.csv')))

    def get_elo_scores(self, freq_frame):
        elo_scores = []
        for index in range(freq_frame.shape[0]):
            elo_scores.append(self.elo_fetcher.get_card_elo(freq_frame.name[index]))
        freq_frame['ELO'] = elo_scores

        self.elo_fetcher.save_cache()

        return freq_frame

    @staticmethod
    def calculate_card_weight(data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates the card weight based on the weight of the cubes in which it appears.
        """
        card_weight_series = data.groupby('name')['Cube Weight'].sum()
        data = data.merge(card_weight_series.rename('Card Weight'), left_on='name', right_index=True)
        data['Card Weight'] = np.log(data['Card Weight'])

        return data

    @staticmethod
    def get_elo_coverage_diff(row):
        return np.abs(row['Normalized Inclusion Rate'] - row['Normalized ELO'])
