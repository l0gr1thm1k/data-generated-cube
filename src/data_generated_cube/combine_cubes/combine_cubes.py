import os
import pandas as pd

from loguru import logger
from pathlib import Path

from data_generated_cube.common.constants import CARD_COLOR_MAP, CUBE_CREATION_RESOURCES_DIRECTORY


class CubeCombiner:
    manual_card_color_mapping = pd.read_csv(CUBE_CREATION_RESOURCES_DIRECTORY / 'manually_mapped_color_cards.csv')

    def __init__(self):
        pass

    def combine_cubes_from_directory(self, data_dir: str) -> pd.DataFrame:
        """
        Combines cubes from a specified directory into a single DataFrame. Additionally, perform cleaning and filtering.

        :param data_dir: Directory path containing the cube files.
        :return: Combined DataFrame.

        """
        chunks = []
        for cube_file_path in Path(data_dir).glob('*.csv'):
            chunk = self.process_cube_file(cube_file_path)
            if chunk is not None:
                chunks.append(chunk)

        if chunks:
            logger.info(f"Sampling {len(chunks)} cubes...")
            concatted_frame = pd.concat(chunks, ignore_index=True)
        else:
            logger.debug("No cubes found in the specified directory...", directory=data_dir)
            concatted_frame = pd.DataFrame()

        return concatted_frame

    def process_cube_file(self, file_path: str) -> pd.DataFrame:
        try:

            chunk = pd.read_csv(file_path)
            chunk = self.remove_maybeboard_cards(chunk)
            chunk = self.manually_map_card_colors(chunk)
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
