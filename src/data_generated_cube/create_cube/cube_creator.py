from collections import defaultdict
from pathlib import Path
from typing import Union

from loguru import logger

from src.common.constants import CARD_COLOR_MAP, COLORS_SET,  RESULTS_DIRECTORY_PATH


class CubeCreator:

    def __init__(self, card_count: int, data_directory: str, card_blacklist: Union[None, list] = None):
        self.card_count = card_count
        self.data_dir = data_directory
        self.card_blacklist = card_blacklist
        self.card_count_dict = {}

    def make_cube(self, frame: pd.DataFrame) -> pd.DataFrame:
        """
        Create a cube from a dataframe of cards. Utilize the path to the cube to sample card counts per color.


        :param frame:

        :return:
        """
        card_counts = self._set_color_counts(frame, self.data_dir)
        blacklist_updated_frame = self.remove_blacklist_cards(frame)
        color_frames = self.make_colors_dict(blacklist_updated_frame, self.data_dir)

        combined_frame = pd.concat([color_frames[xx][:card_counts[xx]] for xx in color_frames])
        combined_frame = self.sort_and_reset_dataframe_index(combined_frame)
        combined_frame = combined_frame[:self.card_count]

        combined_frame.drop(columns=['Cube Weight'], inplace=True)

        printing_override = pd.read_csv(CUBE_CREATION_RESOURCES_DIRECTORY / 'manually_mapped_card_printings.csv',
                                        keep_default_na=False, dtype='str')

        merged_df = pd.merge(combined_frame, printing_override, left_on='name', right_on='Name', how='left',
                             suffixes=('', '_override'))

        merged_df['Set'] = merged_df['Set_override'].combine_first(merged_df['Set'])
        merged_df['Collector Number'] = merged_df['Collector Number_override'].combine_first(
            merged_df['Collector Number'])

        final_df = merged_df.drop(columns=['Set_override', 'Collector Number_override'])

        csv_file_name = "".join([Path(self.data_dir).name, ".csv"])
        csv_file_path = RESULTS_DIRECTORY_PATH / csv_file_name
        final_df.to_csv(csv_file_path, index=False)
        logger.info(f"Cube created at file://{csv_file_path}")

        return combined_frame

    def _set_color_counts(self, frame, directory) -> dict:
        """

        :return:
        """
        logger.info("Calculating color card counts...")
        number_of_sampled_cubes = self.get_number_of_cubes_sampled(directory)
        color_counts = {}
        for color in list(COLORS_SET):
            color_counts[color] = self.get_normalized_card_count(color, frame, number_of_sampled_cubes)

        color_counts = self.adjust_color_counts(color_counts)

        self.card_count_dict = color_counts

        return color_counts

    @staticmethod
    def get_number_of_cubes_sampled(directory_path) -> int:
        return len(list(Path(directory_path).glob('*.csv')))

    def get_normalized_card_count(self, color: str, frame: pd.DataFrame, number_of_sampled_cubes: int) -> int:
        """
        Get the number of cards of a given color to include in the cube. This is done by taking the average number of
        cards of a given color in a cube and normalizing it to the total number of cards in the cube.

        :param color:
        :param frame:
        :param number_of_sampled_cubes:
        :return:
        """
        color_subset_frame = frame[frame['Color Category'] == color]
        color_subset_frame.reset_index(inplace=True, drop=True)
        average_cards_in_cube_per_color = int(color_subset_frame.Frequency.sum() / number_of_sampled_cubes)
        normalized_percent = average_cards_in_cube_per_color / self.card_count
        normalized_card_count = int(normalized_percent * self.card_count)

        return normalized_card_count

    def adjust_color_counts(self, color_counts: dict) -> dict:
        """
        Adjust the color counts to ensure that the total number of cards in the cube is equal to the desired number of
        cards in the cube. This method only adjusts the color counts when the total number of cards is less than the
        desired number of cards due to rounding errors during the get_normalized_card_count return value.

        The order in which the colors are adjusted is manually set by me and based off of what is most played in a
        vintage cube environment.

        :param color_counts:
        :return:
        """
        while sum(color_counts.values()) < self.card_count:
            for color in ["Blue", "Colorless", "Black", "Red", "Green", "White", "Land"]:
                if sum(color_counts.values()) < self.card_count:
                    color_counts[color] += 1
                else:
                    break

        return color_counts

    def remove_blacklist_cards(self, frame: pd.DataFrame) -> pd.DataFrame:
        if not self.card_blacklist:
            return frame

        logger.info(f"Removing blacklisted cards...")
        filtered_frame = frame[~frame.name.isin(self.card_blacklist)]
        filtered_frame.reset_index(inplace=True, drop=True)

        return filtered_frame

    def make_colors_dict(self, frame: pd.DataFrame, path: str) -> dict:
        """
        Makes a dictionary of DataFrames for each color.

        :param frame: Input DataFrame containing card data.
        :param path: Directory of the cube.
        :return: Dictionary of DataFrames for each color.
        """
        color_dict = {}
        for color in list(COLORS_SET):
            color_dict[color] = self.make_color_frame(color, frame)

        return color_dict

    def make_color_frame(self, color: str, frame) -> pd.DataFrame:
        """

        :param color:
        :param frame:
        :return:
        """
        freq_frame = frame[frame['Color Category'] == color]
        freq_frame = self.sort_and_reset_dataframe_index(freq_frame)

        return freq_frame

    @staticmethod
    def count_card_frequencies(frame, color):
        card_counter = defaultdict(int)

        color_subset_frame = frame[frame['Color Category'].isin([CARD_COLOR_MAP[color]])]
        color_subset_frame.reset_index(inplace=True, drop=True)
        for card_name in color_subset_frame.name:
            card_counter[card_name] += 1

        return card_counter

    @staticmethod
    def sort_and_reset_dataframe_index(card_frequency_dataframe):
        card_frequency_dataframe = card_frequency_dataframe.sort_values(['Weighted Rank', 'Inclusion Rate', 'ELO'],
                                                                        ascending=[False, False, False])
        card_frequency_dataframe.reset_index(inplace=True, drop=True)

        return card_frequency_dataframe
