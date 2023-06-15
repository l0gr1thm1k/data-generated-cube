import pandas as pd

from collections import defaultdict
from loguru import logger
from pathlib import Path
from typing import Union

from data_generated_cube.common.constants import (CSV_RESULTS_DIRECTORY_PATH, TXT_RESULTS_DIRECTORY_PATH, COLORS_SET,
                                                  BLACKLIST_DIRECTORY_PATH, CARD_COLOR_MAP)
from data_generated_cube.elo.elo_fetcher import ELOFetcher


class CubeCreator:
    card_count_dicts = []

    def __init__(self, card_count, blacklist_path: Union[None, str] = None):
        self.card_count = card_count
        self._set_black_list_path(blacklist_path)
        self.elo_fetcher = ELOFetcher()

    def _set_black_list_path(self, file_path: Union[None, str]) -> None:
        if not file_path:
            self.blacklist_path = None
        else:
            self.blacklist_path = BLACKLIST_DIRECTORY_PATH / file_path

    def make_cube(self, frame: pd.DataFrame, path: str):
        """
        Create a cube from a dataframe of cards. Utilize the path to the cube to sample card counts per color.

        :param frame:
        :param path:
        :return:
        """
        card_counts = self.get_color_counts(frame, path)
        self.card_count_dicts.append(card_counts)
        blacklist_updated_frame = self.remove_blacklist_cards(frame)
        color_frames = self.make_colors_dict(blacklist_updated_frame, path)

        # TODO: Combine all frames into one massive thing and then add new columns before going to the next step

        combined_frame = pd.concat([color_frames[xx][:card_counts[xx]] for xx in color_frames])
        combined_frame.sort_values(['Inclusion Rate', 'ELO'], ascending=[False, False], inplace=True)
        combined_frame = combined_frame[:self.card_count]
        txt_file_name = "".join([Path(path).name, "_cards.txt"])
        with open(TXT_RESULTS_DIRECTORY_PATH / txt_file_name, 'w') as fstream:
            for name in combined_frame.name:
                fstream.write(name + '\n')

        temp = frame.drop_duplicates('name')
        temp.reset_index(inplace=True, drop=True)
        merged = combined_frame.merge(temp, on='name')
        merged = merged[
            ['name', 'Frequency', 'Inclusion Rate', 'ELO', 'CMC', 'Type', 'Color', 'Set', 'Rarity', 'Color Category']]

        csv_file_name = "".join([Path(path).name, "_dataframe.csv"])
        merged.to_csv(CSV_RESULTS_DIRECTORY_PATH / csv_file_name, index=False)

        logger.info(f"Cube created.", save_location=TXT_RESULTS_DIRECTORY_PATH / txt_file_name)

        return merged

    def get_color_counts(self, frame, directory) -> dict:
        """

        :return:
        """
        logger.info("Calculating color card counts...")
        number_of_sampled_cubes = self.get_number_of_cubes_sampled(directory)
        color_counts = {}
        for color in list(COLORS_SET):
            color_counts[color] = self.get_normalized_card_count(color, frame, number_of_sampled_cubes)

        color_counts = self.adjust_color_counts(color_counts)

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
        average_cards_in_cube_per_color = int(color_subset_frame.shape[0] / number_of_sampled_cubes)
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
        if not self.blacklist_path:

            return frame

        blacklist_cards = [blacklist_card_name.strip() for blacklist_card_name in
                           open(self.blacklist_path, 'r').readlines()]
        logger.info(f"Removing blacklisted cards...")
        filtered_frame = frame[~frame.name.isin(blacklist_cards)]
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
            color_dict[color] = self.make_color_frame(color, frame, path)

        return color_dict

    def make_color_frame(self, color: str, frame, data_path) -> pd.DataFrame:
        """

        :param color:
        :param frame:
        :param data_path:
        :return:
        """
        card_frequencies = self.count_card_frequencies(frame, color)
        freq_frame = self.create_frequency_dataframe(card_frequencies)
        freq_frame = self.calculate_inclusion_rate(freq_frame, self.get_number_of_cubes_sampled(data_path))
        freq_frame = self.get_elo_scores(freq_frame)
        freq_frame = self.sort_and_reset_dataframe(freq_frame)

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
    def create_frequency_dataframe(card_frequencies):
        frequencies = sorted(card_frequencies.items(), key=lambda kv: kv[1], reverse=True)
        freq_frame = pd.DataFrame(columns=['name', 'Frequency'])
        freq_frame.name = [xx[0] for xx in frequencies]
        freq_frame.Frequency = [xx[1] for xx in frequencies]

        return freq_frame

    @staticmethod
    def calculate_inclusion_rate(freq_frame, number_of_sampled_cubes):
        def calculate_card_inclusion_rate(row):
            return round(row.Frequency / number_of_sampled_cubes, 4)

        freq_frame['Inclusion Rate'] = freq_frame.apply(calculate_card_inclusion_rate, axis=1)

        return freq_frame

    def get_elo_scores(self, freq_frame):
        elo_scores = []
        for index in range(freq_frame.shape[0]):
            elo_scores.append(self.elo_fetcher.get_card_elo(freq_frame.name[index]))
        freq_frame['ELO'] = elo_scores

        return freq_frame

    @staticmethod
    def sort_and_reset_dataframe(card_frequency_dataframe):
        card_frequency_dataframe = card_frequency_dataframe.sort_values(['Inclusion Rate', 'ELO'],
                                                                        ascending=[False, False])
        card_frequency_dataframe.reset_index(inplace=True, drop=True)

        return card_frequency_dataframe
