import os
from collections import defaultdict
from typing import List, Union
from warnings import filterwarnings

import pandas as pd
from loguru import logger

filterwarnings('ignore')


class Cube:

    def __init__(self, data_directories: List[str], card_count: int, blacklist_path: Union[None, str] = None):
        self.blacklist_path = None if not blacklist_path else os.path.join(os.path.abspath('./data/blacklists'),
                                                                           blacklist_path)
        self.data_directories = data_directories
        self.card_count = card_count

        self.color_map = {'w': ['w', 'White'], 'u': ['u', 'Blue'], 'b': ['b', 'Black'], 'r': ['r', 'Red'],
                          'g': ['g', 'Green'], 'm': ['m', 'Hybrid', 'Multicolored'], 'c': ['c', 'Colorless'],
                          'l': ['l', 'Lands']}
        self.frames = [self.make_combine_cubes_in_data_dir(directory) for directory in self.data_directories]

        self.cubes = []
        for frame, data_path in zip(self.frames, self.data_directories):
            self.cubes.append(self.make_cube(frame, data_path, self.card_count))

        self.cube = self.weight_and_merge_frames(self.cubes)

    def make_combine_cubes_in_data_dir(self, data_dir: str) -> pd.DataFrame:
        """

        :param data_dir:
        :return:
        """
        cubes = os.listdir(data_dir)
        chunks = []
        for cube_name in cubes:
            try:
                chunk = pd.read_csv(os.path.join(data_dir, cube_name))
                chunks.append(chunk)
            except:
                logger.info(f"Error parsing cube {cube_name}")
        frames = pd.concat(chunks)
        logger.info(f"Concatenating {len(chunks)} cubes from source {data_dir}")
        frames.reset_index(inplace=True, drop=True)

        frames = self.manually_map_card_colors(frames)
        logger.info(f"{frames.shape[0]} cards in combined cubes")

        return frames

    def manually_map_card_colors(self, frame: pd.DataFrame) -> pd.DataFrame:
        """

        :param frame:
        :return:
        """
        remap_frame = pd.read_csv(os.path.join(os.path.abspath('.'), "data/manually_mapped_color_cards.csv"))
        for index in range(remap_frame.shape[0]):
            for frame_index in range(frame.shape[0]):
                if remap_frame.Name[index] == frame.Name[frame_index]:
                    frame['Color Category'][frame_index] = remap_frame['Color Category'][index]
            logger.info(
                f"'{remap_frame.Name[index]}' mapped to '{self.color_map[remap_frame['Color Category'][index]][-1]}'")

        return frame

    def implement_blacklist(self, frame):
        if not self.blacklist_path:
            return frame
        blacklist_cards = [xx.strip() for xx in open(self.blacklist_path, 'r').readlines()]
        logger.info(f"Blacklisted cards to avoid: {blacklist_cards}")
        filtered_frame = frame[~frame.Name.isin(blacklist_cards)]
        filtered_frame.reset_index(inplace=True, drop=True)

        return filtered_frame

    def make_color_frame(self, color: str, frame, data_path) -> pd.DataFrame:
        """

        :param color:
        :param frame:
        :param data_path:
        :return:
        """
        card_counter = defaultdict(int)

        color_subset_frame = frame[frame['Color Category'].isin(self.color_map[color])]
        color_subset_frame.reset_index(inplace=True, drop=True)
        for card_name in color_subset_frame.Name:
            card_counter[card_name] += 1

        color_subset_frame = color_subset_frame.drop_duplicates('Name')
        color_subset_frame.reset_index(inplace=True, drop=True)

        frequencies = sorted(card_counter.items(), key=lambda kv: kv[1], reverse=True)
        freq_frame = pd.DataFrame(columns=['Name', 'Frequency'])
        freq_frame.Name = [xx[0] for xx in frequencies]
        freq_frame.Frequency = [xx[1] for xx in frequencies]

        number_of_cubes = len(os.listdir(data_path))

        def calculate_card_coverage(row):
            return round(row.Frequency / number_of_cubes, 4)

        freq_frame['Coverage'] = freq_frame.apply(calculate_card_coverage, axis=1)

        return freq_frame

    def make_cube(self, frame, path, target_cube_size):
        """

        :return:
        """
        card_counts = self.get_color_counts(frame, path)
        color_frames = self.make_colors_dict(frame, path)
        if not self.blacklist_path:
            color_extension = 0
        else:
            color_extension = int(len(open(self.blacklist_path, 'r').readlines()) / len(color_frames))
            logger.info(f"Extending cards per color frame by {color_extension} due to blacklist length")
        combined_frame = pd.concat([color_frames[xx][:card_counts[xx] + color_extension] for xx in color_frames])
        combined_frame = self.implement_blacklist(combined_frame)
        combined_frame.sort_values(['Coverage', 'Name'], ascending=[False, True], inplace=True)
        combined_frame.reset_index(drop=True, inplace=True)

        combined_frame = combined_frame[:target_cube_size]

        with open(os.path.join(os.path.abspath('.'), f'results/{os.path.split(path)[-1]}_cards.txt'),
                  'w') as fstream:
            for name in combined_frame.Name:
                fstream.write(name + '\n')

        temp = frame.drop_duplicates('Name')
        temp.reset_index(inplace=True, drop=True)
        merged = combined_frame.merge(temp, on='Name')
        merged = merged[['Name', 'Coverage', 'CMC', 'Type', 'Color', 'Set', 'Rarity', 'Color Category']]
        merged.to_csv(
            os.path.join(os.path.abspath('.'), f'results/{os.path.split(path)[-1]}_dataframe.csv'),
            index=False)

        return merged

    def get_color_counts(self, frame, directory) -> dict:
        """

        :return:
        """
        cubes = len(os.listdir(directory))
        color_counts = {}
        for color in self.color_map:
            color_subset_frame = frame[frame['Color Category'].isin(self.color_map[color])]
            color_subset_frame.reset_index(inplace=True, drop=True)
            color_counts[color] = int(color_subset_frame.shape[0] / cubes)
            logger.info(f"{color_counts[color]} '{self.color_map[color][-1]}' cards in the average cube")

        return color_counts

    def make_colors_dict(self, frame, path) -> dict:
        """

        :return:
        """
        color_dict = {}
        for color in self.color_map:
            color_dict[color] = self.make_color_frame(color, frame, path)

        return color_dict

    def weight_and_merge_frames(self, frame_list):
        unique_cards = []
        for frame in frame_list:
            unique_cards.extend(frame.Name.tolist())
        unique_cards = set(unique_cards)

        columns = frame_list[0].columns

        rows = []
        for card in unique_cards:
            card_occurrences = [frame[frame.Name == card] for frame in frame_list]
            weights = 0
            occurrence = None
            for occurrence_frame in card_occurrences:
                try:
                    occurrence = occurrence_frame.iloc[0]
                    weights += occurrence.Coverage
                except:
                    pass
                    # print(f"Had an issue finding a frame for {card}")
            occurrence.Coverage = round(weights / len(frame_list), 4)
            rows.append(occurrence.tolist())

        weighted_frame = pd.DataFrame(columns=columns, data=rows)
        weighted_frame.sort_values(['Coverage', 'Name'], ascending=[False, True], inplace=True)
        weighted_frame.reset_index(inplace=True, drop=True)

        weighted_frame = weighted_frame[:self.card_count]

        with open('/home/daniel/Desktop/weighted_frame_cards.txt', 'w') as fstream:
            for name in weighted_frame.Name:
                fstream.write(name + '\n')

        # temp = weighted_frame.drop_duplicates('Name')
        # temp.reset_index(inplace=True, drop=True)
        # merged = weighted_frame.merge(temp, on='Name')
        # merged = merged[['Name', 'Coverage', 'CMC', 'Type', 'Color', 'Set', 'Rarity', 'Color Category']]
        weighted_frame.to_csv('/home/daniel/Desktop/180_weighted_frame_dataframe.csv', index=False)

        return weighted_frame


if __name__ == '__main__':
    cubes = ['data/cubes/2023Q1vintage', 'data/cubes/boop']
    cube_creator = Cube(cubes,  360, blacklist_path=None)

