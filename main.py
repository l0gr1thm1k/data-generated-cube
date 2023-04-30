import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Union
from warnings import filterwarnings

import dill
import pandas as pd
import requests
from loguru import logger

filterwarnings('ignore')


class Cube:

    def __init__(self, data_directory: str, card_count, blacklist_path: Union[None, str] = None):
        self.elo_fetcher = ELO()
        self.blacklist_path = None if not blacklist_path else os.path.join(os.path.abspath('./data/blacklists'),
                                                                           blacklist_path)
        self.data_directory = data_directory
        self.card_count = card_count
        self.color_map = {'w': ['w', 'White'], 'u': ['u', 'Blue'], 'b': ['b', 'Black'], 'r': ['r', 'Red'],
                          'g': ['g', 'Green'], 'm': ['m', 'Hybrid', 'Multicolored'], 'c': ['c', 'Colorless'],
                          'l': ['l', 'Lands']}
        self.frames = self.make_combine_cubes_in_data_dir(self.data_directory)
        self.cube = self.make_cube(self.card_count)


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
                # logger.info("Frame columns: " + str(frame.columns))
                if remap_frame.Name[index] == frame.name[frame_index]:
                    frame['Color Category'][frame_index] = remap_frame['Color Category'][index]
            logger.info(f"'{remap_frame.Name[index]}' mapped to '{self.color_map[remap_frame['Color Category'][index]][-1]}'")

        # logger.info("Manually mapped card colors")

        return frame

    def implement_blacklist(self, frame):
        if not self.blacklist_path:
            return frame
        blacklist_cards = [xx.strip() for xx in open(self.blacklist_path, 'r').readlines()]
        logger.info(f"Blacklisted cards to avoid: {blacklist_cards}")
        filtered_frame = frame[~frame.name.isin(blacklist_cards)]
        filtered_frame.reset_index(inplace=True, drop=True)

        return filtered_frame

    def make_cube(self, target_cube_size):
        """

        :return:
        """
        card_counts = self.get_color_counts()
        color_frames = self.make_colors_dict()
        if not self.blacklist_path:
            color_extension = 0
        else:
            color_extension = int(len(open(self.blacklist_path, 'r').readlines()) / len(color_frames))
            logger.info(f"Extending cards per color frame by {color_extension} due to blacklist length")
        combined_frame = pd.concat([color_frames[xx][:card_counts[xx] + color_extension] for xx in color_frames])
        combined_frame = self.implement_blacklist(combined_frame)
        combined_frame.sort_values(['Frequency', 'ELO'], ascending=[False, False], inplace=True)
        #combined_frame.reset_index(drop=True, inplace=True)

        combined_frame = combined_frame[:target_cube_size]

        with open(os.path.join(os.path.abspath('.'), f'results/{os.path.split(self.data_directory)[-1]}_cards.txt'),
                  'w') as fstream:
            for name in combined_frame.name:
                fstream.write(name + '\n')

        temp = self.frames.drop_duplicates('name')
        temp.reset_index(inplace=True, drop=True)
        merged = combined_frame.merge(temp, on='name')
        merged = merged[['name', 'Frequency', 'CMC', 'Type', 'Color', 'Set', 'Rarity', 'Color Category']]
        merged.to_csv(
            os.path.join(os.path.abspath('.'), f'results/{os.path.split(self.data_directory)[-1]}_dataframe.csv'),
            index=False)

        return merged

    def get_color_counts(self) -> dict:
        """

        :return:
        """
        cubes = len(os.listdir(self.data_directory))
        color_counts = {}
        for color in self.color_map:
            color_subset_frame = self.frames[(self.frames['Color Category'].isin(self.color_map[color])) &
                                             (~self.frames['maybeboard'])]
            color_subset_frame.reset_index(inplace=True, drop=True)
            cards_of_color_per_cube_average = int(color_subset_frame.shape[0] / cubes)
            normalized_percent = cards_of_color_per_cube_average / self.card_count
            normalized_card_count = int(normalized_percent * self.card_count)
            color_counts[color] = normalized_card_count
            logger.info(f"{color_counts[color]} '{self.color_map[color][-1]}' cards in the average cube")

        while sum(color_counts.values()) < self.card_count:
            for color in ['u', 'c', 'b', 'm', 'r', 'g', 'w', 'l']:
                if sum(color_counts.values()) < self.card_count:
                    color_counts[color] += 1
                else:
                    break

        logger.info(f"{color_counts}")

        return color_counts

    def make_color_frame(self, color: str) -> pd.DataFrame:
        """

        :param color:
        :return:
        """
        card_counter = defaultdict(int)

        color_subset_frame = self.frames[self.frames['Color Category'].isin(self.color_map[color])]
        color_subset_frame.reset_index(inplace=True, drop=True)
        for card_name in color_subset_frame.name:
            card_counter[card_name] += 1
    
        color_subset_frame = color_subset_frame.drop_duplicates('name')
        color_subset_frame.reset_index(inplace=True, drop=True)
    
        frequencies = sorted(card_counter.items(), key=lambda kv: kv[1], reverse=True)
        freq_frame = pd.DataFrame(columns=['name', 'Frequency'])
        freq_frame.name = [xx[0] for xx in frequencies]
        freq_frame.Frequency = [xx[1] for xx in frequencies]

        logger.info(f"Pulling ELO information for {color} category cards")
        elos = []
        for index in range(freq_frame.shape[0]):
            elos.append(self.elo_fetcher.get_card_elo(freq_frame.name[index]))
        freq_frame['ELO'] = elos
        freq_frame = freq_frame.sort_values(['Frequency', 'ELO'], ascending=[False, False])
        freq_frame.reset_index(inplace=True, drop=True)
    
        return freq_frame
    
    def make_colors_dict(self) -> dict:
        """

        :return:
        """
        color_dict = {}
        for color in self.color_map:
            color_dict[color] = self.make_color_frame(color)

        to_pickle(self.elo_fetcher.cache, os.path.join(self.elo_fetcher.data_dir, 'elo_cache.pickle'))
    
        return color_dict


class ELO:

    def __init__(self):
        self.elo_pattern = re.compile(r'"elo".{0,10}')
        self.elo_digit_pattern = re.compile(r"\d+.\d+")
        self.data_dir = os.path.join(os.path.dirname(__file__), 'data')
        self.cache = from_pickle(os.path.join(self.data_dir, 'elo_cache.pickle'))

    def get_elo_from_id(self, card_id: str) -> float:
        url = f"https://cubecobra.com/tool/card/{card_id}?tab=1"
        response = requests.get(url)
        html_content = response.content.decode("utf-8")
        matches = self.elo_pattern.findall(html_content)
        if not matches:
            logger.info(f"Could not find any Elo data on card with ID {card_id}")
        else:
            elo_score = float(self.elo_digit_pattern.findall(matches[0])[0])
            return elo_score

    @staticmethod
    def normalize_card_name(card_name: str) -> str:
        card_name = card_name.lower()
        card_name = re.sub(r'\s+', '%20', card_name)
        card_name = card_name.replace('&', ' ')

        return card_name

    def get_card_stats_from_scryfall(self, card_name: str) -> dict:
        time.sleep(1.0)
        normalized_card_name = self.normalize_card_name(card_name)
        try:
            scryfall_get_url = f"https://api.scryfall.com/cards/named?exact={normalized_card_name}"
            response = requests.get(scryfall_get_url).json()
        except:
            logger.info(f"No card named {card_name} in the Scryfall database", )
            response = {}

        return response

    def get_card_elo_from_cube_cobra(self, card_name: str) -> float:
        normalized_card_name = self.normalize_card_name(card_name)
        scryfall_data = self.get_card_stats_from_scryfall(normalized_card_name)

        try:

            elo_score = self.get_elo_from_id(scryfall_data["id"])
            logger.info(f"Elo score for {card_name} is {elo_score}")

            return elo_score

        except KeyError:
            logger.info(f"No card with name '{card_name}' found in Scryfall data.")

            return None

    def update_card_elo(self, card_name: str):
        try:
            elo_score = self.get_card_elo_from_cube_cobra(card_name)

            self.cache[card_name] = {
                "elo": elo_score,
                "lastUpdated": datetime.now()
            }

        except KeyError as e:
            print(e)
            return


    def get_card_elo(self, card_name: str) -> float:
        cache_data = self.cache.get(card_name)
        now_timestamp = datetime.utcnow().timestamp()

        if cache_data is None or now_timestamp - cache_data["lastUpdated"].timestamp() > 7 * 24 * 60 * 60:
            self.update_card_elo(card_name)
            try:
                cache_data = self.cache[card_name]
            except KeyError:

                return -1.0

        return cache_data["elo"]


def to_pickle(data, path: str, protocol: int = 3) -> None:
    """
    pickle data to a file

    :param data: data to pickle
    :param path: path to write data to
    :param protocol: pickle protocol level to be used (python's current default is 3)
    """

    with open(path, 'wb') as file:
        dill.dump(data, file, protocol=protocol)


def from_pickle(path: str):
    """
    load data from a pickle file

    :param path: path to load
    :return: unpickled data
    """

    with open(path, 'rb') as file:
        return dill.load(file)


if __name__ == '__main__':
    vintage_dir = 'data/cubes/2023_04_30'
    cube_creator = Cube(vintage_dir, 360, blacklist_path=None)
