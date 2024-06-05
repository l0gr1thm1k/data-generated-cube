import pandas as pd
import warnings
import nltk

import heapq

from pathlib import Path
from collections import defaultdict
from data_generated_cube.elo.elo_fetcher import ELOFetcher
from common.common import min_max_normalize_sklearn


try:
    from nltk.tokenize import word_tokenize, sent_tokenize
except:
    nltk.download('punkt')
    from nltk.tokenize import word_tokenize, sent_tokenize

warnings.simplefilter("ignore", category=UserWarning)


class CubeAnalyzer:

    def __init__(self):
        self.elo_fetcher = ELOFetcher()
        self.evergreen_keywords = {"Activate", "Attach", "Cast", "Counter", "Create", "Destroy", "Discard", "Exchange",
                                   "Exile", "Fight", "Mill", "Play", "Reveal", "Sacrifice", "Scry", "Search",
                                   "Shuffle", "Tap/Untap", "Defender", "Double strike", "First strike", "Flying",
                                   "Indestructible", "Lifelink", "Vigilance", "Flash", "Flying", "Hexproof", "Scry",
                                   "Deathtouch", "Menace", "Haste", "Reach", "Trample", "Equip", "Enchant",
                                   "Protection", "Ward"}
        self.triomes = {"Savai Triome", "Indatha Triome", "Ketria Triome", "Raugrin Triome", "Zagoth Triome",
                        "Raffine's Tower", "Spara's Headquarters", "Xander's Lounge", "Jetmir's Garden",
                        "Ziatora's Proving Ground"}
        self._load_all_cubes()
        self._set_unique_card_distribution()
        self.get_outlier_cards()

    def _load_all_cubes(self):
        data_dir = "/home/daniel/Code/mtg/data-generated-cube/data/CubeCon2024"
        cubes = []

        for cube_file_path in Path(data_dir).glob('*.csv'):
            data = pd.read_csv(cube_file_path)
            data['Cube ID'] = cube_file_path.stem
            cubes.append(data)

        self.all_cubes = pd.concat(cubes)
        self.all_cubes.to_csv("/home/daniel/Desktop/all_cubes.csv", index=False)

    def _set_unique_card_distribution(self):
        name_distribution = self.all_cubes.groupby(['name', 'Cube ID']).size().reset_index(name='count')
        self.unique_name_distribution = name_distribution.groupby('name').filter(lambda x: len(x) == 1)

    def analyze_cohort(self):
        data_dir = "/home/daniel/Code/mtg/data-generated-cube/data/CubeCon2024"
        cube_dicts = {}
        for cube_file_path in Path(data_dir).glob('*.csv'):
            file_name = cube_file_path.stem
            cube_dicts[file_name] = self.analyze_cube(cube_file_path)
        results = pd.DataFrame.from_dict(cube_dicts)
        results = results.T
        results = results.reset_index()
        results.rename(columns={'index': 'Cube ID'}, inplace=True)

        for column in ["Keyword Breadth", "Keyword Depth", "Keyword Balance"]:
            results[column] = min_max_normalize_sklearn(results[column].values)

        results["Oracle Text Normalized Mean Word Count"] = min_max_normalize_sklearn(results["Oracle Text Mean Word Count"].values)

        results['Cube Complexity'] = results[['Keyword Breadth', 'Keyword Depth', 'Oracle Text Normalized Mean Word Count']].sum(axis=1)
        results['Normalized Cube Complexity'] = min_max_normalize_sklearn(results['Cube Complexity'].values)
        results = results.sort_values(by='Cube Complexity', ascending=False)

        results["Cube Name"] = self.set_cube_name_hyperlinks(results["Cube ID"].values)
        results["Unique Card Count"] = self.format_unique_cards_column(results)

        column_order = ["Cube Name", "Cube Size", "Unique Card Count", "Unique Card Percentage", "Keyword Breadth",
                        "Keyword Depth", "Defining Keyword Frequency", "Oracle Text Mean Word Count",
                        "Oracle Text Normalized Mean Word Count", "Normalized Cube Complexity"]
        results = results[column_order]

        results.to_csv("results.csv", index=False)

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
                print(row["name"])
                continue

        keyword_breadth = len(keyword_counter) / cube.shape[0]
        keyword_depth = sum(keyword_counter.values()) / cube.shape[0]
        keyword_balance = keyword_breadth / keyword_depth
        most_frequent_keywords = self.get_k_most_frequent(keyword_counter, 3)
        mean_word_count = word_count / cube.shape[0]

        unique_card_count, unique_card_names = self.get_unique_card_count(cube_id)
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

    def get_unique_card_count(self, cube_id):
        names_exclusive_to_data = self.unique_name_distribution[self.unique_name_distribution['Cube ID'] == cube_id]
        unique_card_names = names_exclusive_to_data['name'].tolist()

        return len(names_exclusive_to_data), unique_card_names

    @staticmethod
    def set_cube_name_hyperlinks(cube_ids):
        cube_name_frame = pd.read_csv("/home/daniel/Code/mtg/data-generated-cube/src/cohort-analysis/cube_names_map.csv")
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

    def get_outlier_cards(self, t=2, card_blacklist=['Ornithopter']):
        non_land_cards = self.all_cubes[
            ~self.all_cubes['Type'].str.lower().str.contains('land') &
            ~self.all_cubes['name'].isin(card_blacklist)
        ]
        card_counts = non_land_cards['name'].value_counts()
        mean_occurrences = card_counts.mean()
        stdev_occurrences = card_counts.std()

        # Determine the threshold for being more than t standard deviations above the mean
        threshold = mean_occurrences + (t * stdev_occurrences)

        # Filter cards that occur more than t standard deviations above the mean
        significant_cards = card_counts[card_counts > threshold]

        # Sort these cards by frequency in descending order
        significant_cards = significant_cards.sort_values(ascending=False)[:25]

        # Save the significant cards to a CSV file
        significant_cards.to_csv("outliers.csv", header=True)  # Include header

        return significant_cards

    def make_scryfall_url(self, cube_id, card_list):
        example = """https://cubecobra.com/cube/list/RGCC?f=name%3D%22Aboleth+Spawn%22+or+name%3D%22Access+Denied%22&view=spoiler"""
        base_url = f"https://cubecobra.com/cube/list/{cube_id}?f="
        # base_url = "https://scryfall.com/search?q="
        # card_format = "name%3A%22{}"
        joiner = '%22+or+'
        url_end = "%22&view=spoiler"
        card_list = joiner.join([self.format_card_name(card_name) for card_name in card_list])

        return base_url + card_list + url_end

    @staticmethod
    def format_card_name(card_name):
        return "name%3D%22" + card_name.replace(" ", "+")


if __name__ == '__main__':
    analyzer = CubeAnalyzer()
    analyzer.analyze_cohort()
    unique_cards = ['Bribery', 'Comet, Stellar Pup', 'Library of Alexandria', "Mishra's Workshop",
                    'Seasoned Dungeoneer', 'Shallow Grave', 'True-Name Nemesis', 'Ulamog, the Infinite Gyre',
                    'Undermountain Adventurer']
    # print(analyzer.make_scryfall_url("data", unique_cards))
