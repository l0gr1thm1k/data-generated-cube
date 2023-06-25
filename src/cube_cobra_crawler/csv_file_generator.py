import pandas as pd

from pathlib import Path


class CSVFileGenerator:

    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.columns = ['name', 'CMC', 'Type', 'Color Category', 'Set', 'Collector Number', 'Rarity', 'maybeboard']

    def process_cube_data(self, list_of_card_dicts, cube_name):
        """
        Process a list of card dicts into a pandas dataFrame and save.

        :param list_of_card_dicts:
        :param cube_name:
        :return:
        """
        rows = []
        for card_dict in list_of_card_dicts:
            rows.append(self.generate_row_from_dict(card_dict))
        df = pd.DataFrame.from_records(rows, columns=self.columns)
        df.to_csv(Path(self.data_dir) / f"{cube_name}.csv", index=False)

    def generate_row_from_dict(self, card_dict: dict) -> list:
        """
        Required columns are:

        * name
        * CMC
        * Type
        * Color
        * Set
        * "Collector Number"
        * Rarity
        * status
        * Finish
        * maybeboard
        * "image URL"
        * "image Back URL"
        * tags
        * Notes
        * "MTGO ID"

        :param card_dict:
        :return:
        """
        name = self.get_card_name(card_dict)
        cmc = self.get_cmc(card_dict)
        type_line = self.get_type_line(card_dict)
        color_category = self.get_color_category(card_dict)
        set_identifier = self.get_set_identifier(card_dict)
        collector_number = self.get_collector_number(card_dict)
        rarity = self.get_rarity(card_dict)
        maybeboard = self.get_maybeboard(card_dict)

        return [name, cmc, type_line, color_category, set_identifier, collector_number, rarity,
                maybeboard]

    @staticmethod
    def get_card_name(card_dict):
        try:
            return card_dict['details']['name']
        except KeyError:
            raise KeyError(f"Card dictionary does not have a name key.")

    @staticmethod
    def get_cmc(card_dict):
        try:
            return card_dict['cmc']
        except KeyError:
            try:
                return card_dict['details']['cmc']
            except KeyError:
                raise KeyError(f"Card {card_dict['details']['name']} does not have a cmc key.")

    @staticmethod
    def get_type_line(card_dict):
        try:
            type_line = card_dict['type_line']
        except KeyError:
            if 'type' in card_dict['details']:
                type_line = card_dict['details']['type']
            elif 'type_line' in card_dict['details']:
                type_line = card_dict['details']['type_line']
            else:
                raise KeyError(f"Card {card_dict['details']['name']} does not have a type_line key.")

        return type_line

    @staticmethod
    def get_color_category(card_dict):
        try:
            return card_dict['details']['colorcategory']
        except KeyError:
            raise KeyError(f"Card {card_dict['details']['name']} does not have a colorcategory key.")

    @staticmethod
    def get_set_identifier(card_dict):
        try:
            return card_dict['details']['set']
        except KeyError:
            raise KeyError(f"Card {card_dict['details']['name']} does not have a set key.")

    @staticmethod
    def get_collector_number(card_dict):
        try:
            return card_dict['details']['collector_number']
        except KeyError:
            raise KeyError(f"Card {card_dict['details']['name']} does not have a collector_number key.")

    @staticmethod
    def get_rarity(card_dict):
        try:
            return card_dict['details']['rarity']
        except KeyError:
            raise KeyError(f"Card {card_dict['details']['name']} does not have a rarity key.")

    @staticmethod
    def get_maybeboard(card_dict):
        try:
            if card_dict['board'] == 'mainboard':
                is_maybeboard = False
            else:
                is_maybeboard = True
        except KeyError:
            raise KeyError(f"Card {card_dict['details']['name']} does not have a board key.")

        return is_maybeboard
