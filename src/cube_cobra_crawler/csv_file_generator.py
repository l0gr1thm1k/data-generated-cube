import re

import pandas as pd

from pathlib import Path

from src.data_generated_cube.scryfall.scryfall_cache import shared_scryfall_cache


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
        for card_dict in self._expand_vouchers(list_of_card_dicts):
            rows.append(self.generate_row_from_dict(card_dict))
        df = pd.DataFrame.from_records(rows, columns=self.columns)
        file_name = re.sub(r"(\s+|/)", '_', cube_name)

        initial_file_path = Path(self.data_dir) / f"{file_name}.csv"
        cube_file_path = self.make_filepath_with_backoff(initial_file_path)
        df.to_csv(cube_file_path, index=False)

    @staticmethod
    def _expand_vouchers(card_dicts):
        # Cube Cobra "voucher" entries are bundle cards (e.g. "Urzatron") whose real
        # printings live in a voucher_cards array. They have cardID='voucher' and no
        # name field, so they must be expanded into their constituent cards before
        # downstream processing.
        for card_dict in card_dicts:
            if not isinstance(card_dict, dict):
                yield card_dict
                continue
            voucher_cards = card_dict.get('voucher_cards')
            if card_dict.get('cardID') == 'voucher' or voucher_cards:
                for inner in voucher_cards or []:
                    if isinstance(inner, dict):
                        inner.setdefault('board', card_dict.get('board', 'mainboard'))
                        yield inner
                continue
            yield card_dict

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
    def _get_field(card_dict, key):
        value = card_dict.get(key)
        if value is not None:
            return value
        details = card_dict.get('details') or {}
        if details.get(key) is not None:
            return details[key]
        scryfall = shared_scryfall_cache.get_by_id(card_dict.get('cardID', ''))
        if scryfall.get(key) is not None:
            return scryfall[key]
        faces = scryfall.get('card_faces') or []
        if faces and faces[0].get(key) is not None:
            return faces[0][key]
        raise KeyError(key)

    _COLOR_LETTER_TO_CATEGORY = {
        'W': 'White', 'U': 'Blue', 'B': 'Black', 'R': 'Red', 'G': 'Green',
    }

    @classmethod
    def get_card_name(cls, card_dict):
        try:
            return cls._get_field(card_dict, 'name')
        except KeyError:
            top_keys = list(card_dict.keys()) if isinstance(card_dict, dict) else type(card_dict).__name__
            details_keys = list((card_dict.get('details') or {}).keys()) if isinstance(card_dict, dict) else None
            card_id = card_dict.get('cardID') if isinstance(card_dict, dict) else None
            scryfall_hit = bool(shared_scryfall_cache.get_by_id(card_id or ''))
            raise KeyError(
                "Card dictionary does not have a name key. "
                f"top_keys={top_keys} details_keys={details_keys} "
                f"cardID={card_id!r} scryfall_hit={scryfall_hit} "
                f"sample={str(card_dict)[:400]}"
            )

    @classmethod
    def get_cmc(cls, card_dict):
        try:
            return cls._get_field(card_dict, 'cmc')
        except KeyError:
            raise KeyError(f"Card {cls.get_card_name(card_dict)} does not have a cmc key.")

    @classmethod
    def get_type_line(cls, card_dict):
        for key in ('type_line', 'type'):
            try:
                type_line = cls._get_field(card_dict, key)
            except KeyError:
                continue
            # Double-faced/MDFC cards return "Front // Back"; keep only the front face
            # so downstream type-palette lookups see a normal type.
            if isinstance(type_line, str) and ' // ' in type_line:
                type_line = type_line.split(' // ', 1)[0]
            return type_line
        raise KeyError(f"Card {cls.get_card_name(card_dict)} does not have a type_line key.")

    @classmethod
    def get_color_category(cls, card_dict):
        for key in ('colorCategory', 'colorcategory'):
            value = card_dict.get(key)
            if value:
                return value
            details = card_dict.get('details') or {}
            value = details.get(key)
            if value:
                return value

        type_line = ''
        try:
            type_line = cls.get_type_line(card_dict) or ''
        except KeyError:
            pass
        if 'Land' in type_line:
            return 'Land'

        colors = card_dict.get('colors')
        if not colors:
            details = card_dict.get('details') or {}
            colors = details.get('colors') or details.get('color_identity')
        if not colors:
            scryfall = shared_scryfall_cache.get_by_id(card_dict.get('cardID', ''))
            colors = scryfall.get('colors') or scryfall.get('color_identity')
            if not colors:
                faces = scryfall.get('card_faces') or []
                if faces:
                    colors = faces[0].get('colors') or faces[0].get('color_identity') or []
            colors = colors or []

        if not colors:
            return 'Colorless'
        if len(colors) == 1:
            return cls._COLOR_LETTER_TO_CATEGORY.get(colors[0], 'Colorless')
        return 'Multicolored'

    @classmethod
    def get_set_identifier(cls, card_dict):
        try:
            return cls._get_field(card_dict, 'set')
        except KeyError:
            return ''

    @classmethod
    def get_collector_number(cls, card_dict):
        try:
            return cls._get_field(card_dict, 'collector_number')
        except KeyError:
            return ''

    @classmethod
    def get_rarity(cls, card_dict):
        try:
            return cls._get_field(card_dict, 'rarity')
        except KeyError:
            return ''

    @classmethod
    def get_maybeboard(cls, card_dict):
        board = card_dict.get('board', 'mainboard')
        return board != 'mainboard'

    def make_filepath_with_backoff(self, target_file_path, backoff_level: int = 1):
        """
        Make a backup of a target file's path. We need to do this because we will often sample cubes with duplicate
        names with things like '360 vintage cube' or '360 powered cube' and we don't want to overwrite the original.

        :param target_file_path:
        :param backoff_level:
        :return:
        """
        if target_file_path.exists():
            target_file_path = Path(self.data_dir) / f"{target_file_path.stem}_{backoff_level}.csv"
            target_file_path = self.make_filepath_with_backoff(target_file_path, backoff_level + 1)

        return target_file_path
