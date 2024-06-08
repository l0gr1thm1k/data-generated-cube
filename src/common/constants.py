import os

from pathlib import Path

from common.common import ensure_dir_exists


CUBE_CREATION_RESOURCES_DIRECTORY = Path(__file__).resolve().parent.parent / "data_generated_cube" / "data"
PARENT_DIRECTORY = ensure_dir_exists(Path(__file__).resolve().parent.parent.parent)
ARTIFACTS_DIRECTORY = ensure_dir_exists(PARENT_DIRECTORY / "artifacts")
DATA_DIRECTORY_PATH = ensure_dir_exists(ARTIFACTS_DIRECTORY / "data")
ANALYSIS_DIRECTORY_PATH = ensure_dir_exists(ARTIFACTS_DIRECTORY / "analysis")
COHORT_ANALYSIS_DIRECTORY_PATH = ensure_dir_exists(ARTIFACTS_DIRECTORY / "cohort_analysis")
RESULTS_DIRECTORY_PATH = ensure_dir_exists(ARTIFACTS_DIRECTORY / "results")
EXAMPLE_CONFIGS_DIRECTORY_PATH = PARENT_DIRECTORY / "src" / "cube_config" / "example_configs"

COLORS_SET = {"White", "Blue", "Black", "Red", "Green", "Multicolored", "Colorless", "Land"}
CARD_COLOR_MAP = {
    "w": "White",
    "u": "Blue",
    "b": "Black",
    "r": "Red",
    "g": "Green",
    "m": "Multicolored",
    "c": "Colorless",
    "l": "Land",
    "White": "White",
    "Blue": "Blue",
    "Black": "Black",
    "Red": "Red",
    "Green": "Green",
    "Multicolored": "Multicolored",
    "Hybrid": "Multicolored",
    "Colorless": "Colorless",
    "Lands": "Land",
    "Land": "Land"
}

COLOR_PALETTE = {
    'White': "gold",
    "Blue": "dodgerblue",
    "Black": "dimgray",
    "Red": "crimson",
    "Green": "limegreen",
    "Multicolored": "mediumorchid",
    "Colorless": "tan",
    "Land": "darkgreen"
}
TYPE_PALETTE = {
    "Creature": "Crimson",
    "Artifact": "DodgerBlue",
    "Instant": "Gold",
    "Planeswalker": "LimeGreen",
    "Enchantment": "mediumorchid",
    "Sorcery": "dimgray",
    "Land": "DarkGreen",
    "Conspiracy": "DarkOrange",
    "Battle": "Pink"
}

AWS_ACCESS_KEY_ID = os.environ.get("CUBE_COBRA_AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("CUBE_COBRA_AWS_SECRET_ACCESS_KEY")

BLACKLIST_REGEX = r"""\b(white|black|blue|red|green|esper|grixis|naya|jund|bant|jeskai|temur|mardu|sultai|abzan|old school|oldschool|93|94|border|alpha|beta|antiquities|legends|pre.*modern|mono|frame|nostalgia|\sabu\s|data generated|pre[\b-]|connect the clues)\b"""
