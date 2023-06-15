from pathlib import Path

from data_generated_cube.common.common import ensure_dir_exists


CUBE_CREATION_RESOURCES_DIRECTORY = Path(__file__).resolve().parent.parent / "data"
PARENT_DIRECTORY = ensure_dir_exists(Path(__file__).resolve().parent.parent.parent)
DATA_DIRECTORY_PATH = ensure_dir_exists(PARENT_DIRECTORY / "data")
RESULTS_DIRECTORY_PATH = ensure_dir_exists(PARENT_DIRECTORY / "results")
TXT_RESULTS_DIRECTORY_PATH = ensure_dir_exists(RESULTS_DIRECTORY_PATH / "txt")
CSV_RESULTS_DIRECTORY_PATH = ensure_dir_exists(RESULTS_DIRECTORY_PATH / "csv")
MERGED_RESULTS_DIRECTORY_PATH = ensure_dir_exists(RESULTS_DIRECTORY_PATH / "merged")
BLACKLIST_DIRECTORY_PATH = ensure_dir_exists(DATA_DIRECTORY_PATH / "blacklists")

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
