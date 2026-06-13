import os
import re

from pathlib import Path

from src.common.common import ensure_dir_exists


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

BLACKLIST_REGEX = r"""\b(white|black|blue|red|green|esper|grixis|naya|jund|bant|jeskai|temur|mardu|sultai|abzan|old school|oldschool|93|94|border|alpha|beta|antiquities|legends|pre.*modern|mono|frame|nostalgia|\sabu\s|data generated|pre[\b-]|connect the clues|type (1|one|2|two)|the garfield cube|alternate history| simple is best|Power Vintage 360)\b"""

EVERGREEN_KEYWORDS = {
    "Activate", "Attach", "Cast", "Counter", "Create", "Deathtouch", "Defender", "Destroy", "Discard",
    "Double strike", "Enchant", "Equip", "Exchange", "Exile", "Fight", "First strike", "Flash", "Flying", "Haste",
    "Hexproof", "Indestructible", "Lifelink", "Menace", "Mill", "Play", "Protection", "Reach", "Reveal",
    "Sacrifice", "Scry", "Search", "Shuffle", "Tap/Untap", "Trample", "Vigilance", "Ward"}
INSTANT_SPEED_KEYWORDS = {'Flash', 'Ninjutsu', 'Channel', 'Reinforce', 'Bloodrush', 'Forecast'}
TRIOMES = {"Savai Triome", "Indatha Triome", "Ketria Triome", "Raugrin Triome", "Zagoth Triome", "Raffine's Tower",
           "Spara's Headquarters", "Xander's Lounge", "Jetmir's Garden", "Ziatora's Proving Ground"}


# Cohort Analysis regular expressions
ADDITIONAL_COST_PATTERN = re.compile(r"As (?:an additional cost to|you) cast this spell, (.+)",  re.IGNORECASE)
ACTIVATED_ABILITY_PATTERN = re.compile(r'^[^.]*:[^.]*', re.IGNORECASE)
COST_REDUCTION_PATTERN = r'This spell costs \{?\d+\}? less to cast.*?(?:\.|$)'
ORACLE_TEXT_FLASH_PATTERN = re.compile(r'you may cast .*? as though .*? had flash', re.IGNORECASE)
CYCLE_PATTERN = re.compile(r'when you cycle', re.IGNORECASE)
ACTIVATE_FROM_HAND_ACTION_PATTERN = re.compile(r'((exile|reveal) \S+ from your hand|discard \S+):', re.IGNORECASE)


# Components for the alternate cost regex
FLASHBACK_PATTERN = r'Flashback '
EVOKE_PATTERN = r'Evoke '
MADNESS_PATTERN = r'Madness '
MIRACLE_PATTERN = r'Miracle '
SURGE_PATTERN = r'Surge '
EMERGE_PATTERN = r'Emerge '
FORETELL_PATTERN = r'Foretell '
PROWL_PATTERN = r'Prowl '
SPECTACLE_PATTERN = r'Spectacle '
SUSPEND_PATTERN = r'Suspend '
ESCAPE_PATTERN = r'Escape—'
DISTURB_PATTERN = r'Disturb '
OVERLOAD_PATTERN = r'Overload '
MUTATE_PATTERN = r'Mutate $'
NINJUTSU_PATTERN = r'Ninjutsu '
KICKER_PATTERN = r'Kicker '
PHYREXIAN_MANA_PATTERN = r'\{[WUBRGwubrg]/P\}'
DELVE_PATTERN = r'Delve' # This and convoke don't do anything for how we calc complexity
CONVOKE_PATTERN = r'Convoke'
DASH_PATTERN = r'Dash '
AWAKEN_PATTERN = r'Awaken '
BESTOW_PATTERN = r'Bestow—'
BLITZ_PATTERN = r'Blitz '
CLEAVE_PATTERN = r'Cleave '
PROTOTYPE_PATTERN = r'Prototype '
MORPH_PATTERN = r'Morph '
MEGAMORPH_PATTERN = r'Megamorph '
DISGUISE_PATTERN = r'Disguise '
CHANNEL_PATTERN = r'Channel — [^,]+, \{[^}]+\}:'
ALTERNATE_MODE_CAST_PATTERN = r"You may (.*?) rather than pay this spell's mana cost."


# Combine all alternative cost regexes into a single pattern
ALTERNATIVE_COST_PATTERN = r'(?:' + '|'.join([
    FLASHBACK_PATTERN,
    EVOKE_PATTERN,
    MADNESS_PATTERN,
    MIRACLE_PATTERN,
    SURGE_PATTERN,
    EMERGE_PATTERN,
    FORETELL_PATTERN,
    PROWL_PATTERN,
    SPECTACLE_PATTERN,
    SUSPEND_PATTERN,
    ESCAPE_PATTERN,
    DISTURB_PATTERN,
    OVERLOAD_PATTERN,
    NINJUTSU_PATTERN,
    KICKER_PATTERN,
    DASH_PATTERN,
    AWAKEN_PATTERN,
    BESTOW_PATTERN,
    BLITZ_PATTERN,
    CLEAVE_PATTERN,
    PROTOTYPE_PATTERN,
    MORPH_PATTERN,
    MEGAMORPH_PATTERN,
    DISGUISE_PATTERN,
    CHANNEL_PATTERN,
]) + ')(.+)$'

ALTERNATIVE_COST_PATTERN = "(" + "|".join([ALTERNATE_MODE_CAST_PATTERN, ALTERNATIVE_COST_PATTERN]) + ")"
ALTERNATIVE_COST_PATTERN = re.compile(ALTERNATIVE_COST_PATTERN, re.IGNORECASE)

CUBE_COMPLEXITY_WEIGHTS = {
    'Keyword Breadth': 3.641509434,
    'Keyword Depth': 3.018867925,
    'Oracle Text Normalized Mean Word Count': 4.20754717,
    'Cube Uniqueness': 3.528301887,
    'Normalized Unique Tokens': 2.528301887,
    'Normalized Token Generators': 2.79245283,
    'Normalized Instant Speed Ratio': 2.698113208,
    'Normalized Activated Ability Ratio': 3.320754717,
    'Normalized Median Casting Cost Complexity': 2.188679245
}