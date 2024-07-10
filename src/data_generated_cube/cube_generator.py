from typing import Union

import pandas as pd

from loguru import logger

from data_generated_cube.combine_cubes.combine_cubes import CubeCombiner
from data_generated_cube.create_cube.cube_creator import CubeCreator
from common.args import process_args
from common.common import ensure_dir_exists
from common.constants import DATA_DIRECTORY_PATH, RESULTS_DIRECTORY_PATH
from data_generated_cube.scryfall.scryfall_cache import shared_scryfall_cache
from cube_config.cube_configuration import CubeConfig
from pipeline_object.pipeline_object import PipelineObject


class CubeGenerator(PipelineObject):
    scryfall = shared_scryfall_cache

    @process_args
    def __init__(self, config: Union[str, CubeConfig]):
        super().__init__(config)
        self._set_data_dir(self.config.cubeName)

    def _set_data_dir(self, cube_name: str) -> None:
        data_dir_path = DATA_DIRECTORY_PATH / cube_name
        self.data_dir = ensure_dir_exists(data_dir_path)

    async def generate_cube(self) -> pd.DataFrame:
        """
        Generates a cube based on the specified data directory/directories.

        If the `data_directory` attribute of the object is of type string, it assumes a single directory
        and calls the `generate_cube_from_single_source` method to generate the cube.

        If the `data_directory` attribute is not a string, it assumes multiple directories and calls
        the `generate_cubes_from_multiple_directories` method to generate the cube.
        """
        if "create" not in self.config.stages:
            generated_cube = self.load_existing_cube()
        else:
            cube_combiner_instance = CubeCombiner(self.data_dir)
            frames = await cube_combiner_instance.combine_cubes_from_directory()
            self.update_card_blacklist(frames)

            cube_creator_instance = CubeCreator(card_count=self.config.get("cardCount"),
                                                data_directory=self.data_dir,
                                                card_blacklist=self.config.get("cardBlacklist"))
            generated_cube = cube_creator_instance.make_cube(frames)

        return generated_cube

    def load_existing_cube(self) -> pd.DataFrame:
        """
        Loads an existing cube from directory. If no file exists, raise an Error.

        :return:
        """
        try:
            generated_cube = pd.read_csv(str(RESULTS_DIRECTORY_PATH / self.config.cubeName) + ".csv")
            logger.info("Skipping create cube stage")

            return generated_cube

        except FileNotFoundError:
            raise FileNotFoundError("No cube found, please run create cube stage")

    def update_card_blacklist(self, frames: pd.DataFrame) -> None:
        if self.config.get("forceFoilPrinting", False):
            unique_card_names = frames['name'].unique()
            for card_name in unique_card_names:
                if not self.scryfall.has_foil_printing(card_name):
                    self.config.cardBlacklist.append(card_name)
