from typing import Union

import pandas as pd

from loguru import logger

from data_generated_cube.combine_cubes.combine_cubes import CubeCombiner
from data_generated_cube.create_cube.cube_creator import CubeCreator
from data_generated_cube.create_cube.weight_and_merge import CuberMerger
from common.args import process_args
from common.common import ensure_dir_exists
from common.constants import DATA_DIRECTORY_PATH
from cube_config.cube_configuration import CubeConfig
from pipeline_object.pipeline_object import PipelineObject


class CubeGenerator(PipelineObject):

    @process_args
    def __init__(self, config: Union[str, CubeConfig]):
        super().__init__(config)
        self._set_data_directories(self.config.cubes)
        self.cube_merger = CuberMerger(self.config.cubeName)

    def _set_data_directories(self, cubes_list):
        self.data_directories = [ensure_dir_exists(DATA_DIRECTORY_PATH / cube["cubeName"]) for cube in cubes_list]

    def generate_cube(self) -> pd.DataFrame:
        """
        Generates a cube based on the specified data directory/directories.

        If the `data_directory` attribute of the object is of type string, it assumes a single directory
        and calls the `generate_cube_from_single_source` method to generate the cube.

        If the `data_directory` attribute is not a string, it assumes multiple directories and calls
        the `generate_cubes_from_multiple_directories` method to generate the cube.
        """
        if len(self.config.cubes) == 1:
            generated_cube = self.generate_cube_from_single_source(self.config.cubes[0])
        else:
            generated_cube = self.generate_cubes_from_multiple_sources()

        return generated_cube

    def generate_cube_from_single_source(self, cube_config_dict) -> pd.DataFrame:
        cube_combiner_instance = CubeCombiner(self.data_directories[0])
        cube_creator_instance = CubeCreator(card_count=cube_config_dict["cardCount"],
                                            data_directory=self.data_directories[0],
                                            card_blacklist=cube_config_dict["cardBlacklist"])
        frames = cube_combiner_instance.combine_cubes_from_directory()
        cube = cube_creator_instance.make_cube(frames, merge_frames_component=False)

        return cube

    def generate_cubes_from_multiple_sources(self) -> pd.DataFrame:
        """
        With config entry points, this method is not currently utilized as we have a single data source.
        """
        cubes = []
        card_count_dicts = []
        for cube, directory in zip(self.config.cubes, self.data_directories):
            cube_combiner_instance = CubeCombiner(directory)
            cube_creator_instance = CubeCreator(card_count=cube["cardCount"],
                                                data_directory=directory,
                                                card_blacklist=cube["cardBlacklist"])

            frame = cube_combiner_instance.combine_cubes_from_directory()
            cubes.append(cube_creator_instance.make_cube(frame, merge_frames_component=True))
            card_count_dicts.append(cube_creator_instance.card_count_dict)

        cube = self.cube_merger.weight_and_merge_frames(cubes, card_count_dicts, self.config.weights)
        logger.info("Finished generating cube from multiple sources.")

        return cube
