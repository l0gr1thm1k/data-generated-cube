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
        self._set_data_dir(self.config.dataDirectory)
        self.cube_creator = CubeCreator(card_count=self.config.cardCount,
                                        data_directory=self.data_dir,
                                        card_blacklist=self.config.cardBlacklist)
        self.cube_merger = CuberMerger()

    def _set_data_dir(self, data_dir):
        data_dir = DATA_DIRECTORY_PATH / "cubes" / data_dir
        self.data_dir = ensure_dir_exists(data_dir)

    def generate_cube(self) -> pd.DataFrame:
        """
        Generates a cube based on the specified data directory/directories.

        If the `data_directory` attribute of the object is of type string, it assumes a single directory
        and calls the `generate_cube_from_single_source` method to generate the cube.

        If the `data_directory` attribute is not a string, it assumes multiple directories and calls
        the `generate_cubes_from_multiple_directories` method to generate the cube.
        """
        if isinstance(self.config.dataDirectory, str):
            generated_cube = self.generate_cube_from_single_source()
        else:
            generated_cube = self.generate_cubes_from_multiple_sources()

        return generated_cube

    def generate_cube_from_single_source(self) -> pd.DataFrame:
        cube_combiner_instance = CubeCombiner(self.data_dir)
        frames = cube_combiner_instance.combine_cubes_from_directory()
        cube = self.cube_creator.make_cube(frames)

        return cube

    def generate_cubes_from_multiple_sources(self) -> pd.DataFrame:
        """
        With config entry points, this method is not currently utilized as we have a single data source.
        """
        try:
            cubes = []
            frames = []
            for directory in self.data_sources:
                cube_combiner_instance = CubeCombiner(directory)
                frames.append(cube_combiner_instance.combine_cubes_from_directory())

            for frame, data_path in zip(frames, self.data_sources):
                cubes.append(self.cube_creator.make_cube(frame, data_path))

            cube = self.cube_merger.weight_and_merge_frames(cubes, self.cube_creator.card_count_dicts)
            logger.info("Finished generating cube from multiple sources.")

        except Exception as e:
            raise NotImplementedError("Multiple data sources are not currently supported.")

        return cube
