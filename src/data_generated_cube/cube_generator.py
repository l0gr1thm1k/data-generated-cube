from typing import List, Union

import pandas as pd

from loguru import logger

from data_generated_cube.combine_cubes.combine_cubes import CubeCombiner
from data_generated_cube.create_cube.cube_creator import CubeCreator
from data_generated_cube.create_cube.weight_and_merge import CuberMerger


class CubeGenerator:

    def __init__(self, data_sources: Union[str, List[str]], card_count, blacklist_path: Union[None, str] = None):
        self.card_count = card_count
        self.data_sources = data_sources
        self.cube_creator = CubeCreator(card_count=self.card_count,
                                        blacklist_path=blacklist_path)
        self.cube_merger = CuberMerger()

    def generate_cube(self) -> pd.DataFrame:
        """
        Generates a cube based on the specified data directory/directories.

        If the `data_directory` attribute of the object is of type string, it assumes a single directory
        and calls the `generate_cube_from_single_source` method to generate the cube.

        If the `data_directory` attribute is not a string, it assumes multiple directories and calls
        the `generate_cubes_from_multiple_directories` method to generate the cube.
        """
        if isinstance(self.data_sources, str):
            generated_cube = self.generate_cube_from_single_source()
        else:
            generated_cube = self.generate_cubes_from_multiple_sources()

        return generated_cube

    def generate_cube_from_single_source(self) -> pd.DataFrame:
        cube_combiner_instance = CubeCombiner(self.data_sources)
        frames = cube_combiner_instance.combine_cubes_from_directory()
        cube = self.cube_creator.make_cube(frames, self.data_sources)

        return cube

    def generate_cubes_from_multiple_sources(self) -> pd.DataFrame:
        cubes = []
        frames = []
        for directory in self.data_sources:
            cube_combiner_instance = CubeCombiner(directory)
            frames.append(cube_combiner_instance.combine_cubes_from_directory())

        for frame, data_path in zip(frames, self.data_sources):
            cubes.append(self.cube_creator.make_cube(frame, data_path))

        cube = self.cube_merger.weight_and_merge_frames(cubes, self.cube_creator.card_count_dicts)
        logger.info("Finished generating cube from multiple sources.")

        return cube
