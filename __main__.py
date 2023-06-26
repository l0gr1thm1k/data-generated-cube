import asyncio

from src.common.constants import PARENT_DIRECTORY
from src.pipeline import DataGeneratedCubePipeline


def main(config_file_path):
    pipeline = DataGeneratedCubePipeline(config_file_path)
    asyncio.run(pipeline.run())


if __name__ == '__main__':
    example_config_directory = PARENT_DIRECTORY / "src" / "cube_config" / "example_configs"
    config_file = str(example_config_directory / "2023Q3DataGeneratedVintageCube.json")
    main(config_file)
