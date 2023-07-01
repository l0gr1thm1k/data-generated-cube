import asyncio

from src.common.constants import EXAMPLE_CONFIGS_DIRECTORY_PATH
from src.pipeline import DataGeneratedCubePipeline


def main(config_file_path):
    pipeline = DataGeneratedCubePipeline(config_file_path)
    asyncio.run(pipeline.run())


if __name__ == '__main__':
    config_file = str(EXAMPLE_CONFIGS_DIRECTORY_PATH / "example_config_single_source.json")
    main(config_file)
