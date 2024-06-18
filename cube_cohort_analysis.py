import asyncio

from src.common.constants import EXAMPLE_CONFIGS_DIRECTORY_PATH
from src.cohort_pipeline import CohortAnalysisPipeline


def main(config_file_path):
    pipeline = CohortAnalysisPipeline(config_file_path)
    asyncio.run(pipeline.run())


if __name__ == '__main__':
    config_file = str(EXAMPLE_CONFIGS_DIRECTORY_PATH / "CubeCon2024Cohort.json")
    main(config_file)
