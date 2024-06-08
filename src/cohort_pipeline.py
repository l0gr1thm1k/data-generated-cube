import asyncio
from typing import Union

from common.args import process_args
from cohort_analysis.analyzer import CubeAnalyzer
from cube_cobra_crawler.crawler import CubeCobraScraper
from cube_config.cube_configuration import CubeConfig
from pipeline_object.pipeline_object import PipelineObject
from common.constants import EXAMPLE_CONFIGS_DIRECTORY_PATH


class CohortAnalysisPipeline(PipelineObject):

    @process_args
    def __init__(self, config: Union[str, CubeConfig]):
        super().__init__(config)
        self.scaper = CubeCobraScraper(self.config)
        self.analyzer = CubeAnalyzer(self.config)

    async def run(self):
        await self.scaper.get_cube_data()
        self.analyzer.analyze_cohort()


if __name__ == '__main__':
    config_path = str(EXAMPLE_CONFIGS_DIRECTORY_PATH / "cubecon2024_lists.json")
    pipeline = CohortAnalysisPipeline(config_path)
    asyncio.run(pipeline.run())
