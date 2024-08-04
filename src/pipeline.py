from typing import Union

from src.common.args import process_args
from src.cube_analysis.analyzer import CubeAnalyzer
from src.cube_cobra_crawler.crawler import CubeCobraScraper
from src.cube_config.cube_configuration import CubeConfig
from src.data_generated_cube.cube_generator import CubeGenerator
from src.pipeline_object.pipeline_object import PipelineObject


class DataGeneratedCubePipeline(PipelineObject):

    @process_args
    def __init__(self, config: Union[str, CubeConfig]):
        super().__init__(config)
        self.scaper = CubeCobraScraper(self.config)
        self.cube_creator = CubeGenerator(self.config)
        self.analyzer = CubeAnalyzer(self.config)

    async def run(self):
        await self.scaper.get_cube_data()
        await self.cube_creator.generate_cube()
        self.analyzer.analyze()
