from typing import Union

from common.args import process_args
from cube_cobra_crawler.crawler import CubeCobraScraper
from cube_config.cube_configuration import CubeConfig
from data_generated_cube.cube_generator import CubeGenerator
from pipeline_object.pipeline_object import PipelineObject


class DataGeneratedCubePipeline(PipelineObject):

    @process_args
    def __init__(self, config: Union[str, CubeConfig], overwrite: bool = False):
        super().__init__(config)
        self.scaper = CubeCobraScraper(self.config, overwrite=overwrite)
        self.cube_creator = CubeGenerator(self.config)

    async def run(self):
        await self.scaper.get_cube_data()
        self.cube_creator.generate_cube()
