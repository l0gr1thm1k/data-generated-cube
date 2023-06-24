from cube_cobra_crawler.crawler import CubeCobraScraper
from data_generated_cube.cube_generator import CubeGenerator

from typing import Union


class DataGeneratedCubePipeline:

    def __init__(self, data_save_path: Union[str, None] = None, card_count: Union[str, None] = None,
                 blacklist_path: Union[str, None] = None):
        self.scaper = CubeCobraScraper(data_dir=data_save_path)
        self.cube_creator = CubeGenerator(data_sources=data_save_path, card_count=card_count,
                                          blacklist_path=blacklist_path)

    async def run(self):
        await self.scaper.get_cube_data()
        self.cube_creator.generate_cube()
