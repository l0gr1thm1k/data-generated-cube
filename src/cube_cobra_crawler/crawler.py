import aiohttp
import asyncio
import datetime
import json
import re

from bs4 import BeautifulSoup
from loguru import logger
from pathlib import Path
from typing import Union

from common.args import process_args
from common.common import ensure_dir_exists
from common.constants import DATA_DIRECTORY_PATH
from cube_cobra_crawler.csv_file_generator import CSVFileGenerator
from cube_config.cube_configuration import CubeConfig
from pipeline_object.pipeline_object import PipelineObject


class CubeCobraScraper(PipelineObject):

    @process_args
    def __init__(self, config: Union[str, CubeConfig]):
        super().__init__(config)
        self._set_data_dir(self.config.cubeName)
        self.file_generator = CSVFileGenerator(self.data_dir)

    def _set_data_dir(self, data_dir: str) -> None:
        """
        Set the data directory for the CSV file.

        :param data_dir:
        :return:
        """
        data_dir_path = DATA_DIRECTORY_PATH / "cubes" / data_dir
        self.data_dir = ensure_dir_exists(data_dir_path)

        if self.config.overwrite:
            self._clear_directory(self.data_dir)

    @staticmethod
    def _clear_directory(directory_path: str) -> None:
        """
        Clear the contents of a directory.

        :param directory_path:
        :return:
        """
        directory = Path(directory_path)
        for file_path in directory.iterdir():
            if file_path.is_file():
                file_path.unlink()

    async def get_cube_data(self) -> None:
        tasks = []
        for cube_id in self.config.cubeIds:
            task = asyncio.create_task(self.process_cube(f"https://cubecobra.com/cube/list/{cube_id}"))
            tasks.append(task)

        await asyncio.gather(*tasks)

    async def process_cube(self, cube_link):
        cube_soup_object = await self.get_website_soup_object(cube_link)
        cube_json_object = self.get_json_query(cube_soup_object)

        last_updated = self.convert_timestamp(cube_json_object['cube']['date'])
        today = datetime.datetime.today()
        if (today - last_updated).days > 365:

            return

        cube_name = cube_json_object['cube']['name']
        cube_cards = cube_json_object['cards']['mainboard']

        self.file_generator.process_cube_data(cube_cards, cube_name)

        logger.info(f"Successfully processed cube {cube_link.replace('/list/', '/overview/')}")

        return cube_cards

    @staticmethod
    async def get_website_soup_object(target_url: str):
        """
        Get the soup object for a given url
        :param target_url:
        :return:
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(target_url) as response:
                content = await response.read()

        return BeautifulSoup(content, 'html.parser')

    @staticmethod
    def get_json_query(soup_object):
        script_tag = soup_object.find('script', type="text/javascript", string=lambda text: text and 'lotus' in text)
        raw = script_tag.text
        match = re.findall(r"\{.*", raw)[0].rstrip(';')

        return json.loads(match)

    @staticmethod
    def convert_timestamp(timestamp: int) -> datetime.datetime:
        """
        Convert the timestamp to a datetime object

        :param timestamp: a quirky timestamp in the format of Unix epoch time, but with 3 extra digits. This
        is likely an artifact of some development done in Cube Cobra.
        """
        converted_timestamp = int(str(timestamp)[:10])
        return datetime.datetime.fromtimestamp(converted_timestamp)


