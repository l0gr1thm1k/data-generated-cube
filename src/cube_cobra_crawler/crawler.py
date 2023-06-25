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
    base_url = 'https://cubecobra.com/search/card%3A%22black%20lotus%22?order=pop&ascending=false'

    @process_args
    def __init__(self, config: Union[str, CubeConfig], overwrite: bool = False):
        super().__init__(config)
        self._set_data_dir(self.config.dataDirectory, overwrite=overwrite)
        self.file_generator = CSVFileGenerator(self.data_dir)

    def _set_data_dir(self, data_dir: str, overwrite: bool) -> None:
        """
        Set the data directory for the CSV file.

        :param data_dir:
        overwrite: a boolean stating whether you wish to overwrite the data in an existing data directory
        :return:
        """
        data_dir_path = DATA_DIRECTORY_PATH / "cubes" / data_dir
        self.data_dir = ensure_dir_exists(data_dir_path)

        if overwrite:
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

    async def get_cube_data(self):
        page_soup = await self.get_website_soup_object(self.base_url)
        cube_json_query = self.get_json_query(page_soup)
        links = self.get_cube_links(cube_json_query)
        tasks = []
        for link in links:
            task = asyncio.create_task(self.process_cube(link))
            tasks.append(task)

        await asyncio.gather(*tasks)

        return links

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

    def get_cube_links(self, json_query: dict):
        cube_link_list = []
        for cube in json_query['cubes']:
            category = cube['categoryOverride'],
            cube_id = self.get_cube_id(cube)
            card_count = cube['cardCount']
            last_updated = self.convert_timestamp(cube['date'])
            today = datetime.datetime.today()

            is_in_category = self.config.get('cubeCategory') in category
            acceptable_card_range = int(self.config.cardCount * .9) <= card_count <= int(self.config.cardCount * 1.1)
            updated_within_year = (today - last_updated).days < 365

            if is_in_category and acceptable_card_range and updated_within_year:
                cube_link_list.append(f'https://cubecobra.com/cube/list/{cube_id}')

        return cube_link_list

    @staticmethod
    def get_cube_id(cube: dict) -> str:
        """
        Get the cube id from the cube json object. Try/Except for shortId fetch since not all cubes have
        this property.

        :param cube: The cube json object
        :return: get back the cube's shortId if possible otherwise return the cube's id
        """
        try:
            fetched_cube_id = cube['shortId']
        except KeyError:
            fetched_cube_id = cube['id']

        return fetched_cube_id

    @staticmethod
    def convert_timestamp(timestamp: int) -> datetime.datetime:
        """
        Convert the timestamp to a datetime object

        :param timestamp: a quirky timestamp in the format of Unix epoch time, but with 3 extra digits. This
        is likely an artifact of some development done in Cube Cobra.
        """
        converted_timestamp = int(str(timestamp)[:10])
        return datetime.datetime.fromtimestamp(converted_timestamp)

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

        logger.info(f"Successfully processed cube {cube_link}")

        return cube_cards
