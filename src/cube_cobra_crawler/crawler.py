import aiohttp
import asyncio
import boto3
import datetime
import json
import re

import numpy as np
from bs4 import BeautifulSoup
from loguru import logger
from pathlib import Path
from typing import Union

from common.args import process_args
from common.common import ensure_dir_exists
from common.constants import DATA_DIRECTORY_PATH, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BLACKLIST_REGEX
from cube_cobra_crawler.csv_file_generator import CSVFileGenerator
from cube_cobra_crawler.rss_feed_crawler import RSSFeedParser
from cube_config.cube_configuration import CubeConfig
from pipeline_object.pipeline_object import PipelineObject


class CubeCobraScraper(PipelineObject):

    @process_args
    def __init__(self, config: Union[str, CubeConfig]):
        super().__init__(config)
        self._set_data_dir(self.config.cubeName)
        self.file_generator = CSVFileGenerator(self.data_dir)
        self.feed_parser = RSSFeedParser()
        self.cube_weights = {}

    def _set_data_dir(self, data_dir: str) -> None:
        """
        Set the data directory for the CSV file.

        :param data_dir:
        :return:
        """
        data_dir_path = DATA_DIRECTORY_PATH / data_dir
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
        if "scrape" in self.config.stages:

            if self.config.useCubeCobraBucket:
                await self.update_cube_id_list()

            tasks = []
            lock = asyncio.Lock()
            for cube_id in self.config.cubeIds:
                task = asyncio.create_task(self.process_cube(cube_id, lock))
                tasks.append(task)
            await asyncio.gather(*tasks)

            with open(self.data_dir / "cube_weights.json", "w") as f:
                json.dump(self.cube_weights, f)

        else:
            logger.info("Skipping scrape data stage")

    async def update_cube_id_list(self) -> None:
        logger.info("Fetching Cube Cobra AWS Bucket Data")
        bucket_ids = self.fetch_cube_ids()
        self.config.cubeIds = list(set(self.config.cubeIds + bucket_ids))

    def fetch_cube_ids(self):
        download_path = str(Path(__file__).parent.parent / "data_generated_cube" / "data" / "aws_bucket_data.json")

        self.download_file(bucket_name="cubecobra", object_key="cubes.json", download_path=download_path)
        with open(download_path) as fstream:
            data = json.load(fstream)
        blacklist_regex = re.compile(BLACKLIST_REGEX, re.IGNORECASE)
        ids = []
        for cube in data:
            if 3012 in cube['cards'] and 939 in cube['cards'] and 15981 in cube['cards'] and (
                    self.config.cardCount * .9 <= len(cube['cards']) <= self.config.cardCount * 1.1) and len(cube["following"]) >= 1:
                if not blacklist_regex.search(cube['name']):
                    ids.append(cube['id'])
        return ids

    @staticmethod
    def download_file(bucket_name, object_key, download_path):
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        try:
            s3_client.download_file(bucket_name, object_key, download_path)
        except Exception as e:
            logger.info(f"An error occurred while downloading the file: {e}")

    async def process_cube(self, cube_identifier: str, lock) -> None:
        cube_overview_link = f"https://cubecobra.com/cube/overview/{cube_identifier}"
        cube_list_link = f"https://cubecobra.com/cube/list/{cube_identifier}"
        cube_soup_object = await self.get_website_soup_object(cube_list_link)

        try:
            cube_json_object = self.get_json_query(cube_soup_object)
        except AttributeError:
            logger.warning(f"Failed to process cube {cube_overview_link}")
            return

        last_updated = await self.feed_parser.get_most_recent_update_date(cube_identifier)
        today = datetime.datetime.today()

        if (today - last_updated).days <= 365:
            cube_weight = await self.get_cube_weight(cube_json_object, cube_identifier)
            async with lock:
                self.cube_weights[cube_identifier] = cube_weight

            cube_cards = cube_json_object['cards']['mainboard']
            self.file_generator.process_cube_data(cube_cards, cube_identifier)
            logger.info(f"Successfully processed cube {cube_overview_link}")

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

    async def get_cube_weight(self, cube_json: dict, identifier) -> float:
        cube_follower_weight = self.get_cube_follower_weight(cube_json)
        cube_update_weight = await self.feed_parser.calculate_update_weight(identifier)

        return round(cube_follower_weight + cube_update_weight, 4)

    def get_cube_follower_weight(self, cube_json_object: dict) -> float:
        follower_count = self.get_follower_count(cube_json_object)
        if follower_count < 1:
            follower_count = 1

        return np.log(follower_count) + 1

    @staticmethod
    def get_follower_count(cube_json: dict) -> int:
        return len(cube_json['cube']['following'])
