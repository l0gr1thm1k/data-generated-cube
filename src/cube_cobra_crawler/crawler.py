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

from src.common.args import process_args
from src.common.common import ensure_dir_exists
from src.common.constants import DATA_DIRECTORY_PATH, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BLACKLIST_REGEX,\
    COHORT_ANALYSIS_DIRECTORY_PATH
from src.cube_cobra_crawler.csv_file_generator import CSVFileGenerator
from src.cube_cobra_crawler.rss_feed_crawler import RSSFeedParser
from src.cube_config.cube_configuration import CubeConfig
from src.pipeline_object.pipeline_object import PipelineObject


class CubeCobraScraper(PipelineObject):

    @process_args
    def __init__(self, config: Union[str, CubeConfig]):
        super().__init__(config)
        self._set_data_dir(self.config.cubeName)
        self.file_generator = CSVFileGenerator(self.data_dir)
        self.feed_parser = RSSFeedParser()
        self.cube_weights = {}
        self.power_map = {"Black Lotus": "5089ec1a-f881-4d55-af14-5d996171203b",
                          "Mox Pearl": "824597b8-c89a-47ec-8526-7efc6e24ef0e",
                          "Mox Sapphire": "d5ed1233-df87-4b90-8918-13922ec95249",
                          "Mox Jet": "0677f49e-f8bf-4349-af52-2ccde9287c2e",
                          "Mox Ruby": "ed85fa82-e4fa-434b-92a8-36b6075708d1",
                          "Mox Emerald": "376ee366-e082-402f-b4db-6592fcfcacd2",
                          "Ancestral Recall": "550c74d4-1fcb-406a-b02a-639a760a4380",
                          "Time Walk": "d0209d3f-3f7e-4fd5-bce5-10bce6f29c86",
                          "Mana Crypt": "2c63e4e1-89d2-4bc6-a232-94e75c4b1c8a",
                          "Sol Ring": "6ad8011d-3471-4369-9d68-b264cc027487",
                          }

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

            if self.config.get("cohortAnalysis", False):
                self.setup_cohort_analysis_directory()

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

    @staticmethod
    def extract_date_from_aws_cache_filename(filename: str) -> datetime.datetime:
        """
        Extract date from AWS cache filename pattern: aws_bucket_data-YYYYMMDD.json

        :param filename: Filename to extract date from
        :return: datetime object or None if no date found
        """
        match = re.search(r'(\d{8})', filename)
        if match:
            date_str = match.group(1)
            return datetime.datetime.strptime(date_str, "%Y%m%d")
        return None

    @classmethod
    def get_most_recent_aws_cache(cls, data_dir: Path, prefix: str) -> tuple:
        """
        Get the most recent AWS cache file for a given prefix.

        :param data_dir: Directory containing cache files
        :param prefix: Prefix of cache files (e.g., 'aws_bucket_data', 'indexToOracleMap')
        :return: Tuple of (filename, datetime) or (None, None) if no cache found
        """
        caches = list(data_dir.glob(f"{prefix}-*.json"))
        if not caches:
            return None, None

        max_date = datetime.datetime(1970, 1, 1)
        max_cache = None
        for cache in caches:
            date = cls.extract_date_from_aws_cache_filename(cache.name)
            if date and date > max_date:
                max_date = date
                max_cache = cache

        return max_cache, max_date

    @staticmethod
    def remove_old_aws_caches(data_dir: Path, prefix: str, keep_filename: str) -> None:
        """
        Remove old AWS cache files, keeping only the most recent one.

        :param data_dir: Directory containing cache files
        :param prefix: Prefix of cache files
        :param keep_filename: Filename to keep (all others will be deleted)
        """
        caches = list(data_dir.glob(f"{prefix}-*.json"))
        for cache in caches:
            if cache.name != keep_filename:
                cache.unlink()
                logger.info(f"Removed old cache file: {cache.name}")

    def fetch_cube_ids(self):
        data_dir = Path(__file__).parent.parent / "data_generated_cube" / "data"
        prefix = "aws_bucket_data"

        # Check for existing cache
        most_recent_cache, cache_date = self.get_most_recent_aws_cache(data_dir, prefix)

        should_download = True
        if most_recent_cache and cache_date:
            hours_since_cache = (datetime.datetime.now() - cache_date).total_seconds() / 3600
            if hours_since_cache < 24:
                should_download = False
                download_path = most_recent_cache
                logger.info(f"Using cached AWS bucket data from {cache_date.strftime('%Y-%m-%d')} ({hours_since_cache:.1f} hours old)")
            else:
                logger.info(f"AWS bucket data is {hours_since_cache:.1f} hours old, re-downloading")

        if should_download:
            logger.info("Downloading fresh AWS bucket data from S3")
            download_path_template = str(data_dir / f"{prefix}-{{datestamp}}.json")
            download_path = self.download_file(
                bucket_name="cubecobra",
                object_key="cubes.json",
                download_path=download_path_template
            )
            download_path = Path(download_path)

            # Clean up old caches
            self.remove_old_aws_caches(data_dir, prefix, download_path.name)

        with open(download_path) as fstream:
            data = json.load(fstream)
        category = self.config.get('cubeCategory', '').lower()
        if category == 'vintage':
            ids = self.fetch_vintage_ids(data)
        elif category == 'pioneer':
            ids = self.fetch_pioneer_ids(data)
        elif category == 'pauper':
            ids = self.fetch_pauper_ids(data)
        else:
            ids = []

        return ids

    @staticmethod
    def download_file(bucket_name, object_key, download_path):
        """
        Download a file from S3 bucket. If download_path contains a datestamp placeholder,
        it will be replaced with the current date in YYYYMMDD format.

        :param bucket_name: S3 bucket name
        :param object_key: S3 object key
        :param download_path: Local path to save file (can include {datestamp} placeholder)
        :return: Actual download path used
        """
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )

        # Replace datestamp placeholder with current date
        if '{datestamp}' in download_path:
            datestamp = datetime.datetime.now().strftime('%Y%m%d')
            download_path = download_path.replace('{datestamp}', datestamp)

        try:
            s3_client.download_file(bucket_name, object_key, download_path)
            logger.info(f"Successfully downloaded {object_key} to {download_path}")
            return download_path
        except Exception as e:
            logger.error(f"An error occurred while downloading the file: {e}")
            raise

    def fetch_vintage_ids(self, data_obj: dict) -> list:
        """
        Get a list of cube ids satisfying the following conditions:

        * includes all cards in a list of powered cards
        * Not matching a vintage cube list blacklist regex
        * card count is within +-10% of class config card count
        * Has at minimum 1 follower

        :param data_obj: a json dictionary like object.
        :return ids: a list of string ids.
        """
        blacklist_regex = re.compile(BLACKLIST_REGEX, re.IGNORECASE)
        ids = []
        oracle_id_mapping = self.create_oracle_id_mapping()
        power_card_indices = {oracle_id_mapping[oracle_id] for oracle_id in self.power_map.values()}

        for cube in data_obj:
            if power_card_indices.issubset(set(cube['cards'])) and \
                    (self.config.cardCount * .9 <= len(cube['cards']) <= self.config.cardCount * 1.1) \
                    and len(cube["following"]) >= 1:
                if not blacklist_regex.search(cube['name']):
                    ids.append(cube['id'])

        return ids

    def create_oracle_id_mapping(self) -> dict:
        data_dir = Path(__file__).parent.parent / "data_generated_cube" / "data"
        prefix = "indexToOracleMap"

        # Check for existing cache
        most_recent_cache, cache_date = self.get_most_recent_aws_cache(data_dir, prefix)

        should_download = True
        if most_recent_cache and cache_date:
            hours_since_cache = (datetime.datetime.now() - cache_date).total_seconds() / 3600
            if hours_since_cache < 24:
                should_download = False
                download_path = most_recent_cache
                logger.info(f"Using cached oracle ID mapping from {cache_date.strftime('%Y-%m-%d')} ({hours_since_cache:.1f} hours old)")
            else:
                logger.info(f"Oracle ID mapping is {hours_since_cache:.1f} hours old, re-downloading")

        if should_download:
            logger.info("Downloading fresh oracle ID mapping from S3")
            download_path_template = str(data_dir / f"{prefix}-{{datestamp}}.json")
            download_path = self.download_file(
                bucket_name="cubecobra",
                object_key="indexToOracleMap.json",
                download_path=download_path_template
            )
            download_path = Path(download_path)

            # Clean up old caches
            self.remove_old_aws_caches(data_dir, prefix, download_path.name)

        with open(download_path) as fstream:
            mapping = json.load(fstream)
            return {v: int(k) for k, v in mapping.items()}

    def fetch_pioneer_ids(self, data_obj: dict) -> list:
        """
         Get a list of cube ids satisfying the following conditions.

         * cube name contains the phrase `pioneer`
         * card count is within +-10% of class config card count
         * Has at minimum 1 follower

        :param data_obj: a json dictionary like object.
        :return ids: a list of string ids.
        """
        pioneer_regex = re.compile("pioneer", re.IGNORECASE)
        ids = []

        for cube in data_obj:
            if (self.config.cardCount * .9 <= len(cube['cards']) <= self.config.cardCount * 1.1) \
                    and len(cube["following"]) >= 1 and pioneer_regex.search(cube['name']):
                ids.append(cube['id'])

        return ids

    def fetch_pauper_ids(self, data_obj: dict) -> list:
        """

        :param data_obj:
        :return:
        """
        pauper_regex = re.compile("pauper", re.IGNORECASE)
        ids = []

        for cube in data_obj:
            if (self.config.cardCount * .9 <= len(cube['cards']) <= self.config.cardCount * 1.1) \
                    and len(cube["following"]) >= 1 and pauper_regex.search(cube['name']):
                ids.append(cube['id'])

        return ids

    def setup_cohort_analysis_directory(self) -> None:
        dir_path = COHORT_ANALYSIS_DIRECTORY_PATH / self.config.cubeName
        ensure_dir_exists(dir_path)
        if self.config.overwrite:
            self._clear_directory(dir_path)
        cube_name_map_file_path = dir_path / "cube_names_map.csv"
        with open(cube_name_map_file_path, 'w') as fstream:
            fstream.write("Cube ID,Cube Name")

    async def process_cube(self, cube_identifier: str, lock) -> None:
        cube_overview_link = f"https://cubecobra.com/cube/overview/{cube_identifier}"
        cube_list_link = f"https://cubecobra.com/cube/list/{cube_identifier}"
        cube_soup_object = await self.get_website_soup_object(cube_list_link)

        try:
            cube_json_object = self.get_json_query(cube_soup_object)
            cube_name = cube_json_object['cube']['name']
            cube_name = '"' + cube_name + '"' if "," in cube_name else cube_name
            if self.config.get("cohortAnalysis", False):
                file_path = COHORT_ANALYSIS_DIRECTORY_PATH / self.config.cubeName / "cube_names_map.csv"
                async with lock:
                    with open(file_path, "a") as fstream:
                        fstream.write(f"\n{cube_identifier},{cube_name}")

        except AttributeError:
            logger.warning(f"Failed to process cube {cube_overview_link}")
            return

        last_updated = await self.feed_parser.get_most_recent_update_date(cube_identifier)
        today = datetime.datetime.today()

        if (today - last_updated).days <= self.config.recentUpdatesThreshold or self.config.get("cohortAnalysis", False):
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
        """
        Parse a webpage soup object for a text/javascript script object that contains the cube data

        :param soup_object:
        :return:
        """
        script_tag = soup_object.find('script', type="text/javascript",
                                      string=lambda text: text and '''"cube":{''' in text)
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
