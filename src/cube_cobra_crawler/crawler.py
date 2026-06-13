import aiohttp
import asyncio
import boto3
import datetime
import json
import re

import numpy as np
import requests
from botocore import UNSIGNED
from botocore.client import Config
from bs4 import BeautifulSoup
from loguru import logger
from pathlib import Path
from typing import Union

from src.common.args import process_args
from src.common.common import ensure_dir_exists
from src.common.constants import DATA_DIRECTORY_PATH, BLACKLIST_REGEX, COHORT_ANALYSIS_DIRECTORY_PATH
from src.cube_cobra_crawler.csv_file_generator import CSVFileGenerator
from src.cube_cobra_crawler.rss_feed_crawler import RSSFeedParser
from src.cube_config.cube_configuration import CubeConfig
from src.pipeline_object.pipeline_object import PipelineObject

CUBE_COBRA_PUBLIC_BUCKET = "cubecobra-public"
CUBE_COBRA_EXPORT_CACHE_HOURS = 24 * 7
CUBE_COBRA_SEARCH_URL = "https://cubecobra.com/search"
CUBE_COBRA_SEARCH_MORE_URL = "https://cubecobra.com/search/getmoresearchitems"
CUBE_COBRA_SEARCH_USER_AGENT = "data-generated-cube/1.0 (+https://github.com/l0gr1thm1k/data-generated-cube)"
CUBE_COBRA_DELTA_MAX_PAGES = 250


class CubeCobraScraper(PipelineObject):

    @process_args
    def __init__(self, config: Union[str, CubeConfig]):
        super().__init__(config)
        self._set_data_dir(self.config.cubeName)
        self.file_generator = CSVFileGenerator(self.data_dir)
        self.feed_parser = RSSFeedParser()
        self.cube_weights = {}
        # IDs the user explicitly listed in their config are an allowlist — they bypass the
        # discovery-time filters (size, composition, blacklist) that only apply to auto-discovered
        # candidates. Snapshot the originals before any discovery union mutates config.cubeIds.
        self.user_specified_cube_ids = set(self.config.cubeIds)
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

            lock = asyncio.Lock()
            semaphore = asyncio.Semaphore(10)

            if self.config.get("cohortAnalysis", False):
                self.setup_cohort_analysis_directory()

            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                tasks = []
                for cube_id in self.config.cubeIds:
                    task = asyncio.create_task(self.process_cube(cube_id, lock, session, semaphore))
                    tasks.append(task)
                await asyncio.gather(*tasks)

            with open(self.data_dir / "cube_weights.json", "w") as f:
                json.dump(self.cube_weights, f)

        else:
            logger.info("Skipping scrape data stage")

    async def update_cube_id_list(self) -> None:
        logger.info("Fetching Cube Cobra AWS Bucket Data")
        bucket_ids = self.fetch_cube_ids()

        snapshot_modified = self.get_snapshot_last_modified()
        delta_ids = self.fetch_delta_cube_ids(snapshot_modified)

        combined = set(self.config.cubeIds) | set(bucket_ids) | set(delta_ids)
        new_from_delta = set(delta_ids) - set(bucket_ids) - set(self.config.cubeIds)
        logger.info(
            f"Cube ID sources: config={len(self.config.cubeIds)}, "
            f"bulk={len(bucket_ids)}, delta={len(delta_ids)} "
            f"({len(new_from_delta)} new from delta), total={len(combined)}"
        )
        self.config.cubeIds = list(combined)

    @staticmethod
    def get_snapshot_last_modified() -> datetime.datetime:
        """Return the LastModified time of the bulk cubes.json export in UTC."""
        s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        head = s3_client.head_object(Bucket=CUBE_COBRA_PUBLIC_BUCKET, Key="export/cubes.json")
        last_modified = head["LastModified"]
        if last_modified.tzinfo is not None:
            last_modified = last_modified.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return last_modified

    def fetch_delta_cube_ids(self, since: datetime.datetime) -> list:
        """
        Fetch cubes created after `since` from Cube Cobra's live /search page to fill the gap
        between the quarterly bulk export and the present. One HTTP request per category.

        Filters applied here mirror the bulk-path filters that can be evaluated from the search
        response: cube category, card count window, follower/like floor, and the vintage name
        blacklist. The power-card composition check (vintage) is intentionally not duplicated —
        the live per-cube stage will reject non-powered cubes that slip through.
        """
        category = self.config.get('cubeCategory', '').lower()
        query = self._build_delta_query(category)
        if query is None:
            return []
        since_ms = int(since.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)
        min_count = self.config.cardCount * 0.9
        max_count = self.config.cardCount * 1.1
        blacklist_regex = re.compile(BLACKLIST_REGEX, re.IGNORECASE) if category == 'vintage' else None

        ids = []
        oldest_seen = None
        last_key = None
        pages_fetched = 0
        reached_snapshot = False
        headers = {"User-Agent": CUBE_COBRA_SEARCH_USER_AGENT}

        try:
            for page in range(CUBE_COBRA_DELTA_MAX_PAGES):
                if page == 0:
                    response = requests.get(
                        CUBE_COBRA_SEARCH_URL,
                        params={"q": query, "order": "date", "ascending": "false"},
                        headers=headers,
                        timeout=30,
                    )
                    response.raise_for_status()
                    cubes = self._parse_search_results(response.text)
                    last_key = self._extract_last_key(response.text)
                else:
                    if not last_key:
                        break
                    response = requests.post(
                        CUBE_COBRA_SEARCH_MORE_URL,
                        json={"query": query, "order": "date", "ascending": False, "lastKey": last_key},
                        headers={**headers, "Content-Type": "application/json"},
                        timeout=30,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    if payload.get("success") != "true":
                        break
                    cubes = payload.get("cubes", [])
                    last_key = payload.get("lastKey")

                pages_fetched += 1
                if not cubes:
                    break

                for cube in cubes:
                    # Surface cubes whose mainboard OR metadata changed since the snapshot, not just
                    # newly-created ones — a cube that gained a follower or was renamed past the
                    # blacklist after the snapshot is in the bulk dump but was filtered out using
                    # stale field values.
                    updated = cube.get('dateLastUpdated') or cube.get('date') or cube.get('dateCreated') or 0
                    if oldest_seen is None or updated < oldest_seen:
                        oldest_seen = updated
                    if updated <= since_ms:
                        continue
                    if not (min_count <= cube.get('cardCount', 0) <= max_count):
                        continue
                    if cube.get('likeCount', 0) < 1:
                        continue
                    if blacklist_regex and blacklist_regex.search(cube.get('name', '')):
                        continue
                    ids.append(cube['id'])

                # Results are sorted by date desc, so once the oldest cube on a page is older than
                # the snapshot, no further page can contain new deltas.
                if oldest_seen is not None and oldest_seen <= since_ms:
                    reached_snapshot = True
                    break
                if not last_key:
                    break
        except requests.RequestException as e:
            logger.warning(f"Delta search request failed for category={category} on page {pages_fetched}: {e}")
        except (AttributeError, ValueError, json.JSONDecodeError) as e:
            logger.warning(f"Failed to parse delta search results for category={category} on page {pages_fetched}: {e}")

        if pages_fetched and not reached_snapshot and last_key:
            oldest_str = datetime.datetime.utcfromtimestamp(oldest_seen / 1000).strftime('%Y-%m-%d') if oldest_seen else "unknown"
            logger.warning(
                f"Delta pagination for category={category} hit page cap of {CUBE_COBRA_DELTA_MAX_PAGES} "
                f"before reaching snapshot date {since.strftime('%Y-%m-%d')} (oldest reached: {oldest_str}); "
                f"some recent cubes may be missing"
            )

        logger.info(
            f"Delta search found {len(ids)} {category} cubes updated since {since.strftime('%Y-%m-%d')} "
            f"(scanned {pages_fetched} page(s))"
        )
        return list(set(ids))

    def _build_delta_query(self, category: str):
        """
        Build the /search query that mirrors the discovery filter for a given category.
        Vintage queries by composition (all 10 power cards must be present in the mainboard) —
        the user-assigned 'category' label is unreliable, since people tag all kinds of cubes
        as Vintage. Pioneer/pauper match the name-based bulk filter.
        """
        if category == 'vintage':
            # Mirror fetch_vintage_ids: cube must contain every power card.
            return " ".join(f'card:"{name}"' for name in self.power_map.keys())
        if category == 'pioneer':
            return "pioneer"
        if category == 'pauper':
            return "pauper"
        return None

    @staticmethod
    def _extract_last_key(html: str):
        """Pull lastKey from the initial /search page so subsequent POSTs can paginate."""
        soup = BeautifulSoup(html, 'html.parser')
        script_tag = soup.find(
            'script',
            type="text/javascript",
            string=lambda text: text and 'window.reactProps' in text,
        )
        if not script_tag:
            return None
        raw = script_tag.text
        match = re.search(r'window\.reactProps\s*=\s*', raw)
        if not match:
            return None
        start_pos = match.end()
        brace_count = 0
        in_string = False
        escape_next = False
        end_pos = start_pos
        for i, char in enumerate(raw[start_pos:], start=start_pos):
            if escape_next:
                escape_next = False
                continue
            if char == '\\':
                escape_next = True
                continue
            if char == '"':
                in_string = not in_string
                continue
            if not in_string:
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = i + 1
                        break
        payload = raw[start_pos:end_pos]
        payload = re.sub(r'\bundefined\b', 'null', payload)
        try:
            return json.loads(payload).get('lastKey')
        except json.JSONDecodeError:
            return None

    @staticmethod
    def _parse_search_results(html: str) -> list:
        """Extract the cubes array from a /search page's window.reactProps payload."""
        soup = BeautifulSoup(html, 'html.parser')
        script_tag = soup.find(
            'script',
            type="text/javascript",
            string=lambda text: text and 'window.reactProps' in text and '"cubes"' in text,
        )
        if not script_tag:
            raise AttributeError("Could not find window.reactProps script with cubes payload")

        raw = script_tag.text
        match = re.search(r'window\.reactProps\s*=\s*', raw)
        if not match:
            raise AttributeError("Could not find window.reactProps assignment")

        # Balance braces to find the complete JSON object (same approach as get_json_query).
        start_pos = match.end()
        brace_count = 0
        in_string = False
        escape_next = False
        end_pos = start_pos
        for i, char in enumerate(raw[start_pos:], start=start_pos):
            if escape_next:
                escape_next = False
                continue
            if char == '\\':
                escape_next = True
                continue
            if char == '"':
                in_string = not in_string
                continue
            if not in_string:
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = i + 1
                        break
        if brace_count != 0:
            raise ValueError(f"Unbalanced braces in search JSON: {brace_count} unmatched")

        payload = raw[start_pos:end_pos]
        payload = re.sub(r'\bundefined\b', 'null', payload)
        payload = re.sub(r'\bNaN\b', 'null', payload)
        payload = re.sub(r'\b-?Infinity\b', 'null', payload)

        data = json.loads(payload)
        return data.get('cubes', [])

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
            if hours_since_cache < CUBE_COBRA_EXPORT_CACHE_HOURS:
                should_download = False
                download_path = most_recent_cache
                logger.info(f"Using cached AWS bucket data from {cache_date.strftime('%Y-%m-%d')} ({hours_since_cache:.1f} hours old)")
            else:
                logger.info(f"AWS bucket data is {hours_since_cache:.1f} hours old, re-downloading")

        if should_download:
            logger.info("Downloading fresh AWS bucket data from S3")
            download_path_template = str(data_dir / f"{prefix}-{{datestamp}}.json")
            download_path = self.download_file(
                bucket_name=CUBE_COBRA_PUBLIC_BUCKET,
                object_key="export/cubes.json",
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
        s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

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
            if hours_since_cache < CUBE_COBRA_EXPORT_CACHE_HOURS:
                should_download = False
                download_path = most_recent_cache
                logger.info(f"Using cached oracle ID mapping from {cache_date.strftime('%Y-%m-%d')} ({hours_since_cache:.1f} hours old)")
            else:
                logger.info(f"Oracle ID mapping is {hours_since_cache:.1f} hours old, re-downloading")

        if should_download:
            logger.info("Downloading fresh oracle ID mapping from S3")
            download_path_template = str(data_dir / f"{prefix}-{{datestamp}}.json")
            download_path = self.download_file(
                bucket_name=CUBE_COBRA_PUBLIC_BUCKET,
                object_key="export/indexToOracleMap.json",
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

    def _passes_live_category_filters(self, cube_json_object: dict, cube_identifier: str) -> bool:
        """
        Verify a live cube against the same category-composition rules used at discovery time.
        Applied to every candidate before it is written out, so delta-sourced cubes (which were
        only checked against their self-declared category label) cannot bypass the composition
        check that the bulk path enforces.
        """
        category = self.config.get('cubeCategory', '').lower()
        mainboard = cube_json_object.get('cards', {}).get('mainboard', []) or []
        name = cube_json_object.get('cube', {}).get('name', '') or ''

        if not (self.config.cardCount * 0.9 <= len(mainboard) <= self.config.cardCount * 1.1):
            logger.info(f"Rejecting {cube_identifier}: live mainboard size {len(mainboard)} outside ±10% of {self.config.cardCount}")
            return False

        if category == 'vintage':
            power_names = set(self.power_map.keys())
            mainboard_names = set()
            for card in mainboard:
                try:
                    mainboard_names.add(self.file_generator.get_card_name(card))
                except KeyError:
                    # Card with no resolvable name (corrupt entry, missing Scryfall row); skip it
                    # rather than count its absence as a failed power check.
                    continue
            missing = power_names - mainboard_names
            if missing:
                logger.info(f"Rejecting {cube_identifier} ({name!r}): vintage cube missing power cards: {sorted(missing)}")
                return False
            if re.search(BLACKLIST_REGEX, name, re.IGNORECASE):
                logger.info(f"Rejecting {cube_identifier}: vintage cube name matches blacklist: {name!r}")
                return False
        elif category == 'pioneer':
            if not re.search(r'pioneer', name, re.IGNORECASE):
                logger.info(f"Rejecting {cube_identifier}: pioneer cube name lacks 'pioneer': {name!r}")
                return False
        elif category == 'pauper':
            if not re.search(r'pauper', name, re.IGNORECASE):
                logger.info(f"Rejecting {cube_identifier}: pauper cube name lacks 'pauper': {name!r}")
                return False

        return True

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

    async def process_cube(self, cube_identifier: str, lock, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> None:
        async with semaphore:
            await self._process_cube_inner(cube_identifier, lock, session)

    async def _process_cube_inner(self, cube_identifier: str, lock, session: aiohttp.ClientSession) -> None:
        cube_overview_link = f"https://cubecobra.com/cube/overview/{cube_identifier}"
        cube_list_link = f"https://cubecobra.com/cube/list/{cube_identifier}"

        # Fetch list page (for cards and update date)
        cube_soup_object = await self.get_website_soup_object(cube_list_link, session)

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

        # Live category-composition gate. The bulk-discovery filters in fetch_{vintage,pioneer,pauper}_ids
        # apply against the quarterly snapshot, and the /search delta sweep relies on the user-assigned
        # categoryOverride label (which is not a composition fact). Both can let non-qualifying cubes
        # through — e.g. a 'Vintage'-tagged cube without power, or a renamed bulk cube. Re-validate here
        # against the live mainboard so every candidate clears the same bar regardless of source.
        # User-specified cubeIds bypass this gate: if the user explicitly listed an ID in their config,
        # they want it included regardless of size/composition (e.g. canonical 540+ card vintage cubes
        # like wtwlf123 that exceed the ±10% size window of an auto-discovery filter).
        is_user_specified = cube_identifier in self.user_specified_cube_ids
        if (not self.config.get("cohortAnalysis", False)
                and not is_user_specified
                and not self._passes_live_category_filters(cube_json_object, cube_identifier)):
            return

        # Get last mainboard update date from RSS feed
        try:
            last_updated = await self.feed_parser.get_most_recent_update_date(cube_identifier, session)
        except Exception as e:
            logger.warning(f"Failed to get RSS feed for {cube_identifier}: {e}, using fallback")
            last_updated = datetime.datetime.now() - datetime.timedelta(days=730)

        today = datetime.datetime.today()
        days_since_update = (today - last_updated).days

        logger.info(f"Cube {cube_identifier}: last mainboard update {last_updated.strftime('%Y-%m-%d')} ({days_since_update} days ago), threshold: {self.config.recentUpdatesThreshold} days")

        if days_since_update <= self.config.recentUpdatesThreshold or self.config.get("cohortAnalysis", False):
            cube_weight = await self.get_cube_weight(cube_json_object, cube_identifier)
            async with lock:
                self.cube_weights[cube_identifier] = cube_weight

            cube_cards = cube_json_object['cards']['mainboard']
            self.file_generator.process_cube_data(cube_cards, cube_identifier)
            logger.info(f"Successfully processed cube {cube_overview_link}")

            return cube_cards
        else:
            logger.info(f"Skipping cube {cube_identifier} (last updated {days_since_update} days ago, exceeds {self.config.recentUpdatesThreshold} day threshold)")

    @staticmethod
    async def get_website_soup_object(target_url: str, session: aiohttp.ClientSession, max_retries: int = 3):
        """
        Get the soup object for a given url, with retry logic for transient errors.

        :param target_url:
        :param session: shared aiohttp session
        :param max_retries: number of retry attempts
        :return:
        """
        for attempt in range(max_retries):
            try:
                async with session.get(target_url) as response:
                    content = await response.read()
                return BeautifulSoup(content, 'html.parser')
            except (aiohttp.ClientOSError, aiohttp.ServerDisconnectedError, asyncio.TimeoutError) as e:
                if attempt < max_retries - 1:
                    wait = 2 ** attempt
                    logger.warning(f"Request to {target_url} failed ({e}), retrying in {wait}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"Request to {target_url} failed after {max_retries} attempts: {e}")
                    raise

    @staticmethod
    def get_json_query(soup_object):
        """
        Parse a webpage soup object for a text/javascript script object that contains the cube data

        :param soup_object:
        :return:
        """
        script_tag = soup_object.find('script', type="text/javascript",
                                      string=lambda text: text and '''"cube":{''' in text)
        if not script_tag:
            raise AttributeError("Could not find script tag with cube data")

        raw = script_tag.text

        # Find the start of the JSON object
        match = re.search(r'window\.reactProps\s*=\s*', raw)
        if not match:
            raise AttributeError("Could not find window.reactProps assignment")

        start_pos = match.end()

        # Balance braces to find the complete JSON object
        brace_count = 0
        in_string = False
        escape_next = False
        end_pos = start_pos

        for i, char in enumerate(raw[start_pos:], start=start_pos):
            if escape_next:
                escape_next = False
                continue

            if char == '\\':
                escape_next = True
                continue

            if char == '"' and not escape_next:
                in_string = not in_string
                continue

            if not in_string:
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = i + 1
                        break

        if brace_count != 0:
            raise ValueError(f"Unbalanced braces in JSON: {brace_count} unmatched")

        match_text = raw[start_pos:end_pos]

        # Replace JavaScript-specific values with JSON equivalents
        # Use word boundaries to avoid replacing these in strings
        match_text = re.sub(r'\bundefined\b', 'null', match_text)
        match_text = re.sub(r'\bNaN\b', 'null', match_text)
        match_text = re.sub(r'\bInfinity\b', 'null', match_text)
        match_text = re.sub(r'\b-Infinity\b', 'null', match_text)

        try:
            return json.loads(match_text)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error at position {e.pos}: {e.msg}")
            logger.error(f"Context around error: ...{match_text[max(0, e.pos-100):e.pos+100]}...")
            raise

    async def get_cube_weight(self, cube_json: dict, identifier) -> float:
        cube_follower_weight = self.get_cube_follower_weight(cube_json)
        # RSS feeds are broken, so skip update weight calculation and use constant weight
        # This was previously calculated from RSS feed update frequency but now returns 1.0 for all cubes
        cube_update_weight = 1.0

        return round(cube_follower_weight + cube_update_weight, 4)

    def get_cube_follower_weight(self, cube_json_object: dict) -> float:
        follower_count = self.get_follower_count(cube_json_object)
        if follower_count < 1:
            follower_count = 1

        return np.log(follower_count) + 1

    @staticmethod
    def get_follower_count(cube_json: dict) -> int:
        return cube_json['cube'].get('likeCount', 0)
