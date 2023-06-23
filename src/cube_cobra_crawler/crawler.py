import datetime
import json
import re

import requests
from bs4 import BeautifulSoup
from structlog import get_logger

from cube_cobra_crawler.csv_file_generator import CSVFileGenerator

logger = get_logger()


class CubeCobraScraper:
    base_url = 'https://cubecobra.com/search/card%3A%22black%20lotus%22?order=pop&ascending=false'

    def __init__(self, data_dir):
        self.blacklist = ['data']
        self.file_generator = CSVFileGenerator(data_dir)

    def get_cube_data(self):
        page_soup = self.get_website_soup_object(self.base_url)
        cube_json_query = self.get_json_query(page_soup)
        links = self.get_cube_links(cube_json_query)
        for link in links:
            self.process_cube(link)

        return links

    @staticmethod
    def get_website_soup_object(target_url):
        response = requests.get(target_url)

        return BeautifulSoup(response.content, 'html.parser')

    @staticmethod
    def get_json_query(soup_object):
        script_tag = soup_object.find('script', type="text/javascript", string=lambda text: text and 'lotus' in text)
        raw = script_tag.text
        match = re.findall("\{.*", raw)[0].rstrip(';')

        return json.loads(match)

    def get_cube_links(self, json_query: dict):
        cube_links = []
        for cube in json_query['cubes']:

            category = cube['categoryOverride'],
            id = cube['id']
            card_count = cube['cardCount']
            last_updated = self.convert_timestamp(cube['date'])
            today = datetime.datetime.today()

            if 'Vintage' in category and int(360 * .9) <= card_count <= int(360 * 1.1) and (today - last_updated).days < 365:
                try:
                    cube_id = cube['shortId']
                except KeyError:
                    cube_id = id

                if cube_id not in self.blacklist:

                    cube_links.append(f'https://cubecobra.com/cube/list/{cube_id}')

        return cube_links

    @staticmethod
    def convert_timestamp(timestamp):
        converted_timestamp = int(str(timestamp)[:10])
        return datetime.datetime.fromtimestamp(converted_timestamp)

    def process_cube(self, cube_link):
        cube_soup_object = self.get_website_soup_object(cube_link)
        cube_json_object = self.get_json_query(cube_soup_object)

        cube_name = cube_json_object['cube']['name']
        cube_cards = cube_json_object['cards']['mainboard']

        self.file_generator.process_cube_data(cube_cards, cube_name)

        logger.info(f"Successfully processed cube {cube_link}")

        return cube_cards


if __name__ == '__main__':
    ex = CubeCobraScraper("/home/daniel/Desktop/TestCrawl")
    cube_links = ex.get_cube_data()

