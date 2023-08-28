import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

import numpy as np


class RSSFeedParser:

    def __init__(self, update_window_days=365):
        self.update_window_days = update_window_days
        self.now = datetime.utcnow()

    def calculate_update_weight(self, cube_identifier):
        rss_feed = self.fetch_rss_feed(cube_identifier)
        items = self.parse_feed_for_updates(rss_feed)
        updates = self.get_cube_updates_from_list(items)
        weights = self.get_update_weights(updates)
        weight = np.log(sum(weights)) + 1

        return weight

    @staticmethod
    def fetch_rss_feed(cube_identifier: str):
        url = f"https://cubecobra.com/cube/rss/{cube_identifier}"
        response = requests.get(url)
        return response.content

    @staticmethod
    def parse_feed_for_updates(xml_content):
        soup = BeautifulSoup(xml_content, 'xml')
        items = soup.find_all('item')
        return items

    def get_cube_updates_from_list(self, update_list):
        items = []
        last_update_date = self.now - timedelta(days=730)  # hard code to two years
        for update in update_list:
            pub_date = self.get_item_date(update)
            if (self.now - pub_date).days <= self.update_window_days and "Mainboard" in update:

                if np.abs((pub_date - last_update_date).days) < 7:

                    pass
                else:
                    items.append(pub_date)
                    last_update_date = pub_date
            else:
                break

        return items

    def get_update_weights(self, update_list):
        weights = [1]  # seed weights with a 1 to avoid division by zero / negative weights
        for update in update_list:
            weight = self.get_update_date_weight(update)
            weights.append(weight)

        return weights

    def get_update_date_weight(self, date_obj):
        time_difference = self.now - date_obj
        days_difference = time_difference.days
        weight = 1 / (days_difference + 1)  # Adding 1 to avoid division by zero

        return weight * 10

    @staticmethod
    def get_item_date(item):
        pub_date = item.find('pubDate').text
        return datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %Z")