import pandas as pd

from data_generated_cube.common.common import get_utc_time
from data_generated_cube.common.constants import COLORS_SET, MERGED_RESULTS_DIRECTORY_PATH


class CuberMerger:

    def __init__(self):
        pass

    def weight_and_merge_frames(self, frame_list, card_count_dicts):
        unique_cards = self.get_unique_cards(frame_list)
        columns = frame_list[0].columns
        weighted_frame = self.calculate_weighted_frame(unique_cards, frame_list, columns)
        averaged_color_frames = self.average_card_counts(weighted_frame, card_count_dicts)
        concatted_weighted_frame = self.concatenate_frames(averaged_color_frames)
        self.save_frame_to_file(concatted_weighted_frame)

        return concatted_weighted_frame

    @staticmethod
    def get_unique_cards(frame_list):
        unique_cards = set()
        for frame in frame_list:
            unique_cards.update(frame.name.tolist())
        return unique_cards

    def calculate_weighted_frame(self, unique_cards, frame_list, columns):
        card_data = []
        for card in unique_cards:
            card_data.append(self.process_card(card, frame_list))

        weighted_frame = pd.DataFrame(columns=columns, data=card_data)
        weighted_frame.sort_values(['Inclusion Rate', 'ELO'], ascending=[False, False], inplace=True)
        weighted_frame.reset_index(inplace=True, drop=True)

        return weighted_frame

    @staticmethod
    def process_card(card, frame_list):
        card_occurrences = [frame[frame.name == card] for frame in frame_list]
        weights = 0
        occurrence = None
        for occurrence_frame in card_occurrences:
            try:
                occurrence = occurrence_frame.iloc[0]
                weights += occurrence.Coverage
            except:
                pass
        occurrence.Coverage = round(weights / len(frame_list), 4)

        return occurrence.tolist()

    def average_card_counts(self, weighted_frame, card_count_dicts):
        avg_dict = self.calculate_average_dict(card_count_dicts)
        averaged_color_frames = []
        for color in avg_dict:
            sub_frame = weighted_frame[weighted_frame['Color Category'].isin(COLORS_SET)]
            sub_frame.reset_index(inplace=True, drop=True)
            sub_frame = sub_frame[:avg_dict[color]]
            averaged_color_frames.append(sub_frame)

        return averaged_color_frames

    @staticmethod
    def calculate_average_dict(card_count_dicts):
        avg_dict = {}
        num_dicts = len(card_count_dicts)
        for color in list(COLORS_SET):
            avg_dict[color] = round(sum(d[color] for d in card_count_dicts) / num_dicts)
        return avg_dict

    @staticmethod
    def concatenate_frames(averaged_color_frames):
        return pd.concat(averaged_color_frames)

    @staticmethod
    def save_frame_to_file(concatted_weighted_frame):

        csv_file_path = str(MERGED_RESULTS_DIRECTORY_PATH / f"{get_utc_time()}_weighted_cube.csv")
        txt_file_path = str(MERGED_RESULTS_DIRECTORY_PATH / f"{get_utc_time()}_weighted_cube.txt")

        with open(txt_file_path, 'w') as fstream:
            for name in concatted_weighted_frame.name:
                fstream.write(name + '\n')
        concatted_weighted_frame.to_csv(csv_file_path, index=False)

