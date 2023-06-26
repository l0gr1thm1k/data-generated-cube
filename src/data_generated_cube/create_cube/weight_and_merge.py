import pandas as pd

from common.constants import COLORS_SET, RESULTS_DIRECTORY_PATH


class CuberMerger:

    def __init__(self, cube_name):
        self.cube_name = cube_name

    def weight_and_merge_frames(self, frame_list, card_count_dicts, weights):
        unique_cards = self.get_unique_cards(frame_list)
        columns = frame_list[0].columns
        weighted_frame = self.calculate_weighted_frame(unique_cards, frame_list, columns, weights)
        averaged_color_frames = self.average_card_counts(weighted_frame, card_count_dicts)
        concatted_weighted_frame = self.concatenate_frames(averaged_color_frames)
        self.save_frame_to_file(concatted_weighted_frame)

        return concatted_weighted_frame

    @staticmethod
    def get_unique_cards(frame_list):
        unique_cards = set()
        for frame in frame_list:
            unique_cards.update(frame.name.tolist())

        return list(unique_cards)

    def calculate_weighted_frame(self, unique_cards, frame_list, columns, weights):
        card_data = []
        for card in unique_cards:
            card_data.append(self.process_card(card, frame_list, weights))

        weighted_frame = pd.DataFrame(columns=columns, data=card_data)
        weighted_frame.sort_values(['Weighted Rank', 'Inclusion Rate', 'ELO'], ascending=[False, False, False],
                                   inplace=True)
        weighted_frame.reset_index(inplace=True, drop=True)

        return weighted_frame

    @staticmethod
    def process_card(card, frame_list, cube_weights):
        card_occurrences = [frame[frame.name == card] for frame in frame_list]
        weights = 0
        occurrence = None
        for occurrence_frame, weight in zip(card_occurrences, cube_weights):
            try:
                occurrence = occurrence_frame.iloc[0]
                weights += occurrence['Weighted Rank'] * weight
            except:
                pass
        frame_list[0].loc[occurrence.name, 'Weighted Rank'] = round(weights / len(frame_list), 4)

        return occurrence.tolist()

    def average_card_counts(self, weighted_frame, card_count_dicts):
        avg_dict = self.calculate_average_dict(card_count_dicts)
        averaged_color_frames = []
        for color in list(COLORS_SET):
            sub_frame = weighted_frame[weighted_frame['Color Category'] == color]
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

    def save_frame_to_file(self, concatted_weighted_frame):

        csv_file_path = str(RESULTS_DIRECTORY_PATH / f"{self.cube_name}.csv")
        concatted_weighted_frame.to_csv(csv_file_path, index=False)

