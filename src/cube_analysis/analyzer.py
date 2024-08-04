import math
import re
import warnings
from pathlib import Path
from typing import List, Tuple, Union

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import seaborn as sns
from loguru import logger
from matplotlib.colors import to_rgb
from matplotlib.lines import Line2D
from src.common.args import process_args
from src.common.common import ensure_dir_exists, min_max_normalize_sklearn
from src.common.constants import ANALYSIS_DIRECTORY_PATH, COLOR_PALETTE, RESULTS_DIRECTORY_PATH, TYPE_PALETTE
from src.cube_config.cube_configuration import CubeConfig
from src.pipeline_object.pipeline_object import PipelineObject
from tabulate import tabulate

warnings.simplefilter("ignore", category=UserWarning)


class CubeAnalyzer(PipelineObject):
    color_symbol_map = {"White": "w",
                        "Blue": "u",
                        "Black": "b",
                        "Red": "r",
                        "Green": "g",
                        "Multicolored": "m",
                        "Colorless": "c",
                        "Land": "T"}
    hyphen_regex = re.compile(r" [—-].*")
    legendary_regex = re.compile(r"Legendary ")
    remove_types_regex = re.compile(r"(Artifact|Enchantment|Tribal|Snow|World|Kindred|Basic) ")

    @process_args
    def __init__(self, config: Union[str, CubeConfig]):
        super().__init__(config)
        self._set_analysis_directory()

        sns.set(style="whitegrid")

    def _set_analysis_directory(self) -> None:
        """
        Sets the analysis directory to be the cube name in the analysis directory
        """
        self.analysis_directory = ANALYSIS_DIRECTORY_PATH / self.config.cubeName
        ensure_dir_exists(self.analysis_directory)

    def _set_data_dictionary(self) -> None:
        """
        Sets the data dictionary to analyze data over. The cube is pulled from a previously generated csv file.
        :return:
        """
        cube_file_path = str(RESULTS_DIRECTORY_PATH / self.config.cubeName) + ".csv"
        data = self.load_cube(cube_file_path)
        data['Card Type'] = data.apply(self.clean_types, axis=1)

        filtered_data = data[data["ELO"] < 1750]
        filtered_data.reset_index(inplace=True, drop=True)
        mean, stdev = filtered_data['ELO'].mean(), filtered_data['ELO'].std()
        two_stdev = mean + stdev * 2
        filtered_data = filtered_data[filtered_data['ELO'] <= two_stdev]
        filtered_data.reset_index(inplace=True, drop=True)

        outliers = data[data['ELO'] > two_stdev]
        outliers = outliers.sort_values('ELO', ascending=False)
        outliers.reset_index(inplace=True, drop=True)

        for frame in [data, filtered_data, outliers]:
            for new_col, norm_col in [('Normalized ELO', 'ELO'), ('Normalized Inclusion Rate', 'Inclusion Rate')]:
                frame[new_col] = min_max_normalize_sklearn(frame[norm_col])
            frame['Inclusion Rate ELO Diff'] = frame.apply(self.get_elo_coverage_diff, axis=1)

        self.data = {'data': data, 'filtered': filtered_data, 'outliers': outliers}

    @staticmethod
    def get_elo_coverage_diff(row: pd.Series) -> float:
        """
        Gets the difference between the normalized inclusion rate and the normalized ELO - this was also calculated
        at the time the cube csv file was created. We re-calculate this metric here as it is run over various subsets
        of the cube data.

        :param row: a pd.Series object.
        """
        return np.abs(row['Normalized Inclusion Rate'] - row['Normalized ELO'])

    @staticmethod
    def load_cube(cube_file_path: str) -> pd.DataFrame:
        """
        Load a cube from file. Raise an Error if the cube is not found. This usually happens if you attempt to run
         the analysis stage without first running the generate cube stage.

        :param cube_file_path: file path to the cube csv file.
        :return: the cube csv file.
        """
        try:
            return pd.read_csv(cube_file_path)

        except FileNotFoundError:

            error_message = f"Analysis cube file {cube_file_path} not found"
            logger.debug(error_message)

            raise FileNotFoundError(error_message)

    def analyze(self) -> None:
        """
        Analyze the cube data. This is the main entry point for the CubeAnalyzer class, and generates a number of
        graphs and tables.
        """
        self._set_data_dictionary()
        if "analyze" not in self.config.stages:
            logger.info("Skipping analyze data stage")
        else:
            logger.info("Analyzing cube data")
            self.make_card_type_composition_wheel(self.data['data'])
            self.make_color_composition_wheel(self.data['data'])
            self.make_inclusion_rate_distribution_plot(self.data['data'])
            self.make_elo_outliers_table()
            self.make_elo_by_card_count_plot(self.data['filtered'])
            self.make_elo_by_color_category_swarm(self.data['filtered'])
            self.make_elo_by_color_category_box(self.data['filtered'])
            self.make_elo_by_card_type_swarm(self.data['filtered'])
            self.make_elo_by_card_type_box(self.data['filtered'])
            self.make_inclusion_rate_by_elo_scatter(self.data['filtered'])
            self.make_elo_inclusion_rate_correlated_tables(self.data['filtered'])
            self.make_card_type_inclusion_rate_plot(self.data['data'])
            self.make_color_category_inclusion_rate_plot(self.data['data'])

        return

    def make_inclusion_rate_distribution_plot(self, data: pd.DataFrame) -> None:
        """
        Creates a plot of the inclusion rate distribution of the cube

        :param data: a pandas DataFrame containing the cube data
        """
        plt.figure()
        ax = sns.histplot(
            data['Inclusion Rate'],
            kde=True,
            line_kws={"color": "red"},
            color="blue",
            bins=20
        )
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(self.custom_percent_format))
        kde_line = Line2D([0], [0], color='blue', label='Smoothed Density')
        plt.legend(
            handles=[kde_line]
        )
        plt.title("Card Count by Inclusion Rate of Sampled Cubes")
        plt.ylabel("Card Count")
        plt.xlabel("Card Inclusion Rate")

        image_save_path = str(self.analysis_directory / "inclusion_rate_distribution.png")
        plt.savefig(image_save_path)

    @staticmethod
    def custom_percent_format(x: float, pos) -> str:
        """
        Formats the x-axis of the inclusion rate plot to be a percentage.

        :param x: a float representing the x-axis value
        :param pos: an unused parameter. This is required by the FuncFormatter class where it is used, but is not
        used in this function.
        :return: a string representing the x-axis value as a percentage.
        """
        return f'{100 * x:.0f}%'

    def make_elo_outliers_table(self) -> None:
        """
        Creates a table of the outlier cards by ELO in the cube. This table is saved to the analysis directory.
        """
        outlier_table = self.make_table(self.data['outliers'])
        self.save_raw_text(Path(self.analysis_directory) / "outliers.txt", outlier_table)

    def make_table(self, dataframe: pd.DataFrame, columns: List[str] = None) -> str:
        """
        A method for converting a pandas DataFrame to a markdown table.

        :param dataframe: a pandas DataFrame
        :param columns: a list of columns to display in the table. If None, the default columns are used.
        :return: get back a string markdown table.
        """
        if dataframe.empty:
            return ""

        default_columns = ['Card Name', 'Inclusion Rate', 'ELO', 'Type', 'Color Category']
        if columns is None:
            columns = default_columns

        subset = dataframe.copy()
        subset['Inclusion Rate'] = subset['Inclusion Rate'].apply(lambda x: f"{x * 100:.2f}%")
        subset['Type'] = subset.apply(lambda x: self.clean_types(x), axis=1)
        subset['Color Category'] = subset['Color Category'].apply(
            lambda x: "{T}" if x == 'Land' else "{" + self.color_symbol_map[x] + "}")
        subset['Card Name'] = subset['name']
        subset['Card Name'] = subset['Card Name'].apply(lambda x: "[[" + x + "]]")

        subset_only_keep_columns = subset[columns]
        markdown_table = tabulate(subset_only_keep_columns, headers='keys', tablefmt='pipe', showindex=False)

        return markdown_table

    def clean_types(self, row: pd.Series) -> str:
        """
        Clean the card type line.

        :param row: a pandas Series representing a row in the cube data.
        :return: a string representing the cleaned card type line.
        """
        type_line = row.Type
        try:

            cleaned_type_line = self.legendary_regex.sub("", self.hyphen_regex.sub("", type_line))
            if " " in cleaned_type_line:
                return self.remove_types_regex.sub("", cleaned_type_line).rstrip()
        except Exception as e:
            logger.info(f"Failed parsing type line {type_line} for card {row.name}")
            raise Exception(e)

        return cleaned_type_line

    @staticmethod
    def save_raw_text(file_path: str, text: str) -> None:
        """
        Save raw text to a file. Useful for saving Markdown tables.

        :param file_path: text data's file path.
        :param text: the raw text to save.
        """
        with open(file_path, 'w') as f:
            f.write(text)

    def make_elo_by_card_count_plot(self, dataframe: pd.DataFrame) -> None:
        """
        Creates a plot of the card count by ELO. This plot is saved to the analysis directory.

        :param dataframe: a pandas DataFrame containing the cube data.
        """
        plt.figure()
        ax = sns.histplot(
            dataframe['ELO'],
            kde=True,
            line_kws={'color': "red"},
            color="blue",
            bins=20
        )
        plt.title("Card Count by Card ELO")
        plt.ylabel("Card Count")
        plt.xlabel("ELO")
        mean_value = dataframe["ELO"].mean()
        mean_line = Line2D([0], [0], color='red', linestyle='--', label='Mean')
        plt.axvline(mean_value, color="red", linestyle="--", label="Mean")
        kde_line = Line2D([0], [0], color='blue', label='Smoothed Density')
        legend_elements = [mean_line, kde_line]

        plt.legend(handles=legend_elements)

        image_save_path = str(self.analysis_directory / "card_count_by_elo.png")
        plt.savefig(image_save_path)
        plt.close("all")

    @staticmethod
    def get_ordered_categories_with_colors(input_frame: pd.DataFrame,
                                           column_name1: str,
                                           column_name2: str = "ELO",
                                           aggregate_func: str = "median") -> Tuple[List[str], List[str]]:
        """
        Get the ordered categories and colors for swarm and bar plots. Allows us to map "Blue" cards to our own
        'blue' color instead of using pyplot or seaborn's default colors. Must specifically be used for "Color Category"
        or "Card Type" columns.

        :param input_frame: a pandas DataFrame containing the cube data.
        :param column_name1: a string which is one of "Color Category" or "Card Type"
        :param column_name2: any column name over which we wish to compare the categories.
        :param aggregate_func: how to aggregate the data. Default is median as mean is heavily skewed by outliers.
        :return:
        """
        if column_name1 == "Color Category":
            color_palette = COLOR_PALETTE
        elif column_name1 == "Card Type":
            color_palette = TYPE_PALETTE
        else:
            raise ValueError(f"Invalid column_name1 value: {column_name1}")

        aggregated = input_frame.groupby(column_name1)[column_name2].agg(aggregate_func).reset_index()
        aggregated = aggregated.sort_values(column_name2, ascending=True)

        x_values = aggregated[column_name1].tolist()
        colors = [color_palette[key] for key in x_values]

        return x_values, colors

    def make_elo_by_color_category_swarm(self, dataframe: pd.DataFrame) -> None:
        """
        Creates a swarm plot of ELO by color category. This plot is saved to the analysis directory.

        :param dataframe: a pandas DataFrame containing the cube data.
        """
        order_cols, colors_list = self.get_ordered_categories_with_colors(dataframe,
                                                                          column_name1='Color Category',
                                                                          column_name2='ELO')
        plt.Figure()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=FutureWarning)
            sns.catplot(data=dataframe,
                        x="Color Category",
                        y="ELO",
                        kind="swarm",
                        palette=colors_list,
                        order=order_cols,
                        s=21)
            plt.xlabel('Color Category')
            plt.xticks(rotation=45)
            plt.title('Swarm Plot of ELO by Color Category')
            plt.tight_layout()

        image_save_path = str(self.analysis_directory / "elo_by_color_category_swarm_plot.png")
        plt.savefig(image_save_path)

    def make_elo_by_color_category_box(self, dataframe: pd.DataFrame) -> None:
        """
        Creates a box plot of ELO by color category. This plot is saved to the analysis directory.

        :param dataframe: a pandas DataFrame containing the cube data.
        """
        order_cols, colors_list = self.get_ordered_categories_with_colors(dataframe,
                                                                          column_name1='Color Category',
                                                                          column_name2='ELO')
        plt.Figure()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=FutureWarning)
            sns.catplot(
                data=dataframe,
                x="Color Category",
                y="ELO", kind="box",
                order=order_cols,
                palette=colors_list
            )
            plt.xlabel('Color Category')
            plt.xticks(rotation=45)
            plt.title('Box Plot of ELO by Color Category')
            plt.tight_layout()

        image_save_path = str(self.analysis_directory / "elo_by_color_category_box_plot.png")
        plt.savefig(image_save_path)

    def make_elo_by_card_type_swarm(self, dataframe: pd.DataFrame) -> None:
        """
        Creates a swarm plot of ELO by card type. This plot is saved to the analysis directory.

        :param dataframe: a pandas DataFrame containing the cube data.
        """
        order_cols, colors_list = self.get_ordered_categories_with_colors(dataframe,
                                                                          column_name1='Card Type',
                                                                          column_name2='ELO')
        plt.Figure()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=FutureWarning)

            sns.catplot(data=dataframe,
                        x="Card Type",
                        y="ELO",
                        kind="swarm",
                        palette=colors_list,
                        order=order_cols,
                        s=21)
            plt.xlabel('Card Type')
            plt.xticks(rotation=45)
            plt.title('Swarm Plot of ELO by Card Type')
            plt.tight_layout()

        image_save_path = str(self.analysis_directory / "elo_by_card_type_swarm_plot.png")
        plt.savefig(image_save_path)
        plt.close("all")

    def make_elo_by_card_type_box(self, dataframe: pd.DataFrame) -> None:
        """
        Creates a box plot of ELO by card type. This plot is saved to the analysis directory.

        :param dataframe: a pandas DataFrame containing the cube data.
        """
        order_cols, colors_list = self.get_ordered_categories_with_colors(dataframe,
                                                                          column_name1='Card Type',
                                                                          column_name2='ELO')
        plt.Figure()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=FutureWarning)
            sns.catplot(
                data=dataframe,
                x="Card Type",
                y="ELO", kind="box",
                order=order_cols,
                palette=colors_list
            )
            plt.xlabel('Card Type')
            plt.xticks(rotation=45)
            plt.title('Box Plot of ELO by Card Type')
            plt.tight_layout()

        image_save_path = str(self.analysis_directory / "elo_by_card_type_box_plot.png")
        plt.savefig(image_save_path)

        plt.close('all')

    def make_card_type_composition_wheel(self, input_frame: pd.DataFrame) -> None:
        """
        Creates a pie chart of card type composition.

        :param input_frame: a pandas DataFrame containing the cube data on card types
        """
        card_type_counts = input_frame['Card Type'].value_counts()
        index = card_type_counts.index
        colors = [to_rgb(TYPE_PALETTE[color]) for color in index]
        plt.Figure()
        plt.pie(
            card_type_counts,
            labels=index,
            autopct='%1.1f%%',
            colors=colors
        )
        plt.axis('equal')
        plt.title("Card Types", fontweight='bold')

        image_save_path = str(self.analysis_directory / "card_type_wheel.png")
        plt.savefig(image_save_path)
        plt.close('all')

    def make_color_composition_wheel(self, input_frame: pd.DataFrame) -> None:
        """
        Creates a pie chart of color composition.

        :param input_frame: a pandas DataFrame containing the cube data on color types
        """
        card_color_counts = input_frame['Color Category'].value_counts()
        index = card_color_counts.index
        colors = [to_rgb(COLOR_PALETTE[color]) for color in index]
        plt.Figure()
        plt.pie(
            card_color_counts,
            labels=index,
            autopct='%1.1f%%',
            colors=colors
        )
        plt.axis('equal')
        plt.title("Color Types", fontweight='bold')

        image_save_path = str(self.analysis_directory / "color_type_wheel.png")
        plt.savefig(image_save_path)
        plt.close('all')

    def make_inclusion_rate_by_elo_scatter(self, input_frame: pd.DataFrame) -> None:
        """
        Creates a regression plot/scatter plot of card inclusion rate by ELO rating.

        :param input_frame: a pandas DataFrame containing the cube data on card inclusion rates and ELO ratings
        """
        plt.Figure()
        plt.figure(figsize=(12, 8))
        sns.regplot(data=input_frame, x='ELO', y='Inclusion Rate', line_kws={'color': 'red'})
        r_squared = input_frame['ELO'].corr(input_frame['Inclusion Rate']) ** 2
        plt.text(0.8, 0.1, f'R-squared = {r_squared:.2f}', transform=plt.gca().transAxes, fontweight="bold")
        plt.xlabel('ELO')
        plt.ylabel('Cube Inclusion Rate')
        plt.title('Regression plot of card Inclusion Rate in cubes by ELO rating')
        plt.tight_layout()

        image_save_path = str(self.analysis_directory / "inclusion_rate_by_elo_scatter.png")
        plt.savefig(image_save_path)
        plt.close('all')

    def make_elo_inclusion_rate_correlated_tables(self, dataset: pd.DataFrame) -> None:
        """
        Make tables of cards that are outliers in terms of ELO and Inclusion Rate

        :param dataset: a dataframe of cards with ELO and Inclusion Rate data
        """
        outlier_cards = dataset.sort_values('Inclusion Rate ELO Diff', ascending=False).head(n=15)
        high_play_low_elo = self.make_table(outlier_cards[outlier_cards['Inclusion Rate'] > 0.6])
        low_play_high_elo = self.make_table(outlier_cards[outlier_cards['Inclusion Rate'] < 0.4])

        self.save_raw_text(Path(self.analysis_directory) / "outlier_low_elo_high_play_rate.txt", high_play_low_elo)
        self.save_raw_text(Path(self.analysis_directory) / "outlier_high_elo_low_play_rate.txt", low_play_high_elo)

    def make_card_type_inclusion_rate_plot(self, data: pd.DataFrame) -> None:
        """
        Creates a bar plot of average inclusion rate by card type.

        :param data: a pandas dataframe containing the cube data, including 'Inclusion Rate' and 'Card Type' columns.
        """
        average_inclusion = data.groupby('Card Type')['Inclusion Rate'].mean().reset_index()
        average_inclusion = average_inclusion.sort_values('Inclusion Rate', ascending=True)
        x_index = average_inclusion['Card Type'].tolist()
        colors = [to_rgb(TYPE_PALETTE[card_type]) for card_type in x_index]
        lower_bound = math.floor(average_inclusion['Inclusion Rate'].min() * 10) / 10

        plt.Figure()
        sns.barplot(
            x='Card Type',
            y='Inclusion Rate',
            data=average_inclusion,
            palette=colors,
            order=x_index,
            edgecolor='black'
        )
        plt.xlabel('Card Type')
        plt.ylabel('Average Inclusion Rate')
        plt.title('Average Inclusion Rate by Card Type')
        plt.xticks(rotation=45)
        plt.ylim(lower_bound, None)
        plt.tight_layout()

        image_save_path = str(self.analysis_directory / "average_inclusion_rate_by_card_type.png")
        plt.savefig(image_save_path)
        plt.close('all')

    def make_color_category_inclusion_rate_plot(self, data: pd.DataFrame) -> None:
        """
        Creates a bar plot of average inclusion rate by color category.

        :param data: a pandas dataframe.
        """
        average_inclusion = data.groupby('Color Category')['Inclusion Rate'].mean().reset_index()
        average_inclusion = average_inclusion.sort_values('Inclusion Rate', ascending=True)
        x_values = average_inclusion['Color Category'].tolist()
        color_rgbs = {x_val: to_rgb(COLOR_PALETTE[x_val]) for x_val in x_values}
        colors = [color_rgbs[key] for key in x_values]

        # Calculate the lower bound of the y-axis
        lower_bound = math.floor(average_inclusion['Inclusion Rate'].min() * 10) / 10

        plt.Figure()
        sns.barplot(x='Color Category', y='Inclusion Rate', data=average_inclusion, palette=colors, order=x_values,
                    edgecolor='black')

        plt.xlabel('Card Type')
        plt.xticks(rotation=45)
        plt.ylabel('Average Inclusion Rate')
        plt.title('Average Inclusion Rate by Card Color Category')

        # Set the lower bound of the y-axis
        plt.ylim(lower_bound, None)
        plt.tight_layout()

        image_save_path = str(self.analysis_directory / "average_inclusion_rate_by_color_category.png")
        plt.savefig(image_save_path)
        plt.close('all')
