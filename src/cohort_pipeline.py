
from typing import Union

from common.args import process_args
from cohort_analysis.cohort_analysis import CohortAnalyzer
from cube_cobra_crawler.crawler import CubeCobraScraper
from cube_config.cube_configuration import CubeConfig
from pipeline_object.pipeline_object import PipelineObject


class CohortAnalysisPipeline(PipelineObject):

    @process_args
    def __init__(self, config: Union[str, CubeConfig]):
        super().__init__(config)

    async def run(self):
        scaper = CubeCobraScraper(self.config)
        await scaper.get_cube_data()
        analyzer = CohortAnalyzer(self.config)
        await analyzer.analyze_cohort()
