from typing import Union

from cube_config.cube_configuration import CubeConfig
from common.args import process_args


class PipelineObject:
    config: CubeConfig

    @process_args
    def __init__(self, config: Union[str, CubeConfig]):
        self.config = config
