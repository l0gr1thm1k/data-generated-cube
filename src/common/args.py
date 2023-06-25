from functools import wraps
from inspect import getfullargspec
from typing import Union

from cube_config.cube_configuration import CubeConfig


def process_args(init_function):
    """
    wrap function

    :param init_function: function to wrap
    :return: wrapped function
    """

    @wraps(init_function)
    def wrapper(*args, **kwargs):
        """
        process args into desired types

        :param args: arguments
        :param kwargs: keyword arguments
        :return: object
        """

        func_spec = getfullargspec(init_function)

        # Collapse args and kwargs
        collected_args = dict(zip(func_spec.args, args))
        collected_args.update(kwargs)

        # Add any missing defaults, reversed to ensure defaults match up with correct args
        if func_spec.defaults:
            arg_defaults = {arg: value for arg, value in zip(reversed(func_spec.args), reversed(func_spec.defaults))
                            if arg not in collected_args}
            collected_args.update(arg_defaults)

        # Convert args to appropriate classes
        if 'config' in collected_args:
            collected_args['config'] = load_cube_config(collected_args['config'])

        return init_function(**collected_args)

    return wrapper


def load_cube_config(model_config: Union[str, CubeConfig]) -> CubeConfig:
    """
    load a ModelConfig

    :param model_config: input
    :return: ModelConfig
    """

    if isinstance(model_config, CubeConfig):
        return model_config
    else:
        return CubeConfig(model_config)
