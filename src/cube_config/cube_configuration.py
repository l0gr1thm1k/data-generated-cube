import json


class CubeConfigSettingGroup:
    """
    Class to hold groups of settings
    """

    def __init__(self, config_dict: dict) -> None:
        # Convert JSON objects to CubeConfigSettingGroups
        for key, value in config_dict.items():
            if isinstance(value, dict):
                config_dict[key] = CubeConfigSettingGroup(value)
        self._config = config_dict

    def to_dict(self) -> dict:
        """
        return dict representation of ModelConfig

        :return: config dict
        """

        return {key: value.to_dict() if isinstance(value, CubeConfigSettingGroup) else value for key, value in
                self._config.items()}

    def get(self, item: str, default=None):
        """
        Allows for retrieving items while falling back to a default value

        :param item: item to retrieve
        :param default: default to return in case of KeyError
        :return: value
        """

        try:
            return self._config[item]
        except:
            return default

    def __getattr__(self, item: str):
        """
        allow dot notation access to settings

        :param item: item to retrieve
        :return: value
        """

        if item.startswith('_'):
            return object.__getattribute__(self, item)
        else:
            return object.__getattribute__(self, '_config')[item]

    def __setattr__(self, key: str, value) -> None:
        """
        allow dot notation access to change setting values

        :param key: key to retrieve
        :param value: value to set
        """

        if key.startswith('_'):
            object.__setattr__(self, key, value)
        else:
            self._config[key] = value

    def __getitem__(self, item: str):
        """
        allow dict-like access to settings

        :param item: item to retrieve
        :return: value
        """

        return self._config[item]

    def __setitem__(self, key: str, value) -> None:
        """
        allow dict-like access to change setting values

        :param key: key to retrieve
        :param value: value to set
        """

        self._config[key] = value

    def __str__(self) -> str:
        return str(self.to_dict())

    def __dir__(self) -> list:
        return list(super().__dir__()) + list(self._settings.keys())


class CubeConfig:
    """
    Class to hold model configuration
    """

    # Enables IDE autocompletion
    metadata: CubeConfigSettingGroup

    def __init__(self, cube_config: str = None) -> None:

        # Load config file from disk
        if isinstance(cube_config, str):
            # Store filepath
            self._source = cube_config
            with open(cube_config) as config_file:
                cube_config = json.load(config_file)
        else:
            raise ValueError("Cube config supplied must be a string path to the target cube config json file")

        self._config = cube_config

    def to_dict(self) -> dict:
        """
        return dict representation of ModelConfig
        :return: config dict
        """

        return {key: value.to_dict() if isinstance(value, CubeConfigSettingGroup) else value for key, value in
                self._config.items()}

    def get(self, item: str, default=None):
        """
        Allows for retrieving items while falling back to a default value

        :param item: item to retrieve
        :param default: default to return in case of KeyError
        :return: value
        """

        try:
            return self._config[item]
        except:
            return default

    def __getattr__(self, item: str):
        """
        allow dot notation access to settings

        :param item: item to retrieve
        :return: value
        """

        if item.startswith('_'):
            return object.__getattribute__(self, item)
        else:
            return object.__getattribute__(self, '_config')[item]

    def __setattr__(self, key: str, value) -> None:
        """
        allow dot notation access to change setting values

        :param key: key to retrieve
        :param value: value to set
        """

        if key.startswith('_'):
            object.__setattr__(self, key, value)
        else:
            self._config[key] = value

    def __getitem__(self, item: str):
        """
        allow dict-like access to settings

        :param item: item to retrieve
        :return: value
        """

        return self._config[item]

    def __setitem__(self, key: str, value) -> None:
        """
        allow dict-like access to change setting values

        :param key: key to retrieve
        :param value: value to set
        """

        self._config[key] = value

    def __str__(self) -> str:
        return str(self.to_dict())

    def __dir__(self) -> list:
        return list(super().__dir__()) + list(self._settings.keys())
