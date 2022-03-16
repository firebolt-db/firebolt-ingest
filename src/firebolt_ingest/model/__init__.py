from typing import Any, List

import yaml
from pydantic.main import ModelMetaclass
from yaml import Loader


class YamlModelMixin(metaclass=ModelMetaclass):
    """
    Provides a parse_yaml method to a Pydantic model class.
    """

    @classmethod
    def parse_yaml(cls, yaml_obj: Any):
        obj = yaml.load(yaml_obj, Loader=Loader)
        return cls.parse_obj(obj)
