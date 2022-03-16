from typing import Any, Protocol

import yaml
from pydantic.main import ModelMetaclass
from yaml import Loader


class BaseModelProtocol(Protocol):
    @classmethod
    def parse_obj(self, obj: Any) -> Any:
        ...


class YamlModelMixin(metaclass=ModelMetaclass):
    """
    Provides a parse_yaml method to a Pydantic model class.
    """

    @classmethod
    def parse_yaml(cls: BaseModelProtocol, yaml_obj: Any):
        obj = yaml.load(yaml_obj, Loader=Loader)
        return cls.parse_obj(obj)
