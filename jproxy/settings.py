from pathlib import Path
from typing import List, Dict, Union
from functools import cache

from pydantic import HttpUrl, BaseModel
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource
)

class Target(BaseModel):
    name: str
    hidden: bool = False


class Credentials(BaseModel):
    accessKeyPath: Path
    secretKeyPath: Path


class S3LikeTarget(Target):
    endpoint: HttpUrl = None
    bucket: str
    prefix: str = None
    credentials: Credentials = None


class LocalTarget(Target):
    name: str
    path: Path


class Settings(BaseSettings):
    """ Settings can be read from a settings.yaml file, 
        or from the environment, with environment variables prepended 
        with "jproxy_" (case insensitive). The environment variables can
        be passed in the environment or in a .env file. 
    """

    base_url: HttpUrl = 'http://127.0.0.1:8000/'
    targets: List[Union[S3LikeTarget, LocalTarget]] = []
    target_map: Dict[str, Target] = {}

    model_config = SettingsConfigDict(
        yaml_file="config.yaml",
        env_file='.env',
        env_prefix='jproxy_',
        env_nested_delimiter="__",
        env_file_encoding='utf-8'
    )

    def __init__(self, **data) -> None:
        super().__init__(**data)
        self.target_map = {t.name.lower(): t for t in self.targets}


    def get_targets(self):
        return [k for k in self.target_map if not self.target_map[k].hidden]


    def get_target_config(self, name):
        if name:
            key = name.lower()
            if key in self.target_map:
                return self.target_map[key]
        return None

        
    @classmethod
    def settings_customise_sources(  # noqa: PLR0913
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )


@cache
def get_settings():
    return Settings()
