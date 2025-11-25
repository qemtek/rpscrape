import os
import tomli

from collections.abc import Mapping
from typing import Any

from settings import PROJECT_DIR


class Settings:
    def __init__(self) -> None:
        self.toml: Mapping[str, Any] | None = self.load_toml()

        if self.toml is None:
            self.fields: list[str] = []
            self.csv_header: str = ''
            return

        self.fields = self.get_fields()
        self.csv_header = ','.join(self.fields)

    def get_fields(self) -> list[str]:
        fields: list[str] = []

        if self.toml is None:
            return fields

        for group in self.toml.get('fields', {}):
            if group == 'betfair' and not self.toml.get('betfair_data', False):
                continue
            for field, enabled in self.toml['fields'][group].items():
                if enabled:
                    fields.append(field)

        return fields

    def load_toml(self) -> Mapping[str, Any] | None:
        default_path = f'{PROJECT_DIR}/settings/default_settings.toml'
        user_path = f'{PROJECT_DIR}/settings/user_settings.toml'

        path = user_path if os.path.isfile(user_path) else default_path
        if path == default_path and not os.path.isfile(default_path):
            raise FileNotFoundError(f'{default_path} does not exist')

        try:
            with open(path, 'rb') as f:
                return tomli.load(f)
        except tomli.TOMLDecodeError:
            print(f'TomlParseError: {path}')
            return None


if __name__ == '__main__':
    Settings()
