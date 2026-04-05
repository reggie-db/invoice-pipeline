import json
from dataclasses import dataclass
from typing import Any


@dataclass
class Tile:
    data: dict[str, Any]

    @property
    def tile_id(self) -> str:
        return self.data["tile_id"]

    @property
    def name(self) -> str:
        return self.data["name"]

    @property
    def output_json_schema(self) -> dict[str, Any]:
        output_json_schema_json = self.data["task_spec"]["custom_signature"]["output_json_schema"]
        return json.loads(output_json_schema_json)

