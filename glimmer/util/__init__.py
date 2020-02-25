import dataclasses
import json
import time


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


def generate_node_name() -> str:
    return str(time.time_ns())[5:-5]
