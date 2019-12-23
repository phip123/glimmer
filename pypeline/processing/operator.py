import json
import logging
from json import JSONDecodeError
from typing import Dict

from pypeline.processing import Operator, In, Out
from pypeline.processing.rds import RedisMessage
from pypeline.util import EnhancedJSONEncoder
from pypeline.util.context import Context


class LogOperator(Operator[In, Out]):
    name = "log"

    def apply(self, data: In) -> Out:
        self.logger.info(data)
        return data


class RedisMessageToDictOperator(Operator[RedisMessage, Dict]):
    name = "rds-2-dict"

    def __init__(self, ctx: Context=None) -> None:
        super().__init__(ctx)

    def apply(self, msg: RedisMessage) -> Dict:
        try:
            return json.loads(msg.data)
        except JSONDecodeError:
            self.logger.error("Error parsing message as json: %s" % msg.data)
            return dict()


class ToJsonOperator(Operator[any, str]):
    name = '2-json'

    def apply(self, data: any) -> str:
        try:
            data = json.dumps(data, cls=EnhancedJSONEncoder)
            return data
        except JSONDecodeError:
            self.logger.error("%s could not write object as json, object was: %s" % (self.name, data))
