import json
from json import JSONDecodeError
from typing import Callable

from pypeline.processing import Operator, In, Out
from pypeline.util import EnhancedJSONEncoder


class LogOperator(Operator[In, Out]):
    name = "log"

    def apply(self, data: In, out: Callable[[Out], None]):
        self.logger.info(data)
        out(data)


class ToJsonOperator(Operator[any, str]):
    name = '2-json'

    def apply(self, data: any, out: Callable[[str], None]):
        try:
            data = json.dumps(data, cls=EnhancedJSONEncoder)
            out(data)
        except JSONDecodeError:
            self.logger.error("%s could not write object as json, object was: %s" % (self.name, data))
