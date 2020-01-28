import logging
import logging.config
import os
from typing import MutableMapping, Dict

import yaml
from flatten_dict import flatten

logger = logging.getLogger(__name__)


class Context:
    """
       Factory for various controller services. Below are the environment variables that can be set:

       - Logging
            - home_controller_log_level (DEBUG|INFO|WARN| ... )
       """

    def __init__(self, env: MutableMapping = os.environ, config: dict = None, config_name: str = None, logging_config: Dict = None):
        super().__init__()
        self.config = config or dict()
        self.env = env
        if logging_config is not None:
            logging.config.dictConfig(logging_config)

        if config_name:
            ctx = load_context(config_name, self)
            self.config = ctx.config
            self.env = ctx.env

    def getenv(self, key, default=None):
        default = self.env.get(f'home_controller_{key}', default)
        return self.config.get(key, default)

    def create_logger(self, name: str) -> logging.Logger:
        return logging.getLogger(name)


def merge(ctx: Context, config: dict):
    return Context(ctx.env, config)


def load_context(config_name: str, ctx: Context = None) -> Context:
    ctx = ctx or Context()
    if os.path.exists("%s.yaml" % config_name):
        with open("%s.yaml" % config_name, 'r') as stream:
            try:
                config = yaml.safe_load(stream) or dict()
                # flatten dict to have environment file like keys
                flattened = flatten(config, 'underscore')
                logger.debug("read yaml file %s content: %s" % (config_name, config))
                return merge(ctx, flattened)
            except yaml.YAMLError as exc:
                logger.error(exc)
                raise exc
    else:
        return ctx
