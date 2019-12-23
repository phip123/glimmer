from typing import Optional, Dict, Type

from pypeline.processing import Sink, Operator, Source
from pypeline.processing.operator import LogOperator, RedisMessageToDictOperator, ToJsonOperator
from pypeline.processing.rabbitmq import RabbitMqSink
from pypeline.processing.rds import RedisSource, RedisPublisherSink, SleepingRedisSource
from pypeline.processing.sink import NoopSink
from pypeline.util.context import Context

sinks = dict()
sources = dict()
operators = dict()


def register_sink(sink: Sink):
    sinks[sink.name] = sink


def register_operator(operator: Operator):
    operators[operator.name] = operator


def register_source(source: Source):
    sources[source.name] = source


def get_sink(name: str) -> Optional[Sink]:
    return sinks.get(name)


def get_source(name: str) -> Optional[Source]:
    return sources.get(name)


def get_operator(name: str) -> Optional[Operator]:
    return operators.get(name)

def create_and_register_operator(operator: Type) -> bool:
    """
    """


def init_defaults(ctx: Context = None, contexts: Dict[str, Context] = None):
    """
    Helper method to initialize all sources, sinks and operators provided in their processing module
    Before initializing each operator/sink/source it makes sure that this was not already added, to avoid loose
    connections
    """
    ctx = ctx or Context()
    contexts = contexts or dict()
    if not get_operator(LogOperator.name):
        context = contexts.get(LogOperator.name, ctx)
        register_operator(LogOperator(ctx=context))

    if not get_operator(RedisMessageToDictOperator.name):
        context = contexts.get(RedisMessageToDictOperator.name, ctx)
        register_operator(RedisMessageToDictOperator(ctx=context))

    if not get_source(RedisSource.name):
        context = contexts.get(RedisSource.name, ctx)
        register_source(RedisSource(ctx=context))

    if not get_source(SleepingRedisSource.name):
        context = contexts.get(SleepingRedisSource.name, ctx)
        register_source(SleepingRedisSource(ctx=context))

    if not get_sink(RabbitMqSink.name):
        context = contexts.get(RabbitMqSink.name, ctx)
        register_sink(RabbitMqSink(ctx=context))

    if not get_sink(NoopSink.name):
        context = contexts.get(NoopSink.name, ctx)
        register_sink(NoopSink(ctx=context))

    if not get_operator(ToJsonOperator.name):
        context = contexts.get(ToJsonOperator.name)
        register_operator(ToJsonOperator(ctx=context))

    if not get_sink(RedisPublisherSink.name):
        context = contexts.get(RedisPublisherSink.name)
        register_sink(RedisPublisherSink(ctx=context))
