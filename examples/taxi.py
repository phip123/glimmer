import logging

import glimmer.processing.factory as factory
from examples.taxi.nodes import TaxiSource, CalculateSpeedOp, AverageSpeedOp, TotalDistanceOp, time_to_unix, \
    filter_small_values, merge, persist, raw_persist

logging.basicConfig(level=logging.DEBUG)


def main():
    print("Parallel example - Taxi Topology")
    """
    Topology looks like this
        raw-persist                  Calculate speed - Calculate avg speed   
         /                         /                                        \
    Taxis - convert time to unix    - persist |                             merge - filter small values - Publish data
                                   \                                        /
                                        Calculate total distance                      
    """

    # Define source
    source = TaxiSource()

    # Define Operators
    calc_speed = CalculateSpeedOp()
    avg_speed = AverageSpeedOp()
    total_distance = TotalDistanceOp()
    time_to_unix_op = factory.mk_op(time_to_unix, 'time2unix')
    filter_op = factory.mk_op(filter_small_values, 'filter')
    merge_op = factory.mk_op(merge, 'merge')

    # Define sinks
    persist_op = factory.mk_sink(persist, 'persist')
    sink = factory.mk_sink(lambda x: print(x), 'sink')
    raw_persist_op = factory.mk_sink(raw_persist, 'raw_persist')

    # Connect nodes with each other
    source.send_to([raw_persist_op, time_to_unix_op])
    time_to_unix_op.send_to([total_distance, calc_speed, persist_op])
    calc_speed.send_to(avg_speed)
    merge_op.receive_from([total_distance, avg_speed])
    merge_op.send_to(filter_op)
    filter_op.send_to(sink)

    #  Create execution environment
    env = factory.mk_parallel_env([source], task_factory=factory.process_factory())

    env.start()
    print('Hit enter to stop environment')
    input()
    env.stop()
    print('Stopping')


if __name__ == '__main__':
    main()
