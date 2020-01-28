import threading

import pypeline.processing.parallel as pr
from examples.taxi.nodes import TaxiSource, CalculateSpeedOp, AverageSpeedOp, TotalDistanceOp, time_to_unix, \
    filter_small_values, merge, persist
from pypeline.processing.parallel import ParallelPipe


def raw_persist(raw_taxi):
    print(f'raw persist: {raw_taxi}')


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
    time_to_unix_op = pr.mk_op(time_to_unix, 'time2unix')
    filter_op = pr.mk_op(filter_small_values, 'filter')
    merge_op = pr.mk_op(merge, 'merge')
    operators = [calc_speed, avg_speed, total_distance, time_to_unix_op, filter_op, merge_op]

    # Define sinks
    persist_op = pr.mk_sink(persist, 'persist')
    sink = pr.mk_sink(lambda x: print(x), 'sink')
    raw_persist_op = pr.mk_sink(raw_persist, 'raw_persist')
    sinks = [sink, persist_op, raw_persist_op]

    # Connect nodes with each other
    source.send_to([raw_persist_op, time_to_unix_op])
    time_to_unix_op.send_to([total_distance, calc_speed, persist_op])
    calc_speed.send_to(avg_speed)
    merge_op.receive_from([total_distance, avg_speed])
    merge_op.send_to(filter_op > sink)

    # Define topology
    topology = pr.ParallelTopology(sources=[source], operators=operators, sinks=sinks)
    stop = threading.Event()
    pipe = ParallelPipe(topology, stop)
    try:
        pipe.execute()
    except KeyboardInterrupt:
        print("Stopping pipe")
        stop.set()


if __name__ == '__main__':
    main()
