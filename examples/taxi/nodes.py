import math
from dataclasses import dataclass
from datetime import datetime
from time import sleep
from typing import Optional

from pypeline.processing import Source, Operator
from pypeline.util.context import Context


@dataclass
class RawTaxiData:
    id: int
    time: str
    longitude: float
    latitude: float


@dataclass
class TaxiData:
    id: int
    time: float
    longitude: float
    latitude: float


@dataclass
class TaxiWithSpeed:
    data: TaxiData
    speed: float


@dataclass
class TaxiWithDistance:
    data: TaxiData
    distance: float


def haversine(lat1, lng1, lat2, lng2) -> float:
    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)
    lng1 = math.radians(lng1)
    lng2 = math.radians(lng2)

    lng_ = (lng2 - lng1)
    lat_ = (lat2 - lat1)
    d = math.pow(math.sin(lat_ * 0.5), 2) + math.cos(lat1) * math.cos(lat2) * math.pow(math.sin(lng_ * 0.5), 2)
    return 2 * 6371 * math.asin(math.sqrt(d))


@dataclass
class Checkpoint:
    time: float
    longitude: float
    latitude: float


class CalculateSpeedOp(Operator[TaxiData, TaxiWithSpeed]):
    name = 'calc-speed'

    def __init__(self, ctx: Context = None) -> None:
        super().__init__(ctx)
        self.last_pos = dict()

    def apply(self, data: TaxiData, out):
        print(f'calc speed: {data}')
        last_checkpoint: Checkpoint = self.last_pos.get(data.id, Checkpoint(data.time, data.latitude, data.longitude))
        last_lat = last_checkpoint.latitude
        last_lng = last_checkpoint.longitude

        # kilometers
        distance = haversine(last_lat, last_lng, data.latitude, data.longitude)

        # seconds
        time_passed = data.time - last_checkpoint.time

        # km/h
        if time_passed != 0:
            speed = (distance / time_passed) * 60
        else:
            speed = 0

        out(TaxiWithSpeed(data=data, speed=speed))


class AverageSpeedOp(Operator[TaxiWithSpeed, TaxiWithSpeed]):
    name = 'avg-speed'

    def __init__(self, ctx: Context = None) -> None:
        super().__init__(ctx)
        # key: taxi id, value: (n, avg)
        self.last_speed = dict()

    def apply(self, data: TaxiWithSpeed, out):
        print(f'average speed: {data}')
        n, last_avg = self.last_speed.get(data.data.id, (0, 0))
        if n == 0:
            avg = 0
        else:
            avg = last_avg + ((data.speed - last_avg) / n)
        self.last_speed[data.data.id] = (n + 1, avg)
        out(TaxiWithSpeed(data=data.data, speed=avg))


class TotalDistanceOp(Operator[TaxiData, TaxiWithDistance]):
    name = 'total-distance'

    def __init__(self, ctx: Context = None) -> None:
        super().__init__(ctx)
        # key: id, value: ((last_lat, last_lng), total_distance)
        self.last_update = dict()

    def open(self):
        print('Open Total distance')

    def close(self):
        print('Close total distance')

    def apply(self, data: TaxiData, out):
        print(f'total distance: {data}')
        print(f'total distance sleep for 2 seconds')
        sleep(2)
        print(f'total distance woke up')
        (last_lat, last_lng), total_distance = self.last_update.get(data.id, ((data.latitude, data.longitude), 0))
        distance = haversine(last_lat, last_lng, data.latitude, data.longitude) + total_distance
        self.last_update[data.id] = ((data.latitude, data.longitude), distance)
        out(TaxiWithDistance(data=data, distance=distance))


class TaxiSource(Source[RawTaxiData]):
    name = 'taxi-source'

    def __init__(self, ctx: Context = None) -> None:
        super().__init__(ctx)
        self.index = 0
        raw_data = [
            '1,2008-02-02 15:36:08, 116.51172, 39.92123',
            '2,2008-02-02 13:33:52, 116.36422, 39.88781',
            '2,2008-02-02 13:37:16, 116.37481, 39.88782',
            '1,2008-02-02 15:46:08, 116.51135, 39.93883',
            '1,2008-02-02 15:56:08, 116.51627, 39.91034',
            '2,2008-02-02 13:38:53, 116.37677, 39.88791'
        ]
        self.data = []
        for item in raw_data:
            split = item.split(',')
            id = int(split[0])
            time = split[1]
            lat = float(split[2])
            lng = float(split[3])
            self.data.append(RawTaxiData(id, time, lat, lng))

    def read(self, out):
        print(f'read')
        index = self.index
        self.index += 1
        if index >= len(self.data):
            return
        out(self.data[index])
        sleep(5)


@dataclass
class MeasuredTaxi:
    data: TaxiData
    avg_speed: float
    total_distance: float


def time_to_unix(data: RawTaxiData) -> TaxiData:
    # 2008-02-02 15:36:08
    print(f'time2unix: {data}')
    time = datetime.strptime(data.time, '%Y-%m-%d %H:%M:%S').timestamp()
    return TaxiData(id=data.id, time=time, longitude=data.longitude, latitude=data.latitude)


def merge(items: dict) -> MeasuredTaxi:
    print(f'merge: {items}')
    speed: TaxiWithSpeed = items[AverageSpeedOp.name]
    distance: TaxiWithDistance = items[TotalDistanceOp.name]
    return MeasuredTaxi(data=speed.data, avg_speed=speed.speed, total_distance=distance.distance)


def filter_small_values(taxi: MeasuredTaxi) -> Optional[MeasuredTaxi]:
    print(f'filter small values: {taxi}')
    if taxi.avg_speed < 1 or taxi.total_distance < 1:
        return None
    return taxi


def persist(taxi):
    print(f'persist {taxi}')


def raw_persist(raw_taxi):
    print(f'raw persist: {raw_taxi}')
