from datetime import datetime, time

from .typing import Unit


def estimate_next_call(t, interval):
    return ((t + interval) // interval) * interval


def to_string(t):
    return datetime.utcfromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')


def from_string(str_time, pattern='%M:%S'):
    t = datetime.strptime(str_time, pattern).time()
    return from_time(t)


def from_time(t: time):
    return t.hour * Unit.seconds(Unit.HOUR) + t.minute * Unit.seconds(Unit.MINUTE) + t.second
