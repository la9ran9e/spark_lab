from datetime import datetime


def estimate_next_call(t, interval):
    return ((t + interval) // interval) * interval


def to_string(t):
    return datetime.utcfromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')


def from_string(str_time, pattern='%M:%S'):
    t = datetime.strptime(str_time, pattern).time()
    return t.hour * 3600 + t.minute * 60 + t.second


print(from_string("3:15"))
