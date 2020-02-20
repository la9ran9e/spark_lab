import time

import pytest

from datetime import datetime

from scheduler.job import Job, Unit
from scheduler.task import Task
from scheduler.dag import CyclicDependenceError
from scheduler.utils import estimate_next_call


def foo():
    time.sleep(1)
    print("call", "foo")


def bar():
    print("call", "bar")


def baz():
    print("call", "baz")


def foobar():
    print("call", "foobar")


@pytest.fixture
def job():
    job = Job()
    yield job


def test_upstream(job):
    foo_task = Task(foo, "foo_task", job)
    bar_task = Task(bar, "bar_task", job)
    baz_task = Task(baz, "baz_task", job)
    foobar_task = Task(foobar, "foobar", job)

    foo_task.set_upstream(bar_task)
    foo_task.set_upstream(baz_task)
    foobar_task.set_upstream(foo_task)

    assert foo_task.upstream == {foobar_task}
    assert bar_task.upstream == {foo_task, foobar_task}
    assert foobar_task.upstream == set()


def test_cyclic_stream(job):
    foo_task = Task(foo, "foo_task", job)
    bar_task = Task(bar, "bar_task", job)
    foobar_task = Task(foobar, "foobar", job)
    foo_task.set_upstream(bar_task)

    with pytest.raises(CyclicDependenceError):
        bar_task.set_upstream(foo_task)

    foobar_task.set_upstream(foo_task)

    with pytest.raises(CyclicDependenceError):
        bar_task.set_upstream(foobar_task)


def test_add_duplicated_task_id(job):
    Task(foo, "task_id", job)
    with pytest.raises(KeyError):
        Task(bar, "task_id", job)


def test_get_independent(job):
    assert job.get_independent() == set()

    foo_task = Task(foo, "foo_task", job)
    bar_task = Task(bar, "bar_task", job)
    baz_task = Task(baz, "baz_task", job)
    foobar_task = Task(foobar, "foobar", job)

    assert job.get_independent() == {foo_task, bar_task, baz_task, foobar_task}

    foo_task.set_upstream(bar_task)
    assert job.get_independent() == {bar_task, baz_task, foobar_task}

    foo_task.set_upstream(baz_task)
    assert job.get_independent() == {bar_task, baz_task, foobar_task}

    foobar_task.set_upstream(foo_task)
    assert job.get_independent() == {bar_task, baz_task}

    bar_task.set_upstream(baz_task)
    assert job.get_independent() == {baz_task}


def test_schedule(job):
    job.every(2).hour.at(minute=3, second=15)
    assert job.unit == Unit.HOUR
    assert job.at_time == 3 * 60 + 15
    dt = datetime.utcfromtimestamp(job.next_run)
    assert dt.minute == 3
    assert dt.second == 15
    assert dt.hour % 2 == 0


def test_schedule_every_minute(job):
    job.every().minute.at(second=10)
    assert job.unit == Unit.MINUTE
    assert job.at_time == 10
    dt = datetime.utcfromtimestamp(job.next_run)
    assert dt.second == 10


def test_should_run(job, mocker):
    job.every().minute.at(second=10)
    now = time.time()
    should_run = job.should_run
    d = now - job.next_run
    if d > 0:
        expected = True
        now += (d + 0.01)
    else:
        expected = False
        now -= (d + 0.01)

    assert should_run is expected
    # print(now)
    # mocker.patch("time.time", return_value=now)
    # assert job.should_run is not expected
