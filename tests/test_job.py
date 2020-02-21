import time

import pytest

from datetime import datetime
from unittest.mock import PropertyMock

from scheduler.job import Job, Unit
from scheduler.task import Task
from scheduler.dag import CyclicDependenceError


seq = []


def foo():
    time.sleep(1)
    seq.append("foo")


def bar():
    seq.append("bar")


def baz():
    seq.append("baz")


def foobar():
    seq.append("foobar")


@pytest.fixture
def job():
    job = Job()
    yield job


@pytest.fixture
def cleanup_seq():
    yield
    seq.clear()


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
    next_run = job.next_run
    mocker.patch("scheduler.Job.next_run", new_callable=PropertyMock, return_value=next_run)
    mocker.patch("time.time", return_value=now)

    d = now - job.next_run
    if d > 0:
        expected = True
        now -= (d + 0.01)
    else:
        expected = False
        now -= (d - 0.01)

    assert job.should_run is expected

    mocker.patch("time.time", return_value=now)
    assert job.should_run is not expected


def test_should_run_with_last_update(job, mocker):
    job.every().minute.at(second=10)
    now = time.time()
    next_run = job.next_run
    job.last_run = next_run + 1.0

    mocker.patch("scheduler.Job.next_run", new_callable=PropertyMock, return_value=next_run)
    mocker.patch("time.time", return_value=now)

    assert job.should_run is False


def test_reset_tasks(job):
    Task(foo, "foo_task", job)
    Task(bar, "bar_task", job)
    Task(baz, "baz_task", job)
    Task(foobar, "foobar", job)

    for task in job.tasks.values():
        task.complete()

    for task in job.tasks.values():
        assert task.completed is True

    job.reset_tasks()

    for task in job.tasks.values():
        assert task.pending is True


@pytest.mark.usefixtures("cleanup_seq")
def test_run(job):
    foo_task = Task(foo, "foo_task", job)
    bar_task = Task(bar, "bar_task", job)
    baz_task = Task(baz, "baz_task", job)
    foobar_task = Task(foobar, "foobar", job)

    foo_task.set_upstream(bar_task)
    foo_task.set_upstream(baz_task)
    foobar_task.set_upstream(foo_task)
    bar_task.set_upstream(baz_task)

    job.run()

    assert seq == ["foobar", "foo", "bar", "baz"]


@pytest.mark.usefixtures("cleanup_seq")
def test_complex_case(job):
    # """
    #     -r00 -       - r01
    #     /   |  \     /   |
    #    /    |   \   /    |
    #   v     v    v v     v
    #  r10   r11   r12    r13
    #   -      |   |      -
    #    \     |   |     /
    #     \    |   |    /
    #      \   |   |   /
    #       \  v   v  /
    #        v  r20  v
    # """
    r00 = Task(lambda: seq.append("r00"), "r00", job)
    r01 = Task(lambda: seq.append("r01"), "r01", job)
    r10 = Task(lambda: seq.append("r10"), "r10", job)
    r11 = Task(lambda: seq.append("r11"), "r11", job)
    r12 = Task(lambda: seq.append("r12"), "r12", job)
    r13 = Task(lambda: seq.append("r13"), "r13", job)
    r20 = Task(lambda: seq.append("r20"), "r20", job)

    r00.set_upstream(r10)
    r00.set_upstream(r11)
    r00.set_upstream(r12)
    r01.set_upstream(r12)
    r01.set_upstream(r13)
    r10.set_upstream(r20)
    r11.set_upstream(r20)
    r12.set_upstream(r20)
    r13.set_upstream(r20)

    assert r20.upstream == {r10, r11, r12, r13, r00, r01}
    assert r10.upstream == {r00}
    assert r11.upstream == {r00}
    assert r12.upstream == {r00, r01}
    assert r13.upstream == {r01}
