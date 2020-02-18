import time

import pytest

from scheduler.job import Job
from scheduler.task import Task
from scheduler.dag import CyclicDependenceError


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

    bar_task.set_upstream(foo_task)
    baz_task.set_upstream(foo_task)
    foo_task.set_upstream(foobar_task)

    assert foo_task.downstream() == {foobar_task}
    assert bar_task.downstream() == {foo_task, foobar_task}
    assert foobar_task.downstream() == set()


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

    bar_task.set_upstream(foo_task)
    assert job.get_independent() == {bar_task, baz_task, foobar_task}

    baz_task.set_upstream(foo_task)
    assert job.get_independent() == {bar_task, baz_task, foobar_task}

    foo_task.set_upstream(foobar_task)
    assert job.get_independent() == {bar_task, baz_task}

    baz_task.set_upstream(bar_task)
    assert job.get_independent() == {baz_task}
