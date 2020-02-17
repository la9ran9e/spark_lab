import asyncio
import pytest

from scheduler.job import Job
from scheduler.task import Task
from scheduler.dag import CyclicDependenceError


async def foo():
    await asyncio.sleep(1)
    print("call", "foo")


async def bar():
    print("call", "bar")


async def baz():
    print("call", "baz")


async def foobar():
    print("call", "foobar")


@pytest.fixture
def job(event_loop):
    job = Job(loop=event_loop)
    yield job


@pytest.mark.asyncio
async def test_upstream(job, event_loop):
    foo_task = Task(foo, "foo_task", job, loop=event_loop)
    bar_task = Task(bar, "bar_task", job, loop=event_loop)
    baz_task = Task(baz, "baz_task", job, loop=event_loop)
    foobar_task = Task(foobar, "foobar", job, loop=event_loop)

    bar_task.set_upstream(foo_task)
    baz_task.set_upstream(foo_task)
    foo_task.set_upstream(foobar_task)

    assert foo_task.downstream() == {foobar_task}
    assert bar_task.downstream() == {foo_task, foobar_task}
    assert foobar_task.downstream() == set()


@pytest.mark.asyncio
async def test_cyclic_stream(job, event_loop):
    foo_task = Task(foo, "foo_task", job, loop=event_loop)
    bar_task = Task(bar, "bar_task", job, loop=event_loop)
    foobar_task = Task(foobar, "foobar", job, loop=event_loop)
    foo_task.set_upstream(bar_task)

    with pytest.raises(CyclicDependenceError):
        bar_task.set_upstream(foo_task)

    foobar_task.set_upstream(foo_task)

    with pytest.raises(CyclicDependenceError):
        bar_task.set_upstream(foobar_task)


@pytest.mark.asyncio
async def test_add_duplicated_task_id(job, event_loop):
    Task(foo, "task_id", job, loop=event_loop)
    with pytest.raises(KeyError):
        Task(bar, "task_id", job, loop=event_loop)
