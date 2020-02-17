import asyncio
import pytest

from scheduler.job import Job
from scheduler.task import Task


async def foo():
    await asyncio.sleep(1)
    print("call", "foo")


async def bar():
    print("call", "bar")


async def baz():
    print("call", "baz")


async def foobar():
    print("call", "foobar")


@pytest.mark.asyncio
async def test_upstream():
    job = Job()
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
