import time

import pytest

from unittest.mock import PropertyMock
from scheduler import Scheduler, Job, Task
from scheduler.scheduler import HeartbeatDetails


seq = []


def foo():
    seq.append("foo")


def bar():
    seq.append("bar")


def error_task():
    raise Exception("TestException")


@pytest.fixture
def scheduler():
    sched = Scheduler()
    yield sched


@pytest.fixture
def cleanup_seq():
    yield
    seq.clear()


def test_register(scheduler):
    job0 = Job(name="job0")
    job1 = Job(name="job1")

    scheduler.register(job0)
    scheduler.register(job1)
    assert scheduler.jobs == {job0, job1}


def test_register_already_exists(scheduler):
    job = Job("job")
    scheduler.register(job)
    with pytest.raises(KeyError):
        scheduler.register(job)


@pytest.mark.usefixtures("cleanup_seq")
def test_run_pending(scheduler, mocker):
    job = Job()
    job.every().minute.at(second=10)
    foo_task = Task(foo, "foo_task", job)
    bar_task = Task(bar, "bar_task", job)

    foo_task.set_upstream(bar_task)
    scheduler.register(job)

    mocker.patch("scheduler.Job.next_run", new_callable=PropertyMock, return_value=time.time()-1)
    scheduler.run_pending()
    assert seq == ["foo", "bar"]


def test_on_job_failed(scheduler):
    failed_seq = []
    j = Job(name="test")
    Task(error_task, "error_task", j)

    def on_job_failed(job: Job):
        failed_seq.append(True)
        assert job.name == "test"

    scheduler.on_job_failed = on_job_failed

    scheduler.run_job(j)

    assert len(failed_seq) == 1


def test_heartbeat(scheduler):
    heartbeats_seq = []

    def on_heartbeat(details: HeartbeatDetails):
        heartbeats_seq.append(details.call_time)
        if len(heartbeats_seq) == 3:
            scheduler.should_stop = True

    scheduler.on_heartbeat = on_heartbeat
    scheduler.run(delay=1)
    assert len(heartbeats_seq) == 3
    for call_time in heartbeats_seq:
        assert call_time % 1 == 0.0
