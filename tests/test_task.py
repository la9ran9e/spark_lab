import pytest

from scheduler import Task, Job
from scheduler.task import TaskFailedError


def error_func():
    raise Exception("TestException")


def test_on_failed():
    failed_seq = []

    def on_failed(self):
        failed_seq.append(True)
        assert self.status == Task.FAILED

    job = Job()
    task = Task(error_func, "error_task", job, on_failed=on_failed)
    with pytest.raises(TaskFailedError):
        task.run()

    assert len(failed_seq) == 1
