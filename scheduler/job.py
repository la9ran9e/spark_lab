import datetime
import logging
import time

from .task import Task
from .dag import DAG
from .utils import estimate_next_call, from_time
from .typing import Unit


logger = logging.getLogger(__name__)


class Job:
    def __init__(self, name=None):
        self.name = name
        self.tasks = dict()
        self.dag = DAG()
        self.interval = 0
        self.unit = None
        self.at_time = None
        self.last_run = None

    def add_task(self, task: Task):
        self.tasks[task.id] = task
        self.dag.add_node(task.id)

    def set_upstream(self, ind_task: Task, dep_task: Task):
        self.dag.add_edge(dep_task.id, ind_task.id)

    def upstream(self, task: Task):
        upstream = self.dag.upstream(task.id)
        return set(t for t in self.tasks.values() if t.id in upstream)

    def get_independent(self):
        independent = self.dag.get_independent()
        return set(t for t in self.tasks.values() if t.id in independent)

    def run(self):
        self.last_run = time.time()
        try:
            for task_id in self.dag.travers():
                task = self.tasks[task_id]
                if task.pending:
                    task.run()
                else:
                    print(f"task {task} already complete")
        finally:
            self.reset_tasks()

    def reset_tasks(self):
        for task in self.tasks.values():
            task.reset()

    def every(self, interval=1):
        self.interval = interval
        return self

    @property
    def minute(self):
        self.unit = Unit.MINUTE
        return self

    @property
    def hour(self):
        self.unit = Unit.HOUR
        return self

    @property
    def day(self):
        raise NotImplementedError()

    def at(self, **kwargs):
        t = datetime.time(**kwargs)
        self.at_time = from_time(t)

    @property
    def next_run(self):
        return estimate_next_call(time.time(), self.interval * Unit.seconds(self.unit)) + self.at_time - self.interval * Unit.seconds(self.unit)

    @property
    def should_run(self):
        next_run = self.next_run
        if self.last_run and self.last_run > next_run:
            return False
        return time.time() > next_run

    def __repr__(self):
        return f"<Job name={self.name!r}>"
