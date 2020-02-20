import time

from .task import Task
from .dag import DAG
from .utils import estimate_next_call, from_string


class Unit:
    MINUTE = "minute"
    HOUR = "hour"

    SECONDS = {
        MINUTE: 60,
        HOUR: 3600,
    }

    @classmethod
    def seconds(cls, unit):
        return cls.SECONDS[unit]


class Job:
    def __init__(self):
        self.tasks = dict()
        self.dag = DAG()
        self.interval = 0
        self.unit = None
        self.at_time = None

    def add_task(self, task: Task):
        self.tasks[task.id] = task
        self.dag.add_node(task.id)

    def set_upstream(self, ind_task: Task, dep_task: Task):
        self.dag.add_edge(dep_task.id, ind_task.id)

    def downstream(self, task: Task):
        downstream = self.dag.downstream(task.id)
        return set(t for t in self.tasks.values() if t.id in downstream)

    def get_independent(self):
        independent = self.dag.get_independent()
        return set(t for t in self.tasks.values() if t.id in independent)

    def run(self):
        print(self.dag.graph)
        for task_id in self.dag.travers():
            task = self.tasks[task_id]
            if task.pending:
                task.run()
            else:
                print(f"task {task} already complete")

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

    def at(self, str_time):
        if self.unit == Unit.MINUTE:
            self.at_time = int(str_time)
        elif self.unit == Unit.HOUR:
            self.at_time = from_string(str_time)

    @property
    def next_run(self):
        return estimate_next_call(time.time(), self.interval * Unit.seconds(self.unit)) + self.at_time

    @property
    def should_run(self):
        return time.time() > self.next_run
