import asyncio

from .task import Task
from .dag import DAG


class Job:
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.tasks = dict()
        self.dag = DAG()

    def add_task(self, task: Task):
        self.tasks[task.id] = task
        self.dag.add_node(task.id)

    def set_upstream(self, ind_task: Task, dep_task: Task):
        self.dag.add_edge(ind_task.id, dep_task.id)

    def downstream(self, task: Task):
        downstream = self.dag.downstream(task.id)
        return set(t for t in self.tasks.values() if t.id in downstream)

    def get_independent(self):
        independent = self.dag.get_independent()
        return set(t for t in self.tasks.values() if t.id in independent)

    async def run(self):
        print(self.dag.graph)
        tasks = []

        for task in self.get_independent():
            tasks.append(self._do_run(task))

        await asyncio.gather(*tasks, loop=self.loop)

    async def _do_run(self, task: Task):
        await task.run()
        for dep_task in task.downstream():
            await dep_task.run()
