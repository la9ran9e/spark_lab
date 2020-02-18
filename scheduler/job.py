from .task import Task
from .dag import DAG


class Job:
    def __init__(self):
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

    def run(self):
        print(self.dag.graph)
        for task_id in self.dag.travers():
            task = self.tasks[task_id]
            if task.pending:
                task.run()
            else:
                print(f"task {task} already complete")
