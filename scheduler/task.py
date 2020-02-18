class Task:
    COMPLETED = "completed"
    FAILED = "failed"
    PENDING = "pending"
    RUNNING = "running"

    def __init__(self, task, task_id, job):
        self.task = task
        self.id = task_id
        self.job = job
        self.job.add_task(self)
        self.status = Task.PENDING

    @property
    def completed(self):
        return self.status == Task.COMPLETED

    @property
    def pending(self):
        return self.status == Task.PENDING

    @property
    def running(self):
        return self.status == Task.RUNNING

    def set_running(self):
        self.status = Task.RUNNING

    def complete(self):
        self.status = Task.COMPLETED

    def fail(self):
        self.status = Task.FAILED

    def run(self):
        self.set_running()
        try:
            self.task()
        except Exception as exc:
            self.fail()
        else:
            self.complete()

    def set_upstream(self, task):
        """
        Usage:
            >>> foo_task = Task(...)
            >>> bar_task = Task(...)
            >>> foo_task.set_upstream(bar_task)  # ... --> foo_task -->  bar_task --> ...

        :param task: dependent task
        :return:
        """
        self.job.set_upstream(self, task)

    def downstream(self):
        return self.job.downstream(self)

    def __repr__(self):
        return f"<Task id={self.id!r} status={self.status}>"
