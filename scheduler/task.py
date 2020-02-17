import asyncio


class Task:
    COMPLETED = "completed"
    FAILED = "failed"
    PENDING = "pending"
    RUNNING = "running"

    def __init__(self, task, task_id, job, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.task = task
        self.id = task_id
        self.job = job
        self.job.add_task(self)
        self.status = Task.PENDING
        self.lock = asyncio.Lock(loop=loop)

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

    async def run(self):
        async with self.lock:
            if self.pending:
                await self._do_run()

    async def _do_run(self):
        self.set_running()
        try:
            await self.task()
        except Exception as exc:
            self.fail()
        else:
            self.complete()

    def set_upstream(self, task):
        self.job.set_upstream(self, task)

    def downstream(self):
        return self.job.downstream(self)

    def __repr__(self):
        return f"<Task id={self.id!r} status={self.status} lock={self.lock}>"
