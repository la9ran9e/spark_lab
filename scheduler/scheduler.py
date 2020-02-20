import time

from .job import Job
from .utils import estimate_next_call, to_string


class Scheduler:
    def __init__(self):
        self.jobs = set()

    def register(self, job: Job):
        if job in self.jobs:
            raise KeyError(f"Such job {job} is already registered")
        self.jobs.add(job)

    def run(self, delay=1):
        while True:
            t = time.time()
            print(f"Now is {to_string(t)}")
            call_time = estimate_next_call(t, delay)
            print(f"Call time {to_string(call_time)}")
            time.sleep(call_time - t)
            self.run_pending()

    def run_pending(self):
        for job in self.jobs:
            if job.should_run:
                self.run_job(job)

    def run_job(self, job: Job):
        job.run()
