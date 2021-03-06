import logging
import time

from .job import Job
from .task import TaskFailedError
from .utils import estimate_next_call, to_string

logger = logging.getLogger(__name__)


class HeartbeatDetails:
    def __init__(self, time, call_time):
        self.time = time
        self.call_time = call_time


class Scheduler:
    def __init__(self, on_job_failed=None, on_heartbeat=None):
        self.jobs = set()
        self.on_job_failed = on_job_failed
        self.on_heartbeat = on_heartbeat
        self.should_stop = False

    def register(self, job: Job):
        if job in self.jobs:
            raise KeyError(f"Such job {job} is already registered")
        self.jobs.add(job)

    def run(self, delay=1):
        while not self.should_stop:
            details = self._heartbeat(delay)
            if self.on_heartbeat:
                self.on_heartbeat(details)

    def _heartbeat(self, delay):
        t = time.time()
        logger.debug(f"Now is {to_string(t)}")
        call_time = estimate_next_call(t, delay)
        logger.debug(f"Call time {to_string(call_time)}")
        time.sleep(call_time - t)
        self.run_pending()
        return HeartbeatDetails(t, call_time)

    def run_pending(self):
        for job in self.jobs:
            if job.should_run:
                self.run_job(job)

    def run_job(self, job: Job):
        try:
            job.run()
        except TaskFailedError:
            logger.error(f"Job {job} stream interrupted")
            if self.on_job_failed:
                self.on_job_failed(job)
