import logging.config

from scheduler import Scheduler

from jobs.meat_recipes import job as meat_recipes


logging.config.fileConfig('logging.conf')

sched = Scheduler()


sched.register(meat_recipes)


if __name__ == "__main__":
    sched.run()
