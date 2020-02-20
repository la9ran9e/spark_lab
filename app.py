from scheduler import Scheduler

from meat_recipes import job as meat_recipes


sched = Scheduler()


sched.register(meat_recipes)


if __name__ == "__main__":
    sched.run()
