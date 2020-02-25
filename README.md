# spark_lab

[![Build Status](https://travis-ci.com/la9ran9e/spark_lab.svg?branch=master)](https://travis-ci.com/la9ran9e/spark_lab)
[![Code Coverage Status](https://codecov.io/gh/la9ran9e/spark_lab/branch/master/graph/badge.svg)](https://codecov.io/gh/la9ran9e/spark_lab)


## Example projects

* [Spark pipeline](spark_pipeline.md).


## Scheduler

_Scheduler_ is a library for planning tasks execution.
There is several core terms for this tool:

* Task
* Job 
* Scheduler

Right now this supports only synchronous interface
so you can't parallelize your procedures.

### Quick start

Let's write our first job:
```python
import time

from scheduler.job import Job
from scheduler.task import Task


def foo():
    time.sleep(3)
    print("call", "foo")


def bar():
    print("call", "bar")


def baz():
    print("call", "baz")


def foobar():
    print("call", "foobar")


job = Job()

foo_task = Task(foo, "foo_task", job)
bar_task = Task(bar, "bar_task", job)
baz_task = Task(baz, "baz_task", job)
foobar_task = Task(foobar, "foobar", job)

foo_task.set_upstream(bar_task)  # set foo_task before bar_task
foo_task.set_upstream(baz_task)
foo_task.set_upstream(foobar_task)

print(bar_task.upstream())  # shows set of tasks must be completed before bar task: {foo_task, foobar_task}
```

You can instantly run this job:

```python
job.run()
```

...or schedule this using Scheduler:

```python
from scheduler import Scheduler


job.every(2).minute.at(second=30) # run this job every even minute at 30th second

scheduler = Scheduler()
scheduler.register(job)
scheduler.run()

```
