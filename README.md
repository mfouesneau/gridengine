GridEngine
==========

A lightweight Python library for distributed computing on Sun Grid Engines

This version is a fork of the original one [hbristow](https://github.com/hbristow/gridengine)

Main changes:

* runs with python 3
* starts jobs in the current directory
* lambda functions are usable (limited support)
* options

Introduction
------------
GridEngine streamlines the process of managing distributed computing on a Sun
Grid Engine. It was designed for iterating over algorithm and experiment design
on a computing cluster.

GridEngine was intentionally designed to match the API of the built-in
:class:`multiprocessing.Process` and :class:`threading.Thread` classes. If you
have ever used these, the `gridengine.Job` class will be familiar to you.

At its core, gridengine is designed to transparently schedule and execute `Job`s
on a Sun Grid Engine computing cluster and return the results once the jobs have
completed. All scheduling and communication while jobs are running are handled
by gridengine.

The component layout of gridengine can be visualized as follows:

            |       JobDispatcher  ------>  Scheduler
    Host    |           /\                    /
            |          /  \                  /
                      /    \                /
    Comms   | ZeroMQ /      \              / Sun Grid Engine
                    /        \            /
    Cluster |     Job0  ...  Job1  ...  JobN

Jobs are wrappers around a function and its arguments. Jobs are constructed on
the host and executed on the cluster. The :class:`JobDispatcher` is tasked with
collating and dispatching the jobs, then communicating with them once running.
The :class:`JobDispatcher` passes the jobs to the :class:`Scheduler` to be
invoked.

There are two schedulers:

* :class:`ProcessScheduler` which schedules jobs across processes on a
  multi-core computer (laptop, etc). This is handy for debugging and experiment
  design before scheduling thousands of jobs on the cluster
* :class: `GridEngineScheduler` which schedules jobs across nodes on a Sun Grid
  Engine (cluster). This scheduler can be used in any environment which uses
  DRMAA, it is not strictly limited to SGE.

Once the jobs have have scheduled, they contact the :class:`JobDispatcher` for
their job allocation, run the job, and submit the results back to the dispatcher
before terminating.

Features
--------
 * A distributed functional :func:`map`
 * :class:`ProcessScheduler` and :class:`GridEngineScheduler` schedulers for
   testing tasks on a laptop, then scaling them up to the Grid Engine
 * :class:`gridengine.Job` API compatible with :class:`threading.Thread` and
   :class:`multiprocessing.Process`

Installation
------------
Get gridengine from [github](https://github.com/hbristow/gridengine) and install using pip:

    pip install git+https://github.com/mfouesneau/gridengine

This will automatically pull and build the dependencies.

Example
-------

```python
import gridengine

def f(x):
  """compute the square of a number"""
  return x*x

scheduler = gridengine.schedulers.best_available()

x = [1, 2, 3, 4, 5]
y = gridengine.map(f, x, scheduler=scheduler)
```

See `gridengine/example.py` for a runnable example.

Branches/Contributing
---------------------
The gridengine branching schema is set up as follows:

    master    reflects the current latest stable version
    dev       the development track, sometimes unstable
    issue-x   bugfix branches, with issue number from GitHub
    feat-x    feature branches
