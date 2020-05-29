Changelog
=========

Development version
-------------------

- all cluster classes: add ``scheduler_options`` allows to pass parameters to
  the Dask scheduler. For example ``scheduler_options={'interface': 'eth0',
  dashboard_addresses=':12435')`` (:pr:`384`). Breaking change: using ``port``
  or ``dashboard_addresses`` arguments raises an error. They have to be passed
  through ``scheduler_options``. ``scheduler_options`` can be set through the
  config file in the ``scheduler-options`` section (:pr:`405`).
- all cluster classes: ``processes`` parameter default has changed. By default,
  ``processes ~= sqrt(cores)`` so that the number of processes and the number
  of threads per process is roughly the same. Old default was to use one
  process and only threads, i.e. ``proccesses=1``,
  ``threads_per_process=cores``. (:pr:`375`)
- all cluster classes: ``interface`` was ignored when set in a config file.
  (:pr:`366`)
- all cluster classes: fix a bug that would allow to pass any named parameter without an error (:pr:`398`)
- all cluster classes: fix a bug where ``security`` was not correctly passed through (:pr:`398`)
- ``LSFCluster``: switch to ``use_stdin=True`` by default (:pr:`388`).
- ``LSFCluster``: add ``use_stdin`` to ``LSFCluster``. This switches between
  ``bsub < job_script`` and ``bsub job_script`` to launch a ``LSF`` job
  (:pr:`360`).
- ``HTCondorCluster``: support older ``HTCondor`` versions without ``-file``
  argument (:pr:`351`).
- ``OARCluster``: fix bug (forgotten async def) in ``OARCluster._submit_job`` (:pr:`380`).

0.7.0 / 2019-10-09
------------------

- Base Dask-Jobqueue on top of the core ``dask.distributed.SpecCluster`` class
  (:pr:`307`)

  This is nearly complete reimplementation of the dask-jobqueue logic on top
  of more centralized logic.  This improves standardization and adds new
  features, but does include the following **breaking changes**:

  + The ``cluster.start_workers`` method has been removed. Use
    ``cluster.scale`` instead.
  + The ``cluster.stop_all_jobs()`` method has been removed.
    Please use ``cluster.scale(0)`` instead.
  + The attributes ``running_jobs``, ``pending_jobs``, and
    ``cancelled_jobs`` have been removed.  These have been moved upstream to
    the ``dask.distributed.SpecCluster`` class instead as ``workers`` and
    ``worker_spec``, as well as ``.plan``, ``.requested``, and ``.observed``.
  + The ``name`` attribute has been moved to ``job_name``.
- You can now specify jobs in ``.scale`` and ``.adapt``: for example
  ``cluster.scale(jobs=2)`` and ``cluster.adapt(minimum_jobs=0,
  maximum_jobs=10)``. Specifying scaling in terms of jobs is generally more
  intuitive than in terms of Dask workers. This was part of :pr:`307`.
- Update ``.scale()`` and ``.adapt()`` docstrings (:pr:`346`)
- Update interactive docs (:pr:`340`)
- Improve error message when cores or memory is not specified (:pr:`331`)
- Fix Python 3.5.0 support in setup.py (:pr:`317`)


0.6.3 / 2019-08-18
------------------

- Compatibility with Dask 2.3.0: add scheduler_info from
  local_cluster (:pr:`313`)
- Remove lingering Python 2 specific code (:pr:`308`)
- Remove __future__ imports since we depend on Python >3.5 (:pr:`311`)
- Remove Python 3 check for black in CI (:pr:`315`)

0.6.2 / 2019-07-31
------------------

- Ensure compatibility with Dask 2.2 (:pr:`303`)
- Update documentation

0.6.1 / 2019-07-25
------------------

- more fixes related to ``distributed >= 2`` changes (:pr:`278`, :pr:`291`)
- ``distributed >= 2.1`` is now required (:pr:`295`)
- remove deprecated ``threads`` parameter from all the ``Cluster`` classes (:pr:`297`)
- doc improvements (:pr:`290`, :pr:`294`, :pr:`296`)

0.6.0 / 2019-07-06
------------------

- Drop Python 2 support (:pr:`284`)
- Fix adaptive compatibility with SpecificationCluster in Distributed 2.0 (:pr:`282`)

0.5.0 / 2019-06-20
------------------

- Keeping up to date with Dask and Distributed (:pr:`268`)
- Formatting with Black (:pr:`256`, :pr:`248`)
- Improve some batch scheduler integration (:pr:`274`, :pr:`256`, :pr:`232`)
- Add HTCondor compatibility (:pr:`245`)
- Add the possibility to specify named configuration (:pr: `204`)
- Allow free configuration of Dask diagnostic_port (:pr: `192)`
- Start work on ClusterManager, see https://github.com/dask/distributed/issues/2235 (:pr:`187`, :pr:`184`, :pr:`183`)
- A lot of other tiny fixes and improvements(:pr:`277`, :pr:`261`, :pr:`260`, :pr:`250`, :pr:`244`, :pr:`200`, :pr:`189`)

0.4.1 / 2018-10-18
------------------

- Handle worker restart with clearer message (:pr:`138`)
- Better error handling on job submission failure (:pr:`146`)
- Fixed Python 2.7 error when starting workers (:pr:`155`)
- Better handling of extra scheduler options (:pr:`160`)
- Correct testing of Python 2.7 compatibility (:pr:`154`)
- Add ability to override python used to start workers (:pr:`167`)
- Internal improvements and edge cases handling (:pr:`97`)
- Possibility to specify a folder to store every job logs file (:pr:`145`)
- Require all cores on the same node for LSF (:pr:`177`)

0.4.0 / 2018-09-06
------------------

- Use number of worker processes as an argument to ``scale`` instead of
  number of jobs.
- Bind scheduler bokeh UI to every network interfaces by default.
- Adds an OAR job queue system implementation.
- Adds an LSF job queue system implementation.
- Adds some convenient methods to JobQueueCluster objects: ``__repr__``,
  ``stop_jobs()``, ``close()``.


