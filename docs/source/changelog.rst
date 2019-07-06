Changelog
=========

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


