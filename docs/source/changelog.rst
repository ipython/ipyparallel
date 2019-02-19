Changelog
=========

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


