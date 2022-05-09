(changelog)=

# Changelog

Changes in IPython Parallel

## 8.3

### 8.3.0

([full changelog](https://github.com/ipython/ipyparallel/compare/8.2.1...8.3.0))

8.3.0 is a small release, with some bugfixes and improvements to the release process.

Build fixes:

- Workaround SSL issues with recent builds of nodejs + webpack
- Build with flit, removing setup.py

Fixes:

- Remove remaining references to deprecated `distutils` package (has surprising impact on process memory)
- Improve logging when engine registration times out

Maintenance changes that shouldn't affect users:

- Releases are now built with pip instead of `setup.py`
- Updates to autoformatting configuration

#### Contributors to this release

([GitHub contributors page for this release](https://github.com/ipython/ipyparallel/graphs/contributors?from=2022-04-01&to=2022-05-09&type=c))

[@blink1073](https://github.com/search?q=repo%3Aipython%2Fipyparallel+involves%3Ablink1073+updated%3A2022-04-01..2022-05-09&type=Issues) | [@dependabot](https://github.com/search?q=repo%3Aipython%2Fipyparallel+involves%3Adependabot+updated%3A2022-04-01..2022-05-09&type=Issues) | [@jburroni](https://github.com/search?q=repo%3Aipython%2Fipyparallel+involves%3Ajburroni+updated%3A2022-04-01..2022-05-09&type=Issues) | [@kloczek](https://github.com/search?q=repo%3Aipython%2Fipyparallel+involves%3Akloczek+updated%3A2022-04-01..2022-05-09&type=Issues) | [@minrk](https://github.com/search?q=repo%3Aipython%2Fipyparallel+involves%3Aminrk+updated%3A2022-04-01..2022-05-09&type=Issues) | [@pre-commit-ci](https://github.com/search?q=repo%3Aipython%2Fipyparallel+involves%3Apre-commit-ci+updated%3A2022-04-01..2022-05-09&type=Issues)

## 8.2

### 8.2.1

8.2.1 Fixes some compatibility issues with latest dask, ipykernel, and setuptools,
as well as some typos and improved documentation.

### 8.2.0

8.2.0 is a small release, mostly of small bugfixes and improvements.

Changes:

`len(AsyncMapResult)` and progress ports now use the number of items in the map,
not the number of messages.

Enhancements:

- Show output prior to errors in `%%px`

Bugs fixed:

- Fix cases where engine id could be `-1` in tracebacks
- Add missing `pbs` to engine launcher entrypoints

[All changes on GitHub](https://github.com/ipython/ipyparallel/compare/8.1.0...8.2.0)

## 8.1

8.1.0 is a small release, adding a few new features and bugfixes.

New features:

- relay KeyboardInterrupt to engines in blocking `%px` magics
- add `Cluster.start_and_connect(activate=True)` to include activation of `%px` magics in one-liner startup.
- initial support for Clusters tab in RetroLab

Fixes:

- ensure profile config is always loaded for `Cluster(profile="xyz")`
- build lab extension in production mode, apply trove classifiers
- pass through keyword arguments to constructor in `Client.broadcast_view`

## 8.0

This is marked as a major revision because of the change to pass connection information via environment variables.
BatchSystem launchers with a custom template will need to make sure to set flags that inherit environment variables,
such as `#PBS -V` or `#SBATCH --export=ALL`.

New:

- More convenient `Cluster(engines="mpi")` signature for setting the engine (or controller) launcher class.
- The first (and usually only) engine set can be accessed as {attr}`.Cluster.engine_set`,
  rather than digging through the {attr}`Cluster.engines` dict.
- Add `environment` configuration to all Launchers.
- Support more configuration via environment variables,
  including passing connection info to engines via `$IPP_CONNECTION_INFO`,
  which is used by default, avoiding the need to send connection files to engines in
  cases of non-shared filesystems.
- Launchers send connection info to engines via `$IPP_CONNECTION_INFO` by default.
  This is governed by `Cluster.send_engines_connection_env`, which is True by default.
- Support {meth}`EngineLauncher.get_output` via output files in batch system launchers
- Capture output in Batch launchers by setting output file options in the default templates.
- {meth}`LoadBalancedView.imap` returns a `LazyMapIterator` which has a `.cancel()` method,
  for stopping consumption of the map input.
- Support for `return_when` argument in {meth}`.AsyncResult.wait` and {meth}`~.AsyncResult.wait_interactive`,
  to allow returning on the first error, first completed, or (default) all completed.

Improved:

- {meth}`LoadBalancedView.imap(max_outstanding=n)` limits the number of tasks submitted to the cluster,
  instead of limiting the number not-yet-consumed.
  Prior to this, the cluster could be idle if several results were waiting to be consumed.
- output streamed by `%%px` includes errors and results, for immediate feedback when only one engine fails.

Fixed:

- Various bugs preventing use of non-default Controller launchers
- Fixed crash in jupyterlab extension when IPython directory does not exist
- `ViewExecutor.shutdown()` waits for `imap` results, like Executors in the standard library
- Removed spurious jupyterlab plugin options that had no effect.
- `%autopx` streams output just like `%%px`

Maintenance:

- Add BroadcastView benchmark code
- Tag releases with tbump

## 7.1

New:

- New {meth}`.Client.start_and_connect` method
  for starting a cluster and returning a connected client in one call.
- Support [CurveZMQ][] for transport-level encryption and authentication.
  See [security docs][] for more info.
- Define `_max_workers` attribute on {attr}`view.executor` for better consistency
  with standard library Executors.

[security docs]: secure-network
[curvezmq]: https://rfc.zeromq.org/spec/26/

Improvements:

- {meth}`.Client.wait_for_engines` will raise an informative error
  if the parent Cluster object notices that its engines have halted while waiting,
  or any engine unregisters,
  rather than continuing to wait for engines that will never come
- Show progress if `%px` is taking significant time
- Improved support for streaming output, e.g. with `%px`,
  including support for updating output in-place
  with standard terminal carriage-return progress bars.

Fixes:

- Fix dropped IOPub messages when using large numbers of engines,
  causing {meth}`.AsyncResult.wait_for_output` to hang.
- Use absolute paths for {attr}`.Cluster.profile_dir`,
  fixing issues with {meth}`.Cluster.from_file` when run against a profile created with a relative location,
  e.g. `Cluster(profile_dir="./profile")`
- Fix error waiting for connection files when controller is started over ssh.

## 7.0

### 7.0.1

- Fix missing setupbase.py in tarball

### 7.0.0

Compatibility changes:

- **Require Python 3.6**
- Fix compatibility issues with ipykernel 6 and jupyter-client 7
- Remove dependency on deprecated ipython-genutils
- New dependencies on psutil, entrypoints, tqdm

New features:

- New {class}`.Cluster` API for managing clusters from Python,
  including support for signaling and restarting engines.
  See [docs](../examples/Cluster%20API.ipynb) for more.
- New `ipcluster list` and `ipcluster clean` commands derived from the Cluster API.
- New {meth}`.Client.send_signal` for sending signals to single engines.
- New KernelNanny process for signaling and monitoring engines
  for improved responsiveness of handing engine crashes.
- New prototype {class}`.BroadcastScheduler` with vastly improved scaling in 'do-on-all' operations
  on large numbers of engines,
  c/o Tom-Olav Bøyum's [Master's thesis][] at University of Oslo.
  [Broadcast view documentation][].
- New {meth}`.Client.wait_for_engines` method to wait for engines to be available.
- Nicer progress bars for interactive waits, such as {meth}`.AsyncResult.wait_interactive`.
- Add {meth}`.AsyncResult.stream_output` context manager for streaming output.
  Stream output by default in parallel magics.
- Launchers registered via entrypoints for better support of third-party Launchers.
- New JupyterLab extension (enabled by default) based on dask-labextension
  for managing clusters.
- {meth}`.LoadBalancedView.imap` consumes inputs as-needed,
  producing a generator of results instead of an AsyncMapResult,
  allowing for consumption of very large or infinite mapping inputs.

[broadcast view documentation]: ../examples/broadcast/Broadcast%20view.ipynb
[master's thesis]: https://urn.nb.no/URN:NBN:no-84589

Improvements and other fixes:

- Greatly improved performance of heartbeat and registration with large numbers of engines,
  tested with 5000 engines and default configuration.
- Single `IPController.ports` configuration to specify the pool of ports for the controller to use,
  e.g. `ipcontroller --ports 10101-10120`.
- Allow `f` as keyword-argument to `apply`, e.g. `view.apply(myfunc, f=5)`.
- joblib backend will start and stop a cluster by default if the default cluster is not running.

The repo has been updated to use pre-commit, black, myst, and friends and GitHub Actions for CI, but this should not affect users, only making it a bit nicer for contributors.

## 6.3.0

- **Require Python 3.5**
- Fix compatibility with joblib 0.14
- Fix crash recovery test for Python 3.8
- Fix repeated name when cluster-id is set
- Fix CSS for notebook extension
- Fix KeyError handling heartbeat failures

## 6.2.5

- Fix compatibility with Python 3.8
- Fix compatibility with recent dask

## 6.2.4

- Improve compatibility with ipykernel 5
- Fix `%autopx` with IPython 7
- Fix non-local ip warning when using current hostname

## 6.2.3

- Fix compatibility for execute requests with ipykernel 5 (now require ipykernel >= 4.4)

## 6.2.2

- Fix compatibility with tornado 4, broken in 6.2.0
- Fix encoding of engine and controller logs in `ipcluster --debug` on Python 3
- Fix compatiblity with joblib 0.12
- Include LICENSE file in wheels

## 6.2.1

- Workaround a setuptools issue preventing installation from sdist on Windows

## 6.2.0

- Drop support for Python 3.3. IPython parallel now requires Python 2.7 or >= 3.4.
- Further fixes for compatibility with tornado 5 when run with asyncio (Python 3)
- Fix for enabling clusters tab via nbextension
- Multiple fixes for handling when engines stop unexpectedly
- Installing IPython Parallel enables the Clusters tab extension by default,
  without any additional commands.

## 6.1.1

- Fix regression in 6.1.0 preventing BatchSpawners (PBS, etc.) from launching with ipcluster.

## 6.1.0

Compatibility fixes with related packages:

- Fix compatibility with pyzmq 17 and tornado 5.
- Fix compatibility with IPython ≥ 6.
- Improve compatibility with dask.distributed ≥ 1.18.

New features:

- Add {attr}`namespace` to BatchSpawners for easier extensibility.
- Support serializing partial functions.
- Support hostnames for machine location, not just ip addresses.
- Add `--location` argument to ipcluster for setting the controller location.
  It can be a hostname or ip.
- Engine rank matches MPI rank if engines are started with `--mpi`.
- Avoid duplicate pickling of the same object in maps, etc.

Documentation has been improved significantly.

## 6.0.2

Upload fixed sdist for 6.0.1.

## 6.0.1

Small encoding fix for Python 2.

## 6.0

Due to a compatibility change and semver, this is a major release. However, it is not a big release.
The main compatibility change is that all timestamps are now timezone-aware UTC timestamps.
This means you may see comparison errors if you have code that uses datetime objects without timezone info (so-called naïve datetime objects).

Other fixes:

- Rename {meth}`Client.become_distributed` to {meth}`Client.become_dask`.
  {meth}`become_distributed` remains as an alias.
- import joblib from a public API instead of a private one
  when using IPython Parallel as a joblib backend.
- Compatibility fix in extensions for security changes in notebook 4.3

## 5.2

- Fix compatibility with changes in ipykernel 4.3, 4.4
- Improve inspection of `@remote` decorated functions
- {meth}`Client.wait` accepts any Future.
- Add `--user` flag to {command}`ipcluster nbextension`
- Default to one core per worker in {meth}`Client.become_distributed`.
  Override by specifying `ncores` keyword-argument.
- Subprocess logs are no longer sent to files by default in {command}`ipcluster`.

## 5.1

### dask, joblib

IPython Parallel 5.1 adds integration with other parallel computing tools,
such as [dask.distributed](https://distributed.readthedocs.io) and [joblib](https://joblib.readthedocs.io).

To turn an IPython cluster into a dask.distributed cluster,
call {meth}`~.Client.become_distributed`:

```
executor = client.become_distributed(ncores=1)
```

which returns a distributed {class}`Executor` instance.

To register IPython Parallel as the backend for joblib:

```
import ipyparallel as ipp
ipp.register_joblib_backend()
```

### nbextensions

IPython parallel now supports the notebook-4.2 API for enabling server extensions,
to provide the IPython clusters tab:

```
jupyter serverextension enable --py ipyparallel
jupyter nbextension install --py ipyparallel
jupyter nbextension enable --py ipyparallel
```

though you can still use the more convenient single-call:

```
ipcluster nbextension enable
```

which does all three steps above.

### Slurm support

[Slurm](https://hpc.llnl.gov/training/tutorials/livermore-computing-linux-commodity-clusters-overview-part-one) support is added to ipcluster.

### 5.1.0

[5.1.0 on GitHub](https://github.com/ipython/ipyparallel/milestones/5.1)

## 5.0

### 5.0.1

[5.0.1 on GitHub](https://github.com/ipython/ipyparallel/milestones/5.0.1)

- Fix imports in {meth}`use_cloudpickle`, {meth}`use_dill`.
- Various typos and documentation updates to catch up with 5.0.

### 5.0.0

[5.0 on GitHub](https://github.com/ipython/ipyparallel/milestones/5.0)

The highlight of ipyparallel 5.0 is that the Client has been reorganized a bit to use Futures.
AsyncResults are now a Future subclass, so they can be `yield` ed in coroutines, etc.
Views have also received an Executor interface.
This rewrite better connects results to their handles,
so the Client.results cache should no longer grow unbounded.

```{seealso}

- The Executor API {class}`ipyparallel.ViewExecutor`
- Creating an Executor from a Client: {meth}`ipyparallel.Client.executor`
- Each View has an {attr}`executor` attribute
```

Part of the Future refactor is that Client IO is now handled in a background thread,
which means that {meth}`Client.spin_thread` is obsolete and deprecated.

Other changes:

- Add {command}`ipcluster nbextension enable|disable` to toggle the clusters tab in Jupyter notebook

Less interesting development changes for users:

Some IPython-parallel extensions to the IPython kernel have been moved to the ipyparallel package:

- {mod}`ipykernel.datapub` is now {mod}`ipyparallel.datapub`
- ipykernel Python serialization is now in {mod}`ipyparallel.serialize`
- apply_request message handling is implememented in a Kernel subclass,
  rather than the base ipykernel Kernel.

## 4.1

[4.1 on GitHub](https://github.com/ipython/ipyparallel/milestones/4.1)

- Add {meth}`.Client.wait_interactive`
- Improvements for specifying engines with SSH launcher.

## 4.0

[4.0 on GitHub](https://github.com/ipython/ipyparallel/milestones/4.0)

First release of `ipyparallel` as a standalone package.
