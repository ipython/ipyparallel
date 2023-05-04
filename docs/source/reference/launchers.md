# Launchers

The `Launcher` is the basic abstraction in IPython Parallel
for starting and stopping processes.

A Launcher has two primary methods: {meth}`~.BaseLauncher.start` and {meth}`~.BaseLauncher.stop`,
which should be `async def` coroutines.

There are two basic kinds of Launcher: {class}`~.ControllerLauncher` and {class}`~.EngineLauncher`.
A ControllerLauncher should launch `ipcontroller` somewhere,
and an EngineLauncher should start `n` engines somewhere.
Shared configuration,
principally `profile_dir` and `cluster_id` are typically used to locate the connection files necessary for these two communicate,
though explicit paths can be added to arguments.

Launchers are used through the {class}`~.Cluster` API,
which manages one ControllerLauncher and zero to many EngineLaunchers,
each representing a set of engines.

Launchers are registered via entry points ([more below](entrypoints)),
and can be selected via short lowercase string naming the kind of launcher, e.g. 'mpi' or 'local':

```python
import ipyparallel as ipp
c = ipp.Cluster(engines="mpi")
```

For the most part, Launchers are not interacted-with directly,
but can be _configured_.

If you generate a config file with:

```bash
ipython profile create --parallel
```

you can check out the resulting `ipcluster_config.py`,
which includes configuration options for all available Launcher classes.

You can also check `ipcluster start --help-all` to see them on the command-line.

## Debugging launchers

If a launcher isn't doing what you want,
the first thing to do is probably start your Cluster with `log_level=logging.DEBUG`.

You can also access the Launcher(s) on the Cluster object and call {meth}`~.BaseLauncher.get_output` to retrieve the output from the process.

## Writing your own Launcher(s)

If you want to write your own launcher,
the best place to start is to look at the Launcher classes that ship with IPython Parallel.

There are three key methods to implement:

- [`start()`](writing-start)
- [`stop()`](writing-stop)
- [`from_dict()`](writing-from-dict)

(writing-start)=

### Writing start

A start method on a launcher should do the following:

1. request the process(es) to be started
2. start monitoring to notice when the process exits, such that {meth}`.notify_stop` will be called when the process exits.

The command to launch should be the `self.args` list, inherited from the base class.

The default for the `LocalProcessLauncher`

```{literalinclude} ../../../ipyparallel/cluster/launcher.py
:pyobject: LocalProcessLauncher.start
```

_ControllerLauncher.start_ is always called with no arguments,
whereas `EngineLauncher.start` is called with `n`,
which is an integer or None. If `n` is an integer,
this many engines should be started.
If `n` is None, a 'default' number should be used,
e.g. the number of CPUs on a host.

(writing-stop)=

### Writing stop

A stop method should request that the process(es) stop,
and return only after everything is stopped and cleaned up.
Exactly how to collect these resources will depend greatly on how the resources were requested in `start`.

### Serializing Launchers

Launchers are serialized to disk using JSON,
via the `.to_dict()` method.
The default `.to_dict()` method should rarely need to be overridden.

To declare a property of your launcher as one that should be included in serialization,
register it as a [traitlet][] with `to_dict=True`.
For example:

```python
from traitlets import Integer
from ipyparallel.cluster.launcher import EngineLauncher
class MyLauncher(EngineLauncher):
    pid = Integer(
        help="The pid of the process",
    ).tag(to_dict=True)
```

[traitlet]: https://traitlets.readthedocs.io

This `.tag(to_dict=True)` ensures that the `.pid` property will be persisted to disk,
and reloaded in the default `.from_dict` implementation.
Typically, these are populated in `.start()`:

```python
def start(self):
    process = start_process(self.args, ...)
    self.pid = process.pid
```

Mark whatever properties are required to reconstruct your object from disk with this metadata.

(writing-from-dict)=

#### writing from_dict

{meth}`~.BaseLauncher.from_dict` should be a class method which returns an instance of your Launcher class, loaded from dict.

Most `from_dict` methods will look similar to this:

```{literalinclude} ../../../ipyparallel/cluster/launcher.py
:pyobject: LocalProcessLauncher.from_dict
```

where serializable-state is loaded first, then 'live' objects are loaded from that.
As in the default LocalProcessLauncher:

```{literalinclude} ../../../ipyparallel/cluster/launcher.py
:pyobject: LocalProcessLauncher._reconstruct_process
```

The local process case is the simplest, where the main thing that needs serialization is the PID of the process.

If reconstruction of the object fails because the resource is no longer running
(e.g. check for the PID and it's not there, or a VM / batch job are gone),
the {exc}`.NotRunning` exception should be raised.
This tells the Cluster that the object is gone and should be removed
(handled the same as if it had stopped while we are watching).
Raising other unhandled errors will be assumed to be a bug in the Launcher,
and not result in removing the resource from cluster state.

### Additional methods

Some useful additional methods to implement, if the base class implementations do not work for you:

- {meth}`~.ControllerLauncher.get_connection_info`
- {meth}`~.BaseLauncher.get_output`

TODO: write more docs on these

(entrypoints)=

## Registering your Launcher via entrypoints

Once you have defined your launcher, you can 'register' it for discovery
via entrypoints. In your setup.py:

```python
setup(
    ...
    entry_points={
        'ipyparallel.controller_launchers': [
            'mine = mypackage:MyControllerLauncher',
        ],
        'ipyparallel.engine_launchers': [
            'mine = mypackage:MyEngineSetLauncher',
        ],
    },
)
```

This allows clusters created to use the shortcut:

```python
Cluster(engines="mine")
```

instead of the full import string

```
Cluster(engines="mypackage.MyEngineSetLauncher")
```

though the long form will always still work.

## Launcher API reference

```{eval-rst}
.. automodule:: ipyparallel.cluster.launcher
```
