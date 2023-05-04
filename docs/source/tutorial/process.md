(parallel-process)=

# Starting the IPython controller and engines

To use IPython for parallel computing, you need to start
an IPython cluster.
This includes one instance of the controller and one or more instances of the engine.
The controller and each engine can run on different machines or on the same machine.
Any mechanism for starting processes that can communicate over the network

Broadly speaking, there are two ways of going about starting a controller and engines:

- As managed processes, using {class}`.Cluster` objects.
  This includes via the {command}`ipcluster` command.
- In a more manual way using the {command}`ipcontroller` and
  {command}`ipengine` commands directly.

Starting with IPython Parallel 7,
some features are only available when using the `Cluster` API,
so this is encouraged.
Such features include collective interrupts, signaling support, restart APIs,
and improved crash handling.

This document describes both of these methods. We recommend that new users
start with the `Cluster` API within scripts or notebooks.

## General considerations

Before delving into the details about how you can start a controller and
engines using the various methods, we outline some of the general issues that
come up when starting the controller and engines. These things come up no
matter which method you use to start your IPython cluster.

If you are running engines on multiple machines, you will likely need to instruct the
controller to listen for connections on an external interface. This can be done by specifying
the `ip` argument on the command-line, or the `IPController.ip` configurable in
{file}`ipcontroller_config.py`,
or the `controller_ip` argument when creating a Cluster:

If your machines are on a trusted network, you can safely instruct the controller to listen
on all interfaces with:

```python
cluster = ipp.Cluster(controller_ip='*')
```

or

```
$> ipcontroller --ip="*"
```

Or you can set the same behavior as the default by adding the following line to your {file}`ipcontroller_config.py`:

```python
c.IPController.ip = '*'
# c.IPController.location = 'controllerhost.tld'
```

:::{note}
`--ip=*` instructs ZeroMQ to listen on all interfaces,
but it does not contain the IP needed for engines / clients
to know where the controller is.
This can be specified with the `--location` argument,
such as `--location=10.0.0.1`, or `--location=server.local`,
the specific IP address or hostname of the controller, as seen from engines and/or clients.
IPython uses `socket.gethostname()` for this value by default,
but it may not always be the right value.
Check the `location` field in your connection files if you are having connection trouble.
:::

```{versionchanged} 6.1
Support hostnames in location, in addition to ip addresses.
```

```{note}
Due to the lack of security in ZeroMQ, the controller will only listen for connections on
localhost by default. If you see Timeout errors on engines or clients, then the first
thing you should check is the ip address the controller is listening on, and make sure
that it is visible from the timing out machine.
```

```{seealso}
Our [notes](security) on security in the new parallel computing code.
```

Let's say that you want to start the controller on `host0` and engines on
hosts `host1`-`hostn`. The following steps are then required:

1. Start the controller on `host0` by running {command}`ipcontroller` on
   `host0`. The controller must be instructed to listen on an interface visible
   to the engine machines, via the `ip` command-line argument or `IPController.ip`
   in {file}`ipcontroller_config.py`.
2. Make the JSON file ({file}`ipcontroller-engine.json`) created by the
   controller on `host0` available on hosts `host1`-`hostn`.
3. Start the engines on hosts `host1`-`hostn` by running
   {command}`ipengine`. This command may have to be told where the JSON file
   ({file}`ipcontroller-engine.json`) is located.

At this point, the controller and engines will be connected. By default, the JSON files
created by the controller are put into the {file}`$IPYTHONDIR/profile_default/security`
directory. If the engines share a filesystem with the controller, step 2 can be skipped as
the engines will automatically look at that location.

The final step required to use the running controller from a client is to move
the JSON file {file}`ipcontroller-client.json` from `host0` to any host where clients
will be run. If these file are put into the {file}`IPYTHONDIR/profile_default/security`
directory of the client's host, they will be found automatically. Otherwise, the full path
to them has to be passed to the client's constructor.

## Managing `Clusters`

The {class}`~.ipyparallel.Cluster` class provides a managed way of starting a
controller and engines in the following situations:

1. When the controller and engines are all run on localhost. This is useful
   for testing or running on a multicore computer.
2. When engines are started using the {command}`mpiexec` command that comes
   with most MPI [^cite_mpi] implementations
3. When engines are started using the PBS [^cite_pbs] batch system
   (or other `qsub` systems, such as SGE).
4. When the controller is started on localhost and the engines are started on
   remote nodes using {command}`ssh`.

Under the hood, `Cluster` objects ultimately launch {command}`ipcontroller`
and {command}`ipengine` processes to perform the steps described above.

The simplest way to use clusters requires no configuration, and will
launch a controller and a number of engines on the local machine. For instance,
to start one controller and 4 engines on localhost:

```python
cluster = ipp.Cluster(n=4)
cluster.start_cluster_sync()
```

## Configuring an IPython cluster

Cluster configurations are stored as `profiles`. You can create a new profile with:

```
$ ipython profile create --parallel --profile=myprofile
```

This will create the directory {file}`IPYTHONDIR/profile_myprofile`, and populate it
with the default configuration files for the three IPython cluster commands. Once
you edit those files, you can continue to call ipcluster/ipcontroller/ipengine
with no arguments beyond `profile=myprofile`, and any configuration will be maintained.

There is no limit to the number of profiles you can have, so you can maintain a profile for each
of your common use cases. The default profile will be used whenever the
profile argument is not specified, so edit {file}`IPYTHONDIR/profile_default/*_config.py` to
represent your most common use case.

The configuration files are loaded with commented-out settings and explanations,
which should cover most of the available possibilities.

Additionally, each profile can have many cluster instances at once, identified by a `cluster_id`.

You can see running clusters with:

```
$> ipcluster list
```

### Using various batch systems with {command}`ipcluster`

{command}`ipcluster` has a notion of {class}`Launcher` that can start controllers
and engines via some mechanism. Currently supported
models include {command}`ssh`, {command}`mpiexec`, and PBS-style (Torque, SGE, LSF, Slurm).

In general, these are configured by the {attr}`Cluster.engine_set_launcher_class`,
and {attr}`Cluster.controller_launcher_class` configurables, which can be the
fully specified object name (e.g. `'ipyparallel.cluster.launcher.LocalControllerLauncher'`),
but if you are using IPython's builtin launchers, you can specify a launcher by its short name, e.g:

```python
c.Cluster.engine_launcher_class = 'ssh'
# which is equivalent to
c.Cluster.engine_launcher_class = 'ipyparallel.cluster.launcher.SSHEngineSetLauncher'
```

The shortest form being of particular use on the command line, where all you need to do to
get an IPython cluster running with engines started with MPI is:

```bash
$> ipcluster start --engines=mpi
```

Assuming that the default MPI configuration is sufficient.

```{note}
The Launchers and configuration are designed in such a way that advanced
users can subclass and configure them to fit their own system that we
have not yet supported
```

### Writing custom Launchers

TODO: example writing custom Launchers

(ipcluster-mpi)=

### Using IPython Parallel with MPI

The mpiexec/mpirun mode is useful if:

1. You have MPI installed.
2. Your systems are configured to use the {command}`mpiexec` or
   {command}`mpirun` commands to start MPI processes.

you can usually start with MPI pretty readily without creating a profile.

```python
cluster = ipp.Cluster(engines="mpi", n=4)
client = cluster.start_and_connect_sync()
```

This does the following:

1. Starts the IPython controller on current host.
2. Uses {command}`mpiexec` to start 4 engines.

More details on using MPI with IPython can be found {ref}`here <parallel-mpi>`.

### Starting IPython Parallel on a traditional cluster

For cases that require more extensive configuration,
IPython can use "profiles" to collect related configuration.
These profiles have names, and are located by default in your .ipython directory.

IPython supports several batch systems, including PBS, LSF, Slurm, SGE, and HTCondor.
Many "PBS-like" systems can be supported by configuring the PBSLauncher.
Additional configuration options can be found in the BatchSystemLauncher section of {file}`ipcluster_config`.

We'll use PBS as our example here, but all batch system launchers are configured the same way.

We will start by creating a fresh profile:

```

$ ipython profile create --parallel --profile=pbs

```

And in {file}`ipcluster_config.py`, we will select the PBS launchers for the controller
and engines:

```python
c.Cluster.controller_launcher_class = 'pbs'
c.Cluster.engine_launcher_class = 'pbs'
```

IPython does provide simple default batch templates for supported batch systems, but you may need
to specify your own. Here is a sample PBS script template:

```bash
#!/bin/bash
#PBS -N ipython-parallel  # set the name of the job (convenient)
#PBS -V  # export environment variables, required
#PBS -j oe  # merge stdout and error
#PBS -o {output_file}  # send output to a file
#PBS -l walltime=00:10:00  # max runtime 10 minutes
#PBS -l nodes={n//4}:ppn=4  # 4 processes per node
#PBS -q {queue}  # run on the specified queue

cd $PBS_O_WORKDIR
export PATH=$HOME/usr/local/bin
/usr/local/bin/mpiexec -n {n} {program_and_args}  # start the configured program
# program_and_args is populated via `engine_args` and other configuration
# or you can ignore that and configure the full program in the template
/usr/local/bin/mpiexec -n {n} ipengine --debug
```

There are a few important points about this template:

1. This template will be rendered at runtime using IPython's {class}`EvalFormatter`.
   This is a subclass of {class}`string.Formatter` that allows simple expressions
   on keys.
2. Instead of putting in the actual number of engines, use the notation
   `{n}` to indicate the number of engines to be started. You can also use
   expressions like `{n//4}` in the template to indicate the number of nodes.
   There will always be `{n}` and `{profile_dir}` variables passed to the formatter.
   These allow the batch system to know how many engines, and where the configuration
   files reside. The same is true for the batch queue, with the template variable
   `{queue}`.
3. Any options to {command}`ipengine` can be given in the batch script
   template, or in {file}`ipengine_config.py`.
4. Depending on the configuration of you system, you may have to set
   environment variables in the script template.

The controller template should be similar, but simpler:

```bash
#!/bin/bash
#PBS -N ipython
#PBS -V
#PBS -j oe
#PBS -o {output_file}
#PBS -l walltime=00:10:00
#PBS -l nodes=1:ppn=1
#PBS -q {queue}
#PBS -V

cd $PBS_O_WORKDIR
export PATH=$HOME/usr/local/bin
{program_and_args} # or ipcontroller --ip=*
```

Once you have created these scripts, save them with names like
{file}`pbs.engine.template`. Now you can load them into the {file}`ipcluster_config` with:

```python
c.PBSEngineSetLauncher.batch_template_file = "pbs.engine.template"

c.PBSControllerLauncher.batch_template_file = "pbs.controller.template"
```

Alternately, you can define the templates as strings inside {file}`ipcluster_config`.

Whether you are using your own templates or our defaults, the extra configurables available are
the number of engines to launch (`{n}`, and the batch system queue to which the jobs are to be
submitted (`{queue}`)). These are configurables, and can be specified in
{file}`ipcluster_config`:

```python
c.PBSLauncher.queue = 'veryshort.q'
c.Cluster.n = 64
```

Note that assuming you are running PBS on a multi-node cluster, the Controller's default behavior
of listening only on localhost is likely too restrictive. In this case, also assuming the
nodes are safely behind a firewall, you can instruct the Controller to listen for
connections on all its interfaces, by adding in {file}`ipcontroller_config`:

```python
c.IPController.ip = '*'
```

You can now run the cluster with:

```python
cluster = Cluster(profile="pbs")
```

```
$ ipcluster start --profile=pbs -n 128
```

### Starting a cluster with SSH

The SSH mode uses {command}`ssh` to execute {command}`ipengine` on remote
nodes and {command}`ipcontroller` can be run remotely as well, or on localhost.

```{note}
When using this mode it highly recommended that you have set up SSH keys
and are using ssh-agent [^cite_ssh] for password-less logins.
```

As usual, we start by creating a clean profile:

```
$ ipython profile create --parallel --profile=ssh
```

To use this mode, select the SSH launchers in {file}`ipcluster_config.py`:

```python
c.Cluster.engine_launcher_class = 'ssh' # or 'sshproxy'
# and if the Controller is also to be remote:
c.Cluster.controller_launcher_class = 'ssh'
```

The controller's remote location and configuration can be specified:

```python
# Set the user and hostname for the controller
# c.SSHControllerLauncher.hostname = 'controller.example.com'
# c.SSHControllerLauncher.user = os.environ.get('USER','username')

# Set the arguments to be passed to ipcontroller
# note that remotely launched ipcontroller will not get the contents of
# the local ipcontroller_config.py unless it resides on the *remote host*
# in the location specified by the `profile-dir` argument.
# c.SSHControllerLauncher.controller_args = ['--reuse', '--ip=*', '--profile-dir=/path/to/cd']
```

Engines are specified in a dictionary, by hostname and the number of engines to be run
on that host.

```python
c.SSHEngineSetLauncher.engines = { 'host1.example.com' : 2,
            'host2.example.com' : 5,
            'host3.example.com' : (1, ['--profile-dir=/home/different/location']),
            'host4.example.com' : {'n': 3, 'engine_args': ['--profile-dir=/away/location'], 'engine_cmd': ['/home/venv/bin/python', '-m', 'ipyparallel.engine']},
            'host5.example.com' : 8 }
```

- The `engines` dict, where the keys are the host we want to run engines on and
  the value is the number of engines to run on that host.
- on host3, the value is a tuple, where the number of engines is first, and the arguments
  to be passed to {command}`ipengine` are the second element.
- on host4, a dictionary configures the engine. The dictionary can be used to specify
  the number of engines to be run on that host `n`, the engine arguments `engine_args`,
  as well as the engine command itself `engine_cmd`. This is particularly useful for
  virtual environments on heterogeneous clusters where the location of the python
  executable might vary from host to host.

For engines without explicitly specified arguments, the default arguments are set in
a single location:

```python
c.SSHEngineSetLauncher.engine_args = ['--profile-dir=/path/to/profile_ssh']
```

Current limitations of the SSH mode of {command}`ipcluster` are:

- Untested and unsupported on Windows. Would require a working {command}`ssh` on Windows.
  Also, we are using shell scripts to setup and execute commands on remote hosts.

#### Moving files with SSH

SSH launchers will try to move connection files, controlled by the `to_send` and
`to_fetch` configurables. If your machines are on a shared filesystem, this step is
unnecessary, and can be skipped by setting these to empty lists:

```python
c.SSHLauncher.to_send = []
c.SSHLauncher.to_fetch = []
```

If our default guesses about paths don't work for you, or other files
should be moved, you can manually specify these lists as tuples of `(local_path, remote_path)` for `to_send`,
and `(remote_path, local_path)` for `to_fetch`.
If you do specify these lists explicitly,
IPython _will not_ automatically send connection files,
so you must include this yourself if they should still be sent/retrieved.

### Starting the controller and engines on different hosts

When the controller and engines are running on different hosts, things are
slightly more complicated, but the underlying ideas are the same:

1. Start the controller on a host using {command}`ipcontroller`. The controller must be
   instructed to listen on an interface visible to the engine machines, via the `ip`
   command-line argument or `IPController.ip` in {file}`ipcontroller_config.py`:

   ```
   $ ipcontroller --ip=192.168.1.16
   ```

   ```python
   # in ipcontroller_config.py
   IPController.ip = '192.168.1.16'
   ```

2. Copy {file}`ipcontroller-engine.json` from {file}`IPYTHONDIR/profile_<name>/security` on
   the controller's host to the host where the engines will run.

3. Use {command}`ipengine` on the engine's hosts to start the engines.

The only thing you have to be careful of is to tell {command}`ipengine` where
the {file}`ipcontroller-engine.json` file is located. There are two ways you
can do this:

- Put {file}`ipcontroller-engine.json` in the {file}`IPYTHONDIR/profile_<name>/security`
  directory on the engine's host, where it will be found automatically.
- Call {command}`ipengine` with the `--file=full_path_to_the_file`
  flag.

The `file` flag works like this:

```
$ ipengine --file=/path/to/my/ipcontroller-engine.json
```

```{note}
If the controller's and engine's hosts all have a shared file system
({file}`IPYTHONDIR/profile_<name>/security` is the same on all of them),
then no paths need to be specified or files copied.
```

#### SSH Tunnels

If your engines are not on the same LAN as the controller, or you are on a highly
restricted network where your nodes cannot see each others ports, then you can
use SSH tunnels to connect engines to the controller.

```{note}
This does not work in all cases. Manual tunnels may be an option, but are
highly inconvenient. Support for manual tunnels will be improved.
```

You can instruct all engines to use ssh, by specifying the ssh server in
{file}`ipcontroller-engine.json`
This will be specified if you give the `--enginessh=user@example.com` argument when
starting {command}`ipcontroller`.

Or you can specify an ssh server on the command-line when starting an engine:

```
$> ipengine --profile=foo --ssh=my.login.node
```

For example, if your system is totally restricted, then all connections will be
loopback, and ssh tunnels will be used to connect engines to the controller:

```
[node1] $> ipcontroller --enginessh=node1
[node2] $> ipengine
[node3] $> ipcluster engines --n=4
```

Or if you want to start many engines on each node, the command `ipcluster engines --n=4`
without any configuration is equivalent to running ipengine 4 times.

### An example using ipcontroller/engine with ssh

No configuration files are necessary to use ipcontroller/engine in an SSH environment
without a shared filesystem. You need to make sure that the controller is listening
on an interface visible to the engines, and move the connection file from the controller to
the engines.

1. start the controller, listening on an ip-address visible to the engine machines:

   ```
   [controller.host] $ ipcontroller --ip=192.168.1.16

   [IPController] Using existing profile dir: u'/Users/me/.ipython/profile_default'
   [IPController] Hub listening on tcp://192.168.1.16:63320 for registration.
   [IPController] Hub using DB backend: 'ipyparallel.controller.dictdb.DictDB'
   [IPController] hub::created hub
   [IPController] writing connection info to /Users/me/.ipython/profile_default/security/ipcontroller-client.json
   [IPController] writing connection info to /Users/me/.ipython/profile_default/security/ipcontroller-engine.json
   [IPController] task::using Python leastload Task scheduler
   [IPController] Heartmonitor started
   [IPController] Creating pid file: /Users/me/.ipython/profile_default/pid/ipcontroller.pid
   Scheduler started [leastload]
   ```

2. on each engine, fetch the connection file with scp:

   ```
   [engine.host.n] $ scp controller.host:.ipython/profile_default/security/ipcontroller-engine.json ./
   ```

   ```{note}
   The log output of ipcontroller above shows you where the json files were written.
   They will be in {file}`~/.ipython` under
   {file}`profile_default/security/ipcontroller-engine.json`
   ```

3. start the engines, using the connection file:

   ```
   [engine.host.n] $ ipengine --file=./ipcontroller-engine.json
   ```

A couple of notes:

- You can avoid having to fetch the connection file every time by adding `--reuse` flag
  to ipcontroller, which instructs the controller to read the previous connection file for
  connection info, rather than generate a new one with randomized ports.

- In step 2, if you fetch the connection file directly into the security dir of a profile,
  then you need not specify its path directly, only the profile (assumes the path exists,
  otherwise you must create it first):

  ```
  [engine.host.n] $ scp controller.host:.ipython/profile_default/security/ipcontroller-engine.json ~/.ipython/profile_ssh/security/
  [engine.host.n] $ ipengine --profile=ssh
  ```

  Of course, if you fetch the file into the default profile, no arguments must be passed to
  ipengine at all.

- Note that ipengine _did not_ specify the ip argument. In general, it is unlikely for any
  connection information to be specified at the command-line to ipengine, as all of this
  information should be contained in the connection file written by ipcontroller.

### Make JSON files persistent

At fist glance it may seem that that managing the JSON files is a bit
annoying. Going back to the house and key analogy, copying the JSON around
each time you start the controller is like having to make a new key every time
you want to unlock the door and enter your house. As with your house, you want
to be able to create the key (or JSON file) once, and then use it at
any point in the future.

To do this, the only thing you have to do is specify the `--reuse` flag, so that
the connection information in the JSON files remains accurate:

```
$ ipcontroller --reuse
```

Then copy the JSON files over the first time and you are set. You can
start and stop the controller and engines any many times as you want in the
future, as long as you make sure to tell the controller to reuse the file.

```{note}
You may ask the question: what ports does the controller listen on if you
don't tell is to use specific ones? The default is to use high random port
numbers. We do this for two reasons: i) to increase security through
obscurity and ii) to multiple controllers on a given host to start and
automatically use different ports.
```

### Log files

All of the components of IPython have log files associated with them.
These log files can be extremely useful in debugging problems with
IPython and can be found in the directory {file}`IPYTHONDIR/profile_<name>/log`.
Sending the log files to us will often help us to debug any problems.

### Configuring `ipcontroller`

The IPython Controller takes its configuration from the file {file}`ipcontroller_config.py`
in the active profile directory.

#### Ports and addresses

In many cases, you will want to configure the Controller's network identity. By default,
the Controller listens only on loopback, which is the most secure but often impractical.
To instruct the controller to listen on a specific interface, you can set the
{attr}`IPController.ip` trait. To listen on all interfaces, specify:

```python
c.IPController.ip = '*'
```

When connecting to a Controller that is listening on loopback or behind a firewall, it may
be necessary to specify an SSH server to use for tunnels, and the external IP of the
Controller. If you specified that the IPController listen on loopback, or all interfaces,
then IPython will try to guess the external IP. If you are on a system with VM network
devices, or many interfaces, this guess may be incorrect. In these cases, you will want
to specify the 'location' of the Controller. This is the IP of the machine the Controller
is on, as seen by the clients, engines, or the SSH server used to tunnel connections.

For example, to set up a cluster with a Controller on a work node, using ssh tunnels
through the login node, an example {file}`ipcontroller_config.py` might contain:

```python
# allow connections on all interfaces from engines
# engines on the same node will use loopback, while engines
# from other nodes will use an external IP
c.IPController.ip = '*'

# you typically only need to specify the location when there are extra
# interfaces that may not be visible to peer nodes (e.g. VM interfaces)
c.IPController.location = '10.0.1.5'
# or to get an automatic value, try this:
import socket
hostname = socket.gethostname()
# alternate choices for hostname include `socket.getfqdn()`
# or `socket.gethostname() + '.local'`

ex_ip = socket.gethostbyname_ex(hostname)[-1][-1]
c.IPController.location = ex_ip

# now instruct clients to use the login node for SSH tunnels:
c.IPController.ssh_server = 'login.mycluster.net'
```

After doing this, your {file}`ipcontroller-client.json` file will look something like this:

```python
{
  "url":"tcp:\/\/*:43447",
  "exec_key":"9c7779e4-d08a-4c3b-ba8e-db1f80b562c1",
  "ssh":"login.mycluster.net",
  "location":"10.0.1.5"
}
```

Then this file will be all you need for a client to connect to the controller, tunneling
SSH connections through login.mycluster.net.

#### Database Backend

The Hub stores all messages and results passed between Clients and Engines.
For large and/or long-running clusters, it would be unreasonable to keep all
of this information in memory. For this reason, we have two database backends:
[^cite_mongodb] via [PyMongo], and SQLite with the stdlib {py:mod}`sqlite`.

MongoDB is our design target, and the dict-like model it uses has driven our design. As far
as we are concerned, BSON can be considered essentially the same as JSON, adding support
for binary data and datetime objects, and any new database backend must support the same
data types.

```{seealso}
MongoDB [BSON doc]( https://bsonspec.org)
```

To use one of these backends, you must set the {attr}`IPController.db_class` trait:

```python
# for a simple dict-based in-memory implementation, use dictdb
# This is the default and the fastest, since it doesn't involve the filesystem
c.IPController.db_class = 'ipyparallel.controller.dictdb.DictDB'

# To use MongoDB:
c.IPController.db_class = 'ipyparallel.controller.mongodb.MongoDB'

# and SQLite:
c.IPController.db_class = 'ipyparallel.controller.sqlitedb.SQLiteDB'

# You can use NoDB to disable the database altogether, in case you don't need
# to reuse tasks or results, and want to keep memory consumption under control.
c.IPController.db_class = 'ipyparallel.controller.dictdb.NoDB'
```

When using the proper databases, you can allow for tasks to persist from
one session to the next by specifying the MongoDB database or SQLite table in
which tasks are to be stored. The default is to use a table named for the Hub's Session,
which is a UUID, and thus different every time.

```python
# To keep persistent task history in MongoDB:
c.MongoDB.database = 'tasks'

# and in SQLite:
c.SQLiteDB.table = 'tasks'
```

Since MongoDB servers can be running remotely or configured to listen on a particular port,
you can specify any arguments you may need to the PyMongo {py:class}`~.pymongo.Connection`:

```python
# positional args to pymongo.Connection
c.MongoDB.connection_args = []

# keyword args to pymongo.Connection
c.MongoDB.connection_kwargs = {}
```

But sometimes you are moving lots of data around quickly, and you don't need
that information to be stored for later access, even by other Clients to this
same session. For this case, we have a dummy database, which doesn't
store anything. This lets the Hub stay small in memory, at the obvious expense
of being able to access the information that would have been stored in the
database (used for task resubmission, requesting results of tasks you didn't
submit, etc.). To use this backend, pass `--nodb` to
{command}`ipcontroller` on the command-line, or specify the {class}`NoDB` class
in your {file}`ipcontroller_config.py` as described above.

```{seealso}
For more information on the database backends, see the {ref}`db backend reference <parallel-db>`.
```

### Configuring `ipengine`

The IPython Engine takes its configuration from the file {file}`ipengine_config.py`

The Engine itself also has some amount of configuration. Most of this
has to do with initializing MPI or connecting to the controller.

To instruct the Engine to initialize with an MPI environment set up by
mpi4py, add:

```python
c.MPI.use = 'mpi4py'
```

In this case, the Engine will use our default mpi4py init script to set up
the MPI environment prior to execution. We have default init scripts for
mpi4py and pytrilinos. If you want to specify your own code to be run
at the beginning, specify `c.MPI.init_script`.

You can also specify a file or python command to be run at startup of the
Engine:

```python
c.IPEngine.startup_script = u'/path/to/my/startup.py'

c.IPEngine.startup_command = 'import numpy, scipy, mpi4py'
```

These commands/files will be run again, after each

It's also useful on systems with shared filesystems to run the engines
in some scratch directory. This can be set with:

```python
c.IPEngine.work_dir = u'/path/to/scratch/'
```

[^cite_mongodb]: MongoDB database <https://www.mongodb.com/>
[^cite_pbs]: Portable Batch System <https://www.mcs.anl.gov/research/projects/openpbs/>
[^cite_ssh]: SSH-Agent <https://en.wikipedia.org/wiki/Ssh-agent>

[pymongo]: https://pymongo.readthedocs.io
