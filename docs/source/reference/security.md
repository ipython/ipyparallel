(security)=

# Security details of IPython Parallel

IPython Parallel exposes the full power of the
Python interpreter over a TCP/IP network (or BSD socket) for the purposes of parallel
computing. This feature raises the important question of IPython's security
model. This document gives details about this model and how it is implemented
in IPython's architecture.

## Process and network topology

To enable parallel computing, IPython has a number of different processes that
run. These processes are discussed at length in the IPython documentation and
are summarized here:

- The IPython _engine_. This process is a full blown Python
  interpreter in which user code is executed. Multiple
  engines are started to make parallel computing possible.
- The IPython _hub_. This process monitors a set of
  engines and schedulers, and keeps track of the state of the processes. It listens
  for registration connections from engines and clients, and monitor connections
  from schedulers.
- The IPython _schedulers_. This is a set of processes that relay commands and results
  between clients and engines. They are typically on the same machine as the hub,
  and listen for connections from engines and clients, but connect to the Hub.
- The IPython _client_. This process is typically an
  interactive Python process that is used to coordinate the
  engines to get a parallel computation done.

Collectively, these processes are called an IPython _cluster_, and the hub and schedulers
together are referred to as the _controller_.

These processes communicate over any transport supported by ZeroMQ (tcp, pgm, ipc)
with a well-defined topology. The IPython controller processes listen on sockets.
Upon starting, an engine connects to a hub and sends a message requesting registration,
to which the hub replies with connection information for the schedulers,
and the engine then connects to the schedulers.
These engine->hub and engine->scheduler connections persist for the
lifetime of each engine.

The IPython client also connects to the controller processes using a number of sockets.
This is one socket per scheduler.
These connections persist for the lifetime of the client only.

A given IPython controller and set of engines typically has a relatively
short lifetime, such as the duration of a single parallel
computation performed by a single user.
Finally, the hub, schedulers, engines, and client
processes typically execute with the permissions of that same user. More specifically, the
controller and engines are _not_ executed as root or with any other superuser or shared-user permissions.

## Application logic

When running the IPython kernel to perform a parallel computation, a user
connects an IPython client to send Python commands and data through the
IPython schedulers to the IPython engines, where those commands are executed
and the data processed.

Via the client, a user can instruct the IPython engines to execute
arbitrary Python commands.
These Python commands can include calls to the
system shell, access the filesystem, etc., as required by the user's
application. From this perspective, when a user runs an IPython engine on
a host, that engine has the same capabilities and permissions as the user
themselves (as if they were logged onto the engine's host with a terminal).

### ZeroMQ and Connection files

IPython uses ZeroMQ for networking.
By default, no IPython connections are encrypted.
Open ports listen only on localhost.
When no encryption is used, messages are signed via HMAC digest
using a shared key for authentication.

As of IPython 7.1, all connections _can_ be authenticated and encrypted using [CurveZMQ][].

The key (whether the CurveZMQ key or HMAC digest key) is distributed to engines and clients via connection files.

TCP connections can be tunneled over SSH. IPython supports both shell
(`openssh`) and `paramiko` based tunnels for connections.

In our architecture, the controller is the only process that listens on
network ports, and is thus the main point of vulnerability. The standard model
for secure connections is to designate that the controller listen on
localhost, and use ssh-tunnels to connect clients and/or
engines, or connect over a trusted private network.

To connect and authenticate to the controller an engine or client needs
some information that the controller has stored in a JSON file.
The JSON files may need to be copied to a location where
the clients and engines can find them. Typically, this is the
{file}`~/.ipython/profile_default/security` directory on the host where the
client/engine is running, which could be on a different filesystem than the controller.
Once the JSON files are copied over, everything should work fine.

Currently, there are two JSON files that the controller creates:

ipcontroller-engine.json

: This JSON file has the information necessary for an engine to connect
to a controller.

ipcontroller-client.json

: The client's connection information.
Similar to the engine file, but lists a different collection of ports.

ipcontroller-client.json will look something like this, under default localhost
circumstances:

```python
{
  "ssh": "",
  "interface": "tcp://127.0.0.1",
  "registration": 54886,
  "control": 54888,
  "mux": 54890,
  "hb_ping": 54891,
  "hb_pong": 54892,
  "task": 54894,
  "iopub": 54896,
  "broadcast": [
    54900,
    54901
  ],
  "key": "7e99e423-c437d4daf7cf23ee84cae803",
  "location": "mylaptop",
  "pack": "json",
  "unpack": "json",
  "signature_scheme": "hmac-sha256"
}
```

If, however, you are running the controller on a work node on a cluster, you will likely
need to use ssh tunnels to connect clients from your laptop to it. You will also
probably need to instruct the controller to listen for engines coming from other work nodes
on the cluster. An example of ipcontroller-client.json, as created by:

```
$> ipcontroller --ip=* --ssh=login.mycluster.com
```

```python
{
  "ssh": "login.mycluster.com",
  "interface": "tcp://*",
  "registration": 55836,
  "control": 55837,
  "mux": 55839,
  "task": 55843,
  "task_scheme": "lru",
  "iopub": 55845,
  "notification": 55852,
  "broadcast": [
    55847,
    55848,
    55849
  ],
  "key": "70bc97ac-e66ac5143885ca8b376d4cb7",
  "location": "mylaptop",
  "pack": "json",
  "unpack": "json",
  "signature_scheme": "hmac-sha256"
}
```

More details of how these JSON files are used are given below.

(secure-network)=

## Secure network connections

### Overview

ZeroMQ supports encryption and authentication via a mechanism
called [CurveZMQ][].
IPython Parallel supports CurveZMQ as of version 7.1.

To enable CurveZMQ, set

```python
c.IPController.enable_curve = True
```

in `ipcontroller_config.py`, or set the environment variable `IPP_ENABLE_CURVE=1`.

When using CurveZMQ, all connections are authenticated using the controller's server key,
which is distributed to engines and clients via connection files.
This key is all that is needed to connect to an IPython cluster and execute code.

Additionally, when using CurveZMQ, all communication is _encrypted_ using the controller's server key.
Each client typically generates its own unique, short-lived key pair
for its side of encrypted communication.
When not using CurveZMQ, messages are not encrypted.

### Security without CurveZMQ

When not using CurveZMQ, all ZeroMQ connections are not authenticated and not encrypted.
For this reason, users of IPython must be very careful in managing connections,
because an open TCP/IP socket presents access to arbitrary execution as the user on the engine machines.
As a result, the default behavior of controller processes is to only listen for clients on the loopback interface,
and remote clients must establish SSH tunnels to connect to the controller processes.

```{warning}
If the controller's loopback interface is untrusted,
then IPython should be considered vulnerable without CurveZMQ,
and this extends to the loopback of all connected clients,
which have opened a loopback port that is redirected to the controller's loopback port.
```

### SSH

Without CurveZMQ, ZeroMQ sockets themselves provide no security.
SSH tunnels can be used to encrypt traffic across the network,
but _at least_ loopback traffic will be unencrypted.
A connection file file, such as `ipcontroller-client.json`,
will contain information for connecting to the controller,
possibly including the address of an ssh server through which the client is to tunnel.
The Client object then creates tunnels using either [openssh][] or [paramiko][], depending on the platform.
If users do not wish to use OpenSSH or Paramiko,
or the tunneling utilities are insufficient,
then they may construct the tunnels themselves,
and connect clients and engines as if the controller were on loopback on the connecting machine.

### Authentication

IPython uses a key-distribution model of authentication.
Whether you are using CurveZMQ or message-digest signatures.
In both cases, a single key is used for authentication,
and the same key is distributed in `ipcontroller-{client|engine}.json`.

There is exactly one shared key per cluster - it must be the same everywhere. Typically,
the controller creates this key, and stores it in the private connection files
`ipython-{engine|client}.json`. These files are typically stored in the
`~/.ipython/profile_<name>/security` directory, and are maintained as readable only by the
owner, as is common practice with a user's keys in their `.ssh` directory.

The key distribution model is the same for both security implementations,
however the level of authentication is substantially different.

If you are using CurveZMQ, _connections_ are authenticated using the server key.
This means connections attempted without the key will be rejected,
and no messages can be sent or received by unauthenticated clients.

#### Authentication without CurveZMQ

If not using CurveZMQ, connections are not authenticated,
only _messages_ are authenticated.
This means that _clients_ with access to the controller's ports,
but without the key will be able to connect and send and receive messages,
but the requests will be rejected.
To protect users of shared machines, [HMAC] digests are used to sign messages, using the
shared key.

The Session object that handles the message protocol uses a unique key to verify valid
messages. This can be any value specified by the user, but the default behavior is a
pseudo-random 128-bit number, as generated by `uuid.uuid4()`. This key is used to
initialize an HMAC object, which digests all messages, and includes that digest as a
signature and part of the message. Every message that is unpacked (on Controller, Engine,
and Client) will also be digested by the receiver, ensuring that the sender's key is the
same as the receiver's. No messages that do not contain this key are acted upon in any
way. The key itself is never sent over the network.

```{warning}
It is important to note that the signatures protect against unauthorized messages,
but, as there is no encryption, provide exactly no protection of data privacy. It is
possible, however, to use a custom serialization scheme (via Session.packer/unpacker
traits) that does incorporate your own encryption scheme.
```

### Encryption

Messages are only encrypted when using CurveZMQ,
which provides perfect-forward security by issuing short-lived keys for each session.
Knowing the distributed CurveZMQ key is _not enough_ to decrypt communication from another client.

## Specific security vulnerabilities

There are a number of potential security vulnerabilities present in IPython's
architecture. In this section we discuss those vulnerabilities and detail how
the security architecture described above prevents them from being exploited.

### Unauthorized clients

The IPython client can instruct the IPython engines to execute arbitrary
Python code with the permissions of the user who started the engines. If an
attacker were able to connect their own hostile IPython client to the IPython
controller, they could instruct the engines to execute code.

On the first level, this attack is prevented by requiring access to the controller's
ports, which are recommended to only be open on loopback if the controller is on an
untrusted local network. If the attacker does have access to the Controller's ports, then
the attack is prevented by the capabilities based client authentication of the execution
key. The relevant authentication information is encoded into the JSON file that clients
must present to gain access to the IPython controller. By limiting the distribution of
those keys, a user can grant access to only authorized persons, as with SSH keys.
When using CurveZMQ, the connection will be rejected without the key.
When not using CurveZMQ, _requests_ will be rejected if not signed by the key.

It is highly unlikely that an execution key could be guessed by an attacker
in a brute force guessing attack. A given instance of the IPython controller
only runs for a relatively short amount of time (on the order of hours). Thus
an attacker would have only a limited amount of time to test a search space of
size 2\*\*128. For added security, users can have arbitrarily long keys.

```{warning}
If the attacker has gained enough access to intercept loopback connections on _either_ the
controller or client, then a duplicate message can be sent.
CurveZMQ prevents replay attacks.
 To protect against this, CurveZMQ uses nonces.
recipients only allow each signature once, and consider duplicates invalid. However,
the duplicate message could be sent to _another_ recipient using the same key,
and it would be considered valid.
```

### Unauthorized engines

If an attacker were able to connect a hostile engine to a user's controller,
the user might unknowingly send sensitive code or data to the hostile engine.
This attacker's engine would then have full access to that code and data.

This type of attack is prevented in the same way as the unauthorized client
attack,
by requiring the authentication key to register the engine with the client.

### Unauthorized controllers

It is also possible that an attacker could try to convince a user's IPython
client or engine to connect to a hostile IPython controller. That controller
would then have full access to the code and data sent between the IPython
client and the IPython engines.

Again, this attack is prevented through the capabilities in a connection file, which
ensure that a client or engine connects to the correct controller. It is also important to
note that the connection files also encode the IP address and port that the controller is
listening on, so there is little chance of mistakenly connecting to a controller running
on a different IP address and port.

When starting an engine or client, a user must specify the key to use
for that connection. Thus, in order to introduce a hostile controller, the
attacker must convince the user to use the key associated with the
hostile controller. As long as a user is diligent in only using keys from
trusted sources, this attack is not possible.

## Other security measures

A number of other measures are taken to further limit the security risks
involved in running the IPython kernel.

First, by default, the IPython controller listens on random port numbers.
While this can be overridden by the user, in the default configuration, an
attacker would have to do a port scan to find a controller to attack.
When coupled with the relatively short running time of a typical controller
(on the order of hours), scans would have to be constant.

Second, much of the time, especially when run on supercomputers or clusters,
the controller is running behind a firewall. Thus, for engines or client to
connect to the controller:

- The different processes have to all be behind the firewall.

or:

- The user has to use SSH port forwarding to tunnel the
  connections through the firewall.

In either case, an attacker is presented with additional barriers that prevent
attacking or even probing the system,
because they must have access to localhost on either the controller node or the client's machine.

## Summary

IPython's architecture has been carefully designed with security in mind.
The capabilities-based authentication model,
in conjunction with CurveZMQ,
ipc file permissions,
and and/or SSH tunneled TCP/IP channels,
address the core potential vulnerabilities in the system,
while still enabling user's to use the system in open networks.

[openssh]: https://www.openssh.com
[paramiko]: https://www.lag.net/paramiko
[hmac]: https://datatracker.ietf.org/doc/html/rfc2104
[curvezmq]: https://rfc.zeromq.org/spec/26/
