"""Test CLI application behavior"""
import glob
import json
import os
import sys
import time
import types
from subprocess import check_call
from subprocess import check_output
from subprocess import Popen
from unittest.mock import create_autospec
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import zmq
from ipykernel import iostream
from ipykernel import kernelapp
from ipykernel.ipkernel import IPythonKernel
from IPython.core.profiledir import ProfileDir
from tornado import ioloop
from zmq.eventloop import zmqstream

import ipyparallel as ipp
import ipyparallel.engine.app


def _get_output(cmd):
    out = check_output(cmd)
    if isinstance(out, bytes):
        out = out.decode('utf8', 'replace')
    return out


@pytest.mark.parametrize(
    "submod",
    [
        "cluster",
        "engine",
        "controller",
    ],
)
def test_version(submod):
    out = _get_output([sys.executable, '-m', 'ipyparallel.%s' % submod, '--version'])
    assert out.strip() == ipyparallel.__version__


@pytest.mark.parametrize(
    "submod",
    [
        "cluster",
        "engine",
        "controller",
    ],
)
def test_help_all(submod):
    out = _get_output([sys.executable, '-m', 'ipyparallel.%s' % submod, '--help-all'])


@pytest.mark.parametrize(
    "subcommand",
    [
        "list",
        "engines",
        "start",
        "stop",
        "clean",
    ],
)
def test_ipcluster_help_all(subcommand):
    out = _get_output(
        [sys.executable, '-m', 'ipyparallel.cluster', subcommand, '--help-all']
    )


def bind_kernel(engineapp):
    app = MagicMock(spec=kernelapp.IPKernelApp)
    with patch.object(
        ipyparallel.engine.app, 'IPKernelApp', autospec=True
    ) as MockKernelApp:
        MockKernelApp.return_value = app
        app.shell_port = app.iopub_port = app.stdin_port = 0
        app._bind_socket = types.MethodType(kernelapp.IPKernelApp._bind_socket, app)
        if hasattr(kernelapp.IPKernelApp, '_try_bind_socket'):
            app._try_bind_socket = types.MethodType(
                kernelapp.IPKernelApp._try_bind_socket,
                app,
            )
        app.transport = 'tcp'
        app.ip = '127.0.0.1'
        app.init_heartbeat.return_value = None
        engineapp.bind_kernel()


def test_bind_kernel(request):
    ctx = zmq.Context.instance()
    request.addfinalizer(ctx.destroy)

    class MockIPEngineApp(ipyparallel.engine.app.IPEngine):
        kernel = None
        context = ctx

    app = MockIPEngineApp()
    app.kernel_app = None
    app.kernel = MagicMock(spec=IPythonKernel)

    def socket_spec():
        spec = create_autospec(zmq.Socket, instance=True)
        spec.FD = 2
        return spec

    app.kernel.shell_streams = [
        zmqstream.ZMQStream(
            socket=socket_spec(),
            io_loop=create_autospec(spec=ioloop.IOLoop, spec_set=True, instance=True),
        )
    ]
    app.kernel.control_stream = zmqstream.ZMQStream(
        socket=socket_spec(),
        io_loop=create_autospec(spec=ioloop.IOLoop, spec_set=True, instance=True),
    )

    # testing the case iopub_socket is not replaced with IOPubThread
    iopub_socket = socket_spec()
    app.kernel.iopub_socket = iopub_socket
    assert isinstance(app.kernel.iopub_socket, zmq.Socket)
    bind_kernel(app)
    assert (
        app.kernel.iopub_socket.bind_to_random_port.called
        and app.kernel.iopub_socket.bind_to_random_port.call_count == 1
    )

    # testing the case iopub_socket is replaced with IOPubThread
    class TestIOPubThread(iostream.IOPubThread):
        socket = None

    iopub_socket.reset_mock()
    app.kernel_app = None
    app.kernel.iopub_socket = create_autospec(
        spec=TestIOPubThread, spec_set=True, instance=True
    )
    app.kernel.iopub_socket.socket = iopub_socket
    assert isinstance(app.kernel.iopub_socket, iostream.IOPubThread)
    bind_kernel(app)
    assert (
        app.kernel.iopub_socket.socket.bind_to_random_port.called
        and app.kernel.iopub_socket.socket.bind_to_random_port.call_count == 1
    )


def ipcluster_list(*args):
    return check_output(
        [sys.executable, "-m", "ipyparallel.cluster", "list"] + list(args)
    ).decode("utf8", "replace")


def test_ipcluster_list(Cluster):

    # no clusters
    out = ipcluster_list()
    assert len(out.splitlines()) == 1
    out = ipcluster_list("-o", "json")
    assert json.loads(out) == []

    with Cluster(n=2) as rc:
        out = ipcluster_list()
        head, *rest = out.strip().splitlines()
        assert len(rest) == 1
        assert rest[0].split() == [
            "default",
            rc.cluster.cluster_id,
            "True",
            "2",
            "Local",
        ]
        cluster_list = json.loads(ipcluster_list("-o", "json"))
        assert len(cluster_list) == 1
        assert cluster_list[0]['cluster']['cluster_id'] == rc.cluster.cluster_id

    # after exit, back to no clusters
    out = ipcluster_list()
    assert len(out.splitlines()) == 1
    out = ipcluster_list("-o", "json")
    assert json.loads(out) == []


@pytest.mark.parametrize("daemonize", (False, True))
def test_ipcluster_start_stop(request, ipython_dir, daemonize):
    default_profile = ProfileDir.find_profile_dir_by_name(ipython_dir)
    default_profile_dir = default_profile.location

    # cleanup the security directory to avoid leaking files from one test to the next
    def cleanup_security():
        for f in glob.glob(os.path.join(default_profile.security_dir, "*.json")):
            print(f"Cleaning up {f}")
            try:
                os.remove(f)
            except Exception as e:
                print(f"Error removing {f}: {e}")

    request.addfinalizer(cleanup_security)

    n = 2
    start_args = ["-n", str(n)]
    if daemonize:
        start_args.append("--daemonize")
    start = Popen(
        [sys.executable, "-m", "ipyparallel.cluster", "start", "--debug"] + start_args
    )
    request.addfinalizer(start.terminate)
    if daemonize:
        # if daemonize, should exit after starting
        start.wait(30)
    else:
        # wait for file to appear
        # only need this if not daemonize
        cluster_file = ipp.Cluster(
            profile_dir=default_profile_dir, cluster_id=""
        ).cluster_file
        for i in range(100):
            if os.path.isfile(cluster_file) or start.poll() is not None:
                break
            else:
                time.sleep(0.1)
        assert os.path.isfile(cluster_file)

    # list should show a file
    out = ipcluster_list()
    assert len(out.splitlines()) == 2

    # cluster running, try to connect with default args
    cluster = ipp.Cluster.from_file(log_level=10)
    try:
        with cluster.connect_client_sync() as rc:
            rc.wait_for_engines(n=2, timeout=60)
            rc[:].apply_async(os.getpid).get(timeout=10)
    except Exception:
        print("controller output")
        print(cluster.controller.get_output())
        print("engine output")
        for engine_set in cluster.engines.values():
            print(engine_set.get_output())
        raise

    # stop with ipcluster stop
    check_call([sys.executable, "-m", "ipyparallel.cluster", "stop"])
    # start should exit when this happens
    start.wait(30)

    # and ipcluster list should return empty
    out = ipcluster_list()
    assert len(out.splitlines()) == 1

    # stop all succeeds even if there's nothing to stop
    check_call([sys.executable, "-m", "ipyparallel.cluster", "stop", "--all"])


def test_ipcluster_clean(ipython_dir):
    default_profile = ProfileDir.find_profile_dir_by_name(ipython_dir)
    default_profile_dir = default_profile.location
    log_file = os.path.join(default_profile.log_dir, "test.log")
    with open(log_file, "w") as f:
        f.write("logsssss")
    cluster_file = os.path.join(default_profile.security_dir, "cluster-abc.json")
    c = ipp.Cluster()
    with open(cluster_file, 'w') as f:
        json.dump(c.to_dict(), f)
    connection_file = os.path.join(
        default_profile.security_dir, "ipcontroller-client.json"
    )
    with open(connection_file, 'w') as f:
        f.write("{}")
    check_call([sys.executable, "-m", "ipyparallel.cluster", "clean"])

    assert not os.path.exists(log_file)
    assert not os.path.exists(cluster_file)
    assert not os.path.exists(connection_file)
