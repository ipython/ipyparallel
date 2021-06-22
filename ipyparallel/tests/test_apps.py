"""Test CLI application behavior"""
import sys
import types
from subprocess import check_output
from unittest.mock import create_autospec
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import zmq
from ipykernel import iostream
from ipykernel import kernelapp
from ipykernel.ipkernel import IPythonKernel
from tornado import ioloop
from zmq.eventloop import zmqstream

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
