"""Test CLI application behavior"""
import sys
from subprocess import check_output
import types

import zmq
from zmq.eventloop import zmqstream

import ipyparallel
from ipyparallel.util import ioloop
from ipyparallel.apps import ipengineapp
from ipykernel.ipkernel import IPythonKernel
from ipykernel import kernelapp, iostream

try:
    from unittest.mock import patch, MagicMock, create_autospec
except ImportError: # py2
    from mock import patch, MagicMock, create_autospec


def _get_output(cmd):
    out = check_output([sys.executable, '-m', 'ipyparallel.cluster', '--version'])
    if isinstance(out, bytes):
        out = out.decode('utf8', 'replace')
    return out

def test_version():
    for submod in ['cluster', 'engine', 'controller']:
        out = _get_output([sys.executable, '-m', 'ipyparallel.%s' % submod, '--version'])
        assert out.strip() == ipyparallel.__version__


def bind_kernel(engineapp):
    app = MagicMock(spec=kernelapp.IPKernelApp)
    with patch.object(ipengineapp, 'IPKernelApp', autospec=True) as \
            MockKernelApp:
        MockKernelApp.return_value = app
        app.shell_port = app.iopub_port = app.stdin_port = 0
        app._bind_socket = types.MethodType(kernelapp.IPKernelApp._bind_socket,
                                            app)
        app.transport = 'tcp'
        app.ip = 'localhost'
        app.init_heartbeat.return_value = None
        engineapp.bind_kernel()


def test_bind_kernel():
    class MockIPEngineApp(ipengineapp.IPEngineApp):
        kernel = None
        engine = MagicMock(spec=ipengineapp.EngineFactory)

    app = MockIPEngineApp()
    app.kernel_app = None
    app.kernel = MagicMock(spec=IPythonKernel)
    def socket_spec():
        spec = create_autospec(zmq.Socket, instance=True)
        spec.FD = 2
        return spec
    app.kernel.shell_streams = [zmqstream.ZMQStream(
        socket=socket_spec(),
        io_loop=create_autospec(spec=ioloop.IOLoop, spec_set=True,
                                instance=True))]
    app.kernel.control_stream = zmqstream.ZMQStream(
        socket=socket_spec(),
        io_loop=create_autospec(spec=ioloop.IOLoop, spec_set=True,
                                instance=True))

    # testing the case iopub_socket is not replaced with IOPubThread
    iopub_socket = socket_spec()
    app.kernel.iopub_socket = iopub_socket
    assert(isinstance(app.kernel.iopub_socket, zmq.Socket))
    bind_kernel(app)
    assert(app.kernel.iopub_socket.bind_to_random_port.called and
           app.kernel.iopub_socket.bind_to_random_port.call_count == 1)

    # testing the case iopub_socket is replaced with IOPubThread
    class TestIOPubThread(iostream.IOPubThread):
        socket = None

    iopub_socket.reset_mock()
    app.kernel_app = None
    app.kernel.iopub_socket = create_autospec(
            spec=TestIOPubThread, spec_set=True, instance=True)
    app.kernel.iopub_socket.socket = iopub_socket
    assert(isinstance(app.kernel.iopub_socket, iostream.IOPubThread))
    bind_kernel(app)
    assert(app.kernel.iopub_socket.socket.bind_to_random_port.called and
           app.kernel.iopub_socket.socket.bind_to_random_port.call_count == 1)
