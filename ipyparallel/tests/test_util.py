import socket
try:
    from unittest import mock
except ImportError:
    import mock
from ipyparallel import util
from jupyter_client.localinterfaces import localhost, public_ips


@mock.patch('warnings.warn')
def test_disambiguate_ip(warn_mock):
    # garbage in, garbage out
    public_ip = public_ips()[0]
    assert util.disambiguate_ip_address('garbage') == 'garbage'
    assert util.disambiguate_ip_address('0.0.0.0', socket.gethostname()) == localhost()
    wontresolve = 'this.wontresolve.dns'
    assert util.disambiguate_ip_address('0.0.0.0', wontresolve) == wontresolve
    assert warn_mock.called_once_with(
        'IPython could not determine IPs for {}: '
        '[Errno -2] Name or service not known'.format(wontresolve),
        RuntimeWarning
    )
    assert util.disambiguate_ip_address('0.0.0.0', public_ip) == localhost()
