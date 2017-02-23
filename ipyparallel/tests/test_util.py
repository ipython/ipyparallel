import socket
from ipyparallel import util
from jupyter_client.localinterfaces import localhost, public_ips


def test_disambiguate_ip():
    # garbage in, garbage out
    public_ip = public_ips()[0]
    assert util.disambiguate_ip_address('garbage') == 'garbage'
    assert util.disambiguate_ip_address('0.0.0.0', socket.gethostname()) == localhost()
    wontresolve = 'this.wontresolve.dns'
    assert util.disambiguate_ip_address('0.0.0.0', wontresolve) == wontresolve
    assert util.disambiguate_ip_address('0.0.0.0', public_ip) == localhost()
