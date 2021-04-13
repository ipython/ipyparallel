"""A script for watching all traffic on the IOPub channel (stdout/stderr/pyerr) of engines.

This connects to the default cluster, or you can pass the path to your ipcontroller-client.json

Try running this script, and then running a few jobs that print (and call sys.stdout.flush),
and you will see the print statements as they arrive, notably not waiting for the results
to finish.

You can use the zeromq SUBSCRIBE mechanism to only receive information from specific engines,
and easily filter by message type.

Authors
-------
* MinRK
"""
import json
import sys

import zmq
from ipykernel.connect import find_connection_file
from jupyter_client.session import Session


def main(connection_file):
    """watch iopub channel, and print messages"""

    ctx = zmq.Context.instance()

    with open(connection_file) as f:
        cfg = json.loads(f.read())

    reg_url = cfg['interface']
    iopub_port = cfg['iopub']
    iopub_url = "%s:%s" % (reg_url, iopub_port)

    session = Session(key=cfg['key'].encode('ascii'))
    sub = ctx.socket(zmq.SUB)

    # This will subscribe to all messages:
    sub.SUBSCRIBE = b''
    # replace with b'' with b'engine.1.stdout' to subscribe only to engine 1's stdout
    # 0MQ subscriptions are simple 'foo*' matches, so 'engine.1.' subscribes
    # to everything from engine 1, but there is no way to subscribe to
    # just stdout from everyone.
    # multiple calls to subscribe will add subscriptions, e.g. to subscribe to
    # engine 1's stderr and engine 2's stdout:
    # sub.SUBSCRIBE = b'engine.1.stderr'
    # sub.SUBSCRIBE = b'engine.2.stdout'
    sub.connect(iopub_url)
    while True:
        try:
            idents, msg = session.recv(sub, mode=0)
        except KeyboardInterrupt:
            return
        # ident always length 1 here
        topic = idents[0].decode('utf8', 'replace')
        if msg['msg_type'] == 'stream':
            # stdout/stderr
            # stream names are in msg['content']['name'], if you want to handle
            # them differently
            print("%s: %s" % (topic, msg['content']['text']))
        elif msg['msg_type'] == 'error':
            # Python traceback
            c = msg['content']
            print(topic + ':')
            for line in c['traceback']:
                # indent lines
                print('    ' + line)
        elif msg['msg_type'] == 'error':
            # Python traceback
            c = msg['content']
            print(topic + ':')
            for line in c['traceback']:
                # indent lines
                print('    ' + line)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        pattern = sys.argv[1]
    else:
        # This gets the security file for the default profile:
        pattern = 'ipcontroller-client.json'
    cf = find_connection_file(pattern)
    print("Using connection file %s" % cf)
    main(cf)
