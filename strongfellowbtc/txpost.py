
from __future__ import absolute_import

import argparse
import logging
import requests
import sys

import strongfellowbtc.constants as constants
import strongfellowbtc.zmq
from strongfellowbtc.logging import configure_logging
from strongfellowbtc.protocol import ds256
from strongfellowbtc.hex import big_endian_hex

def post_incoming_transactions(args=None):
    configure_logging()
    if args is None:
        args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument('--txport', type=int, default=str(constants.RAW_TX_PORT))

    params = parser.parse_args(args)
    
    with strongfellowbtc.zmq.socket(port=params.txport, topic='rawtx') as socket:
        while True:
            logging.info('receiving transactions from zeromq')
            topic, tx = socket.recv_multipart()
            h = big_endian_hex(ds256(tx))
            url = 'http://localhost:8080/transactions/%s' % h
            logging.info('received tx, posting to %s', url)
            r = requests.post(url, data=tx)
            logging.info('posted tx %s with status code %d', h, r.status_code)
