
import argparse
import logging
import sys

import strongfellowbtc.hash
import strongfellowbtc.hex
import strongfellowbtc.zmq
import strongfellowbtc.block_putter

def _args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', default='strongfellow.com')
    parser.add_argument('--prefix', default='blocks')
    return parser.parse_args(args)

def stash_incoming_blocks(args=None):
    logging.basicConfig(level=logging.INFO)
    if args is None:
        args = sys.argv[1:]

    args = _args(args)
    logging.info('BEGIN')

    putter = strongfellowbtc.block_putter.BlockPutter(bucket=args.bucket, prefix=args.prefix)
    with strongfellowbtc.zmq.socket(port=28332, topic='rawblock') as socket:
        while True:
            topic, block = socket.recv_multipart()
            putter.put_block(block)
