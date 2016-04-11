
from __future__ import absolute_import

import argparse
import botocore
import hashlib
import sys
import logging

import boto3

import strongfellowbtc.constants as constants
import strongfellowbtc.block_io as block_io
from strongfellowbtc.logging import configure_logging
from strongfellowbtc.protocol import ds256
import strongfellowbtc.hex
import strongfellowbtc.zmq

def _args(args=None):
    if args is None:
        args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument('--network',
                        default=constants.NETWORK_MAIN,
                        choices=constants.NETWORKS.keys())
    parser.add_argument('--region', required=True)
    return parser.parse_args(args)

def _block_putter(network_name, region):
    bucket = 'blocks-{region}.strongfellow.com'.format(region)
    return BlockPutter(bucket=bucket, prefix=network_name)

class BlockPutter:
    def __init__(self, bucket, prefix):
        self._bucket = bucket

        while prefix.endswith('/'):
            prefix = prefix[:-1]
        while prefix.startswith('/'):
            prefix = prefix[1:]

        self._prefix = prefix
        
        self._s3 = boto3.client('s3')
        self._counters = {
            'N': 0,
            'HIT': 0,
            'MISS_NOT_FOUND': 0,
            'MISS_WRONG_MD5': 0,
            'PUT': 0
        }

    def put_block(self, block):
        self._counters['N'] += 1
        md5 = strongfellowbtc.hex.little_endian_hex(hashlib.md5(block).digest())
        block_hash = strongfellowbtc.hex.big_endian_hex(ds256(block[:80]))
        key = block_hash if self._prefix == '' else self._prefix + '/' + block_hash
        logging.info('begin putting %s', block_hash)

        try:
            response = self._s3.head_object(Bucket=self._bucket, Key=key, IfMatch=md5)
            logging.info('cache hit for %s' % key)
            self._counters['HIT'] += 1
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404 or error_code == 412:
                if error_code == 404:
                    self._counters['MISS_NOT_FOUND'] += 1
                    logging.info('not found')
                if error_code == 412:
                    self._counters['MISS_WRONG_MD5'] += 1
                    logging.info('found but md5 mismatch')
                logging.info('putting %s', key)
                self._s3.put_object(Bucket=self._bucket, Key=key, Body=block)
                logging.info('SUCCESS putting %s', key)
                self._counters['PUT'] += 1
            else:
                logging.exception('unknown exception')
                raise
        logging.info('finished putting %s', block_hash)
        logging.info('counters:\n' + "\n".join( '\t%s\t%d' % (k, v) for (k, v) in sorted(self._counters.items())))

def main(args=None):
    configure_logging()
    args = _args(args)
    putter = _block_putter(network_name=args.network, region=args.region)
    expected_magic = constants.NETWORKS[args.network].magic
    logging.info('BEGIN')
    for (magic, block, h) in block_io.generate_blocks(sys.stdin):
        if magic != expected_magic:
            raise Exception('unexpected magic: %s; expected %s' % (strongfellowbtc.hex.little_endian_hex(magic), strongfellowbtc.hex.little_endian_hex(expected_magic)))
        putter.put_block(block)
    logging.info('SUCCESS')

def stash_incoming_blocks(args=None):
    configure_logging()
    args = _args(args)
    putter = _block_putter(network_name=args.network, region=args.region)
    with strongfellowbtc.zmq.socket(port=constants.RAW_BLOCK_PORT, topic='rawblock') as socket:
        while True:
            logging.info('receiving blocks...')
            topic, block = socket.recv_multipart()
            putter.put_block(block)
