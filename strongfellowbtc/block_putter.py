
import argparse
import boto3
import botocore
import hashlib
import sys
import logging
import strongfellowbtc.block_io as io
import strongfellowbtc.hash
import strongfellowbtc.hex

class BlockPutter:
    def __init__(self, bucket, prefix):
        self._bucket = bucket
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
        block_hash = strongfellowbtc.hex.big_endian_hex(strongfellowbtc.hash.double_sha256(block[:80]))
        if not block_hash.startswith('0000'):
            raise Exception('block hash %s doesnt have leading zeroes' % h)
        key = '%s/%s' % (self._prefix, block_hash)
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
        logging.info('counters:\n' + "\n".join( '\t%s\t%d' % (k, v) for (k, v) in sorted(self._counters.items())))



def _args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('--network', default='f9beb4d9')
    parser.add_argument('--bucket', default='strongfellow.com')
    parser.add_argument('--prefix', default='blocks')
    return parser.parse_args(args)

def main(args=None):
    logging.basicConfig(level=logging.INFO)
    if args is None:
        args = sys.argv[1:]

    args = _args(args)
    expected_network = args.network.decode('hex')

    putter = BlockPutter(bucket=args.bucket, prefix=args.prefix)
    for (network, block, h) in io.generate_blocks(sys.stdin):
        if network != expected_network:
            raise Exception('unexpected network: %s; expected %s' % (strongfellowbtc.hex.little_endian_hex(network), strongfellowbtc.hex.little_endian_hex(expected_network)))
        putter.put_block(block)

    logging.info('SUCCESS')
