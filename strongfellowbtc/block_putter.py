
import boto3
import botocore
import hashlib
import sys
import logging
import strongfellowbtc.block_io as io
import strongfellowbtc.hex as hex

MAIN_NETWORK = 'f9beb4d9'

def main():
    logging.basicConfig(level=logging.INFO)
    s3 = boto3.client('s3')
    BUCKET = 'strongfellow.com'
    counters = {
        'N': 0,
        'HIT': 0,
        'MISS_NOT_FOUND': 0,
        'MISS_WRONG_MD5': 0,
        'PUT': 0
    }
    for (network, block, h) in io.generate_blocks(sys.stdin):
        counters['N'] += 1
        network = hex.little_endian_hex(network)
        h = hex.big_endian_hex(h)

        if network != MAIN_NETWORK:
            raise Exception('unexpected network: %s' % network)
        if not h.startswith('0000'):
            raise Exception('block hash %s doesnt have leading zeroes' % h)

        md5 = hex.little_endian_hex(hashlib.md5(block).digest())
        print '%s\t%d\t%s\t%s' % (network, len(block), h, md5)
        key = 'blocks/%s' % h
        try:
            response = s3.head_object(Bucket=BUCKET, Key=key, IfMatch=md5)
            logging.info('cache hit for %s' % key)
            counters['HIT'] += 1
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404 or error_code == 412:
                if error_code == 404:
                    counters['MISS_NOT_FOUND'] += 1
                    logging.info('not found')
                if error_code == 412:
                    counters['MISS_WRONG_MD5'] += 1
                    logging.info('found but md5 mismatch')
                logging.info('putting %s', key)
                s3.put_object(Bucket=BUCKET, Key=key, Body=block)
                logging.info('SUCCESS putting %s', key)
                counters['PUT'] += 1
            else:
                logging.exception('unknown exception')
                raise
        logging.info('counters:\n' + "\n".join( '\t%s\t%d' % (k, v) for (k, v) in sorted(counters.items())))
    logging.info('SUCCESS')

if __name__ == '__main__':
    main()
