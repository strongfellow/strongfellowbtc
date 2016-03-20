
import argparse
import boto3
from datetime import datetime, timedelta
import logging
import sys
import time
import threading
import Queue

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

date_format = '%Y-%m-%dT%H'
def _cta_args(args):
    default_date_time = (datetime.utcnow() + timedelta(hours=3)).replace(hour=0, minute=0, microsecond=0).strftime(date_format)
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--env', required=True)
    parser.add_argument('--rcu', default=10, type=int)
    parser.add_argument('--wcu', default=10, type=int)
    parser.add_argument('--date', default=default_date_time, type=_valid_date)
    return parser.parse_args(args)

def _valid_date(s):
    try:
        return datetime.strptime(s, date_format).strftime(date_format)
    except ValueError:
        raise argparse.ArgumentTypeError('Not a valid date: "{0}".'.format(s))

def create_transactions_table(args=None):
    logging.basicConfig(level=logging.INFO)
    if args is None:
        args = sys.argv[1:]
    args = _cta_args(args)

    table_name = 'tx-{region}-{env}-{host}-{date}'.format(
        host=args.host, region=args.region, env=args.env, date=args.date)

    logging.info('creating table {}', table_name)

    client = boto3.client('dynamodb')
    table = client.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'txhash',
                'KeyType': 'HASH' # Partition Key
            },
            {
                'AttributeName': 'created',
                'KeyType': 'RANGE' # Partition Key
            },
        ],
        AttributeDefinitions=[
            { 'AttributeName': 'txhash',  'AttributeType': 'B'},
            { 'AttributeName': 'created', 'AttributeType': 'N' }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': args.rcu,
            'WriteCapacityUnits': args.wcu
        }
    )
    logging.info('created table {}', table_name)

    logging.info("Table status: {}", table)


def stash_incoming_transactions(args=None):

    logging.basicConfig(level=logging.INFO)
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument('--maxsize', type=int, default='100')
    parser.add_argument('--txport', type=int, default='28332')
    args = parser.parse_args(args)

    q = Queue.Queue(maxsize=args.maxsize)

    def produce(q):
        with strongfellowbtc.zmq.socket(port=args.txport, topic='rawtx') as socket:
            while True:
                topic, tx = socket.recv_multipart()
                delta = datetime.now() - datetime(1970, 1, 1)
                ms = long(delta.total_seconds() * 1000)
                try:
                    q.put((ms, tx))
                except Queue.Full:
                    logging.exception('we cant put %s' % strongfellowbtc.hex(hash))


    def consume(q):
        while True:
            items = []
            while len(items) < 25 and not q.empty():
                ms, tx = q.get()
                items.append(q.get())
                hash = strongfellowbtc.hash.double_sha256(tx)
                logging.info('were going to put %s' % strongfellowbtc.hex(hash))

    t1 = threading.Thread(target=produce, args=(q,))
    t2 = threading.Thread(target=consume, args=(q,))

    t1.start()
    t2.start()
    logging.info('join us, wont you?')
    t1.join()
    t2.join()
