
import argparse
import boto3
from collections import namedtuple
from datetime import datetime, timedelta
from operator import truediv
import logging
import math
import sys
import time
import threading
import Queue

import strongfellowbtc.hash
import strongfellowbtc.hex
import strongfellowbtc.zmq
import strongfellowbtc.block_putter

TableSpecs = namedtuple('TableSpecs', ['rcu', 'wcu', 'name'])

def _table_specs(args):
    def _valid_date(s):
        date_format = '%Y-%m-%dT%H'
        try:
            return datetime.strptime(s, date_format).strftime(date_format)
        except ValueError:
            raise argparse.ArgumentTypeError('Not a valid date: "{0}".'.format(s))

    parser = argparse.ArgumentParser()
    parser.add_argument('--region', required=True)
    parser.add_argument('--env', required=True)
    parser.add_argument('--host', required=True)
    parser.add_argument('--date', required=True, type=_valid_date)
    parser.add_argument('--rcu', default=10, type=int)
    parser.add_argument('--wcu', default=10, type=int)
    vs = parser.parse_args(args)
    name = 'tx-{region}-{env}-{host}-{date}'.format(
        region=vs.region, env=vs.env, host=vs.host, date=vs.date)
    return TableSpecs(rcu=vs.rcu, wcu=vs.wcu, name=name)

def _configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def _s3_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', default='strongfellow.com')
    parser.add_argument('--prefix', default='blocks')
    return parser.parse_args(args)

def stash_incoming_blocks(args=None):
    _configure_logging()
    if args is None:
        args = sys.argv[1:]

    args = _s3_args(args)
    logging.info('BEGIN')

    putter = strongfellowbtc.block_putter.BlockPutter(bucket=args.bucket, prefix=args.prefix)
    with strongfellowbtc.zmq.socket(port=28332, topic='rawblock') as socket:
        while True:
            topic, block = socket.recv_multipart()
            putter.put_block(block)

#
# delete-tx-table --region us-west-2 --env dev --host giraffe --date 2016-03-24T00
#
def delete_transactions_table(args=None):
    _configure_logging()
    if args is None:
        args = sys.argv[1:]
    table_specs = _table_specs(args)
    client = boto3.client('dynamodb')
    logging.info('deleting table %s', table_specs.name)
    response = client.delete_table(
        TableName=table_specs.name
    )
    logging.info('delete response: %s', response)
    logging.info('SUCCESS deleting table %s', table_specs.name)

def create_transactions_table(args=None):
    _configure_logging()
    if args is None:
        args = sys.argv[1:]
    table_specs = _table_specs(args)

    client = boto3.client('dynamodb')
    response = client.create_table(
        TableName=specs.name,
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
            'ReadCapacityUnits': specs.rcu,
            'WriteCapacityUnits': specs.wcu
        }
    )
    logging.info('created table %s', specs.name)
    logging.info("table status: %s", response)

def stash_incoming_transactions(args=None):

    _configure_logging()
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
                    q.put_nowait((ms, tx))
                except Queue.Full:
                    logging.exception('we cant put %s' % strongfellowbtc.hex.big_endian_hex(strongfellowbtc.hash.double_sha256(tx)))

    def consume(q):
        client = boto3.client('dynamodb')
        def _post(items):
            table_name = 'tx-us-west-2-dev-giraffe-2016-03-23T00'
            logging.info('putting %d items to table %s', len(items), table_name)
            response = client.batch_write_item(
                RequestItems={
                    table_name: [{'PutRequest': { 'Item': x }} for x in items]
                },
                ReturnConsumedCapacity='TOTAL',
                ReturnItemCollectionMetrics='SIZE')
            logging.info(response)
            logging.info('SUCCESS putting %d items to table %s', len(items), table_name)

        while True:
            items = []
            if q.empty():
               time.sleep(1)
               logging.info('no transactions, sleeping for a second')
            else:
                n = q.qsize()
                logging.info('%d transactions equeued', n)
                while len(items) < n and len(items) < 25:
                    ms, tx = q.get_nowait()
                    item = {
                        'txhash': { 'B': strongfellowbtc.hash.double_sha256(tx) },
                        'created': { 'N': str(ms) },
                        'tx': { 'B': tx }
                    }
                    items.append(item)
                try:
                    _post(items)
                except:
                    logging.exception('problem posting items')
                    time.sleep(3)

    t1 = threading.Thread(target=produce, args=(q,))
    t2 = threading.Thread(target=consume, args=(q,))

    t1.start()
    t2.start()
    logging.info('join us, wont you?')
    t1.join()
    t2.join()
