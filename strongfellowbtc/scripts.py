
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

def table_time(dt=None, tables_per_day=1, lead_time=2):
    if dt is None:
        dt = datetime.utcnow()
    dt = dt.replace(hour=dt.hour + lead_time)
    new_hour = ((dt.hour * tables_per_day) / 24) * (24 / tables_per_day)
    dt = dt.replace(hour=new_hour, minute=0, second=0, microsecond=0)
    return dt

date_format = '%Y-%m-%dT%H'
def _cta_args(args):
    default_date_time = table_time().strftime(date_format)
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

    logging.info('creating table %s', table_name)

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
    logging.info('created table %s', table_name)

    logging.info("Table status: %s", table)


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
                    q.put_nowait((ms, tx))
                except Queue.Full:
                    logging.exception('we cant put %s' % strongfellowbtc.hex.big_endian_hex(strongfellowbtc.hash.double_sha256(tx)))

    def consume(q):
        client = boto3.client('dynamodb')
        items = []
        def _wcu(item):
            size = 0
            for k,v in item.iteritems():
                size += len(k)
                for _, payload in v.iteritems():
                    size += len(payload)
            return math.ceil(size / 1000)

        def _post(items):
            table_name = 'tx-us-west-2-dev-giraffe-2016-03-23T00'
            logging.info('putting %d items to table %s', len(items), table_name)
            response = client.batch_write_item(
                RequestItems={
                    table_name: [{'PutRequest': { 'Item': x }} for x in items]
                },
                ReturnConsumedCapacity='TOTAL',
                ReturnItemCollectionMetrics='SIZE')
            print response
            logging.info('SUCCESS putting %d items to table %s', len(items), table_name)

        wcu_carry = 0
        carry = []
        while True:
            items = carry
            wcu_sum = wcu_carry
            if q.empty():
               time.sleep(1)
               logging.info('no transactions, sleeping for a second')
            else:
                n = q.qsize()
                logging.info('%d transactions equeued', n)
                while len(items) < n:
                    ms, tx = q.get_nowait()
                    item = {
                        'txhash': { 'B': strongfellowbtc.hash.double_sha256(tx) },
                        'created': { 'N': str(ms) },
                        'tx': { 'B': tx }
                    }
                    wcu = _wcu(item)
                    if wcu_sum + wcu > 25:
                        carry = [item]
                        wcu_carry = wcu
                        break
                    else:
                        items.append(put_request)
                        wcu_sum += wcu
                        carry = []
                        wcu_carry = 0
                _post(items)


    t1 = threading.Thread(target=produce, args=(q,))
    t2 = threading.Thread(target=consume, args=(q,))

    t1.start()
    t2.start()
    logging.info('join us, wont you?')
    t1.join()
    t2.join()
