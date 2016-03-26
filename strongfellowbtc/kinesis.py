
from __future__ import absolute_import

import argparse
from datetime import datetime
import boto3
import logging
import Queue
import sys
import time
import threading
import uuid

import strongfellowbtc.hex
import strongfellowbtc.hash
import strongfellowbtc.zmq
from strongfellowbtc.logging import configure_logging

def _stream_name(region, env, host):
    return 'strongfellow-{region}-{env}-{host}'.format(region=region, env=env, host=host)

def little_endian_long(n):
    bs = bytearray(8)
    i = 0
    while n != 0:
        bs[i] = n & 0xff
        n = (n >> 8)
        i += 1
    return bytes(bs)

def create_stream(args=None):
    configure_logging()
    if args is None:
        args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument('--region', required=True)
    parser.add_argument('--env', required=True)
    parser.add_argument('--host', required=True)
    parser.add_argument('--shard-count', default='1', type=int)
    params = parser.parse_args(args)

    kinesis = boto3.client('kinesis')

    stream_name = _stream_name(region=params.region, env=params.env, host=params.host)
    shard_count = params.shard_count

    logging.info('creating stream %s with shard count %d', stream_name, shard_count)
    response = kinesis.create_stream(
        StreamName=stream_name,
        ShardCount=shard_count
    )
    logging.info('success: created stream %s with shard count %d', stream_name, shard_count)

def stream_incoming_transactions(args=None):
    configure_logging()
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument('--maxsize', type=int, default='300')
    parser.add_argument('--txport', type=int, default='28332')
    parser.add_argument('--region', required=True)
    parser.add_argument('--env', required=True)
    parser.add_argument('--host', required=True)
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
                    logging.exception('Queue is Full: we cant put %s' % strongfellowbtc.hex.big_endian_hex(strongfellowbtc.hash.double_sha256(tx)))

    def consume(q):
        kinesis = boto3.client('kinesis')
        stream_name = _stream_name(region=args.region, env=args.env, host=args.host)
        while True:
            if q.empty():
               time.sleep(1)
               logging.info('no transactions, sleeping for a second')
            else:
                records = []
                n = q.qsize()
                logging.info('%d transactions equeued', n)
                while len(records) < n:
                    ms, tx = q.get_nowait()
                    record = {
                        'Data': little_endian_long(ms) + tx,
                        'PartitionKey': str(uuid.uuid4())
                    }
                    records.append(record)
                try:
                    response = kinesis.put_records(
                        Records=records,
                        StreamName=stream_name
                    )
                    logging.info('SUCCESS putting records')
                except:
                    logging.exception('problem putting records')
                    time.sleep(3)

    t1 = threading.Thread(target=produce, args=(q,))
    t2 = threading.Thread(target=consume, args=(q,))

    t1.start()
    t2.start()
    logging.info('join us, wont you?')
    t1.join()
    t2.join()
