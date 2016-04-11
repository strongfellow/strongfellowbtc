
from __future__ import absolute_import

import argparse
from datetime import datetime
import logging
import Queue
import sys
import time
import threading
import uuid

import boto3
import msgpack

import strongfellowbtc.constants as constants
import strongfellowbtc.hex
from strongfellowbtc.protocol import ds256
import strongfellowbtc.zmq
from strongfellowbtc.logging import configure_logging

def k(region):
    return boto3.client('kinesis', region_name=region)

def _stream_name(region, env):
    return 'strongfellow-tx-{region}-{env}'.format(region=region, env=env)

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

    kinesis = k(params.region)

    stream_name = _stream_name(region=params.region, env=params.env)
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
    parser.add_argument('--txport', type=int, default=str(constants.RAW_TX_PORT))
    parser.add_argument('--region', required=True)
    parser.add_argument('--env', required=True)
    parser.add_argument('--host', required=True)
    parser.add_argument('--network', default='main', choices=constants.NETWORKS.keys())
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
                    logging.exception('Queue is Full: we cant put %s' % strongfellowbtc.hex.little_endian_hex(ds256(tx)))

    def consume(q):
        kinesis = k(region=args.region)
        stream_name = _stream_name(region=args.region, env=args.env)
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
                    data = msgpack.packb({
                        't': ms, # milliseconds since epoch
                        'x': tx, # the transaction
                        'h': args.host, # short name of the host
                        'n': args.network # main, testnet, segnet, etc.
                    })
                    partition_key = strongfellowbtc.hex.big_endian_hex(ds256(tx))
                    record = {
                        'Data': data,
                        'PartitionKey': partition_key
                    }
                    records.append(record)
                try:
                    response = kinesis.put_records(
                        Records=records,
                        StreamName=stream_name
                    )
                    logging.info('response was: %s', response)
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

def test_get_records(args = None):
    configure_logging()
    if args is None:
        args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument('--region', required=True)
    parser.add_argument('--env', required=True)
    parser.add_argument('--host', required=True)
    params = parser.parse_args(args)
    
    kinesis = k(region=params.region)
    stream_name = _stream_name(region=params.region, env=params.env)
    shard_id = 'shardId-000000000000'
    shard_iterator = kinesis.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType="LATEST")['ShardIterator']
    while True:
        response = kinesis.get_records(ShardIterator=shard_iterator, Limit=1000)
        shard_iterator = response['NextShardIterator']
        for record in response['Records']:
            d = msgpack.unpackb(record['Data'])
            for key in d:
                print 'key: %s' % key
            print strongfellowbtc.hex.big_endian_hex(ds256(d['x']))
        print response
        time.sleep(1)

