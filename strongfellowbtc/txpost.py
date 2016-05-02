
import argparse
import msgpack
import requests

def post_incoming_transactions(args=None):
    if args is None:
        args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', required=True)
    parser.add_argument('--network', required=True)
    params = parser.parse_args(args)
    
    with strongfellowbtc.zmq.socket(port=args.txport, topic='rawtx') as socket:
        while True:
            topic, tx = socket.recv_multipart()
            delta = datetime.now() - datetime(1970, 1, 1)
            ms = long(delta.total_seconds() * 1000)
            data = msgpack.packb({
                't': ms, # milliseconds since epoch
                'x': tx, # the transaction
                'h': params.host, # short name of the host
                'n': params.network # main, testnet, segnet, etc.
            })
            r = requests.post('http://localhost:8080/transactions', data=data)
            print r.status_code
