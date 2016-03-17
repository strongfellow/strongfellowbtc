
import argparse
import ConfigParser
import logging
import os.path
import sys
import twitter

import strongfellowbtc.zmq
import strongfellow.hex
import strongfellow.hash

from datetime import datetime

def tweet(t, message):
    logging.info('posting %s', message)
    try:
        status = t.PostUpdate(message)
        logging.info(status)
    except:
        logging.exception('problem with tweet')
    else:
        logging.info('I tweeted, yeah baby...')

def get_twitter():
    config = ConfigParser.ConfigParser()
    config.read(os.path.expanduser('~/.twitrc'))
    print config.sections()
    t = twitter.Api(consumer_key=config.get('twitter', 'consumer_key'),
                    consumer_secret=config.get('twitter', 'consumer_secret'),
                    access_token_key=config.get('twitter', 'access_token_key'),
                    access_token_secret=config.get('twitter', 'access_token_secret'))
    print t.VerifyCredentials()
    return t

TOPIC = 'rawtx'
TEMPLATE = 'Bitcoin Block Mined, %s: http://strongfellow.com/blocks/%s #btc #bitcoin'

def main():
    logging.info('BEGIN')
    logging.basicConfig(level=logging.INFO)

    t = get_twitter()

    with strongfellowbtc.zmq.socket(port=28332, topic=TOPIC) as socket:
        while True:
            topic, body = socket.recv_multipart()
            now = datetime.utcnow()
            if topic == TOPIC:
                block_hash = strongfellowbtc.hex.big_endian_hex(strongfellowbtc.hash.double_sha256(body[:80]))
                message = TEMPLATE % (now, block_hash)
                tweet(t, message)
                sys.exit()
