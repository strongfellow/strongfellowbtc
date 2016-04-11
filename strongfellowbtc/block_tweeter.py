
from __future__ import absolute_import

import argparse
import ConfigParser
import logging
import os.path
import sys
import twitter

import strongfellowbtc.zmq
import strongfellowbtc.hex
from strongfellowbtc.logging import configure_logging
from strongfellowbtc.protocol import ds256

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

TOPIC = 'rawblock'
TEMPLATE = 'Block Mined, %s: http://strongfellow.com/blocks/%s #bitcoin'

def main():
    configure_logging()
    logging.info('BEGIN')
    t = get_twitter()
    with strongfellowbtc.zmq.socket(port=28332, topic=TOPIC) as socket:
        while True:
            topic, body = socket.recv_multipart()
            now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
            block_hash = strongfellowbtc.hex.big_endian_hex(ds256(body[:80]))
            message = TEMPLATE % (now, block_hash)
            tweet(t, message)
