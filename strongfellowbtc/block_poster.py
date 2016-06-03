
import argparse
import binascii
import hashlib
import json
import logging
import requests

import sys

from os import listdir
from os.path import isfile, join

from strongfellowbtc.logging import configure_logging
import strongfellowbtc.block_io as block_io
import strongfellowbtc.constants as constants

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def post_blocks_from_blocks_dir(args=None):
  s = requests.Session()
  configure_logging()
  if args is None:
    args = sys.argv[1:]

  parser = argparse.ArgumentParser()
  parser.add_argument('--blocks-dir', required=False, default='/data/bitcoin/.bitcoin/blocks')
  parser.add_argument('--url', required=False, default='http://localhost:8080/internal/blocks')
  parser.add_argument('--network', required=False, default='main')

  args = parser.parse_args(args)

  url = args.url
  blocks_dir = args.blocks_dir

  cache = {}
  try:
    with open('cache.json') as input:
      cache = json.load(input)
  except IOError:
    cache = {}

  expected_magic = binascii.unhexlify(constants.NETWORKS[args.network].magic)
  while True:
    logging.info('begin iteration')
    for f in listdir(blocks_dir):
      if isfile(join(blocks_dir, f)) and f.startswith('blk') and f.endswith('.dat'):
        logging.info('begin processing %s', f)
        md = md5(join(blocks_dir, f))
        logging.info('considering %s', f)
        if f in cache and cache[f] == md:
          logging.info('skipping %s', f)
        else:
          logging.info('processing %s', f)
          with open(join(blocks_dir, f), 'rb') as input:
            for (magic, block, h) in block_io.generate_blocks(input):
              block_hash= binascii.hexlify(h[::-1])
              logging.info('posting %s', block_hash)
              if magic != expected_magic:
                raise Exception('unexpected magic: %s; expected %s' % (strongfellowbtc.hex.little_endian_hex(magic), strongfellowbtc.hex.little_endian_hex(expected_magic)))

              response = s.post(url, data=block,
                                headers={ 'Content-Type': 'strongfellow/block'})
              status = response.status_code
              print(status)
              logging.info('finished posting %s', block_hash)
          cache[f] = md
          with open('cache.json', 'w') as output:
            json.dump(cache, output)
        logging.info('finished processing %s', f)

    logging.info('finished iteration')

if __name__ == '__main__':
  post_blocks_from_blocks_dir()
