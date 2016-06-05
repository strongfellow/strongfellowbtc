
import argparse
import binascii
import hashlib
import json
import logging
import requests
import requests.exceptions
import sys
import time

from os import listdir
from os.path import isfile, join

from strongfellowbtc.logging import configure_logging
import strongfellowbtc.block_io as block_io
import strongfellowbtc.constants as constants
import strongfellowbtc.hex


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
    total_success_count = 0
    logging.info('begin iteration')
    for f in sorted(listdir(blocks_dir)):
      if isfile(join(blocks_dir, f)) and f.startswith('blk') and f.endswith('.dat'):
        logging.info('begin processing %s', f)
        md = md5(join(blocks_dir, f))
        logging.info('considering %s', f)
        if f in cache and cache[f] == md:
          logging.info('skipping %s', f)
        else:
          logging.info('processing %s', f)
          success_count = 0
          with open(join(blocks_dir, f), 'rb') as input:
            for (magic, block, h) in block_io.generate_blocks(input):
              block_hash = strongfellowbtc.hex.big_endian_hex(h)
              logging.debug('posting %s', block_hash)
              if magic != expected_magic:
                raise Exception('unexpected magic: %s; expected %s' % (strongfellowbtc.hex.little_endian_hex(magic), strongfellowbtc.hex.little_endian_hex(expected_magic)))
              if success_count % 1000 == 0:
                  logging.info('so far in file %s with %d successes', f, success_count)

              backoff = 1
              while True:
                retry = True
                try:
                  response = s.post(url, data=block,
                                    headers={ 'Content-Type': 'strongfellow/block'})
                  status = response.status_code
                  if status == 200:
                    success_count += 1
                    retry = False
                  else:
                    logging.error('non-200 response for block %s -- HTTP %d', block_hash, status)
                except requests.exceptions.RequestException as e:
                  logging.exception('problem posting %s', block_hash)
                if not retry:
                  break
                elif backoff < 60:
                  logging.info('backing off for %d seconds', backoff)
                  time.sleep(backoff)
                  backoff *= 2
                else:
                  logging.info('failed to post %s', block_hash)
                  logging.info('we backed off too many times; failing')
                  raise
          logging.info('finished file %s with %d successes', f, success_count)
          total_success_count += success_count
          logging.info('total success count: %d', total_success_count)
          cache[f] = md
          with open('cache.json', 'w') as output:
            json.dump(cache, output)
        logging.info('finished processing %s', f)
    delay_seconds = 600 if total_success_count == 0 else 10
    logging.info('and now, we sleep for %d seconds', delay_seconds)
    time.sleep(delay_seconds)
    logging.info('finished sleeping')
    logging.info('finished iteration')

if __name__ == '__main__':
  post_blocks_from_blocks_dir()
