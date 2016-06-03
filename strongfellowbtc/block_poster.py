
import argparse
import logging
import sys

from strongfellowbtc.logging import configure_logging

def post_blocks_from_blocks_dir(args=None):
  configure_logging()
  if args is None:
    args = sys.argv[1:]

  parser = argparse.ArgumentParser()
  parser.add_argument('--blocks-dir', required=False, default='/data/bitcoin/.bitcoin/blocks')
  parser.add_argument('--url', required=False, default='http://localhost:8080/internal/blocks')

  args = parser.parse_args(args)

  url = args.url
  blocks_dir =args.url

  while True:
    logging.info('begin iteration')

    logging.info('finished iteration')

if __name__ == '__main__':
  post_blocks_from_blocks_dir()
