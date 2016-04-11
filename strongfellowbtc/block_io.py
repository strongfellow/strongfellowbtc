
from __future__ import absolute_import

from strongfellowbtc.protocol import ds256
from collections import namedtuple

BlockRead = namedtuple('BlockRead', ['magic', 'block', 'hash'])

def _read_exactly(input, n):
    result = input.read(n)
    if len(result) != n:
        raise EOFError
    return result

def _little_endian(input, n):
    bs = _read_exactly(input, n)
    return sum(ord(b) << (i * 8) for i, b in enumerate(bs)) 

def _block_hash(bs):
    return ds256(bs[:80])

def generate_blocks(stream):
    while True:
        magic = stream.read(4)
        if len(magic) == 0:
            break
        elif len(magic) != 4:
            raise EOFError
        else:
            length = _little_endian(stream, 4)
            block = _read_exactly(stream, length)
            hash = _block_hash(block)
            yield BlockRead(magic=magic, block=block, hash=hash)

def write_block(blk, stream):
    magic = blk.magic
    block = blk.block
    stream.write(magic)
    bs = "".join( chr( (len(block) >> (i * 8)) & 0xff ) for i in range(4) )
    length = bs
    stream.write(length)
    stream.write(block)
