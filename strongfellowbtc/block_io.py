
import strongfellowbtc.hash
from collections import namedtuple

BlockRead = namedtuple('BlockRead', ['network', 'block', 'hash'])

def _read_exactly(input, n):
    result = input.read(n)
    if len(result) != n:
        raise EOFError
    return result

def _little_endian(input, n):
    bs = _read_exactly(input, n)
    return sum(ord(b) << (i * 8) for i, b in enumerate(bs)) 

def _block_hash(bs):
    return strongfellowbtc.hash.double_sha256(bs[:80])

def generate_blocks(stream):
    while True:
        network = stream.read(4)
        if len(network) == 0:
            break
        elif len(network) != 4:
            raise EOFError
        else:
            length = _little_endian(stream, 4)
            block = _read_exactly(stream, length)
            hash = _block_hash(block)
            yield BlockRead(network=network, block=block, hash=hash)

def write_block(blk, stream):
    network = blk.network
    block = blk.block
    stream.write(network)
    bs = "".join( chr( (len(block) >> (i * 8)) & 0xff ) for i in range(4) )
    length = bs
    stream.write(length)
    stream.write(block)
