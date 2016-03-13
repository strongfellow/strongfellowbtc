
import nose.tools
import os.path
import strongfellowbtc.block_reader as block_reader
import strongfellowbtc.hex as hex

def test_read_blocks():
    n = 0
    with open('tests/data/blk00000.dat', 'rb') as stream:
        for (network, block, hash) in block_reader.generate_blocks(stream):
#            print hex.big_endian_hex(hash)
            assert hex.big_endian_hex(hash).startswith('00000000')
            n += 1

    nose.tools.eq_(n, 119977
)
