
import nose.tools
import os.path
import strongfellowbtc.block_io as io
import strongfellowbtc.hex as hex

# def test_read_blocks():
#     n = 0
#     with open('tests/data/blk00000.dat', 'rb') as stream:
#         for (network, block, hash) in io.generate_blocks(stream):
#             assert hex.big_endian_hex(hash).startswith('00000000')
#             n += 1
#     nose.tools.eq_(n, 119977)

# def test_write_blocks():
#     with open('tests/data/blk00000.dat', 'rb') as stream:
#         with open('tests/data/out.dat', 'wb') as out:
#             for b in io.generate_blocks(stream):
#                 io.write_block(b, out)
