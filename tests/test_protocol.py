

from nose.tools import eq_

import strongfellowbtc.protocol as protocol

def test_block_parser():
    with open('tests/data/00000000000000000075d42472dce83bac73268f4cf6643d9e20a534c32e7bbe.bin') as bs:
        parser = protocol.BlockParser()
        block = parser.parse_block(bs)
        print protocol.merkle_root(block)

        eq_(4, block.header.version)
        eq_('000000000000000003cdf33bdba4bd6fae78662e248e4aad52696bba289c41d4', block.header.previous_block)
        eq_('c27ed31a1fab49896fc876efacc226d4bd00d82cdb6d023bbdf8eb80b8958ddf', block.header.merkle_root)
        eq_('2016-03-29 16:22:37', block.header.timestamp)
        eq_(403088579, block.header.bits)
        eq_(3052494343, block.header.nonce)

        eq_(2953, len(block.transactions))
        t = block.transactions[0]
        eq_(154, t.metadata.size)
        eq_('d4201e45c3a85ad04fcd25b194fd60c2c7131061d5f26c5129d043bbfbf98e96', t.metadata.hash)

        eq_(998225, block.metadata.size)
        eq_('c27ed31a1fab49896fc876efacc226d4bd00d82cdb6d023bbdf8eb80b8958ddf', protocol.merkle_root(block))
        eq_('00000000000000000075d42472dce83bac73268f4cf6643d9e20a534c32e7bbe', block.metadata.hash)

