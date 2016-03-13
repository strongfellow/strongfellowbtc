
def write_block(blk, stream):
    network = blk.network
    block = blk.block
    stream.write(network)
    bs = "".join( chr( (len(block) >> (i * 8)) & 0xff ) for i in range(4) )
    length = bs
    stream.write(length)
    stream.write(block)
