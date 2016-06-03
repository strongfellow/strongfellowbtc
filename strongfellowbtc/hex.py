
from __future__ import absolute_import

from binascii import hexlify as hexlify
from binascii import unhexlify as unhexlify

def switch_endian(hex_string):
    return hexlify(unhexlify(hex_string)[::-1])

def big_endian_hex(binary):
    return hexlify(binary[::-1])

def little_endian_hex(binary):
    return hexlify(binary)
