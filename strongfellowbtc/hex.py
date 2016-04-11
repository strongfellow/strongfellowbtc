
from __future__ import absolute_import

from binascii import hexlify as hexlify

def switch_endian(hex_string):
    return hex_string.decode('hex')[::-1].encode('hex')

def big_endian_hex(binary):
    return hexlify(binary[::-1])

def little_endian_hex(binary):
    return hexlify(binary)
