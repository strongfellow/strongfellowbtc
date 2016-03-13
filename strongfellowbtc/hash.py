
import hashlib
import binascii

_hex = binascii.hexlify

def double_sha256(bs):
    return hashlib.sha256(hashlib.sha256(bs).digest()).digest()

def switch_endian(hex_string):
    return hex_string.decode('hex')[::-1].encode('hex')

def big_endian_hex(bs):
    return _hex(bs[::-1])

def little_endian_hex(bs):
    return _hex(bs)


