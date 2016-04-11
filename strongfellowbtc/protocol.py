
from __future__ import absolute_import

from collections import namedtuple
from datetime import datetime
from hashlib import sha256
from io import BytesIO
import logging

from strongfellowbtc.hex import big_endian_hex, little_endian_hex

def ds256(bs):
    '''doubld sha; see https://en.bitcoin.it/wiki/Protocol_documentation#Hashes'''
    return sha256(sha256(bs).digest()).digest()

def merkle_root(block):
    hashes = [tx.metadata.hash.decode('hex')[::-1] for tx in block.transactions]
    n = len(hashes)
    while n != 1:
        for i in xrange(n / 2):
            a = hashes[2 * i]
            b = hashes[2 * i + 1]
            hashes[i] = ds256(a + b)
        if n % 2 > 0:
            hashes[n / 2] = ds256(hashes[n - 1] + hashes[n - 1])
            n = n / 2 + 1
        else: 
            n = n / 2
    return big_endian_hex(hashes[0])
                      

Metadata = namedtuple('Metadata', ['hash', 'size'])
Header = namedtuple('Header', [ 'version', 'previous_block', 'merkle_root', 'timestamp', 'bits', 'nonce'])

TxIn = namedtuple('TxIn', ['hash', 'index', 'script', 'sequence'])
TxOut = namedtuple('Txout', ['value', 'script'])

Tx = namedtuple('Tx', ['version', 'inputs', 'outputs', 'lock_time', 'metadata'])
Block = namedtuple('Block', [ 'header', 'transactions', 'metadata'])

Message = namedtuple('Message', ['magic', 'command', 'payload'])

class BlockParser:

    def read(self, n):
        result = self._bs.read(n)
        self._n += n
        self._txn += n
        self._sha256.update(result)
        return result

    def parse_script(self):
        n = self.parse_varint()
        return little_endian_hex(self.read(n))

    def parse_varint(self):
        b = self.parse_uint(1)
        if b < 0xfd:
            return b
        elif b == 0xfd:
            return self.parse_uint(2)
        elif b == 0xfe:
            return self.parse_uint(4)
        else:
            return self.parse_uint(8)

    def parse_uint(self, num_bytes):
        v = self.read(num_bytes)
        return sum((ord(x) << (8 * i)) for i, x in enumerate(v))

    def parse_uint32(self):
        return self.parse_uint(4)
    def parse_uint64(self):
        return self.parse_uint(8)

    def parse_hash(self):
        return big_endian_hex(self.read(32))

    def parse_header(self):
        version = self.parse_uint32()
        previous_block = self.parse_hash()
        merkle_root = self.parse_hash()
        timestamp = datetime.fromtimestamp(self.parse_uint32()).strftime('%Y-%m-%d %H:%M:%S%z')
        bits = self.parse_uint32()
        nonce = self.parse_uint32()
        return Header(version=version,
                      previous_block=previous_block,
                      merkle_root=merkle_root,
                      timestamp=timestamp,
                      bits=bits,
                      nonce=nonce)

    def parse_txin(self):
        hash = self.parse_hash()
        index = self.parse_uint32()
        script = self.parse_script()
        sequence = self.parse_uint32()
        return TxIn(hash=hash, index=index, script=script, sequence=sequence)

    def parse_txout(self):
        value = self.parse_uint64()
        script = self.parse_script()
        return TxOut(value=value, script=script)
        
    def parse_tx(self):
        self._sha256 = sha256()
        self._txn = 0
        version = self.parse_uint32()

        inputs = []
        for i in xrange(self.parse_varint()):
            inputs.append(self.parse_txin())

        outputs = []
        for i in xrange(self.parse_varint()):
            outputs.append(self.parse_txout())

        lock_time = self.parse_uint32()

        metadata = Metadata(size=self._txn, hash=big_endian_hex(sha256(self._sha256.digest()).digest()))
        return Tx(version=version,
                  inputs=inputs,
                  outputs=outputs,
                  lock_time=lock_time,
                  metadata=metadata)

    def parse_block(self, bs):
        self._bs = bs
        self._n = 0
        self._sha256 = sha256()
        self._txn = 0
        header = self.parse_header()
        block_hash = big_endian_hex(sha256(self._sha256.digest()).digest())

        ntx = self.parse_varint()
        transactions = []
        for x in range(ntx):
            transactions.append(self.parse_tx())
        return Block(header=header,
                     transactions=transactions,
                     metadata=Metadata(size=self._n, hash=block_hash))
