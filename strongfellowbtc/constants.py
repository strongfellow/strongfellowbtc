

from collections import namedtuple

Network = namedtuple('Network', ['name', 'description', 'magic'])

HASH_TX_PORT    = 28332
HASH_BLOCK_PORT = 28333
RAW_TX_PORT     = 28334
RAW_BLOCK_PORT  = 28335

NETWORK_MAIN = 'main',
NETWORK_TESTNET3 = 'testnet3'
NETWORK_TESTNET = 'testnet'
NETWORK_NAMECOIN = 'namecoin'

NETWORKS = dict((network.name, network) for network in [
    Network(name='main', description='main bitcoin network', magic='f9beb4d9'),
    Network(name='testnet', description='first gen bitcoin test network', magic='fabfb5da'),
    Network(name='testnet3', description='third gen bitcoin test network', magic='0b110907'),
    Network(name='namecoin', description='namecoin network', magic='f9beb4fe'),
])
