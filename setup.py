
from setuptools import setup

config = {
    'name': 'strongfellowbtc',
    'description': 'Strongfellow BTC',
    'author': 'Strongfellow',
    'author_email': 'strongfellow.bitcoin@gmail.com',
    'version': '0.1',
    'install_requires': ['nose'],
    'packages': ['strongfellowbtc'],
    'entry_points': {
        'console_scripts': [
            'test-get-records=strongfellowbtc.kinesis:test_get_records',
            'create-kinesis-stream=strongfellowbtc.kinesis:create_stream',
            'put-blocks=strongfellowbtc.block_putter:main',
            'stash-incoming-blocks=strongfellowbtc.block_putter:stash_incoming_blocks',
            'stream-incoming-transactions=strongfellowbtc.kinesis:stream_incoming_transactions',
            'post-incoming-transactions=strongfellowbtc.txpost:post_incoming_transactions',
            'post-blocks-from-blocks-dir=strongfellowbtc.block_poster:post_blocks_from_blocks_dir'
        ]
    }
}

setup(**config)
