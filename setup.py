
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
            'put_blocks=strongfellowbtc.block_putter:main',
            'tweet_blocks=strongfellowbtc.block_tweeter:main',
            'stash-incoming-blocks=strongfellowbtc.scripts:stash_incoming_blocks',
            'stash-incoming-transactions=strongfellowbtc.scripts:stash_incoming_transactions',
            'create-tx-table=strongfellowbtc.scripts:create_transactions_table',
        ]
    }
}

setup(**config)
