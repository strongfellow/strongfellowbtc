
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
            'put_blocks=strongfellowbtc.block_putter:main'
        ]
    }
}

setup(**config)
