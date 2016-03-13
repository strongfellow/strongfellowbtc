
from setuptools import setup

config = {
    'name': 'strongfellowbtc',
    'description': 'Strongfellow BTC',
    'author': 'Strongfellow',
    'author_email': 'strongfellow.bitcoin@gmail.com',
    'version': '0.1',
    'install_requires': ['nose'],
    'packages': ['strongfellowbtc'],
    'scripts': []
}

setup(**config)
