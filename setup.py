from setuptools import setup, find_packages

setup(
    name='ccbacktest',
    packages=find_packages(), install_requires=['pandas', 'ccxt', 'numpy']
)