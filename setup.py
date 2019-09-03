from setuptools import setup
import sys


needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)

setup(
    setup_requires = ['pytest-runner'] if needs_pytest else [],
)
