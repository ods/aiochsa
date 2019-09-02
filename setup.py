import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)

setup(
    setup_requires = ['pytest-runner'] if needs_pytest else [],
)
