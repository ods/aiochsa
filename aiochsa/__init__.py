from pkg_resources import get_distribution, DistributionNotFound

from .client import Client
from .exc import DBException, ProtocolError
from .pool import connect, create_pool, Pool
from .sql import select

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    pass
