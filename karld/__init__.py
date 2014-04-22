import sys
from . import _meta



__version__ = _meta.version
__version_info__ = _meta.version_info


def is_py3():
    return sys.version > '3'

from . import io
from . import conversion_operators
from . import merger
from . import path
