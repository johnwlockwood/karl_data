import os
import sys

VERSION = '0.0.4'

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()


def read(fname):
    """
    Open and read a filename in this directory.
    :param fname: `str` file name in this directory

    Returns contents of file fname
    """
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


requires = ['itertools-recipes']

try:
    license_info = open('LICENSE').read()
except:
    license_info = 'APACHE 2.0'

setup(
    name="karld",
    version="0.0.4",
    author="John W Lockwood IV",
    author_email="john@tackletronics.com",
    description=("Doing some data things "
                 "in a memory efficient manner"),
    license=license_info,
    keywords="example documentation tutorial",
    url="https://github.com/johnwlockwood/karl_data",
    packages=['karld', 'tests'],
    package_dir={'karld': 'karld'},
    install_requires=requires,
    long_description=read('README.md'),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
    ],
)
