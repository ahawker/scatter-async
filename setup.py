"""
    setup.py
    ~~~~~~~~
"""

import os
import re

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup


PACKAGE = 'scatter_async'
VERSION_REGEX = re.compile(r"^__version__ = ['\"]([^'\"]*)['\"]", re.MULTILINE)
VERSION_PATH = os.path.join(os.path.dirname(__file__), PACKAGE, '__version__.py')


def get_package_version(path, silent=False):
    """
    Get the version of a python package as defined by the `__version__`
    attribute within the given python module.

    :param path: Path to module which contains version.
    :param silent: (Optional) Flag to indicate if we should raise an exception on error.
    """
    try:
        with open(path, 'rt') as f:
            version = VERSION_REGEX.search(f.read())
            if not version:
                raise RuntimeError('Unable to find __version__ in {0}'.format(path))
            return version.group(1)
    except (IOError, RuntimeError):
        if not silent:
            raise


setup(
    name=PACKAGE,
    version=get_package_version(VERSION_PATH),
    description='Asynchronous components for Scatter.',
    long_description=open('README.md').read(),
    author='Andrew Hawker',
    author_email='andrew.r.hawker@gmail.com',
    url='https://github.com/scatter/scatter',
    #license=open('LICENSE.md').read(),
    license='',
    package_dir={'scatter_async': 'scatter_async'},
    packages=['scatter_async'],
    test_suite='tests',
    classifiers=(
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
    ),
    entry_points={
        'scatter_async': [
            'ext = scatter.ext.async'
        ],
    },
)
