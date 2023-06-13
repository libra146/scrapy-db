#!/usr/bin/env python
# -*- coding: utf-8 -*-
import configparser
import io

from setuptools import setup, find_packages

from scrapy_db import __version__, __email__, __author__


def read_file(filename):
    with io.open(filename) as fp:
        return fp.read().strip()


def read_md(filename):
    # Ignore unsupported directives by pypi.
    return read_file(filename)


def read_requirements(filename):
    config = configparser.ConfigParser()
    config.read(filename)
    return config.options('tool.poetry.dependencies')


setup(
    name='scrapy-db',
    version=__version__,
    description="DB-based components for Scrapy.",
    long_description=read_md('README.md'),
    author=__author__,
    author_email=__email__,
    url='https://github.com/libra146/scrapy-db',
    packages=find_packages(exclude=("tests", "tests.*")),
    install_requires=read_requirements('pyproject.toml'),
    include_package_data=True,
    license="GNU General Public License v3.0",
    keywords='scrapy-db',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    python_requires=">=3.7",
)

if __name__ == '__main__':
    # read_requirements('pyproject.toml')
    list(find_packages('src'))
