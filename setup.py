#! /usr/bin/env python

from distutils.core import setup
from distutils.extension import Extension

setup(name           = 'simhash_db',
    version          = '0.1.0',
    description      = 'Near-Duplicate Detection with Simhash in Databases',
    url              = 'http://github.com/seomoz/simhash-db',
    author           = 'Dan Lecocq',
    author_email     = 'dan@seomoz.org',
    packages         = ['simhash_db'],
    package_dir      = {'simhash_db': 'simhash_db'},
    dependencies     = [],
    classifiers      = [
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP'
    ],
)
