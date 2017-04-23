#!/usr/bin/env python

from distutils.core import setup
import os.path
import re
import sys

import targzstream

MODULE = targzstream

NAME = MODULE.__name__
VERSION =  MODULE.__version__
DOCSTRING = MODULE.__doc__
DESCRIPTION = MODULE.__descr__

AUTHOR, EMAIL = re.match(r'(.*) [(<](.*)[>)]', MODULE.__author__).groups()

URL = 'https://github.com/n2vram/' + NAME


def fix_readme(infile, outfile):
    swap = {
        'VERSION': VERSION,
        'DOC': DOCSTRING,
        'NAME': NAME
    }
    contents = open(infile, 'r').read()
    for key, value in swap.items():
        contents = contents.replace('__%s__' % key, value)
    open(outfile, 'w').write(contents)
    return contents


def do_setup(readme):
    setup(
        name=NAME,
        version=VERSION,
        description=DESCRIPTION,
        long_description=readme,
        license='MIT',
        author=AUTHOR,
        author_email=EMAIL,
        url=URL,
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
            'Topic :: Software Development :: Libraries :: Python Modules',
            'Topic :: System :: Archiving :: Packaging',
            'Topic :: Utilities',
        ],
        keywords=['tarfile', 'gzip', 'streaming'],
        py_modules=[NAME, 'tests'],
        download_url=(URL + '/archive/' + VERSION),
        platforms=['any'],
    )



if os.path.isfile('README.in'):
    readme = fix_readme(infile='README.in', outfile='README.rst')
else:
    readme = open('README.rst').read()

do_setup(readme)
