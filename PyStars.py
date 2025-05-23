#!/usr/bin/python3
"""STARS Server in Python.

Based on Perl STARS server from Takashi Kosuge; KEK Tsukuba
stars.kek.jp

- 2020.06.19 Original release. 1.04
- 2023.01.30 Fixed reconnectable problem, T.Kosuge. 1.05
- 2023.02.13 Fix to avoid command permission problem, J. Szczesny. 1.06
- 2024.07.28 Fixed server crash if no STARS client try to connect, J. Szczesny. 1.07
- 2025.05.23 Bugfix for disconnect in Multiprocess version, J. Szczesny. 1.08
"""

__author__ = 'Jan Szczesny'
__version__ = '1.08'
__date__ = '2025.05.23'
__license__ = 'MIT'

import os
import sys
import time
import multiprocessing as mp
import configparser
from argparse import ArgumentParser
import starsfile
import starskernel
import starskernelmp

DEFAULT_PORT = 6057
LIBDIR = 'takaserv-lib'
KEYDIR = None
CONFIGFILE = 'PyStars.cfg'

def readconfigfile(cfile):
    cfg = configparser.ConfigParser(allow_no_value=True)
    cfg.read(cfile)
    starsport = cfg.getint("param", "starsport")
    starslib = cfg["param"]["starslib"]
    starskey = cfg["param"]["starskey"]
    starsmulti = cfg.getboolean("param", "starsmulti")
    return [starsmulti, starsport, starslib, starskey]

def readparameter():
    _parser = ArgumentParser(description='STARS Server Version: {}'.format(__version__))
    _parser.add_argument('-port', dest='p', type=int, help='Portnumber of the server.', default=DEFAULT_PORT)
    _parser.add_argument('-lib', dest='l', help='Directory with server .cfg files and .key files.', default=LIBDIR)
    _parser.add_argument('-key', dest='k', help='Directory with server .key files.'\
                        'If empty lib directory will be used.', default=KEYDIR)
    _parser.add_argument('-multi', dest='m', help='Switch to multiprocessing mode.'\
                        'If this switch will be configured, the multiprocessing version of STARS server will be used.',
                         action='store_true')
    args = _parser.parse_args()
    return [args.m, args.p, args.l, args.k]

def chooseversion(param):
    if param[0]:
        mp.set_start_method('spawn')
        print('Starting multiprocessing server...')
        return starskernelmp.Starsserver(port=param[1], lib=param[2], key=param[3])
    else:
        print('Starting single thread server...')
        return starskernel.Starsserver(port=param[1], lib=param[2], key=param[3])

if __name__ == "__main__":
    print('\nSTARS Server Version: {}'.format(__version__))
    if not sys.version_info[:2] >= (3, 6):
        print('Python 3.6 or higher is required!')
        print(f'Detected version: {sys.version_info[:2]}')
        input('Press enter to exit...')
        sys.exit(0)
    cfgPath = starsfile.getfilepath(os.path.dirname(os.path.realpath(__file__)), CONFIGFILE)
    if(cfgPath.is_file()):
        stars = chooseversion(readconfigfile(cfgPath))
    else:
        stars = chooseversion(readparameter())

    if stars.startup():
        print('Server started. Time:', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        stars.runserver()
    else:
        print('Initialization failed! Server will not starting!')
        input('Press enter to exit...')
        sys.exit(1)
