import imp
import sys
import socket

import logging
import logging.config

logging.config.fileConfig('logconfig.ini')

from mpi4py import MPI

class Logger(object):
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        self.extra = {
            'clientip': socket.gethostbyname(socket.gethostname()),
            'rank': MPI.COMM_WORLD.Get_rank(),
        }

    def info(self, str):
        self.logger.info(str, extra=self.extra)

    def debug(self, str):
        self.logger.debug(str, extra=self.extra)

    def warning(self, str):
        self.logger.warning(str, extra=self.extra)

    def error(self, str):
        self.logger.error(str, extra=self.extra)

def load_module(mod):
    """
    Load a module regardless of the hierarchal specification.
    It can load modules specified as 'mod1.mod2.module'
    """
    path = sys.path

    for mname in mod.split('.'):
        mfile, mpath, mdesc = imp.find_module(mname, path)
        module = imp.load_module(mname, mfile, mpath, mdesc)
        path = getattr(module, '__path__', '')

    return module

