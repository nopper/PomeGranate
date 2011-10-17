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

from tempfile import NamedTemporaryFile

def create_file(directory, reducer, prefix="map", cont=0, delete=False):
    """
    Utility function to create a unique named temporary file
    @param directory the output directory in which the file will be created
    @param reducer the reducer number
    @param cont an integer representing the continuation id
    @param delete if you wish to delete the file after .close()
    @return a file object
    """
    fname = "{:s}-r{:06d}-p{:06d}-".format(prefix, reducer, cont)
    return NamedTemporaryFile(prefix=fname, dir=directory, delete=delete)

