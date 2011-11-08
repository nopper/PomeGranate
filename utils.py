import os
import imp
import sys
import socket

import logging
import tempfile
import logging.config

from mpi4py import MPI

logging.config.fileConfig('logconfig.ini')

class RandomNameIntegerSequence(tempfile._RandomNameSequence):
    characters = ("123456789")

tempfile._name_sequence = RandomNameIntegerSequence()

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


def create_file(directory, reducer, prefix="output", delete=False):
    """
    Utility function to create a unique named temporary file
    @param directory the output directory in which the file will be created
    @param reducer the reducer number
    @param delete if you wish to delete the file after .close()
    @return a file object
    """
    # TODO: dropa la parte randomica e piazza un randomismo su interi. Sarebbe
    # meglio un range continuativo
    fname = "{:s}-r{:06d}-p".format(prefix, reducer)
    return tempfile.NamedTemporaryFile(prefix=fname, bufsize=1048576, dir=directory, delete=delete)

def get_id(fname):
    return int(os.path.basename(fname).split('-', 3)[2][1:])

def get_file_name(path, reduce_idx, fid):
    return os.path.join(path, "output-r{:06d}-p{:d}".format(reduce_idx, fid))

def count_machines(fname):
    """
    Read the number of MPI slots that we can possibly use
    @param fname the machine file file name
    @return an integer indicating the number of available MPI slots
    """
    count = 0

    for line in open(fname).readlines():
        line = line.strip()

        if line[0] == '#':
            continue

        try:
            # Extract the number from a string like
            # host.domain:2
            count += int(line.rsplit(':', 1)[1])
        except Exception, exc:
            count += 1

        return count
