"""
The module contains utilities functions that can be used in various parts of
the project.
"""

import os
import imp
import sys
import socket

import logging
import tempfile
import logging.config

from mpi4py import MPI

try:
    logging.config.fileConfig('logconfig.ini')
except:
    print("Cannot find logconfig.ini in %s" % os.getcwd())

class RandomNameIntegerSequence(tempfile._RandomNameSequence):
    "The class is used as base for generating unique file IDs"
    characters = ("123456789")

tempfile._name_sequence = RandomNameIntegerSequence()

class Logger(object):
    "A simple Logger object for logging events"
    def __init__(self, name):
        """
        Initialize the logger
        @param param name the name of the logger object
        """

        self.logger = logging.getLogger(name)

        self.extra = {
            'clientip': socket.gethostbyname(socket.gethostname()),
            'rank': MPI.COMM_WORLD.Get_rank(),
        }

    def info(self, str):
        self.logger.info(str, extra=self.extra)

    def dbg(self, str):
        self.logger.debug(str, extra=self.extra)

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
    @param mod a str representing the module (e.g. 'foo.spam')
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
    fname = "{:s}-r{:06d}-p".format(prefix, reducer)
    return tempfile.NamedTemporaryFile(prefix=fname, bufsize=1048576, dir=directory, delete=delete)

def get_id(fname):
    """
    @param fname the path to the file
    @return an integer representing the unique file id
    """
    return int(os.path.basename(fname).split('-', 3)[2][1:])

def get_file_name(path, reduce_idx, fid):
    """
    Construct a filename starting from various components
    @param path the path containing the file
    @param reduce_idx the id of the reducer
    @param fid the unique file ID
    @return a str representing the full path to the file
    """
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
