"""
This module holds the definition for the mapper class computing the some
function exploiting some degree of data locality
"""

import os.path
from utils import Logger

class Mapper(Logger):
    def __init__(self, conf, name="Mapper"):
        """
        @param conf a dictionary corresponding to the parsed json conf file
        @param name name to assign to the logger
        """
        super(Mapper, self).__init__(name)

        self.conf = conf

        self.datadir = self.conf['datadir']
        self.input_prefix = self.conf['input-prefix']
        self.output_prefix = self.conf['output-prefix']

        self.output_path = os.path.join(
            self.datadir,
            self.output_prefix
        )

    def setup(self, vfs):
        self.vfs = vfs

    def execute(self, inp):
        """
        This method must be overriden. The method must return a tuple in the
        form of:
            ((totsize, time_taken), [(ridx:int, fid:int, fsize:int), ..])

        Where the first element:
            (totsize, time_taken) is a performance measure used to derive
            statistics about the computation. Namely:
                - totsize is the cumulative size in bytes of the file produced
                - time_taken is the time expressed in seconds in order to
                  compute the assignment

        The second element is a list of 3-tuples. Each 3-tuple contains:
            1. ridx telling to which reducer the file produced is referred to
            2. fid integer describing the unique identifier of the file
            3. fsize the file size in bytes

        @param input is a list containing (fname, docid) retrieved from the
               Input module
        @return a tuple
        """
        raise Exception("Not implemented")
