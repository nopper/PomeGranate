"""
This module holds the definition for the reducer class which is in charge of
executing the reduce function both on the results of the mappers and on the
results of the reducer. Therefore it should be able to re-reduce output files
"""

import os.path
from utils import Logger

class Reducer(Logger):
    def __init__(self, conf, name="Reducer"):
        """
        @param conf a dictionary corresponding to the parsed json conf file
        @param name name to assign to the logger
        """
        super(Reducer, self).__init__(name)

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

    def execute(self, result):
        """
        This method must be overriden. The input parameter result is a 2 tuple
        in the form (ridx, inp-list):

            1. ridx is an integer indicating which is the reducer we are
               impersonating
            2. inp-list is a list of unique file ids which are inputs of the
               reduce function.

        The method must return a tuple in the form:
            ((totsize, time_taken), [(fid, fsize) ] + inp-list)

        Where the first element:
            (totsize, time_taken) is a performance measure used to derive
            statistics about the computation. Namely:
                - totsize is the cumulative size in bytes of the file produced
                - time_taken is the time expressed in seconds in order to
                  compute the assignment

        The second element is a list formed by inserting in position 0 to the
        input list a tuple (fid, fsize) which elements are indicating:
            1. fid the unique id of the output file just produced by the
               reducer
            2. fsize the file size of the output file expressed in bytes

        @param result is a 2-tuple in the form (ridx:int, [fid:int, ...])
        @return a tuple
        """
        raise Exception("Not implemented")
