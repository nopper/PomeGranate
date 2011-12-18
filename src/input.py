"""
This module holds the definition for the input class generating the input
stream that should feed the map-reduce computation
"""

import json
from utils import Logger

class Input(Logger):
    """
    This class provides to the framework a way to express on which files the
    MapReduce computation should be applied.
    """
    def __init__(self, fconf, name="Input"):
        """
        @param fconf path to the configuration file
        @param name name to assign to the logger
        """
        super(Input, self).__init__(name)
        self.conf = json.load(open(fconf))

    def input(self):
        """
        This method should be implemented as a generator and must return a list
        of tuples in the form of: (absolute-path:str, docid:int)
        @return yields a tuple (fname, docid)
        """
        raise Exception("Not implemented")
