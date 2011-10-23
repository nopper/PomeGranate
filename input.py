"""
This module holds the definition for the input class generating the input
stream that should feed the map-reduce computation
"""

import json
from utils import Logger

class Input(Logger):
    def __init__(self, fconf, name="Input"):
        """
        @param fconf path to the configuration file
        @param name name to assign to the logger
        """
        super(Input, self).__init__(name)
        self.conf = json.load(open(fconf))

    def input(self):
        raise Exception("Not implemented")
