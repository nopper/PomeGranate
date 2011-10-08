"""
Module containing definition of messages exchanged during the MapReduce
computation
"""

MSG_AVAILABLE      = 0x1
MSG_FINISHED       = 0x2
MSG_COMPUTE_MAP    = 0x3
MSG_COMPUTE_REDUCE = 0x4
MSG_SLEEP          = 0x5
MSG_QUIT           = 0x6

MSG_TO_STR = {
    MSG_AVAILABLE:      "AVAILABLE",
    MSG_FINISHED:       "FINISHED",
    MSG_SLEEP:          "SLEEP",
    MSG_COMPUTE_MAP:    "COMPUTE-MAP",
    MSG_COMPUTE_REDUCE: "COMPUTE-REDUCE",
    MSG_QUIT:           "QUIT"
}

class Message(object):
    """
    This class should be like a datatype in MPI
    """
    def __init__(self, command, result):
        self.command = command
        self.result = result

    def __str__(self):
        return "%s %s" % (MSG_TO_STR[self.command], self.result)

    def __repr__(self):
        return str(self)
