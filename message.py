"""
Module containing definition of messages exchanged during the MapReduce
computation
"""

MSG_AVAILABLE       = 0x1
MSG_FINISHED_MAP    = 0x2
MSG_FINISHED_REDUCE = 0x3
MSG_COMPUTE_MAP     = 0x4
MSG_COMPUTE_REDUCE  = 0x5
MSG_SLEEP           = 0x6
MSG_QUIT            = 0x7

MSG_TO_STR = {
    MSG_AVAILABLE:       "AVAILABLE",
    MSG_FINISHED_MAP:    "FINISHED-MAP",
    MSG_FINISHED_REDUCE: "FINISHED-REDUCE",
    MSG_SLEEP:           "SLEEP",
    MSG_COMPUTE_MAP:     "COMPUTE-MAP",
    MSG_COMPUTE_REDUCE:  "COMPUTE-REDUCE",
    MSG_QUIT:            "QUIT"
}

class Message(object):
    """
    This class should be like a datatype in MPI
    """
    def __init__(self, command, tag, result):
        self.tag = tag
        self.command = command
        self.result = result

    def __str__(self):
        return "%s %s" % (MSG_TO_STR[self.command], self.result)

    def __repr__(self):
        return str(self)

TYPE_MAP    = 0
TYPE_REDUCE = 1
TYPE_DUMMY  = 2

class WorkerStatus(object):
    """
    Simple placeholder class to have a more readable code.
    """
    def __init__(self, type, tag, state):
        self.type = type
        self.tag = tag
        self.state = state

    def __str__(self):
        if self.type == TYPE_MAP:
            return "<Map [%s] (%s)>" % (str(self.tag), str(self.state))
        if self.type == TYPE_REDUCE:
            return "<Reduce [%s] (%s)>" % (str(self.tag), str(self.state))
        if self.type == TYPE_DUMMY:
            return "<Dummy sleeping %.2f secs>" % (self.state)

    def __repr__(self):
        return str(self)
