"""
This module holds the definition of the Multiplexer for a set of MPI
communicators.
"""

import time

class Muxer(object):
    def __init__(self, interval=0.1):
        self.index = 0
        self.channels = []
        self.interval = interval

    def add_channel(self, comm):
        self.channels.append(comm)

    def receive(self):
        """
        Implement a non-deterministic receive. If you want you can override
        this method providing other policies.

        This function will sleep if the cycle has not returned a communicator
        to read from.
        """
        temp = self.index

        while True:
            self.index = (self.index + 1) % (len(self.channels))
            comm = self.channels[self.index]

            # TODO: Error handler
            if comm.Iprobe():
                return (self.index, comm)

            if self.index == temp:
                time.sleep(self.interval)
