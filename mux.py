"""
This module holds the definition of the Multiplexer for a set of MPI
communicators.
"""

import sys
import time

from mpi4py import MPI
from threading import Lock

class Muxer(object):
    def __init__(self, nproc, args, interval=0.1):
        self.index = 0
        self.channels = []
        self.interval = interval
        self.arguments = args

        self.lock = Lock()
        self.spawn_more(nproc)

    def spawn_more(self, nproc):
        new_channels = []
        for i in range(nproc):
            comm = MPI.COMM_SELF.Spawn(sys.executable,
                                       args=self.arguments,
                                       maxprocs=1)
            new_channels.append(comm)

        with self.lock:
            self.channels.extend(new_channels)

            print self.channels

    def remove(self, comm):
        with self.lock:
            self.channels.remove(comm)

    def get_total(self):
        with self.lock:
            return len(self.channels)

    def send_all(self, msg):
        with self.lock:
            for comm in self.channels:
                comm.send(msg, dest=0)

    def receive(self):
        """
        Implement a non-deterministic receive. If you want you can override
        this method providing other policies.

        This function will sleep if the cycle has not returned a communicator
        to read from.
        """
        temp = self.index

        while True:
            self.lock.acquire()
            self.index = (self.index + 1) % (len(self.channels))
            comm = self.channels[self.index]

            if comm.Iprobe():
                self.lock.release()
                return (self.index, comm)

            if self.index == temp:
                self.lock.release()
                time.sleep(self.interval)
            else:
                self.lock.release()
